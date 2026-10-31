package bridge

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Coordinator orchestrates protocol adapters, local offline spooling, and upstream gRPC forwarding.
type Coordinator struct {
	cfg       *config.EdgeBridgeConfig
	adapters  []adapter.ProtocolAdapter
	spooler   adapter.OfflineSpooler
	forwarder adapter.GatewayForwarder
	metrics   *metrics.BridgeMetrics

	inChan chan *telemetryv1.TelemetryReading
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

// NewCoordinator constructs a bridge coordinator.
func NewCoordinator(
	cfg *config.EdgeBridgeConfig,
	adapters []adapter.ProtocolAdapter,
	spooler adapter.OfflineSpooler,
	forwarder adapter.GatewayForwarder,
	m *metrics.BridgeMetrics,
) *Coordinator {
	return &Coordinator{
		cfg:       cfg,
		adapters:  adapters,
		spooler:   spooler,
		forwarder: forwarder,
		metrics:   m,
		inChan:    make(chan *telemetryv1.TelemetryReading, 10000),
	}
}

// IngestChannel returns the incoming reading channel for mock or external injection.
func (c *Coordinator) IngestChannel() chan<- *telemetryv1.TelemetryReading {
	return c.inChan
}

// Start launches all protocol adapters and the forwarding/spooling background loop.
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Start all configured protocol adapters
	for _, ad := range c.adapters {
		if err := ad.Start(ctx, c.inChan); err != nil {
			return fmt.Errorf("failed to start adapter %s: %w", ad.Name(), err)
		}
	}

	c.wg.Add(1)
	go c.forwardLoop(ctx)

	return nil
}

// Stop gracefully shuts down adapters, flushes remaining records, and closes connections.
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()

	// Stop all adapters first to stop incoming telemetry flow
	for _, ad := range c.adapters {
		_ = ad.Stop()
	}

	c.wg.Wait()

	if c.spooler != nil {
		_ = c.spooler.Close()
	}
	if c.forwarder != nil {
		_ = c.forwarder.Close()
	}
	return nil
}

func (c *Coordinator) forwardLoop(ctx context.Context) {
	defer c.wg.Done()

	linger := c.cfg.BatchLinger
	if linger <= 0 {
		linger = 10 * time.Millisecond
	}
	maxRecords := c.cfg.BatchMaxRecords
	if maxRecords <= 0 {
		maxRecords = 1000
	}

	ticker := time.NewTicker(linger)
	defer ticker.Stop()

	var batch []*telemetryv1.TelemetryReading

	for {
		select {
		case <-ctx.Done():
			// Flush residual items in inChan before terminating
			for len(c.inChan) > 0 {
				r := <-c.inChan
				batch = append(batch, r)
			}
			if len(batch) > 0 {
				c.dispatchBatch(context.Background(), batch)
			}
			return

		case r := <-c.inChan:
			batch = append(batch, r)
			if len(batch) >= maxRecords {
				c.dispatchBatch(ctx, batch)
				batch = make([]*telemetryv1.TelemetryReading, 0, maxRecords)
			}

		case <-ticker.C:
			// First, attempt to drain historical records from offline spool if connected
			c.drainSpool(ctx, maxRecords)

			if len(batch) > 0 {
				c.dispatchBatch(ctx, batch)
				batch = make([]*telemetryv1.TelemetryReading, 0, maxRecords)
			}
		}
	}
}

func (c *Coordinator) dispatchBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) {
	if len(batch) == 0 {
		return
	}

	// If spool has unread records, new incoming records must be spooled to preserve strict event FIFO
	if c.spooler != nil && c.spooler.BufferedCount() > 0 {
		c.spoolBatch(batch)
		c.drainSpool(ctx, c.cfg.BatchMaxRecords)
		return
	}

	// Try direct forward
	start := time.Now()
	_, err := c.forwarder.ForwardBatch(ctx, batch)
	duration := time.Since(start).Seconds()

	if err != nil {
		// Forward failed: spill to disk spool
		c.spoolBatch(batch)
		if c.metrics != nil {
			c.metrics.ForwardedTotal.WithLabelValues("failed").Add(float64(len(batch)))
			c.metrics.ConnectedStatus.WithLabelValues("gateway").Set(0)
		}
		return
	}

	if c.metrics != nil {
		c.metrics.ForwardedTotal.WithLabelValues("success").Add(float64(len(batch)))
		c.metrics.ForwardLatencySeconds.Observe(duration)
		c.metrics.ConnectedStatus.WithLabelValues("gateway").Set(1)
	}
}

func (c *Coordinator) spoolBatch(batch []*telemetryv1.TelemetryReading) {
	if c.spooler == nil || len(batch) == 0 {
		return
	}
	if err := c.spooler.Write(batch); err == nil {
		if c.metrics != nil {
			c.metrics.SpoolWritesTotal.Add(float64(len(batch)))
			c.metrics.SpoolBufferedCount.Set(float64(c.spooler.BufferedCount()))
		}
	}
}

func (c *Coordinator) drainSpool(ctx context.Context, maxRecords int) {
	if c.spooler == nil || c.spooler.BufferedCount() == 0 {
		return
	}

	for c.spooler.BufferedCount() > 0 {
		select {
		case <-ctx.Done():
			return
		default:
		}

		spooled, err := c.spooler.ReadNext(maxRecords)
		if err != nil || len(spooled) == 0 {
			break
		}

		_, fErr := c.forwarder.ForwardBatch(ctx, spooled)
		if fErr != nil {
			// Gateway still unreachable, reset unacknowledged batch and stop draining
			_ = c.spooler.Ack(0)
			if c.metrics != nil {
				c.metrics.ConnectedStatus.WithLabelValues("gateway").Set(0)
			}
			break
		}

		// Acknowledge read records
		_ = c.spooler.Ack(len(spooled))

		if c.metrics != nil {
			c.metrics.SpoolReadsTotal.Add(float64(len(spooled)))
			c.metrics.SpoolBufferedCount.Set(float64(c.spooler.BufferedCount()))
			c.metrics.ForwardedTotal.WithLabelValues("backfilled").Add(float64(len(spooled)))
			c.metrics.ConnectedStatus.WithLabelValues("gateway").Set(1)
		}
	}
}
