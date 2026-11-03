package opcua

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Adapter implements adapter.ProtocolAdapter for native OPC-UA binary subscriptions.
type Adapter struct {
	cfg      config.OPCUAAdapterConfig
	metrics  *metrics.BridgeMetrics
	channel  *Channel
	sm       *SubscriptionManager
	statusMu sync.RWMutex
	status   adapter.AdapterStatus

	cancel context.CancelFunc
	wg     sync.WaitGroup

	msgCount atomic.Uint64
	errCount atomic.Uint64
}

// NewAdapter constructs a native OPC-UA adapter instance.
func NewAdapter(cfg config.OPCUAAdapterConfig, m *metrics.BridgeMetrics) (*Adapter, error) {
	if cfg.Name == "" {
		cfg.Name = "opcua-" + cfg.Endpoint
	}
	if cfg.PublishInterval <= 0 {
		cfg.PublishInterval = 100 * time.Millisecond
	}
	if cfg.KeepAliveInterval <= 0 {
		cfg.KeepAliveInterval = 5 * time.Second
	}

	ch := NewChannel(cfg)
	sm := NewSubscriptionManager(ch)

	return &Adapter{
		cfg:     cfg,
		metrics: m,
		channel: ch,
		sm:      sm,
	}, nil
}

// Name returns the unique adapter identifier.
func (a *Adapter) Name() string {
	return a.cfg.Name
}

// Status returns runtime operational diagnostics.
func (a *Adapter) Status() adapter.AdapterStatus {
	a.statusMu.RLock()
	defer a.statusMu.RUnlock()
	return adapter.AdapterStatus{
		Connected:        a.channel.State() == StateSubscribed || a.channel.State() == StateConnected,
		MessagesReceived: a.msgCount.Load(),
		ErrorsTotal:      a.errCount.Load(),
		LastError:        a.status.LastError,
	}
}

func (a *Adapter) setError(err error) {
	a.errCount.Add(1)
	a.statusMu.Lock()
	a.status.LastError = err.Error()
	a.statusMu.Unlock()
	if a.metrics != nil {
		a.metrics.OPCUAErrorsTotal.WithLabelValues(a.cfg.Name, "runtime").Inc()
	}
}

// Start initiates the background connection, subscription, and publishing loop.
func (a *Adapter) Start(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	a.wg.Add(1)
	go a.runLifecycle(ctx, out)

	return nil
}

// Stop terminates active subscriptions and closes network sockets.
func (a *Adapter) Stop() error {
	if a.cancel != nil {
		a.cancel()
	}
	err := a.channel.Close()
	a.wg.Wait()
	if a.metrics != nil {
		a.metrics.OPCUAConnectionState.WithLabelValues(a.cfg.Name).Set(0)
		a.metrics.OPCUASubscriptions.WithLabelValues(a.cfg.Name).Set(0)
	}
	return err
}

func (a *Adapter) runLifecycle(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) {
	defer a.wg.Done()

	backoff := 500 * time.Millisecond
	maxBackoff := 10 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if a.metrics != nil {
			a.metrics.OPCUAConnectionState.WithLabelValues(a.cfg.Name).Set(1) // Connecting
		}

		// 1. Establish Channel & Session
		if err := a.channel.Connect(ctx); err != nil {
			a.setError(fmt.Errorf("connection failed: %w", err))
			if a.metrics != nil {
				a.metrics.OPCUAConnectionState.WithLabelValues(a.cfg.Name).Set(0)
			}
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			case <-ctx.Done():
				return
			}
		}

		if a.metrics != nil {
			a.metrics.OPCUAConnectionState.WithLabelValues(a.cfg.Name).Set(2) // Connected
		}

		// 2. Dynamic Discovery if configured
		itemsToMonitor := a.cfg.Nodes
		if a.cfg.Discovery.Enabled {
			discovered, err := a.runDiscovery(ctx)
			if err != nil {
				a.setError(fmt.Errorf("discovery warning: %w", err))
			} else if len(discovered) > 0 {
				itemsToMonitor = append(itemsToMonitor, discovered...)
			}
		}

		// 3. Create Subscription
		_, err := a.sm.CreateSubscription(ctx, a.cfg.PublishInterval)
		if err != nil {
			a.setError(fmt.Errorf("create subscription failed: %w", err))
			_ = a.channel.Close()
			continue
		}

		if a.metrics != nil {
			a.metrics.OPCUASubscriptions.WithLabelValues(a.cfg.Name).Set(1)
		}

		// 4. Register Monitored Items
		handles, err := a.sm.CreateMonitoredItems(ctx, itemsToMonitor)
		if err != nil {
			a.setError(fmt.Errorf("create monitored items failed: %w", err))
			_ = a.channel.Close()
			continue
		}

		if a.metrics != nil {
			a.metrics.OPCUAMonitoredItems.WithLabelValues(a.cfg.Name).Set(float64(len(handles)))
			a.metrics.OPCUAConnectionState.WithLabelValues(a.cfg.Name).Set(3) // Subscribed
		}

		a.channel.state.Store(int32(StateSubscribed))
		backoff = 500 * time.Millisecond // Reset backoff upon successful subscription

		// 5. Ingestion Publishing Loop
		trackedChan := make(chan *telemetryv1.TelemetryReading, 100)
		doneCh := make(chan error, 1)

		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			doneCh <- PublishLoop(ctx, a.channel, a.sm, trackedChan, a.metrics, a.cfg.Name)
		}()

		looping := true
		for looping {
			select {
			case r := <-trackedChan:
				a.msgCount.Add(1)
				select {
				case out <- r:
				case <-ctx.Done():
					looping = false
				}
			case err := <-doneCh:
				if err != nil && ctx.Err() == nil {
					a.setError(fmt.Errorf("publish loop terminated: %w", err))
				}
				looping = false
			case <-ctx.Done():
				looping = false
			}
		}

		// Cleanup before reconnecting or exiting
		_ = a.channel.Close()
		subWg.Wait()
		if a.metrics != nil {
			a.metrics.OPCUAConnectionState.WithLabelValues(a.cfg.Name).Set(0)
			a.metrics.OPCUASubscriptions.WithLabelValues(a.cfg.Name).Set(0)
		}
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
