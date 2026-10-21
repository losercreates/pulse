package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/losercreates/pulse/internal/processor/config"
	"github.com/losercreates/pulse/internal/processor/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionConsumerGroup coordinates Franz-go partition consumer group assignments,
// spawning exactly one worker goroutine per assigned partition in accordance with
// Constitution Principle I.
type PartitionConsumerGroup struct {
	cfg          *config.Config
	client       *kgo.Client
	aggPublisher AggregatePublisher
	lateSink     LateSink
	metrics      *metrics.Metrics

	mu      sync.RWMutex
	workers map[int32]*activeWorker
}

type activeWorker struct {
	worker *PartitionWorker
	cancel context.CancelFunc
	done   chan struct{}
}

// NewPartitionConsumerGroup initializes the consumer group orchestrator.
func NewPartitionConsumerGroup(
	cfg *config.Config,
	aggPublisher AggregatePublisher,
	lateSink LateSink,
	m *metrics.Metrics,
) (*PartitionConsumerGroup, error) {
	pcg := &PartitionConsumerGroup{
		cfg:          cfg,
		aggPublisher: aggPublisher,
		lateSink:     lateSink,
		metrics:      m,
		workers:      make(map[int32]*activeWorker),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.TopicRaw),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(), // Offset commits coordinated by checkpoint manager
		kgo.OnPartitionsAssigned(pcg.onAssigned),
		kgo.OnPartitionsRevoked(pcg.onRevoked),
		kgo.OnPartitionsLost(pcg.onLost),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create franz-go client: %w", err)
	}

	pcg.client = client
	return pcg, nil
}

// Client returns the underlying Franz-go client.
func (g *PartitionConsumerGroup) Client() *kgo.Client {
	return g.client
}

// Start begins polling fetches and dispatching them to partition workers.
func (g *PartitionConsumerGroup) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			g.Stop()
			return nil
		default:
		}

		fetches := g.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// Transient poll error or context cancellation
			if ctx.Err() != nil {
				return nil
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			g.mu.RLock()
			aw, exists := g.workers[record.Partition]
			g.mu.RUnlock()

			if exists && aw != nil {
				select {
				case aw.worker.Inbound() <- record:
				case <-ctx.Done():
				}
			}
		})
	}
}

// Stop shuts down the consumer group, flushes all workers, and closes the client.
func (g *PartitionConsumerGroup) Stop() {
	g.mu.Lock()
	for p, aw := range g.workers {
		aw.cancel()
		<-aw.done
		delete(g.workers, p)
	}
	g.mu.Unlock()

	if g.client != nil {
		g.client.Close()
	}
}

// ActiveWorkers returns a snapshot of active partition IDs and their highest processed offsets.
func (g *PartitionConsumerGroup) ActiveOffsets() map[int32]int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	offsets := make(map[int32]int64, len(g.workers))
	for p, aw := range g.workers {
		offsets[p] = aw.worker.HighOffset()
	}
	return offsets
}

func (g *PartitionConsumerGroup) onAssigned(ctx context.Context, client *kgo.Client, assigned map[string][]int32) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for topic, partitions := range assigned {
		if topic != g.cfg.TopicRaw {
			continue
		}
		for _, partitionID := range partitions {
			if _, exists := g.workers[partitionID]; exists {
				continue
			}

			w := NewPartitionWorker(
				topic,
				partitionID,
				g.cfg.AllowedLateness,
				g.cfg.IdleTimeout,
				g.cfg.TumblingWindowSize,
				g.cfg.SlidingWindowSize,
				g.cfg.SlidingSlideSize,
				g.aggPublisher,
				g.lateSink,
				g.metrics,
				4096,
			)

			workerCtx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})

			aw := &activeWorker{
				worker: w,
				cancel: cancel,
				done:   done,
			}
			g.workers[partitionID] = aw

			go func() {
				defer close(done)
				w.Run(workerCtx)
			}()
		}
	}
}

func (g *PartitionConsumerGroup) onRevoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for topic, partitions := range revoked {
		if topic != g.cfg.TopicRaw {
			continue
		}
		for _, partitionID := range partitions {
			if aw, exists := g.workers[partitionID]; exists {
				aw.cancel()
				<-aw.done
				delete(g.workers, partitionID)
			}
		}
	}
}

func (g *PartitionConsumerGroup) onLost(ctx context.Context, client *kgo.Client, lost map[string][]int32) {
	// Partition ownership was lost (rebalance timeout); terminate immediately without committing
	g.onRevoked(ctx, client, lost)
}
