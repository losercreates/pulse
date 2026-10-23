package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/storage/config"
	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// StorageConsumerSink consumes closed window aggregates from Redpanda topic telemetry.aggregates
// and flushes batches to ChronoDB storage, committing offsets post-fsync.
// Enforces Constitution Principles III & V.
type StorageConsumerSink struct {
	cfg     *config.Config
	client  *kgo.Client
	engine  engine.ChronoEngine
	metrics *metrics.Metrics

	inbound  chan *telemetryv1.StoredAggregate
	stopOnce sync.Once
	workerWg sync.WaitGroup
}

// NewStorageConsumerSink creates a new StorageConsumerSink.
func NewStorageConsumerSink(
	cfg *config.Config,
	client *kgo.Client,
	eng engine.ChronoEngine,
	m *metrics.Metrics,
) *StorageConsumerSink {
	bufferSize := cfg.BatchSize * 4
	if bufferSize <= 0 {
		bufferSize = 4096
	}

	return &StorageConsumerSink{
		cfg:     cfg,
		client:  client,
		engine:  eng,
		metrics: m,
		inbound: make(chan *telemetryv1.StoredAggregate, bufferSize),
	}
}

// Inbound returns the sink's ingestion channel (used for direct ingestion / testing).
func (s *StorageConsumerSink) Inbound() chan<- *telemetryv1.StoredAggregate {
	return s.inbound
}

// Start spawns the batch flusher loop and the Franz-go consumer loop (if client is non-nil).
func (s *StorageConsumerSink) Start(ctx context.Context) error {
	s.workerWg.Add(1)
	go s.flusherLoop(ctx)

	if s.client != nil {
		s.workerWg.Add(1)
		go s.pollConsumer(ctx)
	}

	<-ctx.Done()
	s.Stop()
	return nil
}

// Stop gracefully shuts down the consumer and drains pending batches.
func (s *StorageConsumerSink) Stop() {
	s.stopOnce.Do(func() {
		close(s.inbound)
		s.workerWg.Wait()
		if s.client != nil {
			s.client.Close()
		}
	})
}

func (s *StorageConsumerSink) pollConsumer(ctx context.Context) {
	defer s.workerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := s.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			var agg telemetryv1.WindowAggregate
			if err := proto.Unmarshal(rec.Value, &agg); err != nil {
				log.Printf("Failed to unmarshal WindowAggregate: %v", err)
				continue
			}

			stored := &telemetryv1.StoredAggregate{
				DeviceId:      agg.DeviceId,
				MetricName:    agg.MetricName,
				WindowStartNs: agg.WindowStartNs,
				WindowEndNs:   agg.WindowEndNs,
				Count:         agg.Count,
				Sum:           agg.Sum,
				Min:           agg.Min,
				Max:           agg.Max,
				Mean:          agg.Mean,
			}

			// Natural backpressure: blocks if inbound buffer is full
			select {
			case s.inbound <- stored:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *StorageConsumerSink) flusherLoop(ctx context.Context) {
	defer s.workerWg.Done()

	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	flushTicker := time.NewTicker(s.cfg.FlushInterval)
	defer flushTicker.Stop()

	batch := make([]*telemetryv1.StoredAggregate, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()
		inserted, updated, err := s.engine.PutAggregates(ctx, batch)
		duration := time.Since(start).Seconds()

		if err != nil {
			log.Printf("Error writing aggregate batch to storage: %v", err)
			if s.metrics != nil {
				s.metrics.StorageWritesTotal.WithLabelValues("failure").Add(float64(len(batch)))
			}
			return
		}

		if s.metrics != nil {
			s.metrics.StorageWritesTotal.WithLabelValues("inserted").Add(float64(inserted))
			s.metrics.StorageWritesTotal.WithLabelValues("updated").Add(float64(updated))
			s.metrics.BatchWriteLatencySeconds.WithLabelValues("put").Observe(duration)
			s.metrics.StoredRecordsGauge.Set(float64(s.engine.TotalRecords()))
		}

		// Commit consumer offsets to Redpanda strictly after storage acknowledgment
		if s.client != nil {
			if err := s.client.CommitUncommittedOffsets(ctx); err != nil {
				log.Printf("Failed to commit offsets after storage write: %v", err)
			}
		}

		// Reset batch
		batch = make([]*telemetryv1.StoredAggregate, 0, batchSize)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return

		case stored, ok := <-s.inbound:
			if !ok {
				flush()
				return
			}
			batch = append(batch, stored)
			if len(batch) >= batchSize {
				flush()
			}

		case <-flushTicker.C:
			flush()
		}
	}
}
