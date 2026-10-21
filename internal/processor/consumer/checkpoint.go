package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CheckpointManager coordinates at-least-once offset commits to Redpanda,
// ensuring offsets are committed only after window aggregates are emitted.
// Enforces Constitution Principle III.
type CheckpointManager struct {
	group          *PartitionConsumerGroup
	client         *kgo.Client
	topic          string
	commitInterval time.Duration
}

// NewCheckpointManager creates a new offset CheckpointManager.
func NewCheckpointManager(group *PartitionConsumerGroup, client *kgo.Client, topic string, interval time.Duration) *CheckpointManager {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	return &CheckpointManager{
		group:          group,
		client:         client,
		topic:          topic,
		commitInterval: interval,
	}
}

// Run executes periodic offset commits until context cancellation.
func (cm *CheckpointManager) Run(ctx context.Context) {
	ticker := time.NewTicker(cm.commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = cm.CommitSync(context.Background())
			return
		case <-ticker.C:
			_ = cm.CommitSync(ctx)
		}
	}
}

// CommitSync gathers the highest processed offset across all active partition workers
// and synchronously commits them to the Kafka broker.
func (cm *CheckpointManager) CommitSync(ctx context.Context) error {
	if cm.client == nil || cm.group == nil {
		return nil
	}

	offsets := cm.group.ActiveOffsets()
	if len(offsets) == 0 {
		return nil
	}

	toCommit := make(map[string]map[int32]kgo.EpochOffset)
	partitionOffsets := make(map[int32]kgo.EpochOffset, len(offsets))

	for partition, highOffset := range offsets {
		if highOffset >= 0 {
			// Kafka commit offset is the NEXT offset to consume (highOffset + 1)
			partitionOffsets[partition] = kgo.EpochOffset{
				Offset: highOffset + 1,
				Epoch:  -1,
			}
		}
	}

	if len(partitionOffsets) == 0 {
		return nil
	}

	toCommit[cm.topic] = partitionOffsets

	var commitErr error
	cm.client.CommitOffsetsSync(ctx, toCommit, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
	})

	if commitErr != nil {
		return fmt.Errorf("failed to commit consumer offsets: %w", commitErr)
	}

	return nil
}
