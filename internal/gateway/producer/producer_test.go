package producer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProducer_KeyPartitioningDeterminism(t *testing.T) {
	partitioner := kgo.StickyKeyPartitioner(nil)
	topicPartitioner := partitioner.ForTopic("telemetry.raw")
	numPartitions := 12

	testDevices := []string{
		"turbine-alpha-01",
		"turbine-alpha-02",
		"pump-station-east",
		"generator-main-04",
		"boiler-pressure-sensor-99",
	}

	partitionAssignments := make(map[string]int)

	for _, devID := range testDevices {
		record := &kgo.Record{
			Topic: "telemetry.raw",
			Key:   []byte(devID),
			Value: []byte("payload"),
		}

		firstPartition := topicPartitioner.Partition(record, numPartitions)
		partitionAssignments[devID] = firstPartition

		// Repeat 100 times to guarantee strict determinism (same device always maps to same partition)
		for i := 0; i < 100; i++ {
			p := topicPartitioner.Partition(record, numPartitions)
			assert.Equal(t, firstPartition, p, "Device %s must deterministically hash to partition %d", devID, firstPartition)
		}
	}

	// Verify that distinct devices are not all collapsing into a single partition
	uniquePartitions := make(map[int]struct{})
	for _, p := range partitionAssignments {
		uniquePartitions[p] = struct{}{}
	}
	assert.Greater(t, len(uniquePartitions), 1, "Keys must distribute across multiple partitions")
}
