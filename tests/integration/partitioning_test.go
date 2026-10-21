package integration_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPartitioning_ConcurrentDeviceConsistency(t *testing.T) {
	partitioner := kgo.StickyKeyPartitioner(nil)
	topicPartitioner := partitioner.ForTopic("telemetry.raw")
	numPartitions := 12

	var wg sync.WaitGroup
	numGoroutines := 10
	iterationsPerGoroutine := 500

	devices := []string{"sensor-alpha", "sensor-beta", "sensor-gamma", "sensor-delta"}
	expectedPartitions := make(map[string]int)

	for _, dev := range devices {
		rec := &kgo.Record{Topic: "telemetry.raw", Key: []byte(dev)}
		expectedPartitions[dev] = topicPartitioner.Partition(rec, numPartitions)
	}

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterationsPerGoroutine; i++ {
				dev := devices[i%len(devices)]
				rec := &kgo.Record{
					Topic: "telemetry.raw",
					Key:   []byte(dev),
					Value: []byte(fmt.Sprintf("reading-%d-%d", gid, i)),
				}
				p := topicPartitioner.Partition(rec, numPartitions)
				assert.Equal(t, expectedPartitions[dev], p, "Device partition assignment must never drift")
			}
		}(g)
	}

	wg.Wait()
}
