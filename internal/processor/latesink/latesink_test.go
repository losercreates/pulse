package latesink

import (
	"context"
	"sync"
	"testing"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type mockProducer struct {
	mu      sync.Mutex
	records []*kgo.Record
}

func (m *mockProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, r)
	if promise != nil {
		promise(r, nil)
	}
}

func (m *mockProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, rs...)
	return nil
}

func TestLateDataSink_PublishLate(t *testing.T) {
	prod := &mockProducer{}
	sink := NewLateDataSink(prod, "telemetry.late")

	late := &telemetryv1.LateTelemetryRecord{
		Reading: &telemetryv1.TelemetryReading{
			DeviceId:    "sensor-42",
			MetricName:  "pressure",
			Value:       101.3,
			TimestampNs: 1000,
		},
		PartitionId: 2,
		WatermarkNs: 6000,
		LatenessNs:  5000,
	}

	err := sink.PublishLate(context.Background(), late)
	require.NoError(t, err)

	require.Len(t, prod.records, 1)
	record := prod.records[0]

	assert.Equal(t, "telemetry.late", record.Topic)
	assert.Equal(t, "sensor-42", string(record.Key))

	var deserialized telemetryv1.LateTelemetryRecord
	err = proto.Unmarshal(record.Value, &deserialized)
	require.NoError(t, err)
	assert.Equal(t, "sensor-42", deserialized.Reading.DeviceId)
	assert.Equal(t, int32(2), deserialized.PartitionId)
	assert.Equal(t, int64(6000), deserialized.WatermarkNs)
	assert.Equal(t, int64(5000), deserialized.LatenessNs)

	// Verify headers
	headerMap := make(map[string]string)
	for _, h := range record.Headers {
		headerMap[h.Key] = string(h.Value)
	}
	assert.Equal(t, "2", headerMap["x-pulse-partition"])
	assert.Equal(t, "6000", headerMap["x-pulse-watermark-ns"])
	assert.Equal(t, "5000", headerMap["x-pulse-lateness-ns"])
}
