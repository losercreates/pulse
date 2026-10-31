package adapter_test

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicMatches(t *testing.T) {
	assert.True(t, adapter.TopicMatches("factory/+/+/telemetry", "factory/area1/turbine-01/telemetry"))
	assert.False(t, adapter.TopicMatches("factory/+/+/telemetry", "factory/area1/turbine-01/status"))
	assert.True(t, adapter.TopicMatches("factory/#", "factory/area1/turbine-01/temp"))
	assert.True(t, adapter.TopicMatches("factory/#", "factory/anything"))
	assert.True(t, adapter.TopicMatches("#", "any/arbitrary/nested/topic"))
	assert.False(t, adapter.TopicMatches("plant/+", "plant/area1/device1"))
}

func TestMQTTAdapter_JSONDecoding(t *testing.T) {
	cfg := config.MQTTAdapterConfig{
		Name:         "test-mqtt",
		Topics:       []string{"factory/+/+/telemetry"},
		Format:       "json",
		TopicPattern: "factory/{area}/{device_id}/telemetry",
	}

	ad, err := adapter.NewMQTTAdapter(cfg)
	require.NoError(t, err)

	nowNs := time.Now().UnixNano()
	jsonPayload := []byte(`{"vibration": 3.42, "temperature": 75.8}`)

	readings, err := ad.DecodeMessage("factory/line1/machine-42/telemetry", jsonPayload, nowNs)
	require.NoError(t, err)
	require.Len(t, readings, 2)

	// Verify device_id was extracted from topic
	assert.Equal(t, "machine-42", readings[0].DeviceId)
	assert.Equal(t, "machine-42", readings[1].DeviceId)

	metricNames := []string{readings[0].MetricName, readings[1].MetricName}
	assert.Contains(t, metricNames, "vibration")
	assert.Contains(t, metricNames, "temperature")
}

func TestMQTTAdapter_JSONPayloadWithEmbeddedDeviceID(t *testing.T) {
	cfg := config.MQTTAdapterConfig{
		Name:          "test-mqtt-embedded",
		Topics:        []string{"telemetry/stream"},
		Format:        "json",
		DeviceIDField: "device_id",
	}

	ad, err := adapter.NewMQTTAdapter(cfg)
	require.NoError(t, err)

	jsonPayload := []byte(`{
		"device_id": "pump-99",
		"metric": "flow_rate",
		"value": 150.5,
		"unit": "gpm",
		"timestamp_ns": 1730000000000000000
	}`)

	readings, err := ad.DecodeMessage("telemetry/stream", jsonPayload, 0)
	require.NoError(t, err)
	require.Len(t, readings, 1)

	assert.Equal(t, "pump-99", readings[0].DeviceId)
	assert.Equal(t, "flow_rate", readings[0].MetricName)
	assert.Equal(t, 150.5, readings[0].Value)
	assert.Equal(t, "gpm", readings[0].Unit)
	assert.Equal(t, int64(1730000000000000000), readings[0].TimestampNs)
}

func TestSparkplugB_PayloadDecoding(t *testing.T) {
	// spBv1.0/<group_id>/<message_type>/<edge_node_id>/[<device_id>]
	topic := "spBv1.0/PlantGroup1/DDATA/EdgeNodeA/Motor01"

	// Mock Sparkplug B metrics with multiple aliases and values
	nowMs := uint64(time.Now().UnixMilli())
	payloadBytes := adapter.CreateMockSparkplugPayload(nowMs, []adapter.SparkplugMetric{
		{Name: "Torque", DataType: adapter.SparkplugDataTypeDouble, DoubleValue: 245.8},
		{Name: "RPM", DataType: adapter.SparkplugDataTypeInt32, Int32Value: 1750},
	})

	readings, err := adapter.DecodeSparkplugMessage(topic, payloadBytes)
	require.NoError(t, err)
	require.Len(t, readings, 2)

	assert.Equal(t, "Motor01", readings[0].DeviceId)
	assert.Equal(t, "Torque", readings[0].MetricName)
	assert.Equal(t, 245.8, readings[0].Value)

	assert.Equal(t, "Motor01", readings[1].DeviceId)
	assert.Equal(t, "RPM", readings[1].MetricName)
	assert.Equal(t, float64(1750), readings[1].Value)
}

