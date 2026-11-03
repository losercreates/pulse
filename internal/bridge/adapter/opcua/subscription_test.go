package opcua

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionAndPublishing(t *testing.T) {
	srv := startMockOPCUAServer(t)
	defer srv.Close()

	// Configure mock server to emit a DataChangeNotification on PublishRequest
	srv.OnPublishRequest = func(w io.Writer, seq, reqID uint32, subAckCount int32) {
		var body bytes.Buffer

		// TypeID: PublishResponse (829)
		respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDPublishResponse}
		_ = respNode.Encode(&body)

		resHdr := ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: StatusGood}
		_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
		_ = binary.Write(&body, binary.LittleEndian, reqID)
		_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
		_ = binary.Write(&body, binary.LittleEndian, byte(0))
		_ = binary.Write(&body, binary.LittleEndian, int32(0))
		nullNode := NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}
		_ = nullNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0))

		// SubscriptionID = 101
		_ = binary.Write(&body, binary.LittleEndian, uint32(101))

		// AvailableSequenceNumbers
		_ = binary.Write(&body, binary.LittleEndian, int32(0))

		// MoreNotifications = false
		_ = binary.Write(&body, binary.LittleEndian, byte(0))

		// NotificationMessage: SequenceNumber = 1, PublishTime, NotificationData (1 DataChangeNotification)
		_ = binary.Write(&body, binary.LittleEndian, uint32(1))
		_ = encodeDateTime(&body, time.Now().UTC())
		_ = binary.Write(&body, binary.LittleEndian, int32(1)) // 1 notification data

		// ExtensionObject: TypeID DataChangeNotification (811)
		extNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDDataChangeNotification}
		_ = extNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01)) // byte encoding

		var notifBody bytes.Buffer
		// MonitoredItems array count: 1
		_ = binary.Write(&notifBody, binary.LittleEndian, int32(1))

		// ClientHandle = 1
		_ = binary.Write(&notifBody, binary.LittleEndian, uint32(1))

		// DataValue: Value (Float 42.5), Status (Good), SourceTimestamp (Now)
		// Mask: HasValue (1) | HasStatus (2) | HasSourceTimestamp (4) = 7
		_ = binary.Write(&notifBody, binary.LittleEndian, byte(7))
		// Variant: Float (10)
		_ = binary.Write(&notifBody, binary.LittleEndian, byte(TypeFloat))
		_ = binary.Write(&notifBody, binary.LittleEndian, float32(42.5))
		// Status: Good
		_ = binary.Write(&notifBody, binary.LittleEndian, uint32(StatusGood))
		// SourceTimestamp
		_ = encodeDateTime(&notifBody, time.Now().UTC())

		// DiagnosticInfos array: 0
		_ = binary.Write(&notifBody, binary.LittleEndian, int32(0))

		// Write ExtensionObject body length + bytes
		_ = binary.Write(&body, binary.LittleEndian, int32(notifBody.Len()))
		_, _ = body.Write(notifBody.Bytes())

		// DiagnosticInfos array: 0
		_ = binary.Write(&body, binary.LittleEndian, int32(0))

		srv.sendMSG(w, seq, reqID, body.Bytes())
	}

	cfg := config.OPCUAAdapterConfig{
		Name:            "opcua-test-unit",
		Endpoint:        srv.Endpoint(),
		PublishInterval: 50 * time.Millisecond,
		Nodes: []config.MonitoredItemConfig{
			{
				NodeID:     "ns=2;s=Turbine.Speed",
				DeviceID:   "turbine-01",
				MetricName: "rotor_speed",
			},
		},
	}

	ad, err := NewAdapter(cfg, nil)
	require.NoError(t, err)

	out := make(chan *telemetryv1.TelemetryReading, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = ad.Start(ctx, out)
	require.NoError(t, err)

	select {
	case reading := <-out:
		assert.Equal(t, "turbine-01", reading.DeviceId)
		assert.Equal(t, "rotor_speed", reading.MetricName)
		assert.InDelta(t, 42.5, reading.Value, 0.001)
		assert.Equal(t, "opcua", reading.Metadata["protocol"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for telemetry reading from OPC-UA adapter")
	}

	err = ad.Stop()
	require.NoError(t, err)
}

