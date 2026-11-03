package opcua

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge"
	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/spooler"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockOfflineForwarder simulates an unreachable upstream gateway.
type mockOfflineForwarder struct{}

func (f *mockOfflineForwarder) ForwardBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error) {
	return nil, fmt.Errorf("network down: upstream gateway unreachable")
}

func (f *mockOfflineForwarder) IsConnected() bool {
	return false
}

func (f *mockOfflineForwarder) Close() error {
	return nil
}

func TestOPCUA_KeepAliveAndReconnection(t *testing.T) {
	srv := startMockOPCUAServer(t)

	cfg := config.OPCUAAdapterConfig{
		Name:              "opcua-resilience",
		Endpoint:          srv.Endpoint(),
		PublishInterval:   50 * time.Millisecond,
		KeepAliveInterval: 200 * time.Millisecond,
		ConnectTimeout:    1 * time.Second,
		RequestTimeout:    1 * time.Second,
		Nodes: []config.MonitoredItemConfig{
			{
				NodeID:     "ns=2;s=Motor.Vibration",
				DeviceID:   "motor-01",
				MetricName: "vibration_g",
			},
		},
	}

	ad, err := NewAdapter(cfg, nil)
	require.NoError(t, err)

	out := make(chan *telemetryv1.TelemetryReading, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = ad.Start(ctx, out)
	require.NoError(t, err)

	// Wait for connected status
	require.Eventually(t, func() bool {
		return ad.Status().Connected
	}, 2*time.Second, 50*time.Millisecond)

	// Sever connection by closing server
	srv.Close()

	// Wait for adapter to detect disconnect
	require.Eventually(t, func() bool {
		return !ad.Status().Connected || ad.Status().ErrorsTotal > 0
	}, 3*time.Second, 100*time.Millisecond)

	_ = ad.Stop()
}

func TestOPCUA_OfflineSpoolerDiversion(t *testing.T) {
	srv := startMockOPCUAServer(t)
	defer srv.Close()

	srv.OnPublishRequest = func(w io.Writer, seq, reqID uint32, subAckCount int32) {
		var body bytes.Buffer

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

		_ = binary.Write(&body, binary.LittleEndian, uint32(101))
		_ = binary.Write(&body, binary.LittleEndian, int32(0))
		_ = binary.Write(&body, binary.LittleEndian, byte(0))

		_ = binary.Write(&body, binary.LittleEndian, uint32(1))
		_ = encodeDateTime(&body, time.Now().UTC())
		_ = binary.Write(&body, binary.LittleEndian, int32(1))

		extNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDDataChangeNotification}
		_ = extNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01))

		var notifBody bytes.Buffer
		_ = binary.Write(&notifBody, binary.LittleEndian, int32(1))
		_ = binary.Write(&notifBody, binary.LittleEndian, uint32(1)) // ClientHandle 1

		_ = binary.Write(&notifBody, binary.LittleEndian, byte(7))
		_ = binary.Write(&notifBody, binary.LittleEndian, byte(TypeDouble))
		_ = binary.Write(&notifBody, binary.LittleEndian, float64(123.45))
		_ = binary.Write(&notifBody, binary.LittleEndian, uint32(StatusGood))
		_ = encodeDateTime(&notifBody, time.Now().UTC())
		_ = binary.Write(&notifBody, binary.LittleEndian, int32(0))

		_ = binary.Write(&body, binary.LittleEndian, int32(notifBody.Len()))
		_, _ = body.Write(notifBody.Bytes())
		_ = binary.Write(&body, binary.LittleEndian, int32(0))

		srv.sendMSG(w, seq, reqID, body.Bytes())
	}

	tmpDir, err := os.MkdirTemp("", "pulse-opcua-spool-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	sp, err := spooler.NewDiskSpooler(tmpDir, 10*1024*1024)
	require.NoError(t, err)
	defer sp.Close()

	opcCfg := config.OPCUAAdapterConfig{
		Name:            "opcua-spool-test",
		Endpoint:        srv.Endpoint(),
		PublishInterval: 25 * time.Millisecond,
		Nodes: []config.MonitoredItemConfig{
			{
				NodeID:     "ns=2;s=Turbine.RPM",
				DeviceID:   "turbine-01",
				MetricName: "rotor_speed",
			},
		},
	}

	ad, err := NewAdapter(opcCfg, nil)
	require.NoError(t, err)

	bridgeCfg := &config.EdgeBridgeConfig{
		BridgeID:        "bridge-test",
		BatchLinger:     10 * time.Millisecond,
		BatchMaxRecords: 10,
	}

	offlineFwd := &mockOfflineForwarder{}
	coord := bridge.NewCoordinator(bridgeCfg, []adapter.ProtocolAdapter{ad}, sp, offlineFwd, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = coord.Start(ctx)
	require.NoError(t, err)

	// Since forwarder is offline, coordinator should spool incoming OPC-UA readings to disk
	require.Eventually(t, func() bool {
		return sp.BufferedCount() > 0
	}, 3*time.Second, 50*time.Millisecond)

	assert.GreaterOrEqual(t, sp.BufferedCount(), int64(1))

	_ = coord.Stop()
}

