package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge"
	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/adapter/opcua"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/spooler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_BridgeOPCUA_EndToEnd(t *testing.T) {
	// 1. Setup Mock OPC-UA Server
	srv := opcua.StartMockServer(t)
	defer srv.Close()

	var mu sync.Mutex
	readingCount := 0

	srv.OnPublishRequest = func(w io.Writer, seq, reqID uint32, subAckCount int32) {
		mu.Lock()
		readingCount++
		currentVal := float64(readingCount * 10)
		mu.Unlock()

		var body bytes.Buffer

		respNode := opcua.NodeID{Type: opcua.NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: opcua.TypeIDPublishResponse}
		_ = respNode.Encode(&body)

		resHdr := opcua.ResponseHeader{Timestamp: time.Now().UnixNano(), ServiceResult: opcua.StatusGood}
		_ = binary.Write(&body, binary.LittleEndian, resHdr.Timestamp)
		_ = binary.Write(&body, binary.LittleEndian, reqID)
		_ = binary.Write(&body, binary.LittleEndian, uint32(opcua.StatusGood))
		_ = binary.Write(&body, binary.LittleEndian, byte(0))
		_ = binary.Write(&body, binary.LittleEndian, int32(0))
		nullNode := opcua.NodeID{Type: opcua.NodeIDTypeTwoByte, NumericID: 0}
		_ = nullNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0))

		_ = binary.Write(&body, binary.LittleEndian, uint32(101))
		_ = binary.Write(&body, binary.LittleEndian, int32(0))
		_ = binary.Write(&body, binary.LittleEndian, byte(0))

		_ = binary.Write(&body, binary.LittleEndian, uint32(readingCount))
		_ = binary.Write(&body, binary.LittleEndian, time.Now().UnixNano())
		_ = binary.Write(&body, binary.LittleEndian, int32(1))

		extNode := opcua.NodeID{Type: opcua.NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: opcua.TypeIDDataChangeNotification}
		_ = extNode.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(0x01))

		var notifBody bytes.Buffer
		_ = binary.Write(&notifBody, binary.LittleEndian, int32(1))
		_ = binary.Write(&notifBody, binary.LittleEndian, uint32(1))

		_ = binary.Write(&notifBody, binary.LittleEndian, byte(7))
		_ = binary.Write(&notifBody, binary.LittleEndian, byte(opcua.TypeDouble))
		_ = binary.Write(&notifBody, binary.LittleEndian, currentVal)
		_ = binary.Write(&notifBody, binary.LittleEndian, uint32(opcua.StatusGood))
		_ = binary.Write(&notifBody, binary.LittleEndian, time.Now().UnixNano())
		_ = binary.Write(&notifBody, binary.LittleEndian, int32(0))

		_ = binary.Write(&body, binary.LittleEndian, int32(notifBody.Len()))
		_, _ = body.Write(notifBody.Bytes())
		_ = binary.Write(&body, binary.LittleEndian, int32(0))

		srv.SendMSG(w, seq, reqID, body.Bytes())
	}

	// 2. Setup Spooler
	spoolDir, err := os.MkdirTemp("", "pulse-opcua-integration-spool-*")
	require.NoError(t, err)
	defer os.RemoveAll(spoolDir)

	sp, err := spooler.NewDiskSpooler(spoolDir, 20*1024*1024)
	require.NoError(t, err)
	defer sp.Close()

	// 3. Setup Mock Gateway Forwarder
	fwd := &mockGateway{available: true}

	// 4. Configure OPC-UA Adapter
	opcCfg := config.OPCUAAdapterConfig{
		Name:            "opcua-turbine-e2e",
		Endpoint:        srv.Endpoint(),
		PublishInterval: 20 * time.Millisecond,
		Nodes: []config.MonitoredItemConfig{
			{
				NodeID:     "ns=2;s=Turbine.RPM",
				DeviceID:   "turbine-01",
				MetricName: "rotor_speed",
			},
		},
	}

	ad, err := opcua.NewAdapter(opcCfg, nil)
	require.NoError(t, err)

	bridgeCfg := &config.EdgeBridgeConfig{
		BridgeID:        "bridge-e2e",
		BatchLinger:     10 * time.Millisecond,
		BatchMaxRecords: 5,
	}

	coord := bridge.NewCoordinator(bridgeCfg, []adapter.ProtocolAdapter{ad}, sp, fwd, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = coord.Start(ctx)
	require.NoError(t, err)

	// Verify live forwarding
	require.Eventually(t, func() bool {
		fwd.mu.Lock()
		defer fwd.mu.Unlock()
		return len(fwd.received) >= 3
	}, 3*time.Second, 50*time.Millisecond)

	fwd.mu.Lock()
	firstReading := fwd.received[0]
	assert.Equal(t, "turbine-01", firstReading.DeviceId)
	assert.Equal(t, "rotor_speed", firstReading.MetricName)
	assert.Equal(t, "opcua", firstReading.Metadata["protocol"])
	fwd.mu.Unlock()

	// 5. Simulate Gateway Network Partition
	fwd.mu.Lock()
	fwd.available = false
	prevCount := len(fwd.received)
	fwd.mu.Unlock()

	// Wait for telemetry to spool to disk
	require.Eventually(t, func() bool {
		return sp.BufferedCount() >= 2
	}, 3*time.Second, 50*time.Millisecond)

	assert.GreaterOrEqual(t, sp.BufferedCount(), int64(2))

	// 6. Restore Gateway Network Connection
	fwd.mu.Lock()
	fwd.available = true
	fwd.mu.Unlock()

	// Wait for spooled records to flush completely
	require.Eventually(t, func() bool {
		return sp.BufferedCount() == 0
	}, 5*time.Second, 50*time.Millisecond)

	fwd.mu.Lock()
	totalReceived := len(fwd.received)
	fwd.mu.Unlock()

	assert.Greater(t, totalReceived, prevCount)

	_ = coord.Stop()
}
