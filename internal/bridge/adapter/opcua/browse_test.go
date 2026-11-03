package opcua

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddressSpaceBrowsing_DiscoveryAndCycleDetection(t *testing.T) {
	srv := startMockOPCUAServer(t)
	defer srv.Close()

	// Mock Browse returns 2 variables and 1 object that points back to root (cycle)
	srv.OnBrowseRequest = func(w io.Writer, seq, reqID uint32, root NodeID) {
		var body bytes.Buffer

		respNode := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: TypeIDBrowseResponse}
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

		// Results: 1 result
		_ = binary.Write(&body, binary.LittleEndian, int32(1))
		_ = binary.Write(&body, binary.LittleEndian, uint32(StatusGood))
		_ = writeByteString(&body, nil) // no continuation point

		// References: 3 references (2 variables, 1 cyclic reference)
		_ = binary.Write(&body, binary.LittleEndian, int32(3))

		refType := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 35}
		baseVar := NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 63}

		// Ref 1: RotorSpeed (Variable)
		_ = refType.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(1)) // Forward
		target1 := NodeID{Type: NodeIDTypeString, NamespaceIndex: 2, StringID: "Turbine.RotorSpeed"}
		_ = target1.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, uint16(2))
		_ = writeOPCUAString(&body, "RotorSpeed")
		_ = binary.Write(&body, binary.LittleEndian, byte(0x02))
		_ = writeOPCUAString(&body, "RotorSpeed")
		_ = binary.Write(&body, binary.LittleEndian, uint32(2)) // NodeClass Variable
		_ = baseVar.Encode(&body)

		// Ref 2: BearingTemp (Variable)
		_ = refType.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(1))
		target2 := NodeID{Type: NodeIDTypeString, NamespaceIndex: 2, StringID: "Turbine.BearingTemp"}
		_ = target2.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, uint16(2))
		_ = writeOPCUAString(&body, "BearingTemp")
		_ = binary.Write(&body, binary.LittleEndian, byte(0x02))
		_ = writeOPCUAString(&body, "BearingTemp")
		_ = binary.Write(&body, binary.LittleEndian, uint32(2)) // NodeClass Variable
		_ = baseVar.Encode(&body)

		// Ref 3: Objects folder (Cyclic Object node pointing back to i=85)
		_ = refType.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, byte(1))
		cyclicTarget := NodeID{Type: NodeIDTypeTwoByte, NumericID: 85}
		_ = cyclicTarget.Encode(&body)
		_ = binary.Write(&body, binary.LittleEndian, uint16(0))
		_ = writeOPCUAString(&body, "Objects")
		_ = binary.Write(&body, binary.LittleEndian, byte(0x02))
		_ = writeOPCUAString(&body, "Objects")
		_ = binary.Write(&body, binary.LittleEndian, uint32(1)) // NodeClass Object
		_ = baseVar.Encode(&body)

		_ = binary.Write(&body, binary.LittleEndian, int32(0)) // DiagnosticInfos

		srv.sendMSG(w, seq, reqID, body.Bytes())
	}

	cfg := config.OPCUAAdapterConfig{
		Name:     "opcua-browse-test",
		Endpoint: srv.Endpoint(),
		Discovery: config.DiscoveryConfig{
			Enabled:       true,
			RootNodeID:    "i=85",
			PathFilter:    "Objects/*",
			MaxDepth:      3,
			AutoSubscribe: true,
		},
	}

	ad, err := NewAdapter(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = ad.channel.Connect(ctx)
	require.NoError(t, err)

	discovered, err := ad.runDiscovery(ctx)
	require.NoError(t, err)

	// Should discover RotorSpeed and BearingTemp; cycle should be suppressed
	assert.Len(t, discovered, 2)
	assert.Equal(t, "rotorspeed", discovered[0].MetricName)
	assert.Equal(t, "bearingtemp", discovered[1].MetricName)
	assert.Equal(t, "ns=2;s=Turbine.RotorSpeed", discovered[0].NodeID)
	assert.Equal(t, "ns=2;s=Turbine.BearingTemp", discovered[1].NodeID)
}

