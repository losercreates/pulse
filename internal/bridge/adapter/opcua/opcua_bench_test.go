package opcua

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

func BenchmarkDecodeDataValue(b *testing.B) {
	var buf bytes.Buffer
	// Mask: Value (1) | Status (2) | SourceTimestamp (4) = 7
	_ = binary.Write(&buf, binary.LittleEndian, byte(7))
	_ = binary.Write(&buf, binary.LittleEndian, byte(TypeDouble))
	_ = binary.Write(&buf, binary.LittleEndian, float64(1850.75))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(StatusGood))
	_ = encodeDateTime(&buf, time.Now().UTC())

	payload := buf.Bytes()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(payload)
		dv, err := DecodeDataValue(r)
		if err != nil || dv.Status != StatusGood {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseNodeID(b *testing.B) {
	nodeStr := "ns=2;s=Turbine.Generator.ActivePower"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		nid, err := ParseNodeID(nodeStr)
		if err != nil || nid.Type != NodeIDTypeString {
			b.Fatal(err)
		}
	}
}

func BenchmarkNotificationUnpack(b *testing.B) {
	itemCfg := config.MonitoredItemConfig{
		NodeID:     "ns=2;s=Turbine.RPM",
		DeviceID:   "turbine-01",
		MetricName: "rotor_speed",
	}

	dv := DataValue{
		Value:           Variant{Type: TypeDouble, RawValue: float64(1800.0)},
		Status:          StatusGood,
		SourceTimestamp: time.Now().UTC(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		val, _ := dv.Value.Float64()
		ts := dv.SourceTimestamp.UnixNano()
		r := &telemetryv1.TelemetryReading{
			DeviceId:    itemCfg.DeviceID,
			MetricName:  itemCfg.MetricName,
			Value:       val,
			TimestampNs: ts,
			Metadata: map[string]string{
				"protocol":    "opcua",
				"node_id":     itemCfg.NodeID,
				"status_code": dv.Status.String(),
			},
		}
		if r.Value == 0 {
			b.Fatal("invalid value")
		}
	}
}

