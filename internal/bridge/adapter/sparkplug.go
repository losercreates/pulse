package adapter

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// SparkplugDataType definitions based on the Sparkplug B specification.
const (
	SparkplugDataTypeInt8    uint32 = 1
	SparkplugDataTypeInt16   uint32 = 2
	SparkplugDataTypeInt32   uint32 = 3
	SparkplugDataTypeInt64   uint32 = 4
	SparkplugDataTypeUInt8   uint32 = 5
	SparkplugDataTypeUInt16  uint32 = 6
	SparkplugDataTypeUInt32  uint32 = 7
	SparkplugDataTypeUInt64  uint32 = 8
	SparkplugDataTypeFloat   uint32 = 9
	SparkplugDataTypeDouble  uint32 = 10
	SparkplugDataTypeBoolean uint32 = 11
	SparkplugDataTypeString  uint32 = 12
)

// SparkplugMetric represents an individual measurement within a Sparkplug B payload.
type SparkplugMetric struct {
	Name        string
	Alias       uint64
	TimestampMs uint64
	DataType    uint32
	DoubleValue float64
	FloatValue  float32
	Int32Value  int32
	Int64Value  int64
	BoolValue   bool
	StringValue string
}

// SparkplugPayload represents a decoded Sparkplug B payload structure.
type SparkplugPayload struct {
	TimestampMs uint64
	Metrics     []SparkplugMetric
	Seq         uint64
}

// DecodeSparkplugMessage extracts TelemetryReadings from a Sparkplug B topic and payload.
func DecodeSparkplugMessage(topic string, payloadBytes []byte) ([]*telemetryv1.TelemetryReading, error) {
	// spBv1.0/<group_id>/<message_type>/<edge_node_id>/[<device_id>]
	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid sparkplug b topic: %s", topic)
	}

	edgeNodeID := parts[3]
	deviceID := edgeNodeID
	if len(parts) >= 5 && parts[4] != "" {
		deviceID = parts[4]
	}

	payload, err := ParseSparkplugProtobuf(payloadBytes)
	if err != nil {
		return nil, err
	}

	defaultTimeNs := int64(payload.TimestampMs) * 1e6
	if defaultTimeNs <= 0 {
		defaultTimeNs = time.Now().UnixNano()
	}

	var readings []*telemetryv1.TelemetryReading
	for _, m := range payload.Metrics {
		metricName := m.Name
		if metricName == "" && m.Alias > 0 {
			metricName = fmt.Sprintf("alias_%d", m.Alias)
		}
		if metricName == "" {
			continue
		}

		var val float64
		switch m.DataType {
		case SparkplugDataTypeDouble:
			val = m.DoubleValue
		case SparkplugDataTypeFloat:
			val = float64(m.FloatValue)
		case SparkplugDataTypeInt8, SparkplugDataTypeInt16, SparkplugDataTypeInt32:
			val = float64(m.Int32Value)
		case SparkplugDataTypeInt64:
			val = float64(m.Int64Value)
		case SparkplugDataTypeUInt8, SparkplugDataTypeUInt16, SparkplugDataTypeUInt32, SparkplugDataTypeUInt64:
			val = float64(m.Int64Value)
		case SparkplugDataTypeBoolean:
			if m.BoolValue {
				val = 1.0
			} else {
				val = 0.0
			}
		default:
			val = m.DoubleValue
		}

		metricTsNs := defaultTimeNs
		if m.TimestampMs > 0 {
			metricTsNs = int64(m.TimestampMs) * 1e6
		}

		readings = append(readings, &telemetryv1.TelemetryReading{
			DeviceId:    deviceID,
			MetricName:  metricName,
			Value:       val,
			TimestampNs: metricTsNs,
		})
	}

	return readings, nil
}

// ParseSparkplugProtobuf decodes wire protobuf bytes into SparkplugPayload using a standard tag-based decoder.
func ParseSparkplugProtobuf(data []byte) (*SparkplugPayload, error) {
	payload := &SparkplugPayload{}
	idx := 0

	for idx < len(data) {
		tag, wireType, n := decodeVarint(data[idx:])
		if n <= 0 {
			break
		}
		idx += n
		fieldNum := tag >> 3

		switch fieldNum {
		case 1: // timestamp (uint64, varint)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			payload.TimestampMs = v
		case 2: // metrics (repeated Metric, length-delimited)
			length, _, n := decodeVarint(data[idx:])
			idx += n
			if idx+int(length) > len(data) {
				return nil, fmt.Errorf("truncated sparkplug metric field")
			}
			metric, err := parseSparkplugMetric(data[idx : idx+int(length)])
			if err != nil {
				return nil, err
			}
			payload.Metrics = append(payload.Metrics, *metric)
			idx += int(length)
		case 3: // seq (uint64, varint)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			payload.Seq = v
		default:
			// Skip unknown field based on wire type
			skipLen, err := skipWireField(wireType, data[idx:])
			if err != nil {
				return nil, err
			}
			idx += skipLen
		}
	}

	return payload, nil
}

func parseSparkplugMetric(data []byte) (*SparkplugMetric, error) {
	m := &SparkplugMetric{}
	idx := 0

	for idx < len(data) {
		tag, wireType, n := decodeVarint(data[idx:])
		if n <= 0 {
			break
		}
		idx += n
		fieldNum := tag >> 3

		switch fieldNum {
		case 1: // name (string, wire 2)
			length, _, n := decodeVarint(data[idx:])
			idx += n
			m.Name = string(data[idx : idx+int(length)])
			idx += int(length)
		case 2: // alias (uint64, wire 0)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			m.Alias = v
		case 3: // timestamp (uint64, wire 0)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			m.TimestampMs = v
		case 4: // datatype (uint32, wire 0)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			m.DataType = uint32(v)
		case 9: // int_value (uint32, wire 0)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			m.Int32Value = int32(v)
		case 10: // long_value (uint64, wire 0)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			m.Int64Value = int64(v)
		case 11: // float_value (float, 32-bit, wire 5)
			if idx+4 > len(data) {
				return nil, fmt.Errorf("truncated float")
			}
			bits := binary.LittleEndian.Uint32(data[idx : idx+4])
			m.FloatValue = math.Float32frombits(bits)
			idx += 4
		case 12: // double_value (double, 64-bit, wire 1)
			if idx+8 > len(data) {
				return nil, fmt.Errorf("truncated double")
			}
			bits := binary.LittleEndian.Uint64(data[idx : idx+8])
			m.DoubleValue = math.Float64frombits(bits)
			idx += 8
		case 13: // boolean_value (bool, wire 0)
			v, _, n := decodeVarint(data[idx:])
			idx += n
			m.BoolValue = (v != 0)
		case 14: // string_value (string, wire 2)
			length, _, n := decodeVarint(data[idx:])
			idx += n
			m.StringValue = string(data[idx : idx+int(length)])
			idx += int(length)
		default:
			skipLen, err := skipWireField(wireType, data[idx:])
			if err != nil {
				return nil, err
			}
			idx += skipLen
		}
	}

	return m, nil
}

func decodeVarint(data []byte) (uint64, int, int) {
	if len(data) == 0 {
		return 0, 0, 0
	}
	var res uint64
	var shift uint
	for i, b := range data {
		res |= uint64(b&0x7F) << shift
		shift += 7
		if (b & 0x80) == 0 {
			return res, int(res & 0x7), i + 1
		}
		if i >= 9 {
			return 0, 0, 0
		}
	}
	return 0, 0, 0
}

func encodeVarint(v uint64) []byte {
	var buf []byte
	for v >= 0x80 {
		buf = append(buf, byte(v|0x80))
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

func skipWireField(wireType int, data []byte) (int, error) {
	switch wireType {
	case 0: // varint
		_, _, n := decodeVarint(data)
		return n, nil
	case 1: // 64-bit
		if len(data) < 8 {
			return 0, fmt.Errorf("truncated 64-bit field")
		}
		return 8, nil
	case 2: // length-delimited
		lenVal, _, n := decodeVarint(data)
		return n + int(lenVal), nil
	case 5: // 32-bit
		if len(data) < 4 {
			return 0, fmt.Errorf("truncated 32-bit field")
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("unsupported wire type: %d", wireType)
	}
}

// CreateMockSparkplugPayload serializes Sparkplug metrics into protobuf bytes for testing and simulation.
func CreateMockSparkplugPayload(timestampMs uint64, metrics []SparkplugMetric) []byte {
	var buf []byte

	// Field 1: timestampMs (varint)
	buf = append(buf, encodeVarint((1<<3)|0)...)
	buf = append(buf, encodeVarint(timestampMs)...)

	// Field 2: Metrics (repeated)
	for _, m := range metrics {
		var metricBytes []byte

		// Metric Field 1: name (string, wire 2)
		if m.Name != "" {
			metricBytes = append(metricBytes, encodeVarint((1<<3)|2)...)
			metricBytes = append(metricBytes, encodeVarint(uint64(len(m.Name)))...)
			metricBytes = append(metricBytes, []byte(m.Name)...)
		}

		// Metric Field 4: datatype (varint)
		metricBytes = append(metricBytes, encodeVarint((4<<3)|0)...)
		metricBytes = append(metricBytes, encodeVarint(uint64(m.DataType))...)

		// Metric Values
		switch m.DataType {
		case SparkplugDataTypeDouble:
			metricBytes = append(metricBytes, encodeVarint((12<<3)|1)...)
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(m.DoubleValue))
			metricBytes = append(metricBytes, b[:]...)
		case SparkplugDataTypeFloat:
			metricBytes = append(metricBytes, encodeVarint((11<<3)|5)...)
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], math.Float32bits(m.FloatValue))
			metricBytes = append(metricBytes, b[:]...)
		case SparkplugDataTypeInt32:
			metricBytes = append(metricBytes, encodeVarint((9<<3)|0)...)
			metricBytes = append(metricBytes, encodeVarint(uint64(uint32(m.Int32Value)))...)
		case SparkplugDataTypeInt64:
			metricBytes = append(metricBytes, encodeVarint((10<<3)|0)...)
			metricBytes = append(metricBytes, encodeVarint(uint64(m.Int64Value))...)
		}

		// Append metric to main payload
		buf = append(buf, encodeVarint((2<<3)|2)...)
		buf = append(buf, encodeVarint(uint64(len(metricBytes)))...)
		buf = append(buf, metricBytes...)
	}

	return buf
}

