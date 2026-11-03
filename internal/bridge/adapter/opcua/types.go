package opcua

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// OPC-UA Epoch constant: 100ns intervals between 1601-01-01 and 1970-01-01 UTC.
const opcuaEpochOffset = 116444736000000000

// TypeId Constants for OPC-UA Services (numeric node IDs in ns=0)
const (
	TypeIDCreateSessionRequest        uint32 = 461
	TypeIDCreateSessionResponse       uint32 = 464
	TypeIDActivateSessionRequest      uint32 = 467
	TypeIDActivateSessionResponse     uint32 = 470
	TypeIDCloseSessionRequest         uint32 = 473
	TypeIDCloseSessionResponse        uint32 = 476
	TypeIDCreateSubscriptionRequest   uint32 = 787
	TypeIDCreateSubscriptionResponse  uint32 = 790
	TypeIDCreateMonitoredItemsRequest uint32 = 751
	TypeIDCreateMonitoredItemsResponse uint32 = 754
	TypeIDPublishRequest              uint32 = 826
	TypeIDPublishResponse             uint32 = 829
	TypeIDBrowseRequest               uint32 = 527
	TypeIDBrowseResponse              uint32 = 530
)

// NodeIDType represents the encoding format of a NodeID.
type NodeIDType byte

const (
	NodeIDTypeTwoByte   NodeIDType = 0x00
	NodeIDTypeFourByte  NodeIDType = 0x01
	NodeIDTypeNumeric   NodeIDType = 0x02
	NodeIDTypeString    NodeIDType = 0x03
	NodeIDTypeGUID      NodeIDType = 0x04
	NodeIDTypeByteString NodeIDType = 0x05
)

// NodeID represents an OPC-UA Node Identifier.
type NodeID struct {
	Type           NodeIDType
	NamespaceIndex uint16
	NumericID      uint32
	StringID       string
}

// ParseNodeID parses string formats like "i=85", "ns=2;s=Turbine.RPM", or "ns=2;i=1001".
func ParseNodeID(s string) (NodeID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return NodeID{Type: NodeIDTypeTwoByte, NumericID: 0}, nil
	}

	parts := strings.Split(s, ";")
	var ns uint16 = 0
	valPart := s

	if len(parts) == 2 {
		if strings.HasPrefix(parts[0], "ns=") {
			n, err := strconv.ParseUint(parts[0][3:], 10, 16)
			if err != nil {
				return NodeID{}, fmt.Errorf("invalid namespace in node id %s: %w", s, err)
			}
			ns = uint16(n)
			valPart = parts[1]
		}
	}

	if strings.HasPrefix(valPart, "i=") {
		id, err := strconv.ParseUint(valPart[2:], 10, 32)
		if err != nil {
			return NodeID{}, fmt.Errorf("invalid numeric node id %s: %w", s, err)
		}
		if ns == 0 && id <= 255 {
			return NodeID{Type: NodeIDTypeTwoByte, NamespaceIndex: 0, NumericID: uint32(id)}, nil
		}
		if ns <= 255 && id <= 65535 {
			return NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: ns, NumericID: uint32(id)}, nil
		}
		return NodeID{Type: NodeIDTypeNumeric, NamespaceIndex: ns, NumericID: uint32(id)}, nil
	}

	if strings.HasPrefix(valPart, "s=") {
		return NodeID{Type: NodeIDTypeString, NamespaceIndex: ns, StringID: valPart[2:]}, nil
	}

	// Default fallback to numeric
	if id, err := strconv.ParseUint(valPart, 10, 32); err == nil {
		if ns == 0 && id <= 255 {
			return NodeID{Type: NodeIDTypeTwoByte, NamespaceIndex: 0, NumericID: uint32(id)}, nil
		}
		return NodeID{Type: NodeIDTypeNumeric, NamespaceIndex: ns, NumericID: uint32(id)}, nil
	}

	return NodeID{Type: NodeIDTypeString, NamespaceIndex: ns, StringID: valPart}, nil
}

// String formats the NodeID as standard string notation.
func (n NodeID) String() string {
	switch n.Type {
	case NodeIDTypeTwoByte:
		return fmt.Sprintf("i=%d", n.NumericID)
	case NodeIDTypeFourByte, NodeIDTypeNumeric:
		if n.NamespaceIndex == 0 {
			return fmt.Sprintf("i=%d", n.NumericID)
		}
		return fmt.Sprintf("ns=%d;i=%d", n.NamespaceIndex, n.NumericID)
	case NodeIDTypeString:
		if n.NamespaceIndex == 0 {
			return fmt.Sprintf("s=%s", n.StringID)
		}
		return fmt.Sprintf("ns=%d;s=%s", n.NamespaceIndex, n.StringID)
	default:
		return fmt.Sprintf("ns=%d;i=%d", n.NamespaceIndex, n.NumericID)
	}
}

// Encode writes the binary representation of the NodeID into w.
func (n NodeID) Encode(w io.Writer) error {
	switch n.Type {
	case NodeIDTypeTwoByte:
		return binary.Write(w, binary.LittleEndian, []byte{byte(NodeIDTypeTwoByte), byte(n.NumericID)})
	case NodeIDTypeFourByte:
		buf := make([]byte, 4)
		buf[0] = byte(NodeIDTypeFourByte)
		buf[1] = byte(n.NamespaceIndex)
		binary.LittleEndian.PutUint16(buf[2:], uint16(n.NumericID))
		_, err := w.Write(buf)
		return err
	case NodeIDTypeNumeric:
		buf := make([]byte, 7)
		buf[0] = byte(NodeIDTypeNumeric)
		binary.LittleEndian.PutUint16(buf[1:3], n.NamespaceIndex)
		binary.LittleEndian.PutUint32(buf[3:7], n.NumericID)
		_, err := w.Write(buf)
		return err
	case NodeIDTypeString:
		if err := binary.Write(w, binary.LittleEndian, byte(NodeIDTypeString)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, n.NamespaceIndex); err != nil {
			return err
		}
		return writeOPCUAString(w, n.StringID)
	default:
		buf := make([]byte, 7)
		buf[0] = byte(NodeIDTypeNumeric)
		binary.LittleEndian.PutUint16(buf[1:3], n.NamespaceIndex)
		binary.LittleEndian.PutUint32(buf[3:7], n.NumericID)
		_, err := w.Write(buf)
		return err
	}
}

// DecodeNodeID reads a NodeID from r.
func DecodeNodeID(r io.Reader) (NodeID, error) {
	var encType byte
	if err := binary.Read(r, binary.LittleEndian, &encType); err != nil {
		return NodeID{}, err
	}

	t := NodeIDType(encType & 0x0F)
	switch t {
	case NodeIDTypeTwoByte:
		var id byte
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return NodeID{}, err
		}
		return NodeID{Type: NodeIDTypeTwoByte, NamespaceIndex: 0, NumericID: uint32(id)}, nil
	case NodeIDTypeFourByte:
		var ns byte
		var id uint16
		if err := binary.Read(r, binary.LittleEndian, &ns); err != nil {
			return NodeID{}, err
		}
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return NodeID{}, err
		}
		return NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: uint16(ns), NumericID: uint32(id)}, nil
	case NodeIDTypeNumeric:
		var ns uint16
		var id uint32
		if err := binary.Read(r, binary.LittleEndian, &ns); err != nil {
			return NodeID{}, err
		}
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return NodeID{}, err
		}
		return NodeID{Type: NodeIDTypeNumeric, NamespaceIndex: ns, NumericID: id}, nil
	case NodeIDTypeString:
		var ns uint16
		if err := binary.Read(r, binary.LittleEndian, &ns); err != nil {
			return NodeID{}, err
		}
		str, err := readOPCUAString(r)
		if err != nil {
			return NodeID{}, err
		}
		return NodeID{Type: NodeIDTypeString, NamespaceIndex: ns, StringID: str}, nil
	default:
		return NodeID{}, fmt.Errorf("unsupported NodeID type: 0x%02x", encType)
	}
}

// VariantType enum for OPC-UA built-in types.
type VariantType byte

const (
	TypeNull        VariantType = 0
	TypeBoolean     VariantType = 1
	TypeSByte       VariantType = 2
	TypeByte        VariantType = 3
	TypeInt16       VariantType = 4
	TypeUInt16      VariantType = 5
	TypeInt32       VariantType = 6
	TypeUInt32      VariantType = 7
	TypeInt64       VariantType = 8
	TypeUInt64      VariantType = 9
	TypeFloat       VariantType = 10
	TypeDouble      VariantType = 11
	TypeString      VariantType = 12
	TypeDateTime    VariantType = 13
	TypeGUID        VariantType = 14
	TypeByteString  VariantType = 15
	TypeNodeID      VariantType = 17
	TypeStatusCode  VariantType = 19
)

// Variant holds a typed OPC-UA value.
type Variant struct {
	Type     VariantType
	RawValue any
}

// Float64 converts the variant to a float64 representation.
func (v Variant) Float64() (float64, error) {
	switch val := v.RawValue.(type) {
	case bool:
		if val {
			return 1.0, nil
		}
		return 0.0, nil
	case int8:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert variant type %d to float64", v.Type)
	}
}

// DecodeVariant reads an OPC-UA Variant from r.
func DecodeVariant(r io.Reader) (Variant, error) {
	var mask byte
	if err := binary.Read(r, binary.LittleEndian, &mask); err != nil {
		return Variant{}, err
	}

	vt := VariantType(mask & 0x3F)
	switch vt {
	case TypeNull:
		return Variant{Type: TypeNull, RawValue: nil}, nil
	case TypeBoolean:
		var b byte
		if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeBoolean, RawValue: b != 0}, nil
	case TypeSByte:
		var i int8
		if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeSByte, RawValue: i}, nil
	case TypeByte:
		var u uint8
		if err := binary.Read(r, binary.LittleEndian, &u); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeByte, RawValue: u}, nil
	case TypeInt16:
		var i int16
		if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeInt16, RawValue: i}, nil
	case TypeUInt16:
		var u uint16
		if err := binary.Read(r, binary.LittleEndian, &u); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeUInt16, RawValue: u}, nil
	case TypeInt32:
		var i int32
		if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeInt32, RawValue: i}, nil
	case TypeUInt32:
		var u uint32
		if err := binary.Read(r, binary.LittleEndian, &u); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeUInt32, RawValue: u}, nil
	case TypeInt64:
		var i int64
		if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeInt64, RawValue: i}, nil
	case TypeUInt64:
		var u uint64
		if err := binary.Read(r, binary.LittleEndian, &u); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeUInt64, RawValue: u}, nil
	case TypeFloat:
		var f float32
		if err := binary.Read(r, binary.LittleEndian, &f); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeFloat, RawValue: f}, nil
	case TypeDouble:
		var d float64
		if err := binary.Read(r, binary.LittleEndian, &d); err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeDouble, RawValue: d}, nil
	case TypeString:
		s, err := readOPCUAString(r)
		if err != nil {
			return Variant{}, err
		}
		return Variant{Type: TypeString, RawValue: s}, nil
	default:
		return Variant{Type: vt, RawValue: nil}, nil
	}
}

// StatusCode represents an OPC-UA 32-bit status code.
type StatusCode uint32

const (
	StatusGood                     StatusCode = 0x00000000
	StatusUncertain                StatusCode = 0x40000000
	StatusBad                      StatusCode = 0x80000000
	StatusBadUnexpectedError       StatusCode = 0x80010000
	StatusBadSessionIDInvalid      StatusCode = 0x80250000
	StatusBadSubscriptionIDInvalid StatusCode = 0x80280000
	StatusBadTimeout               StatusCode = 0x800A0000
	StatusBadNodeIDUnknown         StatusCode = 0x80340000
)

func (s StatusCode) IsGood() bool {
	return (s & 0xC0000000) == 0
}

func (s StatusCode) IsUncertain() bool {
	return (s & 0xC0000000) == 0x40000000
}

func (s StatusCode) IsBad() bool {
	return (s & 0x80000000) != 0
}

func (s StatusCode) String() string {
	switch s {
	case StatusGood:
		return "Good"
	case StatusUncertain:
		return "Uncertain"
	case StatusBad:
		return "Bad"
	case StatusBadUnexpectedError:
		return "BadUnexpectedError"
	case StatusBadSessionIDInvalid:
		return "BadSessionIDInvalid"
	case StatusBadSubscriptionIDInvalid:
		return "BadSubscriptionIDInvalid"
	case StatusBadTimeout:
		return "BadTimeout"
	case StatusBadNodeIDUnknown:
		return "BadNodeIDUnknown"
	default:
		if s.IsGood() {
			return fmt.Sprintf("Good(0x%08X)", uint32(s))
		}
		if s.IsUncertain() {
			return fmt.Sprintf("Uncertain(0x%08X)", uint32(s))
		}
		return fmt.Sprintf("Bad(0x%08X)", uint32(s))
	}
}

// DataValue represents a timestamped value with status code.
type DataValue struct {
	Value           Variant
	Status          StatusCode
	SourceTimestamp time.Time
	ServerTimestamp time.Time
}

// DecodeDataValue decodes an OPC-UA DataValue from r.
func DecodeDataValue(r io.Reader) (DataValue, error) {
	var mask byte
	if err := binary.Read(r, binary.LittleEndian, &mask); err != nil {
		return DataValue{}, err
	}

	dv := DataValue{Status: StatusGood}

	if mask&0x01 != 0 {
		val, err := DecodeVariant(r)
		if err != nil {
			return dv, fmt.Errorf("failed to decode variant value: %w", err)
		}
		dv.Value = val
	}

	if mask&0x02 != 0 {
		var st uint32
		if err := binary.Read(r, binary.LittleEndian, &st); err != nil {
			return dv, fmt.Errorf("failed to decode status code: %w", err)
		}
		dv.Status = StatusCode(st)
	}

	if mask&0x04 != 0 {
		t, err := decodeDateTime(r)
		if err != nil {
			return dv, fmt.Errorf("failed to decode source timestamp: %w", err)
		}
		dv.SourceTimestamp = t
	}

	if mask&0x08 != 0 {
		t, err := decodeDateTime(r)
		if err != nil {
			return dv, fmt.Errorf("failed to decode server timestamp: %w", err)
		}
		dv.ServerTimestamp = t
	}

	if mask&0x10 != 0 {
		var pico uint16
		_ = binary.Read(r, binary.LittleEndian, &pico)
	}

	if mask&0x20 != 0 {
		var pico uint16
		_ = binary.Read(r, binary.LittleEndian, &pico)
	}

	return dv, nil
}

// Helper to decode 64-bit datetime (100ns intervals since 1601-01-01).
func decodeDateTime(r io.Reader) (time.Time, error) {
	var ticks int64
	if err := binary.Read(r, binary.LittleEndian, &ticks); err != nil {
		return time.Time{}, err
	}
	if ticks <= 0 || ticks <= opcuaEpochOffset {
		return time.Time{}, nil
	}
	unixNano := (ticks - opcuaEpochOffset) * 100
	sec := unixNano / 1_000_000_000
	nsec := unixNano % 1_000_000_000
	return time.Unix(sec, nsec).UTC(), nil
}

// Helper to encode 64-bit datetime.
func encodeDateTime(w io.Writer, t time.Time) error {
	if t.IsZero() {
		return binary.Write(w, binary.LittleEndian, int64(0))
	}
	unixNano := t.UnixNano()
	ticks := (unixNano / 100) + opcuaEpochOffset
	return binary.Write(w, binary.LittleEndian, ticks)
}

// writeOPCUAString writes a 32-bit length-prefixed string (-1 for null).
func writeOPCUAString(w io.Writer, s string) error {
	if s == "" {
		return binary.Write(w, binary.LittleEndian, int32(0))
	}
	length := int32(len(s))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

// readOPCUAString reads a 32-bit length-prefixed string.
func readOPCUAString(r io.Reader) (string, error) {
	var length int32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	if length <= 0 {
		return "", nil
	}
	if length > 65536 {
		return "", fmt.Errorf("opc-ua string length %d exceeds max buffer", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// writeByteString writes a 32-bit length-prefixed byte array (-1 for null).
func writeByteString(w io.Writer, b []byte) error {
	if b == nil {
		return binary.Write(w, binary.LittleEndian, int32(-1))
	}
	length := int32(len(b))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// readByteString reads a 32-bit length-prefixed byte array.
func readByteString(r io.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		return []byte{}, nil
	}
	if length > 1048576 {
		return nil, fmt.Errorf("opc-ua byte string length %d exceeds limit", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
