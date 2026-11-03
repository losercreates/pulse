package opcua

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNodeID(t *testing.T) {
	tests := []struct {
		input    string
		expected NodeID
	}{
		{"i=85", NodeID{Type: NodeIDTypeTwoByte, NamespaceIndex: 0, NumericID: 85}},
		{"ns=0;i=2258", NodeID{Type: NodeIDTypeFourByte, NamespaceIndex: 0, NumericID: 2258}},
		{"ns=2;i=100000", NodeID{Type: NodeIDTypeNumeric, NamespaceIndex: 2, NumericID: 100000}},
		{"ns=3;s=Turbine.RPM", NodeID{Type: NodeIDTypeString, NamespaceIndex: 3, StringID: "Turbine.RPM"}},
		{"s=SimpleTag", NodeID{Type: NodeIDTypeString, NamespaceIndex: 0, StringID: "SimpleTag"}},
	}

	for _, tc := range tests {
		nid, err := ParseNodeID(tc.input)
		require.NoError(t, err, "input: %s", tc.input)
		assert.Equal(t, tc.expected.Type, nid.Type)
		assert.Equal(t, tc.expected.NamespaceIndex, nid.NamespaceIndex)
		assert.Equal(t, tc.expected.NumericID, nid.NumericID)
		assert.Equal(t, tc.expected.StringID, nid.StringID)
	}
}

func TestNodeID_EncodeDecode(t *testing.T) {
	nids := []NodeID{
		{Type: NodeIDTypeTwoByte, NamespaceIndex: 0, NumericID: 85},
		{Type: NodeIDTypeFourByte, NamespaceIndex: 2, NumericID: 1024},
		{Type: NodeIDTypeNumeric, NamespaceIndex: 3, NumericID: 70000},
		{Type: NodeIDTypeString, NamespaceIndex: 4, StringID: "Sensor.Temp"},
	}

	for _, orig := range nids {
		var buf bytes.Buffer
		err := orig.Encode(&buf)
		require.NoError(t, err)

		decoded, err := DecodeNodeID(&buf)
		require.NoError(t, err)
		assert.Equal(t, orig.Type, decoded.Type)
		assert.Equal(t, orig.NamespaceIndex, decoded.NamespaceIndex)
		assert.Equal(t, orig.NumericID, decoded.NumericID)
		assert.Equal(t, orig.StringID, decoded.StringID)
	}
}

func TestStatusCode(t *testing.T) {
	assert.True(t, StatusGood.IsGood())
	assert.False(t, StatusGood.IsBad())
	assert.False(t, StatusGood.IsUncertain())

	assert.True(t, StatusUncertain.IsUncertain())
	assert.False(t, StatusUncertain.IsGood())

	assert.True(t, StatusBad.IsBad())
	assert.False(t, StatusBad.IsGood())
	assert.True(t, StatusBadTimeout.IsBad())
}

func TestVariant_Float64(t *testing.T) {
	v1 := Variant{Type: TypeBoolean, RawValue: true}
	f1, err := v1.Float64()
	require.NoError(t, err)
	assert.Equal(t, 1.0, f1)

	v2 := Variant{Type: TypeInt32, RawValue: int32(42)}
	f2, err := v2.Float64()
	require.NoError(t, err)
	assert.Equal(t, 42.0, f2)

	v3 := Variant{Type: TypeDouble, RawValue: float64(98.6)}
	f3, err := v3.Float64()
	require.NoError(t, err)
	assert.Equal(t, 98.6, f3)
}

func TestDateTime_EncodeDecode(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	var buf bytes.Buffer
	err := encodeDateTime(&buf, now)
	require.NoError(t, err)

	decoded, err := decodeDateTime(&buf)
	require.NoError(t, err)
	assert.WithinDuration(t, now, decoded, 100*time.Nanosecond)
}

