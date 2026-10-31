package adapter_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeRegisterValue_DataTypesAndScaling(t *testing.T) {
	// Test int16 with scaling
	regInt16 := config.RegisterDefinition{
		DeviceID:     "gen-01",
		MetricName:   "pressure",
		DataType:     "int16",
		Multiplier:   0.1,
		Offset:       5.0,
		StartAddress: 100,
	}

	bufInt16 := make([]byte, 2)
	binary.BigEndian.PutUint16(bufInt16, uint16(250)) // raw = 250 -> 250*0.1 + 5.0 = 30.0
	val, err := adapter.DecodeRegisterValue(regInt16, bufInt16)
	require.NoError(t, err)
	assert.InDelta(t, 30.0, val, 1e-4)

	// Test float32_be (Big Endian)
	regFloatBE := config.RegisterDefinition{
		DeviceID:     "gen-01",
		MetricName:   "oil_temp",
		DataType:     "float32_be",
		Multiplier:   1.0,
		Offset:       0.0,
		StartAddress: 200,
	}

	bufFloatBE := make([]byte, 4)
	binary.BigEndian.PutUint32(bufFloatBE, math.Float32bits(88.5))
	valFloatBE, err := adapter.DecodeRegisterValue(regFloatBE, bufFloatBE)
	require.NoError(t, err)
	assert.InDelta(t, 88.5, valFloatBE, 1e-4)

	// Test float32_le (Little Endian)
	regFloatLE := config.RegisterDefinition{
		DeviceID:     "gen-01",
		MetricName:   "oil_temp_le",
		DataType:     "float32_le",
		Multiplier:   1.0,
		Offset:       0.0,
		StartAddress: 202,
	}

	bufFloatLE := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufFloatLE, math.Float32bits(104.2))
	valFloatLE, err := adapter.DecodeRegisterValue(regFloatLE, bufFloatLE)
	require.NoError(t, err)
	assert.InDelta(t, 104.2, valFloatLE, 1e-4)
}

func TestBuildModbusReadRequest(t *testing.T) {
	// FC 03, starting address 40001 (or 0x0064 = 100), quantity 2, slave ID 1, trans ID 10
	req := adapter.BuildModbusReadRequest(10, 1, 3, 100, 2)
	require.Len(t, req, 12)

	// MBAP Header
	assert.Equal(t, uint16(10), binary.BigEndian.Uint16(req[0:2])) // Transaction ID
	assert.Equal(t, uint16(0), binary.BigEndian.Uint16(req[2:4]))  // Protocol ID
	assert.Equal(t, uint16(6), binary.BigEndian.Uint16(req[4:6]))  // Length
	assert.Equal(t, uint8(1), req[6])                              // Unit/Slave ID

	// PDU
	assert.Equal(t, uint8(3), req[7])                              // Function Code
	assert.Equal(t, uint16(100), binary.BigEndian.Uint16(req[8:10])) // Start Address
	assert.Equal(t, uint16(2), binary.BigEndian.Uint16(req[10:12]))  // Quantity
}
