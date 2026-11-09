package opcua

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadHeader_BoundsAndUnderflowProtection(t *testing.T) {
	t.Run("Valid header message size", func(t *testing.T) {
		raw := []byte{'M', 'S', 'G', 'F', 0x20, 0x00, 0x00, 0x00} // Size = 32
		r := bytes.NewReader(raw)
		h, err := ReadHeader(r)
		require.NoError(t, err)
		assert.Equal(t, "MSG", h.MessageType)
		assert.Equal(t, byte('F'), h.ChunkType)
		assert.Equal(t, uint32(32), h.MessageSize)
	})

	t.Run("Underflow prevention: message size < 8", func(t *testing.T) {
		// Size = 4 (would underflow to 4294967292 in h.MessageSize - 8)
		raw := []byte{'M', 'S', 'G', 'F', 0x04, 0x00, 0x00, 0x00}
		r := bytes.NewReader(raw)
		_, err := ReadHeader(r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid opc-ua message size")
	})

	t.Run("Underflow prevention: message size = 0", func(t *testing.T) {
		raw := []byte{'O', 'P', 'N', 'F', 0x00, 0x00, 0x00, 0x00}
		r := bytes.NewReader(raw)
		_, err := ReadHeader(r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid opc-ua message size")
	})

	t.Run("Overflow prevention: message size exceeds 16MB", func(t *testing.T) {
		// Size = 32MB (0x02000000 = 33554432)
		raw := []byte{'M', 'S', 'G', 'F', 0x00, 0x00, 0x00, 0x02}
		r := bytes.NewReader(raw)
		_, err := ReadHeader(r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds maximum limit")
	})

	t.Run("Truncated header returns error", func(t *testing.T) {
		raw := []byte{'M', 'S', 'G'}
		r := bytes.NewReader(raw)
		_, err := ReadHeader(r)
		require.Error(t, err)
	})
}

