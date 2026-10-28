package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/protobuf/proto"
)

const (
	// WALMagic is the 4-byte sentinel identifier for Pulse WAL files (ASCII "PULS").
	WALMagic uint32 = 0x50554C53

	// Operation codes
	OpPut        uint8 = 0x01
	OpTombstone  uint8 = 0x02
	OpCheckpoint uint8 = 0x03

	// FrameHeaderSize is Magic(4) + Length(4) + CRC32(4) + OpType(1) = 13 bytes.
	FrameHeaderSize = 13
)

// EncodeWALFrame serializes an operation and payload into a binary frame with CRC32 integrity check.
func EncodeWALFrame(op uint8, payload []byte) []byte {
	payloadLen := uint32(len(payload))
	crc := crc32.ChecksumIEEE(payload)

	buf := make([]byte, FrameHeaderSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], WALMagic)
	binary.BigEndian.PutUint32(buf[4:8], payloadLen)
	binary.BigEndian.PutUint32(buf[8:12], crc)
	buf[12] = op
	copy(buf[13:], payload)

	return buf
}

// DecodeWALFrame reads a single framed record from reader, validating magic and CRC32 checksum.
// Returns io.EOF on clean end of stream.
func DecodeWALFrame(r io.Reader) (uint8, []byte, error) {
	header := make([]byte, FrameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, io.EOF
		}
		return 0, nil, err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != WALMagic {
		return 0, nil, fmt.Errorf("%w: expected 0x%X, got 0x%X", ErrInvalidMagic, WALMagic, magic)
	}

	payloadLen := binary.BigEndian.Uint32(header[4:8])
	expectedCRC := binary.BigEndian.Uint32(header[8:12])
	op := header[12]

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("%w: truncated payload (%w)", ErrCorruptWAL, err)
	}

	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != expectedCRC {
		return 0, nil, fmt.Errorf("%w: CRC mismatch (expected 0x%X, got 0x%X)", ErrCorruptWAL, expectedCRC, actualCRC)
	}

	return op, payload, nil
}

// MarshalAggregate wraps proto.Marshal for StoredAggregate.
func MarshalAggregate(agg *telemetryv1.StoredAggregate) ([]byte, error) {
	return proto.Marshal(agg)
}

// UnmarshalAggregate wraps proto.Unmarshal for StoredAggregate.
func UnmarshalAggregate(data []byte) (*telemetryv1.StoredAggregate, error) {
	var agg telemetryv1.StoredAggregate
	if err := proto.Unmarshal(data, &agg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal StoredAggregate: %w", err)
	}
	return &agg, nil
}
