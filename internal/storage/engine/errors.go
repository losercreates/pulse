package engine

import "errors"

var (
	// ErrCorruptWAL is returned when a WAL frame fails CRC32 validation or contains invalid framing.
	ErrCorruptWAL = errors.New("storage: corrupt write-ahead log record")

	// ErrDiskFull is returned when storage volume free capacity falls below safe operational limits.
	ErrDiskFull = errors.New("storage: persistent disk full or capacity exhausted")

	// ErrShardLocked is returned when a shard directory is already locked by another process instance.
	ErrShardLocked = errors.New("storage: shard directory locked by another process")

	// ErrInvalidMagic is returned when a WAL or SSTable file lacks the expected Pulse sentinel magic header.
	ErrInvalidMagic = errors.New("storage: invalid file magic sentinel")

	// ErrSegmentNotFound is returned when an SSTable segment referenced in the manifest cannot be located.
	ErrSegmentNotFound = errors.New("storage: segment file not found")

	// ErrEngineClosed is returned on write or query operations against a closed storage engine.
	ErrEngineClosed = errors.New("storage: engine closed")
)
