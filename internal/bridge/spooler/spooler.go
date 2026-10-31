package spooler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/protobuf/proto"
)

const (
	FrameMagic       uint32 = 0x50554252 // "PUBR"
	HeaderSize              = 16
	DefaultMaxSegLen        = 10 * 1024 * 1024 // 10MB
	FlagProtobuf     uint32 = 0x01
)

var (
	ErrBufferFull = errors.New("spooler buffer exceeded maximum allocated disk capacity")
	ErrCorrupt    = errors.New("corrupt frame detected in spool log")
)

// DiskSpooler implements adapter.OfflineSpooler with a crash-resilient append-only FIFO ring buffer.
type DiskSpooler struct {
	mu           sync.Mutex
	dir          string
	maxDiskBytes int64
	maxSegBytes  int64

	manifest   *SpoolManifest
	writeFd    *os.File
	writeBytes int64

	readFd             *os.File
	readSeq            uint64
	pendingReadLengths []int64 // Byte length of each read frame waiting for Ack
}

// NewDiskSpooler initializes or reopens a spooler in the specified directory.
func NewDiskSpooler(dir string, maxDiskBytes int64) (*DiskSpooler, error) {
	cleanDir := filepath.Clean(dir)
	if err := os.MkdirAll(cleanDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create spool directory %s: %w", cleanDir, err)
	}

	manifest, err := loadManifest(cleanDir)
	if err != nil {
		return nil, err
	}

	s := &DiskSpooler{
		dir:          cleanDir,
		maxDiskBytes: maxDiskBytes,
		maxSegBytes:  DefaultMaxSegLen,
		manifest:     manifest,
	}

	// Open write segment for appending
	if err := s.openWriteSegment(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *DiskSpooler) openWriteSegment() error {
	segPath := segmentFileName(s.dir, s.manifest.CurrentWriteSeq)
	fd, err := os.OpenFile(segPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to open write segment %s: %w", segPath, err)
	}
	stat, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return fmt.Errorf("failed to stat write segment %s: %w", segPath, err)
	}
	s.writeFd = fd
	s.writeBytes = stat.Size()
	return nil
}

// Write appends a batch of telemetry records to the active spool segment.
func (s *DiskSpooler) Write(readings []*telemetryv1.TelemetryReading) error {
	if len(readings) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range readings {
		payload, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("failed to marshal reading for spool: %w", err)
		}

		payloadLen := uint32(len(payload))
		frameTotalLen := int64(HeaderSize + payloadLen)

		// Check capacity
		if s.maxDiskBytes > 0 && s.manifest.TotalDiskBytes+frameTotalLen > s.maxDiskBytes {
			return ErrBufferFull
		}

		// Roll segment if current exceeds max size
		if s.writeBytes+frameTotalLen > s.maxSegBytes {
			if err := s.rollWriteSegment(); err != nil {
				return err
			}
		}

		// Build 16-byte header
		var header [HeaderSize]byte
		binary.BigEndian.PutUint32(header[0:4], FrameMagic)
		binary.BigEndian.PutUint32(header[4:8], payloadLen)
		binary.BigEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(payload))
		binary.BigEndian.PutUint32(header[12:16], FlagProtobuf)

		// Write header and payload
		if _, err := s.writeFd.Write(header[:]); err != nil {
			return fmt.Errorf("failed to write frame header: %w", err)
		}
		if _, err := s.writeFd.Write(payload); err != nil {
			return fmt.Errorf("failed to write frame payload: %w", err)
		}

		s.writeBytes += frameTotalLen
		s.manifest.TotalDiskBytes += frameTotalLen
		s.manifest.TotalBufferedOps++
	}

	// Persist manifest updates
	return saveManifest(s.dir, s.manifest)
}

func (s *DiskSpooler) rollWriteSegment() error {
	if s.writeFd != nil {
		_ = s.writeFd.Sync()
		_ = s.writeFd.Close()
		s.writeFd = nil
	}
	s.manifest.CurrentWriteSeq++
	s.writeBytes = 0
	return s.openWriteSegment()
}

// ReadNext reads up to maxRecords from the spool in FIFO order.
func (s *DiskSpooler) ReadNext(maxRecords int) ([]*telemetryv1.TelemetryReading, error) {
	if maxRecords <= 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If there are already unacknowledged pending reads, do not read ahead past unacked batch
	if len(s.pendingReadLengths) > 0 {
		return nil, nil
	}

	if s.manifest.TotalBufferedOps == 0 {
		return nil, nil
	}

	var results []*telemetryv1.TelemetryReading

	for len(results) < maxRecords {
		// Open read segment if not opened or moved
		if s.readFd == nil || s.readSeq != s.manifest.CurrentReadSeq {
			if s.readFd != nil {
				_ = s.readFd.Close()
				s.readFd = nil
			}
			segPath := segmentFileName(s.dir, s.manifest.CurrentReadSeq)
			fd, err := os.Open(segPath)
			if err != nil {
				if os.IsNotExist(err) && s.manifest.CurrentReadSeq < s.manifest.CurrentWriteSeq {
					// Segment was cleaned or skipped, move forward
					s.manifest.CurrentReadSeq++
					s.manifest.ReadByteOffset = 0
					_ = saveManifest(s.dir, s.manifest)
					continue
				}
				break
			}
			if _, err := fd.Seek(s.manifest.ReadByteOffset, io.SeekStart); err != nil {
				_ = fd.Close()
				return nil, fmt.Errorf("failed to seek read segment: %w", err)
			}
			s.readFd = fd
			s.readSeq = s.manifest.CurrentReadSeq
		}

		// Read header
		var header [HeaderSize]byte
		_, err := io.ReadFull(s.readFd, header[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Reached EOF on current read segment
				if s.manifest.CurrentReadSeq < s.manifest.CurrentWriteSeq {
					// We've exhausted this historical segment
					_ = s.readFd.Close()
					s.readFd = nil
					oldSeg := segmentFileName(s.dir, s.manifest.CurrentReadSeq)
					_ = os.Remove(oldSeg)

					s.manifest.CurrentReadSeq++
					s.manifest.ReadByteOffset = 0
					_ = saveManifest(s.dir, s.manifest)
					continue
				}
				// Current segment still active, just caught up with writer
				// Seek back to ReadByteOffset to be ready for future writes
				_, _ = s.readFd.Seek(s.manifest.ReadByteOffset, io.SeekStart)
				break
			}
			return nil, fmt.Errorf("failed to read frame header: %w", err)
		}

		magic := binary.BigEndian.Uint32(header[0:4])
		if magic != FrameMagic {
			// Torn or corrupt frame at EOF, skip segment
			break
		}
		payloadLen := binary.BigEndian.Uint32(header[4:8])
		checksum := binary.BigEndian.Uint32(header[8:12])

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(s.readFd, payload); err != nil {
			// Incomplete write at tail
			break
		}

		if crc32.ChecksumIEEE(payload) != checksum {
			// Corrupted frame
			break
		}

		var reading telemetryv1.TelemetryReading
		if err := proto.Unmarshal(payload, &reading); err != nil {
			break
		}

		frameLen := int64(HeaderSize + payloadLen)
		s.pendingReadLengths = append(s.pendingReadLengths, frameLen)
		results = append(results, &reading)
	}

	return results, nil
}

// Ack acknowledges that count records from the last ReadNext were processed.
// If count <= 0, unacknowledged pending reads are reset so they can be re-read.
func (s *DiskSpooler) Ack(count int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if count <= 0 {
		s.pendingReadLengths = nil
		if s.readFd != nil {
			_, _ = s.readFd.Seek(s.manifest.ReadByteOffset, io.SeekStart)
		}
		return nil
	}

	if len(s.pendingReadLengths) == 0 {
		return nil
	}

	if count > len(s.pendingReadLengths) {
		count = len(s.pendingReadLengths)
	}

	var bytesAcked int64
	for i := 0; i < count; i++ {
		bytesAcked += s.pendingReadLengths[i]
	}

	s.manifest.ReadByteOffset += bytesAcked
	if s.manifest.TotalDiskBytes >= bytesAcked {
		s.manifest.TotalDiskBytes -= bytesAcked
	} else {
		s.manifest.TotalDiskBytes = 0
	}

	if s.manifest.TotalBufferedOps >= uint64(count) {
		s.manifest.TotalBufferedOps -= uint64(count)
	} else {
		s.manifest.TotalBufferedOps = 0
	}

	s.pendingReadLengths = s.pendingReadLengths[count:]

	// If read offset reached end of segment and writer has moved past it, roll read segment
	if len(s.pendingReadLengths) == 0 && s.manifest.CurrentReadSeq < s.manifest.CurrentWriteSeq {
		if s.readFd != nil {
			stat, err := s.readFd.Stat()
			if err == nil && s.manifest.ReadByteOffset >= stat.Size() {
				_ = s.readFd.Close()
				s.readFd = nil
				oldSeg := segmentFileName(s.dir, s.manifest.CurrentReadSeq)
				_ = os.Remove(oldSeg)
				s.manifest.CurrentReadSeq++
				s.manifest.ReadByteOffset = 0
			}
		}
	}

	return saveManifest(s.dir, s.manifest)
}

// BufferedCount returns the total number of unread records currently spooled on disk.
func (s *DiskSpooler) BufferedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(s.manifest.TotalBufferedOps)
}

// Close flushes open file descriptors and commits state.
func (s *DiskSpooler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	if s.writeFd != nil {
		if err := s.writeFd.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.writeFd.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.writeFd = nil
	}
	if s.readFd != nil {
		if err := s.readFd.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.readFd = nil
	}
	if err := saveManifest(s.dir, s.manifest); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
