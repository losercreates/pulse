package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// SSTableReader provides point-lookup and range scanning over an immutable SSTable file.
type SSTableReader struct {
	mu           sync.RWMutex
	file         *os.File
	path         string
	trailer      SSTableTrailer
	bloom        *BloomFilter
	indexEntries []IndexEntry
}

// OpenSSTableReader opens and validates an SSTable file.
func OpenSSTableReader(filePath string) (*SSTableReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable %s: %w", filePath, err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat sstable %s: %w", filePath, err)
	}

	if stat.Size() < SSTableTrailerSize {
		f.Close()
		return nil, fmt.Errorf("sstable file %s is smaller than trailer size: %w", filePath, ErrInvalidMagic)
	}

	// 1. Read trailer
	trailerBuf := make([]byte, SSTableTrailerSize)
	if _, err := f.ReadAt(trailerBuf, stat.Size()-SSTableTrailerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read trailer: %w", err)
	}

	trailer := SSTableTrailer{
		IndexOffset: binary.BigEndian.Uint64(trailerBuf[0:8]),
		IndexLength: binary.BigEndian.Uint32(trailerBuf[8:12]),
		BloomOffset: binary.BigEndian.Uint64(trailerBuf[12:20]),
		BloomLength: binary.BigEndian.Uint32(trailerBuf[20:24]),
		RecordCount: binary.BigEndian.Uint32(trailerBuf[24:28]),
		Magic:       binary.BigEndian.Uint32(trailerBuf[28:32]),
	}

	if trailer.Magic != SSTableMagic {
		f.Close()
		return nil, fmt.Errorf("invalid sstable magic 0x%X: %w", trailer.Magic, ErrInvalidMagic)
	}

	// 2. Read bloom filter
	bloomBuf := make([]byte, trailer.BloomLength)
	if _, err := f.ReadAt(bloomBuf, int64(trailer.BloomOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}
	bloom := NewBloomFilterFromBytes(bloomBuf, 7)

	// 3. Read sparse index
	indexBuf := make([]byte, trailer.IndexLength)
	if _, err := f.ReadAt(indexBuf, int64(trailer.IndexOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to read sparse index: %w", err)
	}

	if len(indexBuf) < 4 {
		f.Close()
		return nil, fmt.Errorf("corrupted index block: insufficient length")
	}

	entryCount := binary.BigEndian.Uint32(indexBuf[0:4])
	offset := 4
	entries := make([]IndexEntry, 0, entryCount)

	for i := uint32(0); i < entryCount; i++ {
		if offset+2 > len(indexBuf) {
			break
		}
		keyLen := int(binary.BigEndian.Uint16(indexBuf[offset : offset+2]))
		offset += 2

		if offset+keyLen+28 > len(indexBuf) {
			break
		}
		key := string(indexBuf[offset : offset+keyLen])
		offset += keyLen

		blockOffset := binary.BigEndian.Uint64(indexBuf[offset : offset+8])
		blockLen := binary.BigEndian.Uint32(indexBuf[offset+8 : offset+12])
		minStart := int64(binary.BigEndian.Uint64(indexBuf[offset+12 : offset+20]))
		maxEnd := int64(binary.BigEndian.Uint64(indexBuf[offset+20 : offset+28]))
		offset += 28

		entries = append(entries, IndexEntry{
			Key:        key,
			Offset:     blockOffset,
			Length:     blockLen,
			MinStartNs: minStart,
			MaxEndNs:   maxEnd,
		})
	}

	return &SSTableReader{
		file:         f,
		path:         filePath,
		trailer:      trailer,
		bloom:        bloom,
		indexEntries: entries,
	}, nil
}

// SearchPoint performs a key lookup using the bloom filter and sparse index.
func (r *SSTableReader) SearchPoint(compositeKey string) (*telemetryv1.StoredAggregate, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Fast negative check with bloom filter
	if !r.bloom.MayContain(compositeKey) {
		return nil, false, nil
	}

	if len(r.indexEntries) == 0 {
		return nil, false, nil
	}

	// 2. Binary search sparse index for the candidate block
	// Find greatest entry whose Key <= compositeKey
	idx := sort.Search(len(r.indexEntries), func(i int) bool {
		return r.indexEntries[i].Key > compositeKey
	}) - 1

	if idx < 0 {
		idx = 0
	}

	entry := r.indexEntries[idx]

	// 3. Read data block
	blockBuf := make([]byte, entry.Length)
	if _, err := r.file.ReadAt(blockBuf, int64(entry.Offset)); err != nil {
		return nil, false, fmt.Errorf("failed to read data block at offset %d: %w", entry.Offset, err)
	}

	// 4. Scan records in data block
	bufOffset := 0
	for bufOffset < len(blockBuf) {
		if bufOffset+4 > len(blockBuf) {
			break
		}
		recLen := int(binary.BigEndian.Uint32(blockBuf[bufOffset : bufOffset+4]))
		bufOffset += 4

		if bufOffset+recLen > len(blockBuf) {
			break
		}
		recPayload := blockBuf[bufOffset : bufOffset+recLen]
		bufOffset += recLen

		rec, err := UnmarshalAggregate(recPayload)
		if err != nil {
			continue
		}

		k := FormatCompositeKey(rec.DeviceId, rec.MetricName, rec.WindowStartNs, rec.WindowEndNs)
		if k == compositeKey {
			return rec, true, nil
		}
	}

	return nil, false, nil
}

// ScanRange reads records matching (deviceID, metricName) overlapping [startNs, endNs].
func (r *SSTableReader) ScanRange(deviceID, metricName string, startNs, endNs int64) ([]*telemetryv1.StoredAggregate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []*telemetryv1.StoredAggregate

	for _, entry := range r.indexEntries {
		// Prune blocks outside time window
		if entry.MinStartNs > endNs || entry.MaxEndNs < startNs {
			continue
		}

		blockBuf := make([]byte, entry.Length)
		if _, err := r.file.ReadAt(blockBuf, int64(entry.Offset)); err != nil {
			return nil, fmt.Errorf("failed to read data block: %w", err)
		}

		bufOffset := 0
		for bufOffset < len(blockBuf) {
			if bufOffset+4 > len(blockBuf) {
				break
			}
			recLen := int(binary.BigEndian.Uint32(blockBuf[bufOffset : bufOffset+4]))
			bufOffset += 4

			if bufOffset+recLen > len(blockBuf) {
				break
			}
			recPayload := blockBuf[bufOffset : bufOffset+recLen]
			bufOffset += recLen

			rec, err := UnmarshalAggregate(recPayload)
			if err != nil {
				continue
			}

			if rec.DeviceId == deviceID && rec.MetricName == metricName {
				if rec.WindowStartNs >= startNs && rec.WindowStartNs < endNs {
					results = append(results, rec)
				}
			}
		}
	}

	return results, nil
}

// ReadAllRecords reads every valid record sequentially (for merge compaction).
func (r *SSTableReader) ReadAllRecords() ([]*telemetryv1.StoredAggregate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var records []*telemetryv1.StoredAggregate

	for _, entry := range r.indexEntries {
		blockBuf := make([]byte, entry.Length)
		if _, err := r.file.ReadAt(blockBuf, int64(entry.Offset)); err != nil {
			return nil, fmt.Errorf("failed to read block: %w", err)
		}

		bufOffset := 0
		for bufOffset < len(blockBuf) {
			if bufOffset+4 > len(blockBuf) {
				break
			}
			recLen := int(binary.BigEndian.Uint32(blockBuf[bufOffset : bufOffset+4]))
			bufOffset += 4

			if bufOffset+recLen > len(blockBuf) {
				break
			}
			recPayload := blockBuf[bufOffset : bufOffset+recLen]
			bufOffset += recLen

			rec, err := UnmarshalAggregate(recPayload)
			if err != nil {
				continue
			}
			records = append(records, rec)
		}
	}

	return records, nil
}

// RecordCount returns total records declared in the SSTable trailer.
func (r *SSTableReader) RecordCount() uint32 {
	return r.trailer.RecordCount
}

// Path returns the filepath of this SSTable.
func (r *SSTableReader) Path() string {
	return r.path
}

// Close closes the underlying file handle.
func (r *SSTableReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
