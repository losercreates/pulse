package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

const (
	// SSTableMagic identifies valid SSTable files ("SSTB").
	SSTableMagic uint32 = 0x53535442

	// SSTableTrailerSize is the fixed size of the trailer in bytes.
	SSTableTrailerSize = 32

	// DefaultBlockRecordCount is the default number of records per data block.
	DefaultBlockRecordCount = 64
)

// IndexEntry represents a sparse index entry pointing to a data block.
type IndexEntry struct {
	Key        string
	Offset     uint64
	Length     uint32
	MinStartNs int64
	MaxEndNs   int64
}

// SSTableTrailer represents the 32-byte fixed trailer block.
type SSTableTrailer struct {
	IndexOffset uint64
	IndexLength uint32
	BloomOffset uint64
	BloomLength uint32
	RecordCount uint32
	Magic       uint32
}

// WriteSSTable serializes records into an immutable SSTable file at outputPath.
func WriteSSTable(outputPath string, records []*telemetryv1.StoredAggregate) error {
	if len(records) == 0 {
		return fmt.Errorf("cannot write empty SSTable")
	}

	// 1. Sort records by composite key
	sort.Slice(records, func(i, j int) bool {
		ki := FormatCompositeKey(records[i].DeviceId, records[i].MetricName, records[i].WindowStartNs, records[i].WindowEndNs)
		kj := FormatCompositeKey(records[j].DeviceId, records[j].MetricName, records[j].WindowStartNs, records[j].WindowEndNs)
		return ki < kj
	})

	// Deduplicate consecutive identical keys, retaining the latest
	deduped := make([]*telemetryv1.StoredAggregate, 0, len(records))
	for i := 0; i < len(records); i++ {
		ki := FormatCompositeKey(records[i].DeviceId, records[i].MetricName, records[i].WindowStartNs, records[i].WindowEndNs)
		if i+1 < len(records) {
			nextKey := FormatCompositeKey(records[i+1].DeviceId, records[i+1].MetricName, records[i+1].WindowStartNs, records[i+1].WindowEndNs)
			if ki == nextKey {
				continue // skip obsolete record
			}
		}
		deduped = append(deduped, records[i])
	}
	records = deduped

	// 2. Open temporary write file
	tmpPath := outputPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create sstable tmp file %s: %w", tmpPath, err)
	}
	defer func() {
		f.Close()
		_ = os.Remove(tmpPath)
	}()

	// 3. Build bloom filter
	bloom := NewBloomFilter(len(records))
	for _, rec := range records {
		key := FormatCompositeKey(rec.DeviceId, rec.MetricName, rec.WindowStartNs, rec.WindowEndNs)
		bloom.Add(key)
	}

	// 4. Write data blocks & build sparse index
	var indexEntries []IndexEntry
	var currentOffset uint64 = 0

	for i := 0; i < len(records); i += DefaultBlockRecordCount {
		end := i + DefaultBlockRecordCount
		if end > len(records) {
			end = len(records)
		}
		blockRecords := records[i:end]

		firstKey := FormatCompositeKey(blockRecords[0].DeviceId, blockRecords[0].MetricName, blockRecords[0].WindowStartNs, blockRecords[0].WindowEndNs)
		minStart := blockRecords[0].WindowStartNs
		maxEnd := blockRecords[0].WindowEndNs

		var blockBuf bytes.Buffer
		for _, rec := range blockRecords {
			if rec.WindowStartNs < minStart {
				minStart = rec.WindowStartNs
			}
			if rec.WindowEndNs > maxEnd {
				maxEnd = rec.WindowEndNs
			}
			payload, err := MarshalAggregate(rec)
			if err != nil {
				return fmt.Errorf("failed to marshal aggregate: %w", err)
			}
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
			blockBuf.Write(lenBuf[:])
			blockBuf.Write(payload)
		}

		blockBytes := blockBuf.Bytes()
		n, err := f.Write(blockBytes)
		if err != nil {
			return fmt.Errorf("failed to write data block: %w", err)
		}

		indexEntries = append(indexEntries, IndexEntry{
			Key:        firstKey,
			Offset:     currentOffset,
			Length:     uint32(n),
			MinStartNs: minStart,
			MaxEndNs:   maxEnd,
		})
		currentOffset += uint64(n)
	}

	// 5. Write sparse index block
	indexOffset := currentOffset
	var indexBuf bytes.Buffer

	// Number of index entries (uint32)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(indexEntries)))
	indexBuf.Write(countBuf[:])

	for _, entry := range indexEntries {
		keyBytes := []byte(entry.Key)
		var entryHdr [2 + 8 + 4 + 8 + 8]byte
		binary.BigEndian.PutUint16(entryHdr[0:2], uint16(len(keyBytes)))
		binary.BigEndian.PutUint64(entryHdr[2:10], entry.Offset)
		binary.BigEndian.PutUint32(entryHdr[10:14], entry.Length)
		binary.BigEndian.PutUint64(entryHdr[14:22], uint64(entry.MinStartNs))
		binary.BigEndian.PutUint64(entryHdr[22:30], uint64(entry.MaxEndNs))

		indexBuf.Write(entryHdr[:2])
		indexBuf.Write(keyBytes)
		indexBuf.Write(entryHdr[2:])
	}

	indexBytes := indexBuf.Bytes()
	nIdx, err := f.Write(indexBytes)
	if err != nil {
		return fmt.Errorf("failed to write index block: %w", err)
	}
	indexLength := uint32(nIdx)
	currentOffset += uint64(nIdx)

	// 6. Write bloom filter block
	bloomOffset := currentOffset
	bloomBytes := bloom.Bytes()
	nBloom, err := f.Write(bloomBytes)
	if err != nil {
		return fmt.Errorf("failed to write bloom block: %w", err)
	}
	bloomLength := uint32(nBloom)
	currentOffset += uint64(nBloom)

	// 7. Write 32-byte fixed trailer
	trailer := make([]byte, SSTableTrailerSize)
	binary.BigEndian.PutUint64(trailer[0:8], indexOffset)
	binary.BigEndian.PutUint32(trailer[8:12], indexLength)
	binary.BigEndian.PutUint64(trailer[12:20], bloomOffset)
	binary.BigEndian.PutUint32(trailer[20:24], bloomLength)
	binary.BigEndian.PutUint32(trailer[24:28], uint32(len(records)))
	binary.BigEndian.PutUint32(trailer[28:32], SSTableMagic)

	if _, err := f.Write(trailer); err != nil {
		return fmt.Errorf("failed to write trailer: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync sstable: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close sstable file: %w", err)
	}

	// 8. Atomic rename into final output path
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return fmt.Errorf("failed to rename sstable to final path: %w", err)
	}

	return nil
}
