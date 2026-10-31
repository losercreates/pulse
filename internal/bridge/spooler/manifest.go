package spooler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SpoolManifest tracks read/write progress across disk spool segment files.
type SpoolManifest struct {
	Version          int    `json:"version"`
	CurrentWriteSeq  uint64 `json:"current_write_seq"`
	CurrentReadSeq   uint64 `json:"current_read_seq"`
	ReadByteOffset   int64  `json:"read_byte_offset"`
	TotalBufferedOps uint64 `json:"total_buffered_ops"`
	TotalDiskBytes   int64  `json:"total_disk_bytes"`
}

// loadManifest reads the manifest from disk or initializes a default if absent.
func loadManifest(dir string) (*SpoolManifest, error) {
	manifestPath := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			m := &SpoolManifest{
				Version:          1,
				CurrentWriteSeq:  1,
				CurrentReadSeq:   1,
				ReadByteOffset:   0,
				TotalBufferedOps: 0,
				TotalDiskBytes:   0,
			}
			return m, saveManifest(dir, m)
		}
		return nil, fmt.Errorf("failed to read manifest %s: %w", manifestPath, err)
	}

	var m SpoolManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest %s: %w", manifestPath, err)
	}
	return &m, nil
}

// saveManifest writes the manifest atomically via a temporary file.
func saveManifest(dir string, m *SpoolManifest) error {
	manifestPath := filepath.Join(dir, "manifest.json")
	tmpPath := filepath.Join(dir, "manifest.json.tmp")

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write tmp manifest: %w", err)
	}

	if err := os.Rename(tmpPath, manifestPath); err != nil {
		return fmt.Errorf("failed to commit manifest rename: %w", err)
	}

	return nil
}

// segmentFileName returns the formatted segment filename for a given sequence number.
func segmentFileName(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("spool_%06d.log", seq))
}

