package engine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// WALManager manages the append-only Write-Ahead Log for a shard.
type WALManager struct {
	mu         sync.Mutex
	dir        string
	seq        uint64
	activeFile *os.File
	activePath string
	closed     bool
}

// OpenWALManager initializes or opens a WAL in the given shard directory.
func OpenWALManager(shardDir string) (*WALManager, error) {
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shard dir: %w", err)
	}

	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard dir: %w", err)
	}

	var maxSeq uint64 = 0
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			seqStr := strings.TrimPrefix(name, "wal-")
			seqStr = strings.TrimSuffix(seqStr, ".log")
			if s, err := strconv.ParseUint(seqStr, 10, 64); err == nil && s > maxSeq {
				maxSeq = s
			}
		}
	}

	if maxSeq == 0 {
		maxSeq = 1
	}

	activePath := filepath.Join(shardDir, fmt.Sprintf("wal-%06d.log", maxSeq))
	f, err := os.OpenFile(activePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open active WAL file %s: %w", activePath, err)
	}

	return &WALManager{
		dir:        shardDir,
		seq:        maxSeq,
		activeFile: f,
		activePath: activePath,
	}, nil
}

// AppendBatch writes a batch of aggregates atomically to the active WAL and calls Sync.
func (w *WALManager) AppendBatch(records []*telemetryv1.StoredAggregate) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal manager is closed")
	}

	if len(records) == 0 {
		return nil
	}

	var batchBuf []byte
	for _, record := range records {
		payload, err := MarshalAggregate(record)
		if err != nil {
			return fmt.Errorf("failed to marshal aggregate for WAL: %w", err)
		}
		frame := EncodeWALFrame(OpPut, payload)
		batchBuf = append(batchBuf, frame...)
	}

	if _, err := w.activeFile.Write(batchBuf); err != nil {
		return fmt.Errorf("failed to write WAL frame: %w", err)
	}

	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync WAL: %w", err)
	}

	return nil
}

// Sync forces buffered writes to physical disk.
func (w *WALManager) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal manager is closed")
	}
	return w.activeFile.Sync()
}

// Replay reads all WAL segments in sequence, invoking fn on every valid StoredAggregate record.
// If a file ends with truncated data, it discards the corrupted tail.
func (w *WALManager) Replay(fn func(record *telemetryv1.StoredAggregate) error) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, fmt.Errorf("failed to read shard dir during replay: %w", err)
	}

	var walFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "wal-") && (strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".log.checkpointed")) {
			walFiles = append(walFiles, name)
		}
	}

	sort.Strings(walFiles)

	totalReplayed := 0
	for _, fileName := range walFiles {
		filePath := filepath.Join(w.dir, fileName)
		f, err := os.Open(filePath)
		if err != nil {
			return totalReplayed, fmt.Errorf("failed to open WAL %s for replay: %w", fileName, err)
		}

		for {
			op, payload, err := DecodeWALFrame(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				// Corrupted frame or truncated tail at crash boundary: stop replaying this file
				break
			}

			if op == OpPut {
				record, err := UnmarshalAggregate(payload)
				if err != nil {
					// Corrupt payload: skip/stop
					break
				}
				if err := fn(record); err != nil {
					f.Close()
					return totalReplayed, fmt.Errorf("replay callback error: %w", err)
				}
				totalReplayed++
			}
		}
		f.Close()
	}

	return totalReplayed, nil
}

// Rotate seals the active WAL file and opens a new segment.
func (w *WALManager) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal manager is closed")
	}

	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL before rotate: %w", err)
	}
	if err := w.activeFile.Close(); err != nil {
		return fmt.Errorf("failed to close WAL before rotate: %w", err)
	}

	checkpointedPath := w.activePath + ".checkpointed"
	if err := os.Rename(w.activePath, checkpointedPath); err != nil {
		return fmt.Errorf("failed to rename WAL to checkpointed: %w", err)
	}

	w.seq++
	w.activePath = filepath.Join(w.dir, fmt.Sprintf("wal-%06d.log", w.seq))
	f, err := os.OpenFile(w.activePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new WAL segment %s: %w", w.activePath, err)
	}
	w.activeFile = f

	return nil
}

// PurgeCheckpointed deletes all `.checkpointed` WAL files in the shard.
func (w *WALManager) PurgeCheckpointed() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "wal-") && strings.HasSuffix(entry.Name(), ".log.checkpointed") {
			_ = os.Remove(filepath.Join(w.dir, entry.Name()))
		}
	}
	return nil
}

// Close flushes and closes the active WAL file.
func (w *WALManager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		return w.activeFile.Close()
	}
	return nil
}
