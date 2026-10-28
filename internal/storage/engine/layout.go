package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ShardDir returns the absolute directory path for a specific shard index.
func ShardDir(baseDir string, shardID int) string {
	return filepath.Join(baseDir, fmt.Sprintf("shard-%02d", shardID))
}

// WALPath returns the path to a specific WAL sequence file within a shard directory.
func WALPath(shardDir string, seq uint64) string {
	return filepath.Join(shardDir, fmt.Sprintf("wal-%06d.log", seq))
}

// SSTablePath returns the path to a specific SSTable segment within a shard directory.
func SSTablePath(shardDir string, seq uint64) string {
	return filepath.Join(shardDir, fmt.Sprintf("segment-%06d.sst", seq))
}

// CatalogPath returns the path to the persistent device catalog index file.
func CatalogPath(baseDir string) string {
	return filepath.Join(baseDir, "catalog.bin")
}

// LockPath returns the path to the advisory lockfile.
func LockPath(baseDir string) string {
	return filepath.Join(baseDir, "LOCK")
}

// EnsureLayout creates the root storage directory and all shard subdirectories if they do not already exist.
func EnsureLayout(baseDir string, numShards int) error {
	if baseDir == "" {
		return fmt.Errorf("storage base directory cannot be empty")
	}

	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return fmt.Errorf("failed to create base storage directory %s: %w", baseDir, err)
	}

	for i := 0; i < numShards; i++ {
		shardPath := ShardDir(baseDir, i)
		if err := os.MkdirAll(shardPath, 0750); err != nil {
			return fmt.Errorf("failed to create shard directory %s: %w", shardPath, err)
		}
	}

	return nil
}

// CheckDiskAvailable returns the free and total bytes on the filesystem hosting path,
// and checks if free capacity is below the minimum required fraction (e.g. 0.05 for 5%).
func CheckDiskAvailable(path string, minFreePercent float64) (uint64, uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}

	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)

	if total > 0 {
		freeRatio := float64(free) / float64(total)
		if freeRatio < minFreePercent {
			return free, total, ErrDiskFull
		}
	}

	return free, total, nil
}
