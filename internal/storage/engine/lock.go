package engine

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// DirLock manages an advisory exclusive lock on the storage base directory.
type DirLock struct {
	file *os.File
	path string
}

// AcquireDirLock attempts to acquire an exclusive non-blocking advisory file lock on baseDir/LOCK.
// Returns ErrShardLocked if another process holds the lock.
func AcquireDirLock(baseDir string) (*DirLock, error) {
	lockFile := LockPath(baseDir)
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lockfile %s: %w", lockFile, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w (%s)", ErrShardLocked, err)
	}

	// Write PID and acquisition timestamp for debugging
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	info := fmt.Sprintf("pid=%d acquired=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	_, _ = f.WriteString(info)
	_ = f.Sync()

	return &DirLock{
		file: f,
		path: lockFile,
	}, nil
}

// Release releases the advisory file lock and closes the file handle.
func (l *DirLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	return err
}
