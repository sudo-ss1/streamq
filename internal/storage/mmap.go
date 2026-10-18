package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// mmapFile is a read-write MAP_SHARED mapping of a whole file, shared by both index types.
type mmapFile struct {
	f    *os.File
	data []byte
}

// openMmapFile opens or creates path, extends it (sparse) to at least size bytes and maps
// size bytes. It returns the file's size before extension so callers can tell a trimmed file
// from a preallocated one.
func openMmapFile(path string, size int) (m *mmapFile, origSize int64, err error) {
	if size <= 0 {
		return nil, 0, fmt.Errorf("storage: mmap %s: invalid size %d", path, size)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return nil, 0, fmt.Errorf("storage: open %s: %w", path, err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("storage: stat %s: %w", path, err)
	}
	origSize = fi.Size()
	if origSize < int64(size) {
		if err = f.Truncate(int64(size)); err != nil {
			return nil, 0, fmt.Errorf("storage: extend %s: %w", path, err)
		}
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, 0, fmt.Errorf("storage: mmap %s: %w", path, err)
	}
	return &mmapFile{f: f, data: data}, origSize, nil
}

// truncate sets the file size without changing the mapping.
func (m *mmapFile) truncate(size int64) error {
	if err := m.f.Truncate(size); err != nil {
		return fmt.Errorf("storage: truncate %s: %w", m.f.Name(), err)
	}
	return nil
}

// sync fsyncs the file.
func (m *mmapFile) sync() error {
	if err := m.f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync %s: %w", m.f.Name(), err)
	}
	return nil
}

// close unmaps and closes the file.
func (m *mmapFile) close() error {
	var errs []error
	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil {
			errs = append(errs, fmt.Errorf("storage: munmap %s: %w", m.f.Name(), err))
		}
		m.data = nil
	}
	if err := m.f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("storage: close %s: %w", m.f.Name(), err))
	}
	return errors.Join(errs...)
}
