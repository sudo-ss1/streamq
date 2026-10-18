package storage

import "os"

// mmapFile is a read-write MAP_SHARED mapping of a whole file, shared by both index types.
type mmapFile struct {
	f    *os.File
	data []byte
}

// openMmapFile opens or creates path, extends it (sparse) to at least size bytes and maps
// size bytes. It returns the file's size before extension so callers can tell a trimmed file
// from a preallocated one.
func openMmapFile(path string, size int) (m *mmapFile, origSize int64, err error) {
	_, _ = path, size
	return nil, 0, errNotImplemented
}

// truncate sets the file size without changing the mapping.
func (m *mmapFile) truncate(size int64) error {
	_ = size
	return errNotImplemented
}

// sync fsyncs the file.
func (m *mmapFile) sync() error { return errNotImplemented }

// close unmaps and closes the file.
func (m *mmapFile) close() error { return errNotImplemented }
