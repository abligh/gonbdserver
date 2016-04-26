package nbd

import (
	"golang.org/x/net/context"
	"os"
)

// FileBackend implements Backend
type FileBackend struct {
	file *os.File
	size uint64
}

// WriteAt implements Backend.WriteAt
func (fb *FileBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int, error) {
	n, err := fb.file.WriteAt(b, offset)
	if err != nil || !fua {
		return n, err
	}
	err = fb.file.Sync()
	if err != nil {
		return 0, err
	}
	return n, err
}

// ReadAt implements Backend.ReadAt
func (fb *FileBackend) ReadAt(ctx context.Context, b []byte, offset int64) (int, error) {
	return fb.file.ReadAt(b, offset)
}

// TrimAt implements Backend.TrimAt
func (fb *FileBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
	return length, nil
}

// Flush implements Backend.Flush
func (fb *FileBackend) Flush(ctx context.Context) error {
	return nil
}

// Close implements Backend.Close
func (fb *FileBackend) Close(ctx context.Context) error {
	return fb.file.Close()
}

// Size implements Backend.Size
func (fb *FileBackend) Geometry(ctx context.Context) (uint64, uint64, uint64, uint64, error) {
	return fb.size, 1, 32 * 1024, 128 * 1024 * 1024, nil
}

// Size implements Backend.HasFua
func (fb *FileBackend) HasFua(ctx context.Context) bool {
	return true
}

// Size implements Backend.HasFua
func (fb *FileBackend) HasFlush(ctx context.Context) bool {
	return true
}

// Generate a new file backend
func NewFileBackend(ctx context.Context, ec *ExportConfig) (Backend, error) {
	perms := os.O_RDWR
	if ec.ReadOnly {
		perms = os.O_RDONLY
	}
	if s, err := isTrue(ec.DriverParameters["sync"]); err != nil {
		return nil, err
	} else if s {
		perms |= os.O_SYNC
	}
	file, err := os.OpenFile(ec.DriverParameters["path"], perms, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &FileBackend{
		file: file,
		size: uint64(stat.Size()),
	}, nil
}

// Register our backend
func init() {
	RegisterBackend("file", NewFileBackend)
}
