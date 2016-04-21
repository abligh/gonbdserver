// +build linux,!noaio

// The above build tag specifies this file is only to be built on linux (because
// goaio only supports linux). If you are having difficulty building on linux
// or want to build without librados present, then use
//    go build -tags 'noaio'

package nbd

import (
	"errors"
	"github.com/traetox/goaio"
	"golang.org/x/net/context"
	"os"
)

// AioFileBackend implements Backend
type AioFileBackend struct {
	aio  *goaio.AIO
	size uint64
}

// WriteAt implements Backend.WriteAt
func (afb *AioFileBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int, error) {
	if err := afb.aio.Wait(); err != nil {
		return 0, err
	}
	requestId, err := afb.aio.WriteAt(b, offset)
	if err != nil {
		return 0, err
	}
	afb.aio.WaitFor(requestId)
	return len(b), err
}

// ReadAt implements Backend.ReadAt
func (afb *AioFileBackend) ReadAt(ctx context.Context, b []byte, offset int64) (int, error) {
	if err := afb.aio.Wait(); err != nil {
		return 0, err
	}
	requestId, err := afb.aio.ReadAt(b, offset)
	if err != nil {
		return 0, err
	}
	afb.aio.WaitFor(requestId)
	return len(b), err
}

// TrimAt implements Backend.TrimAt
func (afb *AioFileBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
	return length, nil
}

// Flush implements Backend.Flush
func (afb *AioFileBackend) Flush(ctx context.Context) error {
	return errors.New("Flush not supported")
}

// Close implements Backend.Close
func (afb *AioFileBackend) Close(ctx context.Context) error {
	return afb.aio.Close()
}

// Size implements Backend.Size
func (afb *AioFileBackend) Geometry(ctx context.Context) (uint64, uint64, uint64, uint64, error) {
	return afb.size, 1, 4096, 128 * 1024 * 1024, nil
}

// Size implements Backend.HasFua
func (afb *AioFileBackend) HasFua(ctx context.Context) bool {
	return false
}

// Size implements Backend.HasFua
func (afb *AioFileBackend) HasFlush(ctx context.Context) bool {
	return false
}

// Generate a new aio backend
func NewAioFileBackend(ctx context.Context, ec *ExportConfig) (Backend, error) {
	perms := os.O_RDWR
	if ec.ReadOnly {
		perms = os.O_RDONLY
	}
	if s, err := isSet(ec.DriverParameters["sync"]); err != nil {
		return nil, err
	} else if s {
		perms |= os.O_SYNC
	}
	aio, err := goaio.NewAIO(ec.DriverParameters["path"], perms, 0666)
	if err != nil {
		return nil, err
	}
	// sadly aio doesn't implement stat. Use os.Stat even though it's racy
	stat, err := os.Stat(ec.DriverParameters["path"])
	if err != nil {
		aio.Close()
		return nil, err
	}
	return &AioFileBackend{
		aio:  aio,
		size: uint64(stat.Size()),
	}, nil
}

// Register our backend
func init() {
	RegisterBackend("aiofile", NewAioFileBackend)
}
