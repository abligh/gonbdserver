// +build linux,!noceph

// The above build tag specifies this file is only to be built on linux (because
// building librados is difficult elsewhere). If you want it to build elsewhere,
// add your OS as appropriately. If you are having difficulty building on linux
// or want to build without librados present, then use
//    go build -tags 'noceph'

package nbd

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"golang.org/x/net/context"
)

// RbdBackend implements Backend
type RbdBackend struct {
	conn  *rados.Conn
	ioctx *rados.IOContext
	image *rbd.Image
	size  uint64
}

// WriteAt implements Backend.WriteAt
func (rb *RbdBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int, error) {
	n, err := rb.image.WriteAt(b, offset)
	if err != nil || !fua {
		return n, err
	}
	err = rb.image.Flush()
	if err != nil {
		return 0, err
	}
	return n, err
}

// ReadAt implements Backend.ReadAt
func (rb *RbdBackend) ReadAt(ctx context.Context, b []byte, offset int64) (int, error) {
	return rb.image.ReadAt(b, offset)
}

// TrimAt implements Backend.TrimAt
func (rb *RbdBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
	return length, nil
}

// Flush implements Backend.Flush
func (rb *RbdBackend) Flush(ctx context.Context) error {
	return nil
}

// Close implements Backend.Close
func (rb *RbdBackend) Close(ctx context.Context) error {
	rb.image.Close()
	rb.ioctx.Destroy()
	rb.conn.Shutdown()
	return nil
}

// Size implements Backend.Size
func (rb *RbdBackend) Geometry(ctx context.Context) (uint64, uint64, uint64, uint64, error) {
	return rb.size, 4096, 4096, 32 * 1024 * 1024, nil
}

// Generate a new file backend
func NewRbdBackend(ctx context.Context, ec *ExportConfig) (Backend, error) {
	cluster := ec.DriverParameters["cluster"]
	if cluster == "" {
		cluster = "ceph"
	}
	user := ec.DriverParameters["user"]
	if user == "" {
		user = "client.admin"
	}
	pool := ec.DriverParameters["pool"]
	if pool == "" {
		pool = "rbd"
	}
	imageName := ec.DriverParameters["image"]
	conn, err := rados.NewConnWithClusterAndUser(cluster, user)
	if err != nil {
		return nil, fmt.Errorf("rbd connection: %s", err)
	}
	if err = conn.ReadDefaultConfigFile(); err != nil {
		return nil, fmt.Errorf("rbd configuration: %s", err)
	}
	if err = conn.Connect(); err != nil {
		return nil, fmt.Errorf("rbd connect: %s", err)
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		conn.Shutdown()
		return nil, fmt.Errorf("rbd OpenIOContext: %s", err)
	}
	image := rbd.GetImage(ioctx, imageName)
	if image == nil {
		ioctx.Destroy()
		conn.Shutdown()
		return nil, fmt.Errorf("Cannot get RBD image %s", imageName)
	}
	if err = image.Open(ec.ReadOnly); err != nil {
		ioctx.Destroy()
		conn.Shutdown()
		return nil, fmt.Errorf("Cannot open RBD image %s", imageName)
	}
	size, err := image.GetSize()
	if err != nil {
		image.Close()
		ioctx.Destroy()
		conn.Shutdown()
		return nil, fmt.Errorf("rbd cannot get size: %s", err)
	}

	return &RbdBackend{
		conn:  conn,
		ioctx: ioctx,
		image: image,
		size:  size,
	}, nil
}

// Register our backend
func init() {
	RegisterBackend("rbd", NewRbdBackend)
}
