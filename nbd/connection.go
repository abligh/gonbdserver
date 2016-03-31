package nbd

import (
	"encoding/binary"
	"errors"
	"golang.org/x/net/context"
	"io"
	"log"
	"net"
	"time"
)

type ConnectionParameters struct {
	ConnectionTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
}

type Connection struct {
	params   *ConnectionParameters
	conn     net.Conn
	logger   *log.Logger
	listener *Listener
	export   *Export
	backend  Backend
}

// Backend is an interface implemented by the various backend drivers
type Backend interface {
	WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int, error) // write data b at offset, with force unit access optional
	ReadAt(ctx context.Context, b []byte, offset int64) (int, error)            // read to b at offset
	TrimAt(ctx context.Context, length int, offset int64) (int, error)          // trim
	Flush(ctx context.Context) error                                            // flush
	Close(ctx context.Context) error                                            // close
	Size(ctx context.Context) (uint64, error)                                   // size
}

type Export struct {
	size        uint64
	exportFlags uint16
	name        string
}

func newConnection(listener *Listener, logger *log.Logger, conn net.Conn) (*Connection, error) {
	params := &ConnectionParameters{
		ConnectionTimeout: time.Second * 5,
		ReadTimeout:       time.Second * 3600,
		WriteTimeout:      time.Second * 30,
	}
	c := &Connection{
		conn:     conn,
		listener: listener,
		logger:   logger,
		params:   params,
	}
	return c, nil
}

func (c *Connection) Serve(parentCtx context.Context) {
	ctx, cancelFunc := context.WithCancel(parentCtx)

	defer func() {
		if c.backend != nil {
			c.backend.Close(ctx)
		}
		c.conn.Close()
		cancelFunc()
		c.logger.Printf("[INFO] Closing connection from %s", c.conn.RemoteAddr())
	}()

	if err := c.Negotiate(ctx); err != nil {
		c.logger.Printf("[INFO] Negotiation failed with %s: %v", c.conn.RemoteAddr(), err)
		return
	}

	c.logger.Printf("[INFO] Negotiation succeeded with %s for export %s", c.conn.RemoteAddr(), c.export.name)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		//		c.conn.SetReadDeadline(time.Now().Add(5*time.Second/*c.params.ReadTimeout*/))
		//		c.conn.SetWriteDeadline(time.Now().Add(5*time.Second/*c.params.ReadTimeout*/))

		var req nbdRequest
		if err := binary.Read(c.conn, binary.BigEndian, &req); err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Timeout() {
					c.logger.Printf("[INFO] Client %s timeout, closing connection", c.conn.RemoteAddr())
					return
				}
			}
			c.logger.Printf("[ERROR] Cient %s could not read request: %s", c.conn.RemoteAddr(), err)
			return
		}

		if req.NbdRequestMagic != NBD_REQUEST_MAGIC {
			c.logger.Printf("[ERROR] Client %s had bad magic number in request", c.conn.RemoteAddr())
			return
		}

		rep := nbdReply{
			NbdReplyMagic: NBD_REPLY_MAGIC,
			NbdHandle:     req.NbdHandle,
			NbdError:      0,
		}
		repdata := make([]byte, 0)

		length := int(req.NbdLength)
		offset := int64(req.NbdOffset)

		// c.logger.Printf("[DEBUG] Client %s command %d", c.conn.RemoteAddr(), req.NbdCommandType)

		switch req.NbdCommandType {
		case NBD_CMD_READ:
			if length <= 0 || offset < 0 {
				c.logger.Printf("[ERROR] Client %s gave bad length or offest", c.conn.RemoteAddr())
				return
			}
			// c.logger.Printf("[DEBUG] Client %s NBD_CMD_READ offset=%d length=%d", c.conn.RemoteAddr(), offset, length)
			repdata = make([]byte, length, length)
			n, err := c.backend.ReadAt(ctx, repdata, offset)
			if err != nil {
				c.logger.Printf("[WARN] Client %s got read I/O error: %s", c.conn.RemoteAddr(), err)
				rep.NbdError = NBD_EIO //TODO: work out proper error mapping
			} else if n != length {
				c.logger.Printf("[WARN] Client %s got incomplete read (%d != %d) at offset %d", n, length, offset)
				rep.NbdError = NBD_EIO
			}
		case NBD_CMD_WRITE:
			if length <= 0 || offset < 0 {
				c.logger.Printf("[ERROR] Client %s gave bad length or offest", c.conn.RemoteAddr())
				return
			}
			// c.logger.Printf("[DEBUG] Client %s NBD_CMD_WRITE offset=%d length=%d", c.conn.RemoteAddr(), offset, length)
			data := make([]byte, length, length)
			n, err := io.ReadFull(c.conn, data)
			if err != nil {
				c.logger.Printf("[ERROR] Client %s cannot read data to write: %s", c.conn.RemoteAddr(), err)
				return
			}

			if n != len(data) {
				c.logger.Printf("[ERROR] Client %s cannot read all data to write: %d != %d", c.conn.RemoteAddr(), n, len(data))
				return
			}

			fua := req.NbdCommandFlags&NBD_CMD_FLAG_FUA != 0
			n, err = c.backend.WriteAt(ctx, data, offset, fua)
			if err != nil {
				c.logger.Printf("[WARN] Client %s got write I/O error: %s", c.conn.RemoteAddr(), err)
				rep.NbdError = NBD_EIO //TODO: work out proper error mapping
			} else if n != length {
				c.logger.Printf("[WARN] Client %s got incomplete write (%d != %d) at offset %d", n, length, offset)
				rep.NbdError = NBD_EIO
			}
		case NBD_CMD_FLUSH:
			c.backend.Flush(ctx)
		case NBD_CMD_TRIM:
			n, err := c.backend.TrimAt(ctx, length, offset)
			if err != nil {
				c.logger.Printf("[WARN] Client %s got write I/O error: %s", c.conn.RemoteAddr(), err)
				rep.NbdError = NBD_EIO //TODO: work out proper error mapping
			} else if n != length {
				c.logger.Printf("[WARN] Client %s got incomplete write (%d != %d) at offset %d", n, length, offset)
				rep.NbdError = NBD_EIO
			}
		case NBD_CMD_DISC:
			c.logger.Printf("[INFO] Client %s requested disconnect", c.conn.RemoteAddr())
			return
		default:
			c.logger.Printf("[ERROR] Client %s sent unknown command %d", c.conn.RemoteAddr(), req.NbdCommandType)
			return
		}

		if err := binary.Write(c.conn, binary.BigEndian, rep); err != nil {
			c.logger.Printf("[ERROR] Client %s cannot write reply", c.conn.RemoteAddr())
		}
		if len(repdata) > 0 {
			if n, err := c.conn.Write(repdata); err != nil || n != len(repdata) {
				c.logger.Printf("[ERROR] Client %s cannot write reply", c.conn.RemoteAddr())
			}
		}
	}
}

func (c *Connection) Negotiate(ctx context.Context) error {
	c.conn.SetDeadline(time.Now().Add(c.params.ConnectionTimeout))

	// We send a newstyle header
	nsh := nbdNewStyleHeader{
		NbdMagic:       NBD_MAGIC,
		NbdOptsMagic:   NBD_OPTS_MAGIC,
		NbdGlobalFlags: NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES,
	}

	if err := binary.Write(c.conn, binary.BigEndian, nsh); err != nil {
		return errors.New("Cannot write magic header")
	}

	// next they send client flags
	var clf nbdClientFlags

	if err := binary.Read(c.conn, binary.BigEndian, &clf); err != nil {
		return errors.New("Cannot read client flags")
	}

	done := false
	// now we get options
	for !done {
		var opt nbdClientOpt
		if err := binary.Read(c.conn, binary.BigEndian, &opt); err != nil {
			return errors.New("Cannot read option")
		}
		if opt.NbdOptMagic != NBD_OPTS_MAGIC {
			return errors.New("Bad option magic")
		}
		switch opt.NbdOptId {
		case NBD_OPT_EXPORT_NAME:
			name := make([]byte, opt.NbdOptLen)
			n, err := io.ReadFull(c.conn, name)
			if err != nil {
				return err
			}
			if uint32(n) != opt.NbdOptLen {
				return errors.New("Incomplete name")
			}
			export, err := c.getExport(ctx, string(name))
			if err != nil {
				return err
			}

			// this option has a unique reply format
			ed := nbdExportDetails{
				NbdExportSize:  export.size,
				NbdExportFlags: export.exportFlags,
			}
			if err := binary.Write(c.conn, binary.BigEndian, ed); err != nil {
				return errors.New("Cannot write export details")
			}
			if clf.NbdClientFlags&NBD_FLAG_C_NO_ZEROES == 0 {
				// send 124 bytes of zeroes.
				zeroes := make([]byte, 124, 124)
				if err := binary.Write(c.conn, binary.BigEndian, zeroes); err != nil {
					return errors.New("Cannot write zeroes")
				}
			}

			c.export = export
			done = true
		default:
			// eat the option
			discard := make([]byte, opt.NbdOptLen, opt.NbdOptLen)
			n, err := io.ReadFull(c.conn, discard)
			if err != nil {
				return err
			}
			if uint32(n) != opt.NbdOptLen {
				return errors.New("Could not discard option")
			}
			// say it's unsuppported
			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptId:          opt.NbdOptId,
				NbdOptReplyType:   NBD_REP_ERR_UNSUP,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot reply to unsupported option")
			}
		}
	}

	c.conn.SetDeadline(time.Time{})
	return nil
}

func (c *Connection) getExport(ctx context.Context, name string) (*Export, error) {
	backend, err := NewFileBackend(ctx, name)
	if err != nil {
		return nil, err
	}
	size, err := backend.Size(ctx)
	if err != nil {
		backend.Close(ctx)
		return nil, err
	}
	if c.backend != nil {
		c.backend.Close(ctx)
	}
	c.backend = backend
	return &Export{
		size:        size,
		exportFlags: NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA,
		name:        name,
	}, nil
}
