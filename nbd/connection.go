package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// Default number of workers
var DefaultWorkers = 5

// ConnectionParameters holds parameters for each inbound connection
type ConnectionParameters struct {
	ConnectionTimeout time.Duration // maximum time to complete negotiation
}

// Connection holds the details for each connection
type Connection struct {
	params     *ConnectionParameters // parameters
	conn       net.Conn              // the underlying connection
	logger     *log.Logger           // a logger
	listener   *Listener             // the listener than invoked us
	export     *Export               // a pointer to the export
	backend    Backend               // the backend implementation
	wg         sync.WaitGroup        // a waitgroup for the session; we mark this as done on exit
	rxCh       chan Request          // a channel of requests that have been received, and need to be dispatched to a worker
	txCh       chan Request          // a channel of outputs from the worker. By this time they have replies in that need to be transmitted
	name       string                // the name of the connection for logging purposes
	selectName string                // the selected export name

	killCh    chan struct{} // closed by workers to indicate a hard close is required
	killed    bool          // true if killCh closed already
	killMutex sync.Mutex    // protects killed
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

// BackendMap is a map between backends and the generator function for them
var BackendMap map[string]func(ctx context.Context, e *ExportConfig) (Backend, error) = make(map[string]func(ctx context.Context, e *ExportConfig) (Backend, error))

// Details of an export
type Export struct {
	size        uint64 // size in bytes
	exportFlags uint16 // export flags in NBD format
	name        string // name of the export
	readonly    bool   // true if read only
	workers     int    // number of workers
}

// Request is an internal structure for propagating requests through the channels
type Request struct {
	nbdReq  nbdRequest // the request in nbd format
	nbdRep  nbdReply   // the reply in nbd format
	length  int        // the checked length
	offset  int64      // the checked offset
	reqData []byte     // request data (e.g. for a write)
	repData []byte     // reply data (e.g. for a read)
	flags   uint64     // our internal flag structure characterizing the request
}

// newConection returns a new Connection object
func newConnection(listener *Listener, logger *log.Logger, conn net.Conn) (*Connection, error) {
	params := &ConnectionParameters{
		ConnectionTimeout: time.Second * 5,
	}
	c := &Connection{
		conn:     conn,
		listener: listener,
		logger:   logger,
		params:   params,
	}
	return c, nil
}

// NbdError translates an error returned by golang into an NBD error
//
// This function could do with some serious work!
func NbdError(err error) uint32 {
	return NBD_EIO
}

// isClosedErr returns true if the error related to use of a closed connection.
//
// this is particularly foul but is used to surpress errors that relate to use of a closed connection. This is because
// they only arise as we ourselves close the connection to get blocking reads/writes to safely terminate, and thus do
// not want to report them to the user as an error
func isClosedErr(err error) bool {
	return strings.HasSuffix(err.Error(), "use of closed network connection") // YUCK!
}

// Kill kills a connection. This safely ensures the kill channel is closed if it isn't already, which will
// kill all the goroutines
func (c *Connection) Kill(ctx context.Context) {
	c.killMutex.Lock()
	defer c.killMutex.Unlock()
	if !c.killed {
		close(c.killCh)
		c.killed = true
	}
}

// Receive is the goroutine that handles decoding conncetion data from the socket
func (c *Connection) Receive(ctx context.Context) {
	defer func() {
		c.logger.Printf("[INFO] Receiver exiting for %s", c.name)
		c.Kill(ctx)
		c.wg.Done()
	}()
	for {
		req := Request{
			repData: make([]byte, 0),
			reqData: make([]byte, 0),
		}
		if err := binary.Read(c.conn, binary.BigEndian, &req.nbdReq); err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Timeout() {
					c.logger.Printf("[INFO] Client %s timeout, closing connection", c.name)
					return
				}
			}
			if isClosedErr(err) {
				// Don't report this - we closed it
				return
			}
			c.logger.Printf("[ERROR] Client %s could not read request: %s", c.name, err)
			return
		}

		if req.nbdReq.NbdRequestMagic != NBD_REQUEST_MAGIC {
			c.logger.Printf("[ERROR] Client %s had bad magic number in request", c.name)
			return
		}

		req.nbdRep = nbdReply{
			NbdReplyMagic: NBD_REPLY_MAGIC,
			NbdHandle:     req.nbdReq.NbdHandle,
			NbdError:      0,
		}

		cmd := req.nbdReq.NbdCommandType
		var ok bool
		if req.flags, ok = CmdTypeMap[int(cmd)]; !ok {
			c.logger.Printf("[ERROR] Client %s unknown command %d", c.name, cmd)
			return
		}

		if req.flags&CMDT_CHECK_LENGTH_OFFSET != 0 {
			req.length = int(req.nbdReq.NbdLength)
			req.offset = int64(req.nbdReq.NbdOffset)
			if req.length <= 0 || req.offset < 0 || int64(req.length)+req.offset > int64(c.export.size) {
				c.logger.Printf("[ERROR] Client %s gave bad offset or length", c.name)
				return
			}
		}

		if req.flags&CMDT_REQ_PAYLOAD != 0 {
			req.reqData = make([]byte, req.length, req.length)
			if req.length <= 0 {
				c.logger.Printf("[ERROR] Client %s gave bad length", c.name)
				return
			}
			n, err := io.ReadFull(c.conn, req.reqData)
			if err != nil {
				if isClosedErr(err) {
					// Don't report this - we closed it
					return
				}

				c.logger.Printf("[ERROR] Client %s cannot read data to write: %s", c.name, err)
				return
			}

			if n != len(req.reqData) {
				c.logger.Printf("[ERROR] Client %s cannot read all data to write: %d != %d", c.name, n, len(req.reqData))
				return

			}
		} else if req.flags&CMDT_REQ_FAKE_PAYLOAD != 0 {
			req.reqData = make([]byte, req.length, req.length)
		}

		if req.flags&CMDT_REP_PAYLOAD != 0 {
			req.repData = make([]byte, req.length, req.length)
		}

		if req.flags&CMDT_CHECK_NOT_READ_ONLY != 0 && c.export.readonly {
			req.nbdRep.NbdError = NBD_EPERM
			select {
			case c.txCh <- req:
			case <-ctx.Done():
				return
			}
		} else {
			select {
			case c.rxCh <- req:
			case <-ctx.Done():
				return
			}
		}

	}
}

// checkpoint is an internal debugging routine
func checkpoint(t *time.Time) time.Duration {
	t1 := time.Now()
	d := t1.Sub(*t)
	*t = t1
	return d
}

// Dispatch is the goroutine used to process received items, passing the reply to the transmit goroutine
//
// one of these is run for each worker
func (c *Connection) Dispatch(ctx context.Context, n int) {
	defer func() {
		c.logger.Printf("[INFO] Dispatcher %d exiting for %s", n, c.name)
		c.Kill(ctx)
		c.wg.Done()
	}()
	//t := time.Now()
	for {
		//c.logger.Printf("[DEBUG] Client %s dispatcher %d waiting latency %s", c.name, n, checkpoint(&t))
		select {
		case <-ctx.Done():
			return
		case req, ok := <-c.rxCh:
			if !ok {
				return
			}
			//c.logger.Printf("[DEBUG] Client %s dispatcher %d command %d latency %s", c.name, n, req.nbdReq.NbdCommandType, checkpoint(&t))
			fua := req.nbdReq.NbdCommandFlags&NBD_CMD_FLAG_FUA != 0

			switch req.nbdReq.NbdCommandType {
			case NBD_CMD_READ:
				n, err := c.backend.ReadAt(ctx, req.repData, req.offset)
				if err != nil {
					c.logger.Printf("[WARN] Client %s got read I/O error: %s", c.name, err)
					req.nbdRep.NbdError = NbdError(err)
				} else if n != req.length {
					c.logger.Printf("[WARN] Client %s got incomplete read (%d != %d) at offset %d", c.name, n, req.length, req.offset)
					req.nbdRep.NbdError = NBD_EIO
				}
			case NBD_CMD_WRITE, NBD_CMD_WRITE_ZEROES:
				n, err := c.backend.WriteAt(ctx, req.reqData, req.offset, fua)
				if err != nil {
					c.logger.Printf("[WARN] Client %s got write I/O error: %s", c.name, err)
					req.nbdRep.NbdError = NbdError(err)
				} else if n != req.length {
					c.logger.Printf("[WARN] Client %s got incomplete write (%d != %d) at offset %d", c.name, n, req.length, req.offset)
					req.nbdRep.NbdError = NBD_EIO
				}
			case NBD_CMD_FLUSH:
				c.backend.Flush(ctx)
			case NBD_CMD_TRIM:
				n, err := c.backend.TrimAt(ctx, req.length, req.offset)
				if err != nil {
					c.logger.Printf("[WARN] Client %s got write I/O error: %s", c.name, err)
					req.nbdRep.NbdError = NbdError(err)
				} else if n != req.length {
					c.logger.Printf("[WARN] Client %s got incomplete write (%d != %d) at offset %d", c.name, n, req.length, req.offset)
					req.nbdRep.NbdError = NBD_EIO
				}
			case NBD_CMD_DISC:
				c.logger.Printf("[INFO] Client %s requested disconnect", c.name)
				return
			default:
				c.logger.Printf("[ERROR] Client %s sent unknown command %d", c.name, req.nbdReq.NbdCommandType)
				return
			}
			select {
			case c.txCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Transmit is the goroutine run to transmit the processed requests (now replies)
func (c *Connection) Transmit(ctx context.Context) {
	defer func() {
		c.logger.Printf("[INFO] Transmitter exiting for %s", c.name)
		c.Kill(ctx)
		c.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-c.txCh:
			if !ok {
				return
			}
			if err := binary.Write(c.conn, binary.BigEndian, req.nbdRep); err != nil {
				c.logger.Printf("[ERROR] Client %s cannot write reply", c.name)
				return
			}
			if req.flags&CMDT_REP_PAYLOAD != 0 && len(req.repData) > 0 {
				if n, err := c.conn.Write(req.repData); err != nil || n != len(req.repData) {
					c.logger.Printf("[ERROR] Client %s cannot write reply", c.name)
					return
				}
			}
		}
	}
}

// Serve negotiates, then starts all the goroutines for processing a connection, then waits for them to be ended
func (c *Connection) Serve(parentCtx context.Context) {
	ctx, cancelFunc := context.WithCancel(parentCtx)

	c.rxCh = make(chan Request, 1024)
	c.txCh = make(chan Request, 1024)
	c.killCh = make(chan struct{})

	c.name = c.conn.RemoteAddr().String()

	defer func() {
		if c.backend != nil {
			c.backend.Close(ctx)
		}
		c.conn.Close()
		cancelFunc()
		c.Kill(ctx) // to ensure the kill channel is closed
		c.wg.Wait()
		close(c.rxCh)
		close(c.txCh)
		c.logger.Printf("[INFO] Closed connection from %s", c.name)
	}()

	if err := c.Negotiate(ctx); err != nil {
		c.logger.Printf("[INFO] Negotiation failed with %s: %v", c.name, err)
		return
	}

	c.name = c.name + "/" + c.export.name

	workers := c.export.workers

	if workers < 1 {
		workers = DefaultWorkers
	}

	c.logger.Printf("[INFO] Negotiation succeeded with %s, serving with %d worker(s)", c.name, workers)

	c.wg.Add(2)
	go c.Receive(ctx)
	go c.Transmit(ctx)
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go c.Dispatch(ctx, i)
	}

	// Wait until either we are explicitly killed or one of our
	// workers dies
	select {
	case <-c.killCh:
		c.logger.Printf("[INFO] Worker forced close for %s", c.name)
	case <-ctx.Done():
		c.logger.Printf("[INFO] Parent forced close for %s", c.name)
	}
}

// Negotiate negotiates a connection
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
		case NBD_OPT_EXPORT_NAME, NBD_OPT_SELECT, NBD_OPT_GO:
			name := make([]byte, opt.NbdOptLen)
			n, err := io.ReadFull(c.conn, name)
			if err != nil {
				return err
			}
			if uint32(n) != opt.NbdOptLen {
				return errors.New("Incomplete name")
			}

			if opt.NbdOptId == NBD_OPT_SELECT {
				c.selectName = string(name)
			} else if opt.NbdOptId == NBD_OPT_GO && len(name) == 0 {
				name = []byte(c.selectName)
			}

			if len(name) == 0 {
				name = []byte(c.listener.defaultExport)
			}

			export, err := c.getExport(ctx, string(name))
			if err != nil {
				return err // TODO: return NBD_REP_ERR_UNSUP instead?
			}
			// for the reply
			name = []byte(export.name)

			if opt.NbdOptId != NBD_OPT_EXPORT_NAME {
				nameLength := uint32(len(name))
				if err := binary.Write(c.conn, binary.BigEndian, nameLength); err != nil {
					return errors.New("Cannot write name length")
				}
				if err := binary.Write(c.conn, binary.BigEndian, name); err != nil {
					return errors.New("Cannot write name")
				}
			}

			// this option has a unique reply format
			ed := nbdExportDetails{
				NbdExportSize:  export.size,
				NbdExportFlags: export.exportFlags,
			}
			if err := binary.Write(c.conn, binary.BigEndian, ed); err != nil {
				return errors.New("Cannot write export details")
			}

			if clf.NbdClientFlags&NBD_FLAG_C_NO_ZEROES == 0 && opt.NbdOptId == NBD_OPT_EXPORT_NAME {
				// send 124 bytes of zeroes.
				zeroes := make([]byte, 124, 124)
				if err := binary.Write(c.conn, binary.BigEndian, zeroes); err != nil {
					return errors.New("Cannot write zeroes")
				}
			}

			if opt.NbdOptId != NBD_OPT_SELECT {
				c.export = export
				done = true
			}
		case NBD_OPT_ABORT:
			return errors.New("Connection aborted by client")
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

// getExport generates an export for a given name, choosing the specified backend
func (c *Connection) getExport(ctx context.Context, name string) (*Export, error) {
	for _, e := range c.listener.exports {
		if e.Name == name {
			if backendgen, ok := BackendMap[e.Driver]; !ok {
				return nil, fmt.Errorf("No such driver %s", e.Driver)
			} else {
				if backend, err := backendgen(ctx, &e); err != nil {
					return nil, err
				} else {
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
						exportFlags: NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA | NBD_FLAG_SEND_WRITE_ZEROES,
						name:        name,
						readonly:    e.ReadOnly,
						workers:     e.Workers,
					}, nil

				}
			}
		}
	}
	return nil, errors.New("No such export")
}

func RegisterBackend(name string, generator func(ctx context.Context, e *ExportConfig) (Backend, error)) {
	BackendMap[name] = generator
}

func GetBackendNames() []string {
	b := make([]string, len(BackendMap))
	i := 0
	for k, _ := range BackendMap {
		b[i] = k
		i++
	}
	sort.Strings(b)
	return b
}
