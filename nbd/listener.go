package nbd

import (
	"golang.org/x/net/context"
	"log"
	"net"
	"sync"
	"time"
)

// A single li on a given TCP address
type Listener struct {
	logger   *log.Logger
	protocol string
	addr     string
	exports  []ExportConfig
}

// An listener type that does what we want
type DeadlineListener interface {
	SetDeadline(t time.Time) error
	net.Listener
}

func (l *Listener) Listen(parentCtx context.Context) {

	var wg sync.WaitGroup
	ctx, cancelFunc := context.WithCancel(parentCtx)

	addr := l.protocol + ":" + l.addr

	defer func() {
		cancelFunc()
		wg.Wait()
	}()

	nli, err := net.Listen(l.protocol, l.addr)
	if err != nil {
		l.logger.Printf("[ERROR] Could not listen on address %s", addr)
		return
	}

	defer func() {
		l.logger.Printf("[INFO] Stopping listening on %s", addr)
		nli.Close()
	}()

	li, ok := nli.(DeadlineListener)
	if !ok {
		l.logger.Printf("[ERROR] Invalid protocol to listen on %s", addr)
		return
	}

	l.logger.Printf("[INFO] Starting listening on %s", addr)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		li.SetDeadline(time.Now().Add(time.Second))
		if conn, err := li.Accept(); err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			l.logger.Printf("[ERROR] Error %s listening on %s", err, addr)
		} else {
			l.logger.Printf("[INFO] Connect to %s from %s", addr, conn.RemoteAddr())
			if connection, err := newConnection(l, l.logger, conn); err != nil {
				l.logger.Printf("[ERROR] Error %s establishing connection to %s from %s", err, addr, conn.RemoteAddr())
				conn.Close()
			} else {
				go func() {
					wg.Add(1)
					connection.Serve(ctx)
					wg.Done()
				}()
			}
		}
	}

}

func NewListener(logger *log.Logger, protocol string, addr string, exports []ExportConfig) (*Listener, error) {
	l := &Listener{
		logger:   logger,
		protocol: protocol,
		addr:     addr,
		exports:  exports,
	}
	return l, nil
}
