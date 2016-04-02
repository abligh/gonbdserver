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

func (l *Listener) Listen(parentCtx context.Context, sessionParentCtx context.Context, sessionWaitGroup *sync.WaitGroup) {

	addr := l.protocol + ":" + l.addr

	ctx, cancelFunc := context.WithCancel(parentCtx)

	// I know this isn't a session, but this ensures all listeners have terminated when we terminate the
	// whole thing
	sessionWaitGroup.Add(1)
	defer func() {
		cancelFunc()
		sessionWaitGroup.Done()
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
					// do not use our parent ctx as a context, as we don't want it to cancel when
					// we reload config and cancel this listener
					ctx, cancelFunc := context.WithCancel(sessionParentCtx)
					defer cancelFunc()
					sessionWaitGroup.Add(1)
					connection.Serve(ctx)
					sessionWaitGroup.Done()
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
