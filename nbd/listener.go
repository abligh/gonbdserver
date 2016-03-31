package nbd

import (
	"golang.org/x/net/context"
	"log"
	"net"
	"time"
)

// A single listener on a given TCP address
type Listener struct {
	logger *log.Logger
	addr   string
}

func (l *Listener) Listen(parentCtx context.Context) {

	ctx, cancelFunc := context.WithCancel(parentCtx)

	defer cancelFunc()

	tcp := "tcp"
	netAddr, err := net.ResolveTCPAddr(tcp, l.addr)
	if err != nil {
		l.logger.Printf("[ERROR] Could not resolve address %s", l.addr)
		return
	}
	listener, err := net.ListenTCP(tcp, netAddr)
	if err != nil {
		l.logger.Printf("[ERROR] Could not listen on address %s", netAddr)
	}

	defer func() {
		l.logger.Printf("[INFO] Stopping listening on %s", netAddr)
		listener.Close()
	}()

	l.logger.Printf("[INFO] Starting listening on %s", netAddr)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		listener.SetDeadline(time.Now().Add(time.Second))
		if conn, err := listener.AcceptTCP(); err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			l.logger.Printf("[ERROR] Error %s listening on %s", err, netAddr)
		} else {
			l.logger.Printf("[INFO] Connect to %s from %s", netAddr, conn.RemoteAddr())
			if connection, err := newConnection(l, l.logger, conn); err != nil {
				l.logger.Printf("[ERROR] Error %s establishing connection to %s from %s", err, netAddr, conn.RemoteAddr())
				conn.Close()
			} else {
				go connection.Serve(ctx)
			}
		}
	}

}

func NewListener(logger *log.Logger, addr string) (*Listener, error) {
	l := &Listener{
		logger: logger,
		addr:   addr,
	}
	return l, nil
}
