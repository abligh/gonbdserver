package nbd

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// A single listener on a given net.Conn address
type Listener struct {
	logger          *log.Logger    // a logger
	protocol        string         // the protocol we are listening on
	addr            string         // the address
	exports         []ExportConfig // a list of export configurations associated
	defaultExport   string         // name of default export
	tls             TlsConfig      // the TLS configuration
	tlsconfig       *tls.Config    // the TLS configuration
	disableNoZeroes bool           // disable the 'no zeroes' extension
}

// An listener type that does what we want
type DeadlineListener interface {
	SetDeadline(t time.Time) error
	net.Listener
}

// Listen listens on an given address for incoming connections
//
// When sessions come in they are started on a separate context (sessionParentCtx), so that the listener can be killed without
// killing the sessions
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

// make an appropriate TLS config
func (l *Listener) initTls() error {
	keyFile := l.tls.KeyFile
	if keyFile == "" {
		return nil // no TLS
	}
	certFile := l.tls.CertFile
	if certFile == "" {
		certFile = keyFile
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	var clientCAs *x509.CertPool
	if l.tls.CaCertFile != "" {
		clientCAs = x509.NewCertPool()
		clientCAbytes, err := ioutil.ReadFile(l.tls.CaCertFile)
		if err != nil {
			return err
		}
		if ok := clientCAs.AppendCertsFromPEM(clientCAbytes); !ok {
			return errors.New("Could not append CA certficates from PEM file")
		}
	}

	serverName := l.tls.ServerName
	if serverName == "" {
		serverName, err = os.Hostname()
		if err != nil {
			return err
		}
	}
	var minVersion uint16
	var maxVersion uint16
	var ok bool
	if l.tls.MinVersion != "" {
		minVersion, ok = tlsVersionMap[strings.ToLower(l.tls.MinVersion)]
		if !ok {
			return fmt.Errorf("Bad minimum TLS version: '%s'", l.tls.MinVersion)
		}
	}
	if l.tls.MaxVersion != "" {
		minVersion, ok = tlsVersionMap[strings.ToLower(l.tls.MaxVersion)]
		if !ok {
			return fmt.Errorf("Bad maximum TLS version: '%s'", l.tls.MaxVersion)
		}
	}

	var clientAuth tls.ClientAuthType
	if l.tls.ClientAuth != "" {
		clientAuth, ok = tlsClientAuthMap[strings.ToLower(l.tls.ClientAuth)]
		if !ok {
			return fmt.Errorf("Bad TLS client auth type: '%s'", l.tls.ClientAuth)
		}
	}

	l.tlsconfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   serverName,
		ClientAuth:   clientAuth,
		ClientCAs:    clientCAs,
		MinVersion:   minVersion,
		MaxVersion:   maxVersion,
	}
	return nil
}

// NewListener returns a new listener object
func NewListener(logger *log.Logger, s ServerConfig) (*Listener, error) {
	l := &Listener{
		logger:          logger,
		protocol:        s.Protocol,
		addr:            s.Address,
		exports:         s.Exports,
		defaultExport:   s.DefaultExport,
		disableNoZeroes: s.DisableNoZeroes,
		tls:             s.Tls,
	}
	if err := l.initTls(); err != nil {
		return nil, err
	}
	return l, nil
}
