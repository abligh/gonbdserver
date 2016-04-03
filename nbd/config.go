package nbd

import (
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

/* Example configuration:

servers:
- protocol: tcp
  address: 127.0.0.1:6666
  exports:
  - name: foo
    driver: file
    path: /tmp/test
    workers: 5
  - name: bar
    readonly: true
    driver: rbd
    readonly: false
    rdbname: rbdbar
    timeout: 5s
- protocol: unix
  address: /var/run/nbd.sock
  exports:
  - name: baz
    driver: file
    readonly: false
    path: /tmp/baz
*/

// Location of the config file on disk; overriden by flags
var configFile = flag.String("configfile", "/etc/gonbdserver.conf", "Path to YAML config file")

// Config holds the config that applies to all servers (currently none), and an array of server configs
type Config struct {
	Servers []ServerConfig // array of server configs
}

// ServerConfig holds the config that applies to each server (i.e. listener)
type ServerConfig struct {
	Protocol string         // protocol it should listen on (in net.Conn form)
	Address  string         // address to listen on
	Exports  []ExportConfig // array of configurations of exported items
}

// ExportConfig holds the config for one exported item
type ExportConfig struct {
	Name             string                 // name of the export
	Driver           string                 // name of the driver
	ReadOnly         bool                   // true of the export should be opened readonly
	Workers          int                    // number of concurrent workers
	DriverParameters DriverParametersConfig `yaml:",inline"` // driver parameters. These are an arbitrary map. Inline means they go aside teh foregoing
}

// DriverConfig is an arbitrary map of other parameters in string format
type DriverParametersConfig map[string]string

// ParseConfig parses the YAML configuration provided
func ParseConfig() (*Config, error) {
	flag.Parse()
	if buf, err := ioutil.ReadFile(*configFile); err != nil {
		return nil, err
	} else {
		c := &Config{}
		if err := yaml.Unmarshal(buf, c); err != nil {
			return nil, err
		}
		for i, _ := range c.Servers {
			if c.Servers[i].Protocol == "" {
				c.Servers[i].Protocol = "tcp"
			}
			if c.Servers[i].Protocol == "tcp" && c.Servers[i].Address == "" {
				c.Servers[i].Protocol = fmt.Sprintf("0.0.0.0:%d", NBD_DEFAULT_PORT)
			}
		}
		return c, nil
	}
}

// Startserver starts a single server.
//
// A parent context is given in which the listener runs, as well as a session context in which the sessions (connections) themselves run.
// This enables the sessions to be retained when the listener is cancelled on a SIGHUP
func StartServer(parentCtx context.Context, sessionParentCtx context.Context, sessionWaitGroup *sync.WaitGroup, logger *log.Logger, s ServerConfig) {
	ctx, cancelFunc := context.WithCancel(parentCtx)

	defer func() {
		cancelFunc()
		logger.Printf("[INFO] Stopping server %s:%s", s.Protocol, s.Address)
	}()

	logger.Printf("[INFO] Starting server %s:%s", s.Protocol, s.Address)

	if l, err := NewListener(logger, s.Protocol, s.Address, s.Exports); err != nil {
		logger.Printf("[ERROR] Could not create listener for %s:%s: %v", s.Protocol, s.Address, err)
	} else {
		l.Listen(ctx, sessionParentCtx, sessionWaitGroup)
	}
}

// RunConfig - this is effectively the main entry point of the program
//
// We parse the config, then start each of the listeners, restarting them when we get SIGHUP, but being sure not to kill the sessions
func RunConfig() {
	logger := log.New(os.Stdout, "gonbdserver", log.Lmicroseconds|log.Ldate|log.Lshortfile)
	var sessionWaitGroup sync.WaitGroup
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer func() {
		logger.Println("[INFO] Shutting down")
		cancelFunc()
		sessionWaitGroup.Wait()
		logger.Println("[INFO] Shutdown complete")
	}()

	intr := make(chan os.Signal, 1)
	term := make(chan os.Signal, 1)
	hup := make(chan os.Signal, 1)
	signal.Notify(intr, os.Interrupt)
	signal.Notify(term, syscall.SIGTERM)
	signal.Notify(hup, syscall.SIGHUP)

	for {
		var wg sync.WaitGroup
		configCtx, configCancelFunc := context.WithCancel(ctx)
		logger.Println("[INFO] Loading configuration")
		if c, err := ParseConfig(); err != nil {
			logger.Println("[ERROR] Cannot parse configuration file: %v", err)
			return
		} else {
			for _, s := range c.Servers {
				s := s // localise loop variable
				go func() {
					wg.Add(1)
					StartServer(configCtx, ctx, &sessionWaitGroup, logger, s)
					wg.Done()
				}()
			}

			select {
			case <-ctx.Done():
				logger.Println("[INFO] Interrupted")
				return
			case <-intr:
				logger.Println("[INFO] Interrupt signal received")
				return
			case <-term:
				logger.Println("[INFO] Terminate signal received")
				return
			case <-hup:
				logger.Println("[INFO] Reload signal received; reloading configuration which will be effective for new connections")
				configCancelFunc() // kill the listeners but not the sessions
				wg.Wait()
			}
		}
	}
}
