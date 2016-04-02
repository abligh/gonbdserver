package nbd

import (
	"flag"
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
  address: 0.0.0.0:6666
  exports:
  - name: foo
    driver: file
    readonly: true
    path: /tmp/foo
  - name: bar
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

var configFile = flag.String("configfile", "/etc/gonbdserver.conf", "Path to YAML config file")

type Config struct {
	Servers []ServerConfig
}

type ServerConfig struct {
	Protocol string
	Address  string
	Exports  []ExportConfig
}

type ExportConfig struct {
	Name             string
	Driver           string
	ReadOnly         bool
	DriverParameters DriverParametersConfig `yaml:",inline"`
}

type DriverParametersConfig map[string]string

func ParseConfig() (*Config, error) {
	flag.Parse()
	if buf, err := ioutil.ReadFile(*configFile); err != nil {
		return nil, err
	} else {
		c := &Config{}
		if err := yaml.Unmarshal(buf, c); err != nil {
			return nil, err
		}
		return c, nil
	}
}

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
