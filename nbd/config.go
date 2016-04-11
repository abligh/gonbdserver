package nbd

import (
	"flag"
	"fmt"
	"github.com/sevlyar/go-daemon"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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
    rdbname: rbdbar
    timeout: 5s
- protocol: unix
  address: /var/run/nbd.sock
  exports:
  - name: baz
    driver: file
    readonly: false
    path: /tmp/baz
    sync: true
logging:
  syslogfacility: local1
*/

// Location of the config file on disk; overriden by flags
var configFile = flag.String("c", "/etc/gonbdserver.conf", "Path to YAML config file")
var pidFile = flag.String("p", "/var/run/gonbdserver.pid", "Path to PID file")
var sendSignal = flag.String("s", "", "Send signal to daemon (either \"stop\" or \"reload\")")
var foreground = flag.Bool("f", false, "Run in foreground (not as daemon)")

const (
	ENV_CONFFILE = "_GONBDSERVER_CONFFILE"
	ENV_PIDFILE  = "_GONBDSERVER_PIDFILE"
)

// Config holds the config that applies to all servers (currently just logging), and an array of server configs
type Config struct {
	Servers []ServerConfig // array of server configs
	Logging LogConfig      // Configuration for logging
}

// ServerConfig holds the config that applies to each server (i.e. listener)
type ServerConfig struct {
	Protocol      string         // protocol it should listen on (in net.Conn form)
	Address       string         // address to listen on
	DefaultExport string         // name of default export
	Exports       []ExportConfig // array of configurations of exported items
	Tls           TlsConfig      // TLS configuration
}

// ExportConfig holds the config for one exported item
type ExportConfig struct {
	Name             string                 // name of the export
	Description      string                 // description of export
	Driver           string                 // name of the driver
	ReadOnly         bool                   // true of the export should be opened readonly
	Workers          int                    // number of concurrent workers
	TlsOnly          bool                   // true if the export should only be served over TLS
	DriverParameters DriverParametersConfig `yaml:",inline"` // driver parameters. These are an arbitrary map. Inline means they go aside teh foregoing
}

// TlsConfig has the configuration for TLS
type TlsConfig struct {
	KeyFile    string // path to TLS key file
	CertFile   string // path to TLS cert file
	ServerName string // server name
	CaCertFile string // path to certificate file
	ClientAuth string // client authentication strategy
	MinVersion string // minimum TLS version
	MaxVersion string // maximum TLS version
}

// DriverConfig is an arbitrary map of other parameters in string format
type DriverParametersConfig map[string]string

// LogConfig specifies configuration for logging
type LogConfig struct {
	File           string // a file to log to
	FileMode       string // file mode
	SyslogFacility string // a syslog facility name - set to enable syslog
	Date           bool   // log the date - i.e. log.Ldate
	Time           bool   // log the time - i.e. log.Ltime
	Microseconds   bool   // log microseconds - i.e. log.Lmicroseconds
	UTC            bool   // log time in URC - i.e. LUTC
	SourceFile     bool   // log source file - i.e. Lshortfile
}

// SyslogWriter is a WriterCloser that logs to syslog with an extracted priority
type SyslogWriter struct {
	facility syslog.Priority
	w        *syslog.Writer
}

// facilityMap maps textual
var facilityMap map[string]syslog.Priority = map[string]syslog.Priority{
	"kern":     syslog.LOG_KERN,
	"user":     syslog.LOG_USER,
	"mail":     syslog.LOG_MAIL,
	"daemon":   syslog.LOG_DAEMON,
	"auth":     syslog.LOG_AUTH,
	"syslog":   syslog.LOG_SYSLOG,
	"lpr":      syslog.LOG_LPR,
	"news":     syslog.LOG_NEWS,
	"uucp":     syslog.LOG_UUCP,
	"cron":     syslog.LOG_CRON,
	"authpriv": syslog.LOG_AUTHPRIV,
	"ftp":      syslog.LOG_FTP,
	"local0":   syslog.LOG_LOCAL0,
	"local1":   syslog.LOG_LOCAL1,
	"local2":   syslog.LOG_LOCAL2,
	"local3":   syslog.LOG_LOCAL3,
	"local4":   syslog.LOG_LOCAL4,
	"local5":   syslog.LOG_LOCAL5,
	"local6":   syslog.LOG_LOCAL6,
	"local7":   syslog.LOG_LOCAL7,
}

// levelMap maps textual levels to syslog levels
var levelMap map[string]syslog.Priority = map[string]syslog.Priority{
	"EMERG":   syslog.LOG_EMERG,
	"ALERT":   syslog.LOG_ALERT,
	"CRIT":    syslog.LOG_CRIT,
	"ERR":     syslog.LOG_ERR,
	"ERROR":   syslog.LOG_ERR,
	"WARN":    syslog.LOG_WARNING,
	"WARNING": syslog.LOG_WARNING,
	"NOTICE":  syslog.LOG_NOTICE,
	"INFO":    syslog.LOG_INFO,
	"DEBUG":   syslog.LOG_DEBUG,
}

// isTruthy determines whether an argument is true
func isSet(v string) (bool, error) {
	if v == "true" {
		return true, nil
	} else if v == "false" || v == "" {
		return false, nil
	}
	return false, fmt.Errorf("Unknown boolean value: %s", v)
}

// Create a new syslog writer
func NewSyslogWriter(facility string) (*SyslogWriter, error) {
	f := syslog.LOG_DAEMON
	if ff, ok := facilityMap[facility]; ok {
		f = ff
	}

	if w, err := syslog.New(f|syslog.LOG_INFO, "gonbdserver:"); err != nil {
		return nil, err
	} else {
		return &SyslogWriter{
			w: w,
		}, nil
	}
}

// Close the channel
func (s *SyslogWriter) Close() error {
	return s.w.Close()
}

var deletePrefix *regexp.Regexp = regexp.MustCompile("gonbdserver:")
var replaceLevel *regexp.Regexp = regexp.MustCompile("\\[[A-Z]+\\] ")

// Write to the syslog, removing the prefix and setting the appropriate level
func (s *SyslogWriter) Write(p []byte) (n int, err error) {
	p1 := deletePrefix.ReplaceAllString(string(p), "")
	level := ""
	tolog := string(replaceLevel.ReplaceAllStringFunc(p1, func(l string) string {
		level = l
		return ""
	}))
	switch level {
	case "[DEBUG] ":
		s.w.Debug(tolog)
	case "[INFO] ":
		s.w.Info(tolog)
	case "[NOTICE] ":
		s.w.Notice(tolog)
	case "[WARNING] ", "[WARN] ":
		s.w.Warning(tolog)
	case "[ERROR] ", "[ERR] ":
		s.w.Err(tolog)
	case "[CRIT] ":
		s.w.Crit(tolog)
	case "[ALERT] ":
		s.w.Alert(tolog)
	case "[EMERG] ":
		s.w.Emerg(tolog)
	default:
		s.w.Notice(tolog)
	}
	return len(p), nil
}

// ParseConfig parses the YAML configuration provided
func ParseConfig() (*Config, error) {
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

	if l, err := NewListener(logger, s); err != nil {
		logger.Printf("[ERROR] Could not create listener for %s:%s: %v", s.Protocol, s.Address, err)
	} else {
		l.Listen(ctx, sessionParentCtx, sessionWaitGroup)
	}
}

func (c *Config) GetLogger() (*log.Logger, io.Closer, error) {
	logFlags := 0
	if c.Logging.Date {
		logFlags |= log.Ldate
	}
	if c.Logging.Time {
		logFlags |= log.Ltime
	}
	if c.Logging.Microseconds {
		logFlags |= log.Lmicroseconds
	}
	if c.Logging.SourceFile {
		logFlags |= log.Lshortfile
	}
	if c.Logging.File != "" {
		mode := os.FileMode(0644)
		if c.Logging.FileMode != "" {
			if i, err := strconv.ParseInt(c.Logging.FileMode, 8, 32); err != nil {
				return nil, nil, fmt.Errorf("Cannot read file logging mode: %v", err)
			} else {
				mode = os.FileMode(i)
			}
		}
		if file, err := os.OpenFile(c.Logging.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, mode); err != nil {
			return nil, nil, err
		} else {
			return log.New(file, "gonbdserver:", logFlags), file, nil
		}
	}
	if c.Logging.SyslogFacility != "" {
		if s, err := NewSyslogWriter(c.Logging.SyslogFacility); err != nil {
			return nil, nil, err
		} else {
			return log.New(s, "gonbdserver:", logFlags), s, nil
		}
	} else {
		return log.New(os.Stderr, "gonbdserver:", logFlags), nil, nil
	}
}

// RunConfig - this is effectively the main entry point of the program
//
// We parse the config, then start each of the listeners, restarting them when we get SIGHUP, but being sure not to kill the sessions
func RunConfig() {
	// just until we read the configuration
	logger := log.New(os.Stderr, "gonbdserver:", log.LstdFlags)
	var logCloser io.Closer
	var sessionWaitGroup sync.WaitGroup
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer func() {
		logger.Println("[INFO] Shutting down")
		cancelFunc()
		sessionWaitGroup.Wait()
		logger.Println("[INFO] Shutdown complete")
		if logCloser != nil {
			logCloser.Close()
		}
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
		if c, err := ParseConfig(); err != nil {
			logger.Println("[ERROR] Cannot parse configuration file: %v", err)
			return
		} else {
			if nlogger, nlogCloser, err := c.GetLogger(); err != nil {
				logger.Println("[ERROR] Could not load logger: %v", err)
			} else {
				if logCloser != nil {
					logCloser.Close()
				}
				logger = nlogger
				logCloser = nlogCloser
			}
			logger.Printf("[INFO] Loaded configuration. Available backends: %s.", strings.Join(GetBackendNames(), ", "))
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

func Run() {
	// Just for this routine
	logger := log.New(os.Stderr, "gonbdserver:", log.LstdFlags)

	daemon.AddFlag(daemon.StringFlag(sendSignal, "stop"), syscall.SIGTERM)
	daemon.AddFlag(daemon.StringFlag(sendSignal, "reload"), syscall.SIGHUP)
	flag.Parse()

	if daemon.WasReborn() {
		if val := os.Getenv(ENV_CONFFILE); val != "" {
			*configFile = val
		}
		if val := os.Getenv(ENV_PIDFILE); val != "" {
			*pidFile = val
		}
	}

	var err error
	if *configFile, err = filepath.Abs(*configFile); err != nil {
		logger.Fatalf("[CRIT] Error canonicalising config file path: %s", err)
	}
	if *pidFile, err = filepath.Abs(*pidFile); err != nil {
		logger.Fatalf("[CRIT] Error canonicalising pid file path: %v", err)
	}

	// check the configuration parses. We do nothing with this at this stage
	// but it eliminates a problem where the log of the configuration failing
	// is invisible when daemonizing naively (e.g. when no alternate log
	// destination is supplied) and the config file cannot be read
	if _, err := ParseConfig(); err != nil {
		logger.Fatalf("[CRIT] Cannot parse configuration file: %v", err)
		return
	}

	if *foreground {
		RunConfig()
		return
	}

	os.Setenv(ENV_CONFFILE, *configFile)
	os.Setenv(ENV_PIDFILE, *pidFile)

	// Define daemon context
	d := &daemon.Context{
		PidFileName: *pidFile,
		PidFilePerm: 0644,
		Umask:       027,
	}

	// Send commands if needed
	if len(daemon.ActiveFlags()) > 0 {
		p, err := d.Search()
		if err != nil {
			logger.Fatalf("[CRIT] Unable send signal to the daemon - not running")
		}
		if err := p.Signal(syscall.Signal(0)); err != nil {
			logger.Fatalf("[CRIT] Unable send signal to the daemon - not running, perhaps PID file is stale")
		}
		daemon.SendCommands(p)
		return
	}

	if !daemon.WasReborn() {
		if p, err := d.Search(); err == nil {
			if err := p.Signal(syscall.Signal(0)); err == nil {
				logger.Fatalf("[CRIT] Daemon is already running (pid %d)", p.Pid)
			} else {
				logger.Printf("[INFO] Removing stale PID file %s", *pidFile)
				os.Remove(*pidFile)
			}
		}
	}

	// Process daemon operations - send signal if present flag or daemonize
	child, err := d.Reborn()
	if err != nil {
		logger.Fatalf("[CRIT] Daemonize: %s", err)
	}
	if child != nil {
		return
	}

	defer func() {
		d.Release()
		// for some reason this is not removing the pid file
		os.Remove(*pidFile)
	}()

	RunConfig()
}
