# gonbdserver [![Build Status](https://travis-ci.org/abligh/gonbdserver.svg?branch=master)](https://travis-ci.org/abligh/gonbdserver) [![GoDoc](http://godoc.org/github.com/abligh/gonbdserver?status.png)](http://godoc.org/github.com/abligh/gonbdserver/nbd) [![GitHub release](https://img.shields.io/github/release/abligh/gonbdserver.svg)](https://github.com/abligh/gonbdserver/releases)

`gonbdserver` is an NBD server written in Go. Its purpose is not to
be especially performant, but rather to act as a simple demonstration
of the implementation of the NBD protocol. That said, where tested
it appears to be at least as fast as the reference nbdserver implementation.

Features
--------

* **Wide protocol support**. Supports both FLUSH and FUA.

* **Multithreaded**. Defaults to 5 worker threads per connection, so able to
  process requests in parallel.

* **Pluggable backends**. By default a file backend is provided, but it would
  be possible to supply any backend.

* **Reloadable configuration**. It is possible to reload the configuration
  using `SIGHUP` without affecting existing servers.

* **Newstyle negotiation (only)**. Oldstyle negotiation is not supported. This is
  a feature, not a bug.

* **Logging**. To syslog, a file, or stderr
 
Invocation
----------

Invocation is very easy. It takes a minimum number of command-line flags. Most
of the configuration is within the configuration file.

```
$ ./gonbdserver --help
Usage of ./gonbdserver:
  -c string
    	Path to YAML config file (default "/etc/gonbdserver.conf")
  -f	Run in foreground (not as daemon)
  -p string
    	Path to PID file (default "/var/run/gonbdserver.pid")
  -s string
    	Send signal to daemon (either 'stop' or 'reload')
```

By default `gonbdserver` runs as a daemon. You can use `-f` to make it run in the foreground.
If you are running on Linux and want to run from `init`, you may wish to consider using
`start-stop-daemon` with the `-b` flag, and invoking `gondbserver` with the `-f` flag,
as `start-stop-daemon` is probably better at dealing with the many possible failure modes.

When running in foreground mode, the `pid` file is not used, and `-s` is irrelevant.

Configuration
-------------

Configuration is provided through a YAML file which by default lives at
`/etc/gonbdserver.conf`, though this can be specified using the `-c` option.

An example of a configuration is set out below:

```
servers:
- protocol: tcp                  # A first server, using TCP
  address: 127.0.0.1:6666        # on port 6666
  exports:                       # It has two exports
  - name: foo                    # The first is named 'foo' and
    driver: file                 # Uses the 'file' driver
    path: /tmp/test              # This uses /tmp/test as the file
    workers: 4                   # Use 4 workers
  - name: bar                    # The second export is called 'bar'
    readonly: true               # This is readonly
    driver: rbd                  # And uses the (currently imaginary) rbd driver
    rdbname: rbdbar              # on this rados block device name
    timeout: 5s                  # imaginary extra parameter for RDB
- protocol: unix                 # Another server uses UNIX
  address: /var/run/nbd.sock     # served on this socket
  exports:                       # it has one export
  - name: baz                    # named bar
    driver: file                 # using the file driver
    path: /tmp/baz               # on this file
    sync: true                   # open with O_SYNC
logging:                         # log to
  syslogfacility: local1         # local1
```    

Licence
-------

The code is licensed under the MIT licence.
