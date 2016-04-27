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
  
* **TLS support**. With client certificates if required.

* **Ceph RBD support**. Almost entirely untested.

* **Linux AIO support**. Experimental.

* **Pluggable backends**. By default a file backend is provided, as well as
  a Ceph/RBD backend on linux, but it would be possible to supply any backend.
  The ceph driver is there mostly to illustrate just how easy this is.

* **Reloadable configuration**. It is possible to reload the configuration
  using `SIGHUP` without affecting existing servers.

* **Newstyle negotiation (only)**. Oldstyle negotiation is not supported. This is
  a feature, not a bug.

* **Logging**. To syslog, a file, or stderr
 
NBD Experimental Extensions Implemented
---------------------------------------

* `WRITE_ZEROES` - support for `NBD_CMD_WRITE_ZEROES`

* `INFO` - support for `NBD_OPT_INFO`, `NBD_OPT_GO` and `NBD_OPT_BLOCK_SIZE`.

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
    	Send signal to daemon (either "stop" or "reload")
```

By default `gonbdserver` runs as a daemon. You can use `-f` to make it run in the foreground.
If you are running on Linux and want to run from `init`, you may wish to consider using
`start-stop-daemon` with the `-b` flag, and invoking `gondbserver` with the `-f` flag,
as `start-stop-daemon` is probably better at dealing with the many possible failure modes.

When running in foreground mode, the `pid` file is not used, and `-s` is irrelevant.

Note that to use the `-s` option, it is necessary to specify the `-c` and `-p` options
that you used in launching the daemon.

Signals
-------

* `SIGHUP` (or `gonbdserver -s reload`) will cleanly reload the configuration. Existing
  connections to the server will be unaffected (i.e. they will run with the previous
  configuration) until a disconnect / reconnect occurs.
  
* `SIGTERM` (or `gonbdserver -s stop`) will cleanly terminate the daemon. Existing
  connections to the server will be terminated.

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
    image: rbdbar                # on this rados block device name
    tlsonly: true                # require TLS on this device
  tls:                           # use the following certificates
    keyfile: /path/to/server-key.pem
    certfile: /path/to/server-cert.pem
    cacertfile: /path/to/ca-cert.pem
    servername: foo.example.com  # present the server name as 'foo.example.com'
    clientauth: requireverify    # require and verify client certificates
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

A description of the configuration file's sections is set out below

#### Top level

The top level of the configuration file consists of the following sections:
* `servers:` A list of zero or more `server` items
* `logging:` A `logging` item (optional)

#### `server` items

Each `server` item specifies a TCP port or unix socket that is listened to for new connections.

Each `server` item consists of the following:
* `protocol:` a description of the protocol it should listen. Valid values are `tcp`, `tcp4` (TCP on IPv4 only), `tcp6` (TCP on IPv6 ony), or `unix`. Optional, defaults to `tcp`.
* `address:` the address to listen on. For TCP protocols, this takes the form `address:port` in the normal manner. For UNIX protocols, this is the path to a Unix domain socket. Mandatory.
* `exports:` a list of zero or more `export` items each representing an export to be served by this server. This section is optional (and can be empty), but the server will be of little use if so.
* `defaultexport:` the name of the default export, which should be selected if no name is specified by the client. Optional, defaults to none.
* `tls:` a TLS item

#### `export` items

Each `export` item represents an export (i.e. an NBD disk) to be served by the server. Each export is served by a driver, and the drivers parameters (which are specific to the driver) may be intermingled with the export parameters.

Each `export` item consists of the following (common to all drivers):
* `name:` the name of the export as served over NBD. Mandatory.
* `description:` the human readable description of the export. Optional, defaults to an empty string.
* `driver:` the driver. Currently valid drivers are: `file`. Mandatory.
* `readonly:` set to `true` for readonly, `false` otherwise. Optional, defaults to `false`.
* `workers:` the number of simultaneous worker threads. Optional, defaults to 5.
* `tlsonly:` set to `true` if the export is only to be provided over TLS, `false` otherwise. Optional, defaults to `false`
* `minimumblocksize:` set to the minimum block size (must be a power of two). Optional, defaults to driver's minimum block size
* `preferredblocksize:` set to the preferred block size (must be a power of two). Optional, defaults to driver's preferred block size
* `maximumblocksize:` set to the maximum block size (must be a multiple of preferredblocksize). Optional, defaults to driver's maximum block size
* `flush:` set to `true` to forcibly enable support of the flush command, even if the driver does not support it; set to `false` to forcibly disable support for the flush command, even if the driver does support it. Optional, defaults to unset (i.e. use the driver's own setting)
* `fua:` set to `true` to forcibly enable support of the FUA (force unit access) flag, even if the driver does not support it; set to `false` to forcibly disable support for the FUA flag, even if the driver does support it. Optional, defaults to unset (i.e. use the driver's own setting)

The `file` driver reads the disk from a file on the host OS's disks. It has the following options:

* `path:` path to the file. Mandatory.
* `sync:` set to `true` to open the file with `O_SYNC`, else to `false`. Optional, defaults to `false`.

The `aiofile` driver reads the disk from a file on the host OS's disks using AIO (available on Linux only). This driver is experimental; do not use it in production. It has the following options:

* `path:` path to the file. Mandatory.
* `sync:` set to `true` to open the file with `O_SYNC`, else to `false`. Optional, defaults to `false`.

The `rbd` driver reads the disk from Ceph. It relies on your `ceph.conf` file being set up correctly, and has the following options:

* `image:` RBD name of image. Mandatory.
* `pool:` RBD pool for image. Optional, defaults to `rbd`.
* `cluster:` ceph cluster name. Defaults to `ceph`.
* `user:` ceph user name. Defaults to `client.admin`.

*Note the Ceph driver is almost entirely untested*

#### `tls` item

The `tls` item is used to enable TLS encryption on a server. If TLS is enabled on a server, the exports will be available over TLS. To make individual exports available *only* over TLS, add `tlsonly: true` to the export

* `keyfile:` Path to TLS key file in PEM format. Mandatory.
* `certfile:` Path to TLS cert file in PEM format. Optional, if not provided, defaults to `KeyFile` and assumes the PEM at `keyfile` has the certificate in as well as the private key.
* `servername:` Server name as announced by TLS. Optional, if not provided defaults to host name.
* `cacertfile:` Path to a file containing one or more CA certificates in PEM format. Optional, but required if validating client certificates
* `clientauth:` Client authentication strategy. Optional, defaulting to `none`. Must be one of the following values: `none` (no client certificate is requested or verified), `request` (a client certificate is requested but not verified), `require` (a client certificate is requested and required, but not verified), `verify` (a client certificate is requested and if provided is verified), or `requireverify` (a client certificate is requested and required, then verified)
* `minversion:` minimum TLS version. Optional, defaults to no minimum version. Must be one of the following values: `ssl3.0`, `tls1.0`, `tls1.1` or `tls1.2`.
* `maxversion:` maximum TLS version. Optional, defaults to no maximum version. Must be one of the following values: `ssl3.0`, `tls1.0`, `tls1.1` or `tls1.2`.

#### `logging` item

The `logging` item controls logging. There are three types of logging supported:
* Logging to `stderr` (the default)
* Logging to a file
* Logging to syslog

The `logging` item consists of the following:
* `File:` a the path to a file to log to. Optional. If not specified, will not log to a file. May not be specified together with `SyslogFacility`.
* `FileMode:` the permission mode (in octal) used to create the file. Optional. Defaults to `0644`.
* `SyslogFacility:` the name of a syslog facility. Optional. If not specified, will not log to syslog. May not be specified together with `File:`.
* `Date`: set to `true` to log the date, else set to `false`. Optional. Defaults to `false`. Note if logging to syslog, your syslog daemon may add the date anyway.
* `Time`: set to `true` to log the time, else set to `false`. Optional. Defaults to `false`. Note if logging to syslog, your syslog daemon may add the time anyway.
* `Microseconds`: set to `true` to log the time in microseconds, else set to `false`. Optional. Defaults to `false`. Note if logging to syslog, your syslog daemon may add the time anyway.
* `UTC`: set to `true` to log the time in UTC, else set to `false`. Optional. Defaults to `false`. Note if logging to syslog, your syslog daemon may add the time anyway.
* `SourceFile`: set to `true` to log the source file emitting the log message, else set to `false`. Optional. Defaults to `false`.

Licence
-------

The code is licensed under the MIT licence.
