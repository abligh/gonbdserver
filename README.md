gonbdserver
===========

gonbdserver is an NBD server written in Go. It's purpose is not to
be especially performant, but rather to act as a simple demonstration
of the implementation of the NBD protocol. It supports pluggable
backends, and it would be possible to supply a backend which
works rather better than the default file backend.

Oldstyle negotiation is not supported. Other than that, most NBD
options are.

The code is licensed under the MIT licence.
