package main

import (
	"github.com/abligh/gonbdserver/nbd"
)

// main() is the main program entry
//
// this is a wrapper to enable us to put the interesting stuff in a package
func main() {
	nbd.RunConfig()
}
