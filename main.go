package main

import (
	"github.com/abligh/gonbdserver/nbd"
	"golang.org/x/net/context"
	"log"
	"os"
)

func main() {
	ctx := context.Background()
	logger := log.New(os.Stdout, "gonbdserver", log.Lmicroseconds|log.Ldate|log.Lshortfile)
	if l, err := nbd.NewListener(logger, "127.0.0.1:6666"); err != nil {
		logger.Printf("[ERROR] Could not create listener: %v", err)
	} else {
		l.Listen(ctx)
	}
}
