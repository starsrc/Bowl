//go:build linux

// Command bowl-sandbox-guest runs inside a sandbox MicroVM as its init and executes the
// commands the host sends over vsock. It is not meant to be run by hand.
package main

import (
	"fmt"
	"os"

	"github.com/starsrc/bowl/client/internal/sandbox"
)

func main() {
	if err := sandbox.ServeGuest(); err != nil {
		fmt.Fprintf(os.Stderr, "bowl-sandbox-guest: %v\n", err)
		os.Exit(1)
	}
}
