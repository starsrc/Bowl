//go:build linux

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
