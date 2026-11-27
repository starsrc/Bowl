package main

import (
	"os"

	"github.com/starsrc/bowl/client/internal/sandboxcli"
)

func main() {
	os.Exit(sandboxcli.Execute(os.Args[1:], os.Stdout, os.Stderr))
}
