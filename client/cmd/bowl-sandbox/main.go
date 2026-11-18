// Package main 启动 bowl-sandbox 命令行程序:跑以 Bowl 虚拟磁盘为盘的 sandbox。
package main

import (
	"os"

	"github.com/starsrc/bowl/client/internal/cli"
)

func main() {
	os.Exit(cli.ExecuteSandbox(os.Args[1:], os.Stdout, os.Stderr))
}
