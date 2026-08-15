package main

import (
	"os"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
)

func main() {
	os.Exit(buildinfo.Run(buildinfo.For("pgshard-cdc"), os.Args[1:], os.Stdout, os.Stderr))
}
