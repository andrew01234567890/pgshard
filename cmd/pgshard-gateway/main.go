package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrew01234567890/pgshard/internal/buildinfo"
)

func main() {
	info := buildinfo.For("pgshard-gateway")
	flag.Usage = func() { fmt.Print(info.UsageText()) }
	showVersion := flag.Bool("version", false, "print version information")
	flag.Parse()

	if *showVersion {
		fmt.Println(info.VersionText())
		return
	}
	if flag.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "%s: runtime is not yet configured\n", info.Component)
		os.Exit(2)
	}
	flag.Usage()
}
