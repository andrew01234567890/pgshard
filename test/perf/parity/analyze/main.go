// Command parity-analyze prints a comparison table for a pooling-parity
// results.csv produced by test/perf/parity/run.sh, with each front-end's
// throughput delta against the direct-to-PostgreSQL arm.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: parity-analyze <results.csv>")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rows, err := Parse(f)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(Table(rows))
}
