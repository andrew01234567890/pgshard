// Package perf holds Go benchmarks compared across commits by hack/perf/benchstat.sh.
//
// Only benchmarks whose name contains "Gate" block a pull request, and only on
// a statistically significant sec/op increase above both a relative and an
// absolute threshold (see hack/perf/gate.sh). Other benchmarks are reported
// for information.
package perf
