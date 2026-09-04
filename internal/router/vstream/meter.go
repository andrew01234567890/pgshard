package vstream

import "github.com/prometheus/client_golang/prometheus"

// PrometheusMeter adapts the router's metric set to Meter. The set is
// passed as its three collectors rather than whole, so this package keeps
// no dependency on the metrics package and the wiring stays visible at the
// call site. A collector left nil is skipped rather than panicking, which
// is what lets a test wire up only the one it asserts on.
type PrometheusMeter struct {
	Buffered prometheus.Gauge
	Open     prometheus.Gauge
	Exceeded *prometheus.CounterVec
}

// BufferedBytes implements Meter.
func (m PrometheusMeter) BufferedBytes(delta int) {
	if m.Buffered != nil {
		m.Buffered.Add(float64(delta))
	}
}

// OpenTransactions implements Meter.
func (m PrometheusMeter) OpenTransactions(delta int) {
	if m.Open != nil {
		m.Open.Add(float64(delta))
	}
}

// TooLarge implements Meter.
func (m PrometheusMeter) TooLarge(bound string) {
	if m.Exceeded != nil {
		m.Exceeded.WithLabelValues(bound).Inc()
	}
}
