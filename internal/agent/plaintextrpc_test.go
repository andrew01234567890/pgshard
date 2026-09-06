package agent

import "testing"

// internalTLS.issue mounts the certificates on every member without
// requiring them, so a cluster can carry a full internal PKI and still
// serve the lifecycle RPCs behind a bearer token in clear. Nothing said so:
// the listener starts, the RPCs work, and the only evidence is a field that
// was not set. Plaintext is what the startup warning is keyed on, so it has
// to mean exactly "no material at all" -- a partly configured listener is a
// misconfiguration to fail on, not a plaintext one to warn about.
func TestPlaintextMeansNoTLSMaterialAtAll(t *testing.T) {
	for _, c := range []struct {
		name string
		tls  TLSFiles
		want bool
	}{
		{"nothing", TLSFiles{}, true},
		{"full", TLSFiles{CertFile: "c", KeyFile: "k", CAFile: "a"}, false},
		{"cert only", TLSFiles{CertFile: "c"}, false},
		{"ca only", TLSFiles{CAFile: "a"}, false},
	} {
		if got := c.tls.Plaintext(); got != c.want {
			t.Errorf("%s: Plaintext() = %v, want %v", c.name, got, c.want)
		}
	}
}
