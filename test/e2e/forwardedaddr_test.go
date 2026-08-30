//go:build e2e || chaos

package e2e

import "testing"

func TestForwardedAddr(t *testing.T) {
	for _, c := range []struct{ name, out, want string }{
		{"the line kubectl prints", "Forwarding from 127.0.0.1:39217 -> 8081\n", "127.0.0.1:39217"},
		{"after other output", "some warning\nForwarding from 127.0.0.1:1024 -> 5432\nForwarding from [::1]:1024 -> 5432\n", "127.0.0.1:1024"},
		{"the IPv6 line alone is not an address we dial", "Forwarding from [::1]:1024 -> 5432\n", ""},
		{"nothing yet", "", ""},
		{"partial line", "Forwarding fr", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := forwardedAddr(c.out); got != c.want {
				t.Fatalf("forwardedAddr(%q) = %q, want %q", c.out, got, c.want)
			}
		})
	}
}
