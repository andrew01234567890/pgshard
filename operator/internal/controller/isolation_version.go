package controller

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
)

const (
	// isolationSupportedMinMinor and isolationSupportedMaxMinor bound the
	// Kubernetes minor range whose pinned normal-form residue and schema the
	// operator knows. The vendored k8s.io/api is 1.36; the range includes the
	// overlapping-bridge-release window. Outside it activation is withheld and
	// readiness fails closed — the normal form is only known within the range.
	isolationSupportedMinMinor = 36
	isolationSupportedMaxMinor = 36
)

// serverVersionGate reads the live API server version and checks its minor is in
// the supported range.
type serverVersionGate struct {
	discovery discovery.ServerVersionInterface
	minMinor  int
	maxMinor  int
}

func NewServerVersionGate(discovery discovery.ServerVersionInterface) *serverVersionGate {
	return &serverVersionGate{discovery: discovery, minMinor: isolationSupportedMinMinor, maxMinor: isolationSupportedMaxMinor}
}

func (g *serverVersionGate) SupportedMinor(ctx context.Context) (bool, string, error) {
	info, err := g.discovery.ServerVersion()
	if err != nil {
		return false, "", err
	}
	minor, ok := parseMinor(info.Minor)
	if !ok {
		return false, info.GitVersion, fmt.Errorf("unparseable API server minor %q", info.Minor)
	}
	return minor >= g.minMinor && minor <= g.maxMinor, versionLabel(info), nil
}

func versionLabel(info *version.Info) string {
	if info.GitVersion != "" {
		return info.GitVersion
	}
	return info.Major + "." + info.Minor
}

// parseMinor extracts the numeric minor from a possibly-annotated field such as
// "36", "36+", or "36.2".
func parseMinor(raw string) (int, bool) {
	digits := strings.Builder{}
	for _, r := range raw {
		if r < '0' || r > '9' {
			break
		}
		digits.WriteRune(r)
	}
	if digits.Len() == 0 {
		return 0, false
	}
	minor := 0
	for _, r := range digits.String() {
		minor = minor*10 + int(r-'0')
	}
	return minor, true
}
