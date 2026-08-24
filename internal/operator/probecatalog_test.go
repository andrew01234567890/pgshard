package operator

import (
	"strings"
	"testing"
)

func TestCatalogCutoverResume(t *testing.T) {
	if already, err := catalogCutoverResume(true, true); already || err != nil {
		t.Fatalf("fresh run: %t %v", already, err)
	}
	if already, err := catalogCutoverResume(false, false); !already || err != nil {
		t.Fatalf("re-run after the cut: %t %v", already, err)
	}
	if _, err := catalogCutoverResume(false, true); err == nil || !strings.Contains(err.Error(), "slot is gone") {
		t.Fatalf("missing slot must error: %v", err)
	}
	if _, err := catalogCutoverResume(true, false); err == nil || !strings.Contains(err.Error(), "subscription is gone") {
		t.Fatalf("missing subscription must error: %v", err)
	}
}
