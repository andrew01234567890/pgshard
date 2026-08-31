//go:build e2e

package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"text/template"
	"time"
)

// A registry hiccup backs off once and then succeeds; a suite that fails on
// the first sighting trades a slow failure for a flaky one, which is the
// trade PGS-540 is about. Only a container still backing off after the grace
// is called.
func TestOnlyADurableImagePullFailureIsCalled(t *testing.T) {
	const grace = 90 * time.Second
	line := `demo-router-0: Back-off pulling image "ghcr.io/x/pgshard-router:latest"`
	start := time.Now()

	since := map[string]time.Time{}
	if got := durableImagePullFailure([]string{line}, since, start, grace); got != "" {
		t.Fatalf("the first sighting was called immediately: %q", got)
	}
	if got := durableImagePullFailure([]string{line}, since, start.Add(grace-time.Second), grace); got != "" {
		t.Fatalf("called inside the grace: %q", got)
	}
	if got := durableImagePullFailure([]string{line}, since, start.Add(grace), grace); got != line {
		t.Fatalf("a container backing off for the whole grace was not called: %q", got)
	}

	// Recovery forgets it, so a later hiccup starts its own grace rather
	// than inheriting the first one's age.
	if got := durableImagePullFailure(nil, since, start.Add(grace), grace); got != "" {
		t.Fatalf("nothing is backing off, yet: %q", got)
	}
	if len(since) != 0 {
		t.Fatalf("a recovered container was remembered: %v", since)
	}
	if got := durableImagePullFailure([]string{line}, since, start.Add(2*grace), grace); got != "" {
		t.Fatalf("a fresh sighting inherited the old one's age: %q", got)
	}

	// Every stuck container is named, not just the first.
	other := `demo-admin-0: Back-off pulling image "ghcr.io/x/pgshard-admin:latest"`
	both := []string{line, other}
	durableImagePullFailure(both, since, start.Add(2*grace), grace)
	got := durableImagePullFailure(both, since, start.Add(4*grace), grace)
	if got != line+"\n"+other {
		t.Fatalf("not every stuck container was named: %q", got)
	}
}

// The template walks fields that are absent on a pod the scheduler has only
// just placed, and text/template fails the whole render on one missing field
// rather than skipping the pod. Exercise it against the shapes a real list
// contains at once: pulled and running, backing off, pulling for the first
// time, and not started at all.
func TestTheBackOffTemplateReadsAPodListWithoutFailing(t *testing.T) {
	const pods = `{"items":[
	  {"metadata":{"name":"demo-catalog-0"},
	   "status":{"containerStatuses":[{"name":"postgres","state":{"running":{"startedAt":"2026-08-31T10:00:00Z"}}}]}},
	  {"metadata":{"name":"demo-router-1"},
	   "status":{"containerStatuses":[{"name":"router","state":{"waiting":{"reason":"ImagePullBackOff","message":"Back-off pulling image \"ghcr.io/x/pgshard-router:latest\""}}}]}},
	  {"metadata":{"name":"demo-router-2"},
	   "status":{"containerStatuses":[{"name":"router","state":{"waiting":{"reason":"ErrImagePull","message":"pulling"}}}]}},
	  {"metadata":{"name":"demo-admin-0"},"status":{"phase":"Pending"}}
	]}`
	var list any
	if err := json.Unmarshal([]byte(pods), &list); err != nil {
		t.Fatal(err)
	}
	tmpl, err := template.New("t").Parse(imagePullBackOffTemplate)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	if err := tmpl.Execute(&out, list); err != nil {
		t.Fatalf("the template failed on a pod list a suite really sees: %v", err)
	}
	got := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(got) != 1 || !strings.HasPrefix(got[0], "demo-router-1: Back-off pulling image") {
		t.Fatalf("wanted only the backing-off pod named, got %q", out.String())
	}
}
