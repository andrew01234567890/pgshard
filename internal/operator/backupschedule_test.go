package operator

import (
	"os"
	"regexp"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// crdSchedulePattern reads the admission pattern out of the CRD that is
// actually shipped, so this test cannot drift from what the API server
// enforces.
func crdSchedulePattern(t *testing.T) *regexp.Regexp {
	t.Helper()
	raw, err := os.ReadFile("../../config/crd/bases/pgshard.io_pgshardbackuppolicies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}
	spec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	pattern := spec.Properties["barrierSchedule"].Pattern
	if pattern == "" {
		t.Fatal("barrierSchedule has no pattern")
	}
	for _, name := range []string{"full", "differential", "incremental"} {
		if got := spec.Properties["schedules"].Properties[name].Pattern; got != pattern {
			t.Fatalf("schedules.%s pattern %q differs from barrierSchedule's", name, got)
		}
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatal(err)
	}
	return re
}

// TestAdmissionNeverRefusesAScheduleTheParserAccepts: the pattern is a
// shape check that runs before the parser ever sees the expression, so the
// one thing it must not do is refuse something that works. The parser stays
// the authority on what a field may contain.
func TestAdmissionNeverRefusesAScheduleTheParserAccepts(t *testing.T) {
	re := crdSchedulePattern(t)
	for _, expr := range []string{
		"0 2 * * *", "*/15 * * * *", "0 0 1 * *", "0 3 * * 0",
		"5 4 * * sun", "*/15 1-5 * JAN-MAR MON,TUE", "0 0,12 1 */2 *",
		"0 22 * * 1-5", "23 0-20/2 * * *", "0 4 8-14 * *",
		"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly",
		"@every 1h", "@every 1h30m", "@every 90m", "@every 30s", "@every 1h30m10s",
		// The parser tolerates space around the five fields, so the
		// pattern must too. It does not around a descriptor.
		"0 2 * * * ", " 0 2 * * *",
	} {
		if _, err := ParseSchedule(expr); err != nil {
			t.Fatalf("the corpus is wrong, %q does not parse: %v", expr, err)
		}
		if !re.MatchString(expr) {
			t.Errorf("admission refuses %q, which the parser accepts", expr)
		}
	}
}

// TestAdmissionRefusesASchedulesThatCanNeverFire: the shapes worth catching
// before an object is stored and clusters bind to it.
func TestAdmissionRefusesASchedulesThatCanNeverFire(t *testing.T) {
	re := crdSchedulePattern(t)
	for _, expr := range []string{
		"every night", "nightly", "0 2 * *", "0 2 * * * *", "@fortnightly",
		"@every", "daily please", "   ", " @daily ", "@daily ", "@EVERY 1h",
	} {
		if _, err := ParseSchedule(expr); err == nil {
			t.Fatalf("the corpus is wrong, %q parses", expr)
		}
		if re.MatchString(expr) {
			t.Errorf("admission accepts %q, which the parser rejects", expr)
		}
	}
	if !re.MatchString("") {
		t.Error("an unset schedule must stay writable")
	}
}
