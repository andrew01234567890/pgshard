package dockertest

import "testing"

// TestUnavailableFailsWhenRequired: the whole point of the package is that a
// missing daemon or image stops being a silent skip in CI. A skip is not a
// failure, so a job whose container-backed suites all skipped still reports
// green -- which is how every PostgreSQL-backed test came to contribute
// nothing to the merge gate.
func TestUnavailableFailsWhenRequired(t *testing.T) {
	t.Setenv(RequireEnv, "1")
	if !Required() {
		t.Fatal("Required() is false with the variable set to 1")
	}
	fake := &testing.T{}
	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		Unavailable(fake, "image %s unavailable", "x")
	}()
	<-done
	if !fake.Failed() {
		t.Fatal("Unavailable did not fail the test while containers were required")
	}
}

func TestUnavailableSkipsOtherwise(t *testing.T) {
	t.Setenv(RequireEnv, "")
	if Required() {
		t.Fatal("Required() is true with the variable empty")
	}
	fake := &testing.T{}
	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		Unavailable(fake, "image %s unavailable", "x")
	}()
	<-done
	if fake.Failed() {
		t.Fatal("Unavailable failed the test when a developer simply has no docker")
	}
	if !fake.Skipped() {
		t.Fatal("Unavailable did not skip")
	}
}

// Any value other than exactly "1" must not arm the guard, so an accidental
// PGSHARD_REQUIRE_DOCKER=0 or =false does not silently make CI green again.
func TestOnlyOneArmsTheGuard(t *testing.T) {
	for _, v := range []string{"0", "false", "no", "true", "yes"} {
		t.Setenv(RequireEnv, v)
		if Required() {
			t.Errorf("%q armed the guard; only \"1\" may", v)
		}
	}
}
