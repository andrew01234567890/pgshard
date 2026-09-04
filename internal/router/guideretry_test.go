package router

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestGuideRetryExamplesMatchTheContract keeps the client guide's retry
// loops honest.
//
// PGS-446 left them out on the grounds that examples people copy into
// production belong somewhere they can be run rather than only read. Two of
// the three drivers have no harness here -- running them would mean a JVM
// and a pip install inside a container on every build -- so this asserts
// the property that mattered instead: a change to the retry contract breaks
// the examples rather than silently outdating them.
//
// What it checks is that each example retries exactly the codes the router
// refuses with, and that none of them retries the in-doubt code. That is
// the mistake the guide calls the dangerous one: 08007 means the original
// transaction may still commit, and a blanket retry duplicates it.
func TestGuideRetryExamplesMatchTheContract(t *testing.T) {
	const guide = "../../docs/guide/transactions.md"
	b, err := os.ReadFile(guide)
	if err != nil {
		t.Fatalf("reading the client guide: %v", err)
	}
	doc := string(b)

	start := strings.Index(doc, "### Retry loops")
	if start < 0 {
		t.Fatal("the guide has no retry-loops section; the examples PGS-584 asked for are what this test exists to protect")
	}
	end := strings.Index(doc[start:], "\nMetrics:")
	if end < 0 {
		t.Fatal("could not find the end of the retry-loops section")
	}
	section := doc[start : start+end]

	fence := regexp.MustCompile("(?s)```(go|python|java)\n(.*?)```")
	blocks := fence.FindAllStringSubmatch(section, -1)
	if len(blocks) != 3 {
		t.Fatalf("found %d examples, want one each for pgx, psycopg and JDBC", len(blocks))
	}

	sqlstate := regexp.MustCompile(`"(\d[0-9A-Z]{4})"`)
	for _, b := range blocks {
		lang, body := b[1], b[2]
		t.Run(lang, func(t *testing.T) {
			seen := map[string]bool{}
			for _, m := range sqlstate.FindAllStringSubmatch(body, -1) {
				seen[m[1]] = true
			}
			// Exactly the two safe-to-retry codes, and no others: an
			// example naming a third would be retrying something the
			// router does not promise is safe.
			for _, want := range []string{codeRetryable, deadlockDetected} {
				if !seen[want] {
					t.Errorf("the %s example does not test for %s, which the router uses for an outcome that is safe to retry", lang, want)
				}
				delete(seen, want)
			}
			if seen[codeInDoubt] {
				t.Errorf("the %s example names %s: an in-doubt commit may still land, and retrying it duplicates the transaction", lang, codeInDoubt)
			}
			delete(seen, codeInDoubt)
			for extra := range seen {
				t.Errorf("the %s example tests for %s, which is not part of the retry contract", lang, extra)
			}
		})
	}
}
