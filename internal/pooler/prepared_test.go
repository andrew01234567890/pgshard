package pooler

import (
	"strings"
	"testing"
)

// TestTouchesPreparedMatchesTheUppercaseScan pins the folding scan against
// the uppercase copy it replaced. A false negative here leaves stale
// prepared state on a backend that goes back to the pool, so the two have
// to agree on every input, including the Unicode ones that fold into ASCII.
func TestTouchesPreparedMatchesTheUppercaseScan(t *testing.T) {
	cases := []string{
		"select 1",
		"SELECT deallocated_at FROM t",
		"deallocate all",
		"DEALLOCATE st1",
		"  discard all ",
		"DiScArD SEQUENCES",
		"select 'deallocate' from t",
		"select * from discard",
		"",
		"D",
		"DEALLOCAT",
		"DISCARD",
		"select 'dıscard'",
		"DİSCARD",
		"sélect 1",
	}
	for _, sql := range cases {
		u := strings.ToUpper(sql)
		want := strings.Contains(u, "DEALLOCATE") || strings.Contains(u, "DISCARD")
		if got := touchesPrepared(sql); got != want {
			t.Errorf("touchesPrepared(%q) = %v, want %v", sql, got, want)
		}
	}
}

func BenchmarkTouchesPrepared(b *testing.B) {
	sql := "SELECT id, name, email, created_at FROM users WHERE tenant_id = $1 AND status = 'active' ORDER BY created_at DESC LIMIT 50"
	b.ReportAllocs()
	for b.Loop() {
		if touchesPrepared(sql) {
			b.Fatal("unexpected match")
		}
	}
}
