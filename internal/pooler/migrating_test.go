package pooler

import (
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestFenceMigratingRefusesOnlyNewPrepares(t *testing.T) {
	simple := func(sql string) *pgshardv1.ExecuteRequest {
		return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_SimpleQuery{SimpleQuery: &pgshardv1.SimpleQuery{Sql: sql}}}
	}
	parse := func(sql string) *pgshardv1.ExecuteRequest {
		return &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Sql: sql}}}
	}
	cases := []struct {
		name    string
		view    View
		req     *pgshardv1.ExecuteRequest
		refused bool
	}{
		{"not migrating", View{}, simple("PREPARE TRANSACTION 'x'"), false},
		{"prepare", View{Migrating: true}, simple("PREPARE TRANSACTION 'x'"), true},
		{"prepare lower case with leading space", View{Migrating: true}, simple("  prepare transaction 'x'"), true},
		{"commit prepared passes", View{Migrating: true}, simple("COMMIT PREPARED 'x'"), false},
		{"rollback prepared passes", View{Migrating: true}, simple("ROLLBACK PREPARED 'x'"), false},
		{"prepared statement passes", View{Migrating: true}, simple("PREPARE s AS SELECT 1"), false},
		{"read passes", View{Migrating: true}, simple("SELECT 1"), false},
		{"other extended messages pass", View{Migrating: true}, &pgshardv1.ExecuteRequest{Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}}, false},
		{"prepare through Parse", View{Migrating: true}, parse("PREPARE TRANSACTION 'x'"), true},
		{"other Parse passes", View{Migrating: true}, parse("SELECT 1"), false},
	}
	for _, tc := range cases {
		e := fenceMigrating(tc.view, tc.req)
		if (e != nil) != tc.refused {
			t.Errorf("%s: refused=%v want %v", tc.name, e != nil, tc.refused)
		}
		if e != nil && (e.Sqlstate != "57P03" || e.Hint == "") {
			t.Errorf("%s: %+v must be 57P03 with a retry hint", tc.name, e)
		}
	}
}
