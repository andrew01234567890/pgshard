package plan

import (
	"context"
	"strings"
	"testing"
)

// Building the merge rewrote a SELECT a DECLARE does not have at its top,
// and the planner dereferenced nil. The statement gets the refusal the
// router documents for it instead.
func TestADeclaredMultiShardCursorIsRefusedNotAPanic(t *testing.T) {
	for _, sql := range []string{
		"declare c cursor for select id from orders order by id limit 2",
		"declare c cursor for select id from orders offset 3",
		"declare c cursor for select id from orders order by qty + 1",
		"declare c cursor for select count(*) from orders",
		"create view v as select id from orders order by id limit 2",
	} {
		_, err := New().Plan(context.Background(), session(fixture(t)), sql)
		if err == nil || !strings.Contains(err.Error(), "only a plain SELECT can run on multiple shards") {
			t.Fatalf("%s: err = %v", sql, err)
		}
	}
}
