package router

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Over the extended protocol a COPY FROM STDIN cannot end: PostgreSQL
// swallows the batch's Sync in copy-in mode, so no ReadyForQuery follows
// and the session waited for one for good. It is refused by name, and the
// simple-protocol COPY on the same connection still works.
func TestACopyFromStdinOverTheExtendedProtocolIsRefusedNotHung(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rr := conn.PgConn().ExecParams(ctx, "copy t from stdin", nil, nil, nil, nil).Read()
	if rr.Err == nil || !strings.Contains(rr.Err.Error(), "COPY FROM STDIN is available only as a simple query") {
		t.Fatalf("err = %v", rr.Err)
	}
	tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1\n2\n"), "copy t from stdin")
	if err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("simple COPY after the refusal: %q %v", tag, err)
	}
}
