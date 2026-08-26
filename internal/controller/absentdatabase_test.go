package controller

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestAbsentDatabaseOnlyMatchesTheMissingCatalog guards the one error the role
// verifier is allowed to pass over. A reshard target has no application
// database until the copier creates it, so grants cannot be applied there yet
// and reporting a failure every pass is noise. Every other dial failure is a
// real problem and must still be reported.
func TestAbsentDatabaseOnlyMatchesTheMissingCatalog(t *testing.T) {
	missing := &pgconn.PgError{Code: "3D000", Message: `database "app" does not exist`}
	if !absentDatabase(missing) {
		t.Error("a missing database must be skipped")
	}
	if !absentDatabase(fmt.Errorf("dial target: %w", missing)) {
		t.Error("a wrapped missing database must be skipped")
	}

	for _, other := range []error{
		&pgconn.PgError{Code: "28P01", Message: "password authentication failed"},
		&pgconn.PgError{Code: "57P03", Message: "the database system is starting up"},
		errors.New("connection refused"),
		nil,
	} {
		if absentDatabase(other) {
			t.Errorf("%v must still be reported", other)
		}
	}
}
