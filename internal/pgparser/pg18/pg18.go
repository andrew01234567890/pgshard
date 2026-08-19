// Package pg18 parses SQL with the PostgreSQL 18 grammar (libpg_query 18)
// and returns the typed protobuf AST from pgquerypb.
package pg18

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	libpgquery "github.com/andrew01234567890/pgshard/third_party/libpg_query/18"
)

// Major is the PostgreSQL major version whose grammar this package implements.
const Major = 18

// SyntaxErrorSQLState is the SQLSTATE PostgreSQL reports for grammar errors.
const SyntaxErrorSQLState = "42601"

// Error is a grammar error. Position is the 1-based character offset into
// the input, or 0 when the parser did not report one.
type Error struct {
	Message  string
	Position int
	SQLState string
}

func (e *Error) Error() string { return e.Message }

func wrap(err error) error {
	if err == nil {
		return nil
	}
	var le *libpgquery.Error
	if errors.As(err, &le) {
		return &Error{Message: le.Message, Position: le.Cursorpos, SQLState: SyntaxErrorSQLState}
	}
	return err
}

// Parse returns the parse tree for sql or an *Error.
func Parse(sql string) (*pgquerypb.ParseResult, error) {
	b, err := libpgquery.ParseProtobuf(sql)
	if err != nil {
		return nil, wrap(err)
	}
	var res pgquerypb.ParseResult
	if err := proto.Unmarshal(b, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Scan returns the lexer tokens of sql.
func Scan(sql string) (*pgquerypb.ScanResult, error) {
	b, err := libpgquery.ScanProtobuf(sql)
	if err != nil {
		return nil, wrap(err)
	}
	var res pgquerypb.ScanResult
	if err := proto.Unmarshal(b, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Deparse renders a parse tree back to SQL text.
func Deparse(tree *pgquerypb.ParseResult) (string, error) {
	b, err := proto.Marshal(tree)
	if err != nil {
		return "", err
	}
	s, err := libpgquery.DeparseProtobuf(b)
	return s, wrap(err)
}

// Fingerprint returns the stable hash of the statement shape of sql.
func Fingerprint(sql string) (string, error) {
	s, err := libpgquery.Fingerprint(sql)
	return s, wrap(err)
}

// Normalize replaces literal constants in sql with $n placeholders.
func Normalize(sql string) (string, error) {
	s, err := libpgquery.Normalize(sql)
	return s, wrap(err)
}
