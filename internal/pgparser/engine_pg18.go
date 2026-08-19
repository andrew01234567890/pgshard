//go:build !pg19

package pgparser

import (
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18"
	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
	"google.golang.org/protobuf/proto"
)

// Major is the PostgreSQL major version of the bound grammar.
const Major = pg18.Major

func engineParse(sql string) (proto.Message, error) {
	tree, err := pg18.Parse(sql)
	if err != nil {
		return nil, wrapEngineError(err)
	}
	return tree, nil
}

func engineDeparse(tree proto.Message) (string, error) {
	pr, ok := tree.(*pgquerypb.ParseResult)
	if !ok {
		return "", &Error{Message: "deparse: tree is not a PostgreSQL 18 parse result", SQLState: InternalErrorSQLState}
	}
	s, err := pg18.Deparse(pr)
	return s, wrapEngineError(err)
}

func engineFingerprint(sql string) (string, error) {
	s, err := pg18.Fingerprint(sql)
	return s, wrapEngineError(err)
}

func engineNormalize(sql string) (string, error) {
	s, err := pg18.Normalize(sql)
	return s, wrapEngineError(err)
}

func wrapEngineError(err error) error {
	if err == nil {
		return nil
	}
	if pe, ok := err.(*pg18.Error); ok { //nolint:errorlint // pg18 never wraps its errors
		return &Error{Message: pe.Message, Position: pe.Position, SQLState: pe.SQLState}
	}
	return err
}
