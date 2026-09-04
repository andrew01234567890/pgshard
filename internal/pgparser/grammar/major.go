//go:build !pg19

// Package grammar carries the bound grammar's major version alone, so a
// caller that needs only the number does not link libpg_query. The control
// plane binaries are built CGO_ENABLED=0 and cannot import the parser.
package grammar

// Major is the PostgreSQL major version the parser is built against.
const Major = 18
