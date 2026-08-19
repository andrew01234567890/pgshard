// Package libpgquery18 is the cgo binding to the vendored libpg_query sources
// for PostgreSQL 18. It exposes the raw C entry points and nothing else;
// internal/pgparser/pg18 builds the typed API on top of it.
package libpgquery18

/*
#cgo CFLAGS: -I${SRCDIR}/include -I${SRCDIR}/include/postgres -I${SRCDIR}/include/protobuf-c -I${SRCDIR}/include/xxhash
#cgo CFLAGS: -O2 -g -fstack-protector -std=gnu99 -fno-strict-aliasing -Wno-unknown-warning-option -Wno-typedef-redefinition -Wno-unused-function -Wno-unused-value -Wno-unused-variable -DXXH_NAMESPACE=PG_QUERY_
#include "pg_query.h"
#include <stdlib.h>

static PgQueryDeparseResult deparse_bytes(void *data, unsigned int len) {
	PgQueryProtobuf p;
	p.data = (char *) data;
	p.len = len;
	return pg_query_deparse_protobuf(p);
}
*/
import "C"

import (
	"unsafe"
)

// Error is a parse-time error reported by libpg_query.
type Error struct {
	Message   string
	Funcname  string
	Filename  string
	Lineno    int
	Cursorpos int
	Context   string
}

func (e *Error) Error() string { return e.Message }

func newError(c *C.PgQueryError) *Error {
	e := &Error{
		Message:   C.GoString(c.message),
		Lineno:    int(c.lineno),
		Cursorpos: int(c.cursorpos),
	}
	if c.funcname != nil {
		e.Funcname = C.GoString(c.funcname)
	}
	if c.filename != nil {
		e.Filename = C.GoString(c.filename)
	}
	if c.context != nil {
		e.Context = C.GoString(c.context)
	}
	return e
}

// ParseProtobuf parses sql and returns the serialized pg_query.ParseResult.
func ParseProtobuf(sql string) ([]byte, error) {
	in := C.CString(sql)
	defer C.free(unsafe.Pointer(in))
	res := C.pg_query_parse_protobuf(in)
	defer C.pg_query_free_protobuf_parse_result(res)
	if res.error != nil {
		return nil, newError(res.error)
	}
	return C.GoBytes(unsafe.Pointer(res.parse_tree.data), C.int(res.parse_tree.len)), nil
}

// ScanProtobuf tokenizes sql and returns the serialized pg_query.ScanResult.
func ScanProtobuf(sql string) ([]byte, error) {
	in := C.CString(sql)
	defer C.free(unsafe.Pointer(in))
	res := C.pg_query_scan(in)
	defer C.pg_query_free_scan_result(res)
	if res.error != nil {
		return nil, newError(res.error)
	}
	return C.GoBytes(unsafe.Pointer(res.pbuf.data), C.int(res.pbuf.len)), nil
}

// DeparseProtobuf renders a serialized pg_query.ParseResult back to SQL.
func DeparseProtobuf(tree []byte) (string, error) {
	var ptr unsafe.Pointer
	if len(tree) > 0 {
		ptr = unsafe.Pointer(&tree[0])
	}
	res := C.deparse_bytes(ptr, C.uint(len(tree)))
	defer C.pg_query_free_deparse_result(res)
	if res.error != nil {
		return "", newError(res.error)
	}
	return C.GoString(res.query), nil
}

// Fingerprint returns the libpg_query fingerprint of sql.
func Fingerprint(sql string) (string, error) {
	in := C.CString(sql)
	defer C.free(unsafe.Pointer(in))
	res := C.pg_query_fingerprint(in)
	defer C.pg_query_free_fingerprint_result(res)
	if res.error != nil {
		return "", newError(res.error)
	}
	return C.GoString(res.fingerprint_str), nil
}

// Normalize replaces constants in sql with positional placeholders.
func Normalize(sql string) (string, error) {
	in := C.CString(sql)
	defer C.free(unsafe.Pointer(in))
	res := C.pg_query_normalize(in)
	defer C.pg_query_free_normalize_result(res)
	if res.error != nil {
		return "", newError(res.error)
	}
	return C.GoString(res.normalized_query), nil
}
