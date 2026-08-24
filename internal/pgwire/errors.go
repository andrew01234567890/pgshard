package pgwire

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// SQLSTATE codes used by the wire layer itself.
const (
	CodeProtocolViolation    = "08P01"
	CodeFeatureNotSupported  = "0A000"
	CodeInvalidPassword      = "28P01"
	CodeInvalidAuthorization = "28000"
	CodeTooManyConnections   = "53300"
	CodeAdminShutdown        = "57P01"
	CodeQueryCanceled        = "57014"
	CodeSyntaxError          = "42601"
	CodeInternalError        = "XX000"
)

// Error is a PostgreSQL-style error that is sent to the client as an
// ErrorResponse. Executors return it to control the SQLSTATE the client sees.
type Error struct {
	Severity string
	Code     string
	Message  string
	Detail   string
	Hint     string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Errorf builds an ERROR-severity Error with the given SQLSTATE.
func Errorf(code, format string, args ...any) *Error {
	return &Error{Severity: "ERROR", Code: code, Message: fmt.Sprintf(format, args...)}
}

func toErrorResponse(err error) *pgproto3.ErrorResponse {
	var pe *Error
	if !errors.As(err, &pe) {
		pe = &Error{Severity: "ERROR", Code: CodeInternalError, Message: err.Error()}
	}
	sev := pe.Severity
	if sev == "" {
		sev = "ERROR"
	}
	return &pgproto3.ErrorResponse{
		Severity:            sev,
		SeverityUnlocalized: sev,
		Code:                pe.Code,
		Message:             pe.Message,
		Detail:              pe.Detail,
		Hint:                pe.Hint,
	}
}
