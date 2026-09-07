package pgwire

import (
	"errors"

	"github.com/jackc/pgx/v5/pgproto3"
)

// discardWriter drops everything an executor writes. The implicit
// transaction around a multi-statement simple query is the caller's, not
// the client's: PostgreSQL reports no CommandComplete for a BEGIN the
// client never sent, and a client counting tags against its own statements
// would be one ahead for the whole batch.
type discardWriter struct{}

func (discardWriter) RowDescription([]pgproto3.FieldDescription) error { return nil }
func (discardWriter) DataRow([][]byte) error                           { return nil }
func (discardWriter) CommandComplete(string) error                     { return nil }
func (discardWriter) EmptyQueryResponse() error                        { return nil }
func (discardWriter) ParameterDescription([]uint32) error              { return nil }
func (discardWriter) NoData() error                                    { return nil }
func (discardWriter) PortalSuspended() error                           { return nil }
func (discardWriter) Notice(*pgproto3.NoticeResponse) error            { return nil }
func (discardWriter) Notification(*pgproto3.NotificationResponse) error {
	return nil
}
func (discardWriter) ParameterStatus(string, string) error { return nil }
func (discardWriter) CopyIn(byte, []uint16) (CopyInStream, error) {
	return nil, errors.New("COPY FROM STDIN has no client to read from here")
}
func (discardWriter) CopyOut(byte, []uint16) error { return nil }
func (discardWriter) CopyData([]byte) error        { return nil }
func (discardWriter) CopyDone() error              { return nil }
