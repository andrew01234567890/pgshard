package pgwire

import (
	"context"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TxStatus is the transaction indicator carried by ReadyForQuery.
type TxStatus byte

// Transaction states as defined by the protocol.
const (
	TxIdle    TxStatus = 'I'
	TxInBlock TxStatus = 'T'
	TxFailed  TxStatus = 'E'
)

// DescribeKind selects the target of Describe and Close.
type DescribeKind byte

// Targets of Describe and Close.
const (
	DescribeStatement DescribeKind = 'S'
	DescribePortal    DescribeKind = 'P'
)

// SessionInfo describes an authenticated client session.
type SessionInfo struct {
	ID              uint64
	User            string
	Database        string
	Params          map[string]string
	ProtocolVersion uint32
	Auth            *AuthResult
	// RemoteAddr is the peer address string, empty when unknown.
	RemoteAddr string
}

// ResultWriter is how an Executor streams protocol messages back to the
// client while it processes one query or portal execution.
type ResultWriter interface {
	RowDescription(fields []pgproto3.FieldDescription) error
	DataRow(values [][]byte) error
	CommandComplete(tag string) error
	EmptyQueryResponse() error
	// ParameterDescription and NoData answer Describe.
	ParameterDescription(oids []uint32) error
	NoData() error
	// PortalSuspended reports that Execute stopped at its row limit.
	PortalSuspended() error
	// ParseComplete, BindComplete and CloseComplete answer Parse, Bind and
	// Close. They belong to the executor rather than to the session
	// because an executor that stages a batch answers them where the
	// backend does -- in among the batch's other responses -- and a
	// session that answered them as the messages arrived would send the
	// second statement's ParseComplete before the first statement's
	// results.
	ParseComplete() error
	BindComplete() error
	CloseComplete() error
	Notice(*pgproto3.NoticeResponse) error
	Notification(*pgproto3.NotificationResponse) error
	// ParameterStatus reports a GUC_REPORT setting whose value changed on
	// the server, so a driver's cached view of the session matches the
	// backend that ran the statement.
	ParameterStatus(name, value string) error
	// CopyIn starts a COPY FROM STDIN transfer; the returned stream yields
	// CopyData payloads until CopyDone (io.EOF) or CopyFail (ErrCopyFail).
	CopyIn(overallFormat byte, columnFormats []uint16) (CopyInStream, error)
	CopyOut(overallFormat byte, columnFormats []uint16) error
	CopyData([]byte) error
	CopyDone() error
}

// CopyInStream reads client-supplied COPY data.
type CopyInStream interface {
	Next() ([]byte, error)
}

// Executor runs queries on behalf of one session. Every method is called from
// the session goroutine; the context is cancelled on client cancel or drain.
type Executor interface {
	SimpleQuery(ctx context.Context, sql string, w ResultWriter) error
	// Parse, Bind and Close take a writer because they own their own
	// completion message: an executor that answers the statement itself
	// writes it at once, and one that stages the statement leaves it to
	// whatever answers the batch, so that the client reads one statement's
	// messages before the next one's.
	Parse(ctx context.Context, name, sql string, paramOIDs []uint32, w ResultWriter) error
	Bind(ctx context.Context, portal, statement string, paramFormats []int16, params [][]byte, resultFormats []int16, w ResultWriter) error
	Describe(ctx context.Context, kind DescribeKind, name string, w ResultWriter) error
	Execute(ctx context.Context, portal string, maxRows int32, w ResultWriter) error
	Close(ctx context.Context, kind DescribeKind, name string, w ResultWriter) error
	// Sync is called for every Sync message so the executor can end an
	// implicit transaction block.
	Sync(ctx context.Context) error
	// Flush is called for every Flush message: the batch staged so far
	// runs and its responses reach the client, without ending the batch
	// and without a ReadyForQuery. An executor that cannot answer a
	// particular batch early returns nil and leaves it staged for Sync.
	Flush(ctx context.Context, w ResultWriter) error
	// BeginImplicit opens the transaction a multi-statement simple query
	// runs in. PostgreSQL wraps such a batch in one transaction nobody
	// asked for: each statement sees the ones before it, and an error
	// anywhere undoes all of them. It is called only when the session is
	// not already in a transaction of its own.
	BeginImplicit(ctx context.Context) error
	// EndImplicit commits or rolls back what BeginImplicit opened. It does
	// nothing when a BEGIN in the batch took that transaction over: it is
	// the client's then, as in PostgreSQL.
	EndImplicit(ctx context.Context, commit bool) error
	TransactionStatus() TxStatus
	// Release frees any resources when the session ends.
	Release()
}
