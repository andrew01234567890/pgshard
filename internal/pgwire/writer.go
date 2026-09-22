package pgwire

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// flushEveryBytes bounds what a result may hold before it reaches the
// client. Rows used to be flushed every 256 of them, which says nothing
// about how much is queued: a row is anything from a few bytes to a
// megabyte, so a wide result held far more than the client had asked for
// and a narrow one paid for a write every few kilobytes. Counting bytes
// bounds the memory either way and is the same thing a COPY needs, whose
// chunks have no fixed size at all.
const flushEveryBytes = 64 << 10

// resultWriter implements ResultWriter over the session's backend buffer.
type resultWriter struct {
	s     *session
	ioErr error
}

func (w *resultWriter) send(m pgproto3.BackendMessage) error {
	if w.ioErr != nil {
		return w.ioErr
	}
	if err := w.s.sendMsg(m); err != nil {
		w.ioErr = err
		return err
	}
	return nil
}

func (w *resultWriter) flush() error {
	if w.ioErr != nil {
		return w.ioErr
	}
	w.ioErr = w.s.flush()
	return w.ioErr
}

// queued records bytes handed to the backend buffer and flushes once the
// client is owed enough of them.
func (w *resultWriter) queued(n int) error {
	w.s.queued += n
	if w.s.queued >= flushEveryBytes {
		return w.flush()
	}
	return nil
}

func (w *resultWriter) RowDescription(fields []pgproto3.FieldDescription) error {
	return w.send(&pgproto3.RowDescription{Fields: fields})
}

func (w *resultWriter) DataRow(values [][]byte) error {
	if w.ioErr != nil {
		return w.ioErr
	}
	if err := w.s.appendRow(values); err != nil {
		w.ioErr = err
		return err
	}
	return w.queued(dataRowBytes(values))
}

// dataRowBytes is what a DataRow takes on the wire: the message header and
// column count, then a length per column and the value behind it. A NULL
// is the length alone.
func dataRowBytes(values [][]byte) int {
	n := 5 + 2
	for _, v := range values {
		n += 4 + len(v)
	}
	return n
}

func (w *resultWriter) CommandComplete(tag string) error {
	return w.send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

func (w *resultWriter) EmptyQueryResponse() error { return w.send(&pgproto3.EmptyQueryResponse{}) }

func (w *resultWriter) ParameterDescription(oids []uint32) error {
	return w.send(&pgproto3.ParameterDescription{ParameterOIDs: oids})
}

func (w *resultWriter) NoData() error { return w.send(&pgproto3.NoData{}) }

func (w *resultWriter) PortalSuspended() error { return w.send(&pgproto3.PortalSuspended{}) }
func (w *resultWriter) ParseComplete() error   { return w.send(&pgproto3.ParseComplete{}) }
func (w *resultWriter) BindComplete() error    { return w.send(&pgproto3.BindComplete{}) }
func (w *resultWriter) CloseComplete() error   { return w.send(&pgproto3.CloseComplete{}) }

func (w *resultWriter) ParameterStatus(name, value string) error {
	return w.send(&pgproto3.ParameterStatus{Name: name, Value: value})
}

func (w *resultWriter) Notice(n *pgproto3.NoticeResponse) error { return w.send(n) }

// Flush writes everything sent so far to the client now, for a statement
// that tells the client something long before it answers.
func (w *resultWriter) Flush() error { return w.flush() }
func (w *resultWriter) Notification(n *pgproto3.NotificationResponse) error {
	return w.send(n)
}

func (w *resultWriter) CopyIn(overallFormat byte, columnFormats []uint16) (CopyInStream, error) {
	if err := w.send(&pgproto3.CopyInResponse{OverallFormat: overallFormat, ColumnFormatCodes: columnFormats}); err != nil {
		return nil, err
	}
	if err := w.flush(); err != nil {
		return nil, err
	}
	cs := &copyInStream{s: w.s}
	w.s.setCopyIn(cs)
	return cs, nil
}

// CopyOut flushes the response rather than queueing it, so the client
// learns the COPY started before flushEveryBytes of it exists. A COPY that
// produces its rows slowly otherwise showed the client nothing at all until
// enough had accumulated, which is not what the byte bound is for.
func (w *resultWriter) CopyOut(overallFormat byte, columnFormats []uint16) error {
	if err := w.send(&pgproto3.CopyOutResponse{OverallFormat: overallFormat, ColumnFormatCodes: columnFormats}); err != nil {
		return err
	}
	return w.flush()
}

func (w *resultWriter) CopyData(data []byte) error {
	if err := w.send(&pgproto3.CopyData{Data: data}); err != nil {
		return err
	}
	return w.queued(5 + len(data))
}

func (w *resultWriter) CopyDone() error { return w.send(&pgproto3.CopyDone{}) }

type copyInStream struct {
	s    *session
	done bool
}

// Next returns the next CopyData payload, io.EOF after CopyDone or
// ErrCopyFail after CopyFail. Flush and Sync are ignored in copy-in mode,
// as PostgreSQL ignores them; any other message ends the COPY with an
// error, as it does in PostgreSQL (copyfromparse.c, CopyGetData).
//
// Terminate is one of those. It used to read as CopyDone, so a client
// that went away in the middle of a load had the rows it had sent so far
// -- a half-sent last row included -- committed as if it had finished.
func (c *copyInStream) Next() ([]byte, error) {
	if c.done {
		return nil, io.EOF
	}
	for {
		msg, err := c.s.be.Receive()
		if err != nil {
			// The cancel path wakes this read by putting the deadline in
			// the past. Clear it, or every later read on this connection
			// fails too, and report the cancellation rather than the
			// timeout it arrived as.
			if c.s.queryCancelled() {
				_ = c.s.conn.SetReadDeadline(time.Time{})
				c.done = true
				c.s.setCopyIn(nil)
				return nil, ErrCopyFail
			}
			c.done = true
			c.s.setCopyIn(nil)
			// A read that failed part-way -- a CopyData declaring a body
			// larger than the frontend accepts, an unknown message type --
			// leaves the stream in the middle of a message. Carrying on
			// parsed the rest of that body as new messages: a query
			// smuggled inside a CopyData ran. PostgreSQL ends the
			// connection here too.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				c.s.copyLost = copyLostTerminated
			} else {
				c.s.copyLost = copyLostProtocol
			}
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			return append([]byte(nil), m.Data...), nil
		case *pgproto3.CopyDone:
			c.done = true
			c.s.setCopyIn(nil)
			return nil, io.EOF
		case *pgproto3.CopyFail:
			c.done = true
			c.s.setCopyIn(nil)
			return nil, ErrCopyFail
		case *pgproto3.Flush, *pgproto3.Sync:
		case *pgproto3.Terminate:
			c.done = true
			c.s.setCopyIn(nil)
			c.s.copyLost = copyLostTerminated
			return nil, ErrCopyTerminated
		default:
			c.done = true
			c.s.setCopyIn(nil)
			c.s.copyLost = copyLostProtocol
			return nil, Errorf(CodeProtocolViolation, "unexpected message type 0x%02X during COPY from stdin", messageType(m))
		}
	}
}

// ErrCopyTerminated is a client that sent Terminate in the middle of a COPY
// FROM STDIN: the load did not finish and must not be committed.
var ErrCopyTerminated = errors.New("pgwire: client terminated the connection during COPY from stdin")

// messageType is the protocol type byte of a frontend message.
func messageType(m pgproto3.FrontendMessage) byte {
	b, err := m.Encode(nil)
	if err != nil || len(b) == 0 {
		return 0
	}
	return b[0]
}
