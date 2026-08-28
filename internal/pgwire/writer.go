package pgwire

import (
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

const flushEveryRows = 256

// flushEveryCopyBytes bounds what a COPY OUT may hold before it reaches the
// client. Rows are flushed by count, but a COPY chunk is of no fixed size,
// so this one counts bytes: without it an export sat in the backend buffer
// until ReadyForQuery, making the router's memory proportional to the size
// of the export and the client's first byte wait for its last.
const flushEveryCopyBytes = 64 << 10

// resultWriter implements ResultWriter over the session's backend buffer.
type resultWriter struct {
	s     *session
	ioErr error
}

func (w *resultWriter) send(m pgproto3.BackendMessage) error {
	if w.ioErr != nil {
		return w.ioErr
	}
	w.s.be.Send(m)
	return nil
}

func (w *resultWriter) flush() error {
	if w.ioErr != nil {
		return w.ioErr
	}
	w.ioErr = w.s.be.Flush()
	return w.ioErr
}

func (w *resultWriter) RowDescription(fields []pgproto3.FieldDescription) error {
	return w.send(&pgproto3.RowDescription{Fields: fields})
}

func (w *resultWriter) DataRow(values [][]byte) error {
	if err := w.send(&pgproto3.DataRow{Values: values}); err != nil {
		return err
	}
	w.s.dataRows++
	if w.s.dataRows%flushEveryRows == 0 {
		return w.flush()
	}
	return nil
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

func (w *resultWriter) Notice(n *pgproto3.NoticeResponse) error { return w.send(n) }
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
	w.s.copyIn = &copyInStream{s: w.s}
	return w.s.copyIn, nil
}

func (w *resultWriter) CopyOut(overallFormat byte, columnFormats []uint16) error {
	w.s.copyBytes = 0
	return w.send(&pgproto3.CopyOutResponse{OverallFormat: overallFormat, ColumnFormatCodes: columnFormats})
}

func (w *resultWriter) CopyData(data []byte) error {
	if err := w.send(&pgproto3.CopyData{Data: data}); err != nil {
		return err
	}
	w.s.copyBytes += len(data)
	if w.s.copyBytes >= flushEveryCopyBytes {
		w.s.copyBytes = 0
		return w.flush()
	}
	return nil
}

func (w *resultWriter) CopyDone() error { return w.send(&pgproto3.CopyDone{}) }

type copyInStream struct {
	s    *session
	done bool
}

// Next returns the next CopyData payload, io.EOF after CopyDone or
// ErrCopyFail after CopyFail. Other messages during COPY are ignored, as
// PostgreSQL does (Flush and Sync are dropped in copy-in mode).
func (c *copyInStream) Next() ([]byte, error) {
	if c.done {
		return nil, io.EOF
	}
	for {
		msg, err := c.s.be.Receive()
		if err != nil {
			c.done = true
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			return append([]byte(nil), m.Data...), nil
		case *pgproto3.CopyDone:
			c.done = true
			c.s.copyIn = nil
			return nil, io.EOF
		case *pgproto3.CopyFail:
			c.done = true
			c.s.copyIn = nil
			return nil, ErrCopyFail
		case *pgproto3.Terminate:
			c.done = true
			return nil, io.EOF
		}
	}
}
