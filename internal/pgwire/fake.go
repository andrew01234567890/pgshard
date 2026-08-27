package pgwire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
)

// FakeExecutor is a canned Executor for tests and the router's development
// mode. It answers "select 1", tracks BEGIN/COMMIT/ROLLBACK for the
// transaction indicator and rejects everything else.
type FakeExecutor struct {
	mu         sync.Mutex
	tx         TxStatus
	statements map[string]string
	portals    map[string]portal
	// Delay, when set, is a hook that runs before every SimpleQuery; tests
	// use it to hold a query open while exercising cancel and drain.
	Delay func(ctx context.Context) error
	// SyncDelay is the same hook for Sync, where an extended-protocol batch
	// actually runs.
	SyncDelay func(ctx context.Context) error
	// FlushFn answers a client Flush; nil leaves the batch for Sync.
	FlushFn func(ctx context.Context, w ResultWriter) error
}

// NewFakeExecutor returns a fresh idle FakeExecutor.
func NewFakeExecutor() *FakeExecutor {
	return &FakeExecutor{tx: TxIdle, statements: map[string]string{}, portals: map[string]portal{}}
}

type portal struct {
	sql    string
	binary bool
}

var selectOneFields = []pgproto3.FieldDescription{{
	Name: []byte("?column?"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1,
}}

func normalize(sql string) string {
	return strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";")))
}

func (f *FakeExecutor) fail(err error) error {
	if f.tx == TxInBlock {
		f.tx = TxFailed
	}
	return err
}

func (f *FakeExecutor) run(sql string, w ResultWriter, maxRows int32, binary bool) error {
	switch q := normalize(sql); q {
	case "":
		return w.EmptyQueryResponse()
	case "select 1":
		if f.tx == TxFailed {
			return Errorf("25P02", "current transaction is aborted, commands ignored until end of transaction block")
		}
		if maxRows < 0 {
			if err := w.RowDescription(selectOneFields); err != nil {
				return err
			}
		}
		one := []byte("1")
		if binary {
			one = []byte{0, 0, 0, 1}
		}
		if err := w.DataRow([][]byte{one}); err != nil {
			return err
		}
		return w.CommandComplete("SELECT 1")
	case "begin", "start transaction":
		f.tx = TxInBlock
		return w.CommandComplete("BEGIN")
	case "commit", "end":
		tag := "COMMIT"
		if f.tx == TxFailed {
			tag = "ROLLBACK"
		}
		f.tx = TxIdle
		return w.CommandComplete(tag)
	case "rollback", "abort":
		f.tx = TxIdle
		return w.CommandComplete("ROLLBACK")
	case "copy fake from stdin":
		return f.copyIn(w)
	default:
		return f.fail(Errorf(CodeSyntaxError, "fake executor: unsupported statement"))
	}
}

// copyIn accepts COPY data and reports the number of newline-terminated rows.
func (f *FakeExecutor) copyIn(w ResultWriter) error {
	in, err := w.CopyIn(0, nil)
	if err != nil {
		return err
	}
	rows := 0
	for {
		chunk, err := in.Next()
		if errors.Is(err, io.EOF) {
			return w.CommandComplete(fmt.Sprintf("COPY %d", rows))
		}
		if err != nil {
			return f.fail(Errorf("57014", "COPY from stdin failed: %v", err))
		}
		rows += bytes.Count(chunk, []byte{'\n'})
	}
}

// SimpleQuery implements Executor.
func (f *FakeExecutor) SimpleQuery(ctx context.Context, sql string, w ResultWriter) error {
	if f.Delay != nil {
		if err := f.Delay(ctx); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.run(sql, w, -1, false)
}

// Parse implements Executor.
func (f *FakeExecutor) Parse(_ context.Context, name, sql string, _ []uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if q := normalize(sql); q != "" && q != "select 1" && q != "copy fake from stdin" && q != "begin" && q != "commit" && q != "rollback" {
		return f.fail(Errorf(CodeSyntaxError, "fake executor: unsupported statement"))
	}
	f.statements[name] = sql
	return nil
}

// Bind implements Executor.
func (f *FakeExecutor) Bind(_ context.Context, name, statement string, _ []int16, _ [][]byte, resultFormats []int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	sql, ok := f.statements[statement]
	if !ok {
		return f.fail(Errorf("26000", "prepared statement %q does not exist", statement))
	}
	f.portals[name] = portal{sql: sql, binary: len(resultFormats) > 0 && resultFormats[0] == 1}
	return nil
}

// Describe implements Executor.
func (f *FakeExecutor) Describe(_ context.Context, kind DescribeKind, name string, w ResultWriter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sql string
	var ok bool
	if kind == DescribeStatement {
		sql, ok = f.statements[name]
		if !ok {
			return f.fail(Errorf("26000", "prepared statement %q does not exist", name))
		}
		if err := w.ParameterDescription(nil); err != nil {
			return err
		}
	} else {
		p, found := f.portals[name]
		if !found {
			return f.fail(Errorf("34000", "portal %q does not exist", name))
		}
		sql = p.sql
	}
	if normalize(sql) == "select 1" {
		return w.RowDescription(selectOneFields)
	}
	return w.NoData()
}

// Execute implements Executor.
func (f *FakeExecutor) Execute(_ context.Context, portal string, maxRows int32, w ResultWriter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.portals[portal]
	if !ok {
		return f.fail(Errorf("34000", "portal %q does not exist", portal))
	}
	if maxRows < 0 {
		maxRows = 0
	}
	return f.run(p.sql, w, maxRows, p.binary)
}

// Close implements Executor.
func (f *FakeExecutor) Close(_ context.Context, kind DescribeKind, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if kind == DescribeStatement {
		delete(f.statements, name)
	} else {
		delete(f.portals, name)
	}
	return nil
}

// Sync implements Executor.
func (f *FakeExecutor) Sync(ctx context.Context) error {
	if f.SyncDelay != nil {
		return f.SyncDelay(ctx)
	}
	return nil
}

// Flush implements Executor. FlushFn lets a test answer a Flush; the
// default leaves the batch staged for Sync, which is what an executor that
// cannot answer this batch early does.
func (f *FakeExecutor) Flush(ctx context.Context, w ResultWriter) error {
	if f.FlushFn != nil {
		return f.FlushFn(ctx, w)
	}
	return nil
}

// TransactionStatus implements Executor.
func (f *FakeExecutor) TransactionStatus() TxStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tx
}

// Release implements Executor.
func (f *FakeExecutor) Release() {}
