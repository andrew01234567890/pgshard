// Package pgrepl is a minimal PostgreSQL streaming replication protocol
// client built on pgconn: IDENTIFY_SYSTEM, logical slot creation and
// removal, START_REPLICATION over CopyBoth and the XLogData, keepalive and
// standby status frames that flow on it.
package pgrepl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// LSN is a WAL position.
type LSN uint64

// String renders the LSN as X/Y.
func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l)) }

// ParseLSN parses the X/Y form.
func ParseLSN(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("pgrepl: bad lsn %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("pgrepl: bad lsn %q", s)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("pgrepl: bad lsn %q", s)
	}
	return LSN(h<<32 | l), nil
}

// Conn is a replication connection.
type Conn struct {
	pc *pgconn.PgConn
}

// Connect opens a logical replication connection (replication=database).
func Connect(ctx context.Context, dsn string) (*Conn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return ConnectConfig(ctx, cfg)
}

// ConnectConfig is Connect from a parsed config; cfg is copied before the
// replication parameter is added.
func ConnectConfig(ctx context.Context, cfg *pgconn.Config) (*Conn, error) {
	cfg = cfg.Copy()
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	pc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{pc: pc}, nil
}

// Close closes the connection.
func (c *Conn) Close(ctx context.Context) error { return c.pc.Close(ctx) }

// SystemInfo is the IDENTIFY_SYSTEM result.
type SystemInfo struct {
	SystemID string
	Timeline int32
	XLogPos  LSN
	DBName   string
}

func (c *Conn) simple(ctx context.Context, sql string) ([][]string, error) {
	results, err := c.pc.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	var rows [][]string
	for _, r := range results {
		for _, row := range r.Rows {
			cols := make([]string, len(row))
			for i, v := range row {
				cols[i] = string(v)
			}
			rows = append(rows, cols)
		}
	}
	return rows, nil
}

// IdentifySystem runs IDENTIFY_SYSTEM.
func (c *Conn) IdentifySystem(ctx context.Context) (SystemInfo, error) {
	rows, err := c.simple(ctx, "IDENTIFY_SYSTEM")
	if err != nil {
		return SystemInfo{}, err
	}
	if len(rows) != 1 || len(rows[0]) < 4 {
		return SystemInfo{}, errors.New("pgrepl: unexpected IDENTIFY_SYSTEM result")
	}
	tl, err := strconv.ParseInt(rows[0][1], 10, 32)
	if err != nil {
		return SystemInfo{}, err
	}
	pos, err := ParseLSN(rows[0][2])
	if err != nil {
		return SystemInfo{}, err
	}
	return SystemInfo{SystemID: rows[0][0], Timeline: int32(tl), XLogPos: pos, DBName: rows[0][3]}, nil
}

// SlotOptions configures CREATE_REPLICATION_SLOT ... LOGICAL.
type SlotOptions struct {
	Temporary bool
	TwoPhase  bool
	Failover  bool
	// Snapshot is export, use, nothing or empty for the server default.
	Snapshot string
}

// SlotInfo is the CREATE_REPLICATION_SLOT result.
type SlotInfo struct {
	Name            string
	ConsistentPoint LSN
	SnapshotName    string
	OutputPlugin    string
}

// CreateLogicalSlot creates a logical slot with the PG17+ option syntax.
func (c *Conn) CreateLogicalSlot(ctx context.Context, name, plugin string, o SlotOptions) (SlotInfo, error) {
	var opts []string
	if o.TwoPhase {
		opts = append(opts, "TWO_PHASE")
	}
	if o.Failover {
		opts = append(opts, "FAILOVER")
	}
	if o.Snapshot != "" {
		opts = append(opts, "SNAPSHOT "+quoteLiteral(o.Snapshot))
	}
	sql := "CREATE_REPLICATION_SLOT " + name
	if o.Temporary {
		sql += " TEMPORARY"
	}
	sql += " LOGICAL " + plugin
	if len(opts) > 0 {
		sql += " (" + strings.Join(opts, ", ") + ")"
	}
	rows, err := c.simple(ctx, sql)
	if err != nil {
		return SlotInfo{}, err
	}
	if len(rows) != 1 || len(rows[0]) < 4 {
		return SlotInfo{}, errors.New("pgrepl: unexpected CREATE_REPLICATION_SLOT result")
	}
	pos, err := ParseLSN(rows[0][1])
	if err != nil {
		return SlotInfo{}, err
	}
	return SlotInfo{Name: rows[0][0], ConsistentPoint: pos, SnapshotName: rows[0][2], OutputPlugin: rows[0][3]}, nil
}

// DropSlot drops a slot, waiting for an active user to disconnect when
// wait is true.
func (c *Conn) DropSlot(ctx context.Context, name string, wait bool) error {
	sql := "DROP_REPLICATION_SLOT " + name
	if wait {
		sql += " WAIT"
	}
	_, err := c.simple(ctx, sql)
	return err
}

// StartReplication issues START_REPLICATION SLOT ... LOGICAL and returns once
// the server entered CopyBoth mode; options are rendered in key order.
func (c *Conn) StartReplication(ctx context.Context, slot string, start LSN, options map[string]string) error {
	keys := make([]string, 0, len(options))
	for k := range options {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	opts := make([]string, len(keys))
	for i, k := range keys {
		opts[i] = k + " " + quoteLiteral(options[k])
	}
	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s", slot, start)
	if len(opts) > 0 {
		sql += " (" + strings.Join(opts, ", ") + ")"
	}
	fe := c.pc.Frontend()
	fe.SendQuery(&pgproto3.Query{String: sql})
	if err := fe.Flush(); err != nil {
		return err
	}
	for {
		msg, err := c.pc.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.NoticeResponse:
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(m)
		case *pgproto3.CopyBothResponse:
			return nil
		default:
			return fmt.Errorf("pgrepl: unexpected %T before CopyBothResponse", msg)
		}
	}
}

// XLogData is one WAL payload frame.
type XLogData struct {
	WALStart     LSN
	ServerWALEnd LSN
	ServerTime   time.Time
	Data         []byte
}

// PrimaryKeepalive is a server liveness frame.
type PrimaryKeepalive struct {
	ServerWALEnd   LSN
	ServerTime     time.Time
	ReplyRequested bool
}

// ErrStreamEnded reports that the server closed the CopyBoth stream.
var ErrStreamEnded = errors.New("pgrepl: replication stream ended")

// Receive reads the next replication frame: *XLogData or *PrimaryKeepalive.
// A deadline on ctx surfaces as a timeout error that leaves the connection
// usable, so callers can interleave status updates with idle waits.
func (c *Conn) Receive(ctx context.Context) (any, error) {
	for {
		msg, err := c.pc.ReceiveMessage(ctx)
		if err != nil {
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			return parseFrame(m.Data)
		case *pgproto3.ErrorResponse:
			return nil, pgconn.ErrorResponseToPgError(m)
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		case *pgproto3.CopyDone, *pgproto3.CommandComplete, *pgproto3.ReadyForQuery:
			return nil, ErrStreamEnded
		default:
			return nil, fmt.Errorf("pgrepl: unexpected %T on replication stream", msg)
		}
	}
}

// IsTimeout reports whether err is a deadline expiry from Receive.
func IsTimeout(err error) bool { return pgconn.Timeout(err) }

func parseFrame(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	switch data[0] {
	case 'w':
		if len(data) < 25 {
			return nil, fmt.Errorf("pgrepl: short XLogData frame (%d bytes)", len(data))
		}
		return &XLogData{
			WALStart:     LSN(binary.BigEndian.Uint64(data[1:])),
			ServerWALEnd: LSN(binary.BigEndian.Uint64(data[9:])),
			ServerTime:   pgTime(int64(binary.BigEndian.Uint64(data[17:]))),
			Data:         append([]byte(nil), data[25:]...),
		}, nil
	case 'k':
		if len(data) < 18 {
			return nil, fmt.Errorf("pgrepl: short keepalive frame (%d bytes)", len(data))
		}
		return &PrimaryKeepalive{
			ServerWALEnd:   LSN(binary.BigEndian.Uint64(data[1:])),
			ServerTime:     pgTime(int64(binary.BigEndian.Uint64(data[9:]))),
			ReplyRequested: data[17] != 0,
		}, nil
	default:
		return nil, fmt.Errorf("pgrepl: unknown replication frame %q", data[0])
	}
}

// StandbyStatus is the positions the client reports back.
type StandbyStatus struct {
	Written        LSN
	Flushed        LSN
	Applied        LSN
	ReplyRequested bool
}

// SendStandbyStatus sends a Standby status update frame.
func (c *Conn) SendStandbyStatus(st StandbyStatus) error {
	buf := make([]byte, 34)
	buf[0] = 'r'
	binary.BigEndian.PutUint64(buf[1:], uint64(st.Written))
	binary.BigEndian.PutUint64(buf[9:], uint64(st.Flushed))
	binary.BigEndian.PutUint64(buf[17:], uint64(st.Applied))
	binary.BigEndian.PutUint64(buf[25:], uint64(toPGTime(time.Now())))
	if st.ReplyRequested {
		buf[33] = 1
	}
	fe := c.pc.Frontend()
	fe.Send(&pgproto3.CopyData{Data: buf})
	return fe.Flush()
}

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func pgTime(micros int64) time.Time { return pgEpoch.Add(time.Duration(micros) * time.Microsecond) }

func toPGTime(t time.Time) int64 { return t.Sub(pgEpoch).Microseconds() }

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
