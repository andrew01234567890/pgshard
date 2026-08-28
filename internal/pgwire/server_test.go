package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type testServer struct {
	*Server
	addr string
	// sessions records every SessionInfo handed to NewExecutor.
	mu       sync.Mutex
	infos    []SessionInfo
	execs    []*FakeExecutor
	newExec  func(SessionInfo) (Executor, error)
	serveErr chan error
}

func startServer(t *testing.T, cfg Config) *testServer {
	t.Helper()
	return startServerOn(t, cfg, "tcp", "127.0.0.1:0")
}

func startServerOn(t *testing.T, cfg Config, network, listenAddr string) *testServer {
	t.Helper()
	ts := &testServer{serveErr: make(chan error, 1)}
	if cfg.Authenticator == nil {
		cfg.Authenticator = TrustAuthenticator{}
	}
	if cfg.NewExecutor == nil {
		cfg.NewExecutor = func(info SessionInfo) (Executor, error) {
			ts.mu.Lock()
			defer ts.mu.Unlock()
			ts.infos = append(ts.infos, info)
			if ts.newExec != nil {
				return ts.newExec(info)
			}
			e := NewFakeExecutor()
			ts.execs = append(ts.execs, e)
			return e, nil
		}
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen(network, listenAddr)
	if err != nil {
		t.Fatal(err)
	}
	ts.Server = srv
	ts.addr = l.Addr().String()
	go func() { ts.serveErr <- srv.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-ts.serveErr
	})
	return ts
}

func (ts *testServer) lastInfo(t *testing.T) SessionInfo {
	t.Helper()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.infos) == 0 {
		t.Fatal("no session was created")
	}
	return ts.infos[len(ts.infos)-1]
}

// rawClient is a pgproto3.Frontend over a TCP connection.
type rawClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	fe   *pgproto3.Frontend
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	return &rawClient{t: t, conn: conn, r: r, fe: pgproto3.NewFrontend(r, conn)}
}

func (c *rawClient) send(msgs ...pgproto3.FrontendMessage) {
	c.t.Helper()
	for _, m := range msgs {
		c.fe.Send(m)
	}
	if err := c.fe.Flush(); err != nil {
		c.t.Fatal(err)
	}
}

func (c *rawClient) recv() pgproto3.BackendMessage {
	c.t.Helper()
	m, err := c.fe.Receive()
	if err != nil {
		c.t.Fatalf("receive: %v", err)
	}
	return m
}

// rawStartup writes a startup packet with an arbitrary version number.
func (c *rawClient) rawStartup(version uint32, params map[string]string) {
	c.t.Helper()
	body := binary.BigEndian.AppendUint32(nil, version)
	for k, v := range params {
		body = append(body, k...)
		body = append(body, 0)
		body = append(body, v...)
		body = append(body, 0)
	}
	body = append(body, 0)
	pkt := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
	if _, err := c.conn.Write(append(pkt, body...)); err != nil {
		c.t.Fatal(err)
	}
}

// startupUntilReady drains the startup response and returns what it saw.
type startupResult struct {
	negotiate *pgproto3.NegotiateProtocolVersion
	params    map[string]string
	key       *pgproto3.BackendKeyData
	ready     *pgproto3.ReadyForQuery
	errResp   *pgproto3.ErrorResponse
}

func (c *rawClient) readStartup() startupResult {
	c.t.Helper()
	res := startupResult{params: map[string]string{}}
	for {
		m, err := c.fe.Receive()
		if err != nil {
			if res.errResp != nil && errors.Is(err, io.ErrUnexpectedEOF) {
				return res
			}
			c.t.Fatalf("receive: %v (so far %+v)", err, res)
		}
		switch m := m.(type) {
		case *pgproto3.NegotiateProtocolVersion:
			cp := *m
			res.negotiate = &cp
		case *pgproto3.AuthenticationOk:
		case *pgproto3.ParameterStatus:
			res.params[m.Name] = m.Value
		case *pgproto3.BackendKeyData:
			cp := *m
			cp.SecretKey = append([]byte(nil), m.SecretKey...)
			res.key = &cp
		case *pgproto3.ReadyForQuery:
			cp := *m
			res.ready = &cp
			return res
		case *pgproto3.ErrorResponse:
			cp := *m
			res.errResp = &cp
			return res
		default:
			c.t.Fatalf("unexpected startup message %T", m)
		}
	}
}

func (c *rawClient) startup(version uint32) startupResult {
	c.t.Helper()
	return c.startupAs(version, "alice")
}

func (c *rawClient) startupAs(version uint32, user string) startupResult {
	c.t.Helper()
	c.rawStartup(version, map[string]string{"user": user, "database": "db"})
	return c.readStartup()
}

func TestSimpleQueryProtocol30(t *testing.T) {
	ts := startServer(t, Config{ServerVersion: "18.6 (pgshard)"})
	c := dialRaw(t, ts.addr)
	res := c.startup(ProtocolVersion30)
	if res.negotiate != nil {
		t.Fatalf("unexpected negotiation %+v", res.negotiate)
	}
	if res.params["server_version"] != "18.6 (pgshard)" || res.params["session_authorization"] != "alice" {
		t.Fatalf("params = %v", res.params)
	}
	if _, has := res.params["search_path"]; has {
		t.Fatal("search_path must not be reported to a 3.0 client")
	}
	if len(res.key.SecretKey) != CancelKeyLen30 {
		t.Fatalf("3.0 cancel key len = %d", len(res.key.SecretKey))
	}
	if res.ready.TxStatus != 'I' {
		t.Fatalf("tx = %c", res.ready.TxStatus)
	}
	c.send(&pgproto3.Query{String: "select 1"})
	rd := c.recv().(*pgproto3.RowDescription)
	if string(rd.Fields[0].Name) != "?column?" || rd.Fields[0].DataTypeOID != 23 {
		t.Fatalf("row description %+v", rd)
	}
	if dr := c.recv().(*pgproto3.DataRow); string(dr.Values[0]) != "1" {
		t.Fatalf("data row %+v", dr)
	}
	if cc := c.recv().(*pgproto3.CommandComplete); string(cc.CommandTag) != "SELECT 1" {
		t.Fatalf("tag %s", cc.CommandTag)
	}
	if rq := c.recv().(*pgproto3.ReadyForQuery); rq.TxStatus != 'I' {
		t.Fatalf("tx = %c", rq.TxStatus)
	}
	c.send(&pgproto3.Query{String: "BEGIN"})
	c.recv()
	if rq := c.recv().(*pgproto3.ReadyForQuery); rq.TxStatus != 'T' {
		t.Fatalf("tx after BEGIN = %c", rq.TxStatus)
	}
	c.send(&pgproto3.Query{String: "garbage"})
	if er := c.recv().(*pgproto3.ErrorResponse); er.Code != CodeSyntaxError {
		t.Fatalf("code %s", er.Code)
	}
	if rq := c.recv().(*pgproto3.ReadyForQuery); rq.TxStatus != 'E' {
		t.Fatalf("tx after error = %c", rq.TxStatus)
	}
	c.send(&pgproto3.Query{String: ""})
	if _, ok := c.recv().(*pgproto3.EmptyQueryResponse); !ok {
		t.Fatal("expected EmptyQueryResponse")
	}
	c.recv()
	c.send(&pgproto3.Terminate{})
	if _, err := c.fe.Receive(); err == nil {
		t.Fatal("connection should close after Terminate")
	}
	waitNoSessions(t, ts.Server)
}

func waitNoSessions(t *testing.T, s *Server) {
	const n = 0
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.Sessions() != n {
		if time.Now().After(deadline) {
			t.Fatalf("sessions = %d, want %d", s.Sessions(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestProtocol32(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	res := c.startup(ProtocolVersion32)
	if res.negotiate != nil {
		t.Fatalf("unexpected negotiation %+v", res.negotiate)
	}
	if len(res.key.SecretKey) != CancelKeyLen32 {
		t.Fatalf("3.2 cancel key len = %d", len(res.key.SecretKey))
	}
	if binary.BigEndian.Uint32(res.key.SecretKey[:4]) != ts.InstanceID() {
		t.Fatal("cancel key does not carry the instance prefix")
	}
	if binary.BigEndian.Uint32(res.key.SecretKey[4:8]) != res.key.ProcessID {
		t.Fatal("cancel key does not carry the connection id")
	}
	if res.params["search_path"] != `"$user", public` {
		t.Fatalf("search_path = %q", res.params["search_path"])
	}
	if ts.lastInfo(t).ProtocolVersion != ProtocolVersion32 {
		t.Fatal("session did not record 3.2")
	}
}

func TestProtocol33NegotiatesDownTo32(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	res := c.startup(196611)
	if res.negotiate == nil || res.negotiate.NewestMinorProtocol != ProtocolVersion32 || len(res.negotiate.UnrecognizedOptions) != 0 {
		t.Fatalf("negotiate = %+v", res.negotiate)
	}
	if res.ready == nil || len(res.key.SecretKey) != CancelKeyLen32 {
		t.Fatalf("session did not continue as 3.2: %+v", res)
	}
	if ts.lastInfo(t).ProtocolVersion != ProtocolVersion32 {
		t.Fatal("effective version should be 3.2")
	}
}

func TestPQOptionsAreReportedUnsupported(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.rawStartup(ProtocolVersion30, map[string]string{"user": "u", "_pq_.zeta": "1", "_pq_.alpha": "2"})
	res := c.readStartup()
	if res.negotiate == nil || res.negotiate.NewestMinorProtocol != ProtocolVersion32 ||
		strings.Join(res.negotiate.UnrecognizedOptions, ",") != "_pq_.alpha,_pq_.zeta" {
		t.Fatalf("negotiate = %+v", res.negotiate)
	}
	if res.ready == nil || len(res.key.SecretKey) != CancelKeyLen30 {
		t.Fatalf("session should stay 3.0: %+v", res)
	}
	info := ts.lastInfo(t)
	if _, leaked := info.Params["_pq_.zeta"]; leaked || info.Database != "u" {
		t.Fatalf("info = %+v", info)
	}
}

func TestUnsupportedMajorVersion(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	res := c.startup(4 << 16)
	if res.errResp == nil || res.errResp.Code != CodeProtocolViolation || res.errResp.Severity != "FATAL" {
		t.Fatalf("err = %+v", res.errResp)
	}
	c = dialRaw(t, ts.addr)
	c.rawStartup(ProtocolVersion30, map[string]string{"database": "x"})
	if res := c.readStartup(); res.errResp == nil || res.errResp.Code != CodeInvalidAuthorization {
		t.Fatalf("missing user: %+v", res.errResp)
	}
}

func TestSSLAndGSSRequestsWithoutTLS(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.send(&pgproto3.SSLRequest{})
	if b, _ := c.r.ReadByte(); b != 'N' {
		t.Fatalf("SSLRequest answer %c", b)
	}
	c.send(&pgproto3.GSSEncRequest{})
	if b, _ := c.r.ReadByte(); b != 'N' {
		t.Fatalf("GSSEncRequest answer %c", b)
	}
	if res := c.startup(ProtocolVersion30); res.ready == nil {
		t.Fatalf("startup after refusals: %+v", res)
	}
}

func selfSigned(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "pgwire-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
}

func TestSSLRequestUpgradesToTLS(t *testing.T) {
	ts := startServer(t, Config{TLSConfig: selfSigned(t)})
	c := dialRaw(t, ts.addr)
	c.send(&pgproto3.SSLRequest{})
	if b, _ := c.r.ReadByte(); b != 'S' {
		t.Fatalf("SSLRequest answer %c", b)
	}
	tc := tls.Client(c.conn, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	c.conn = tc
	c.r = bufio.NewReader(tc)
	c.fe = pgproto3.NewFrontend(c.r, tc)
	if res := c.startup(ProtocolVersion30); res.ready == nil {
		t.Fatalf("startup over TLS: %+v", res)
	}
	c.send(&pgproto3.Query{String: "select 1"})
	c.recv()
	c.recv()
	c.recv()
	c.recv()

	for _, mode := range []string{"require", "prefer"} {
		conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://alice@%s/db?sslmode=%s", ts.addr, mode))
		if err != nil {
			t.Fatalf("pgx sslmode=%s: %v", mode, err)
		}
		if _, isTLS := conn.PgConn().Conn().(*tls.Conn); !isTLS {
			t.Fatalf("sslmode=%s: no TLS", mode)
		}
		_ = conn.Close(context.Background())
	}
	conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://alice@%s/db?sslmode=require&sslnegotiation=direct", ts.addr))
	if err != nil {
		t.Fatalf("direct TLS: %v", err)
	}
	_ = conn.Close(context.Background())
}

func pgxConnect(t *testing.T, addr, user, password string, extra string) (*pgx.Conn, error) {
	t.Helper()
	dsn := fmt.Sprintf("postgres://%s:%s@%s/db?sslmode=disable%s", user, password, addr, extra)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err == nil {
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
	}
	return conn, err
}

func selectOne(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("select 1: %d %v", n, err)
	}
}

func lookup(secrets map[string]string) PasswordLookup {
	return func(_ context.Context, user string) (string, error) {
		s, ok := secrets[user]
		if !ok {
			return "", errors.New("no such user")
		}
		return s, nil
	}
}

func assertAuthFailure(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("err = %v, want SQLSTATE %s", err, code)
	}
}

func TestAuthenticators(t *testing.T) {
	scram, err := BuildSCRAMVerifier("s3cret", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		auth Authenticator
	}{
		{"cleartext", CleartextAuthenticator{Lookup: lookup(map[string]string{"alice": "s3cret", "bob": "s3cret"})}},
		{"scram", SCRAMAuthenticator{Lookup: lookup(map[string]string{"alice": scram.String(), "bob": scram.String()})}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts := startServer(t, Config{Authenticator: c.auth})
			for _, user := range []string{"alice", "bob"} {
				conn, err := pgxConnect(t, ts.addr, user, "s3cret", "")
				if err != nil {
					t.Fatalf("%s: %v", user, err)
				}
				selectOne(t, conn)
				info := ts.lastInfo(t)
				if c.name == "scram" {
					if info.Auth == nil || info.Auth.SCRAM == nil || len(info.Auth.SCRAM.ClientKey) != 32 || string(info.Auth.SCRAM.ServerKey) != string(scram.ServerKey) {
						t.Fatal("SCRAM keys were not exposed to the executor factory")
					}
					sum := sha256Sum(info.Auth.SCRAM.ClientKey)
					if string(sum) != string(scram.StoredKey) {
						t.Fatal("recovered ClientKey does not hash to StoredKey")
					}
				} else if info.Auth == nil || info.Auth.SCRAM != nil {
					t.Fatalf("auth result = %+v", info.Auth)
				}
				_ = conn.Close(context.Background())
			}
			_, err := pgxConnect(t, ts.addr, "alice", "wrong", "")
			assertAuthFailure(t, err, CodeInvalidPassword)
			_, err = pgxConnect(t, ts.addr, "carol", "s3cret", "")
			assertAuthFailure(t, err, CodeInvalidPassword)
			waitNoSessions(t, ts.Server)
		})
	}
}

func TestSCRAMPlusIsRefused(t *testing.T) {
	scram, _ := BuildSCRAMVerifier("pw", nil, 0)
	ts := startServer(t, Config{Authenticator: SCRAMAuthenticator{Lookup: lookup(map[string]string{"alice": scram.String()})}})
	c := dialRaw(t, ts.addr)
	c.rawStartup(ProtocolVersion30, map[string]string{"user": "alice"})
	if _, ok := c.recv().(*pgproto3.AuthenticationSASL); !ok {
		t.Fatal("expected AuthenticationSASL")
	}
	c.send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256-PLUS", Data: []byte("p=tls-server-end-point,,n=alice,r=abc")})
	er, ok := c.recv().(*pgproto3.ErrorResponse)
	if !ok || er.Code != CodeInvalidAuthorization || !strings.Contains(er.Message, "SCRAM-SHA-256-PLUS") {
		t.Fatalf("got %+v", er)
	}
	// pgx with channel_binding=require must fail because only plain SCRAM is advertised.
	_, err := pgxConnect(t, ts.addr, "alice", "pw", "&channel_binding=require")
	if err == nil {
		t.Fatal("channel_binding=require should not succeed without -PLUS")
	}
}

func TestExtendedProtocolAndErrorRecovery(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(
		&pgproto3.Parse{Name: "s1", Query: "select 1"},
		&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "s1"},
		&pgproto3.Describe{ObjectType: 'S', Name: "s1"},
		&pgproto3.Describe{ObjectType: 'P', Name: "p1"},
		&pgproto3.Execute{Portal: "p1"},
		&pgproto3.Close{ObjectType: 'P', Name: "p1"},
		&pgproto3.Sync{},
	)
	wantSeq := []string{"*pgproto3.ParseComplete", "*pgproto3.BindComplete", "*pgproto3.ParameterDescription", "*pgproto3.RowDescription",
		"*pgproto3.RowDescription", "*pgproto3.DataRow", "*pgproto3.CommandComplete", "*pgproto3.CloseComplete", "*pgproto3.ReadyForQuery"}
	for _, want := range wantSeq {
		if got := fmt.Sprintf("%T", c.recv()); got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
	}
	// Error in Bind: everything until Sync is discarded, then ReadyForQuery.
	c.send(
		&pgproto3.Bind{DestinationPortal: "", PreparedStatement: "missing"},
		&pgproto3.Execute{Portal: ""},
		&pgproto3.Parse{Name: "s2", Query: "select 1"},
		&pgproto3.Flush{},
		&pgproto3.Sync{},
	)
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != "26000" {
		t.Fatalf("expected 26000, got %+v", er)
	}
	if _, ok := c.recv().(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("expected ReadyForQuery right after the error")
	}
	// s2 was skipped, so describing it fails; s1 still exists.
	c.send(&pgproto3.Describe{ObjectType: 'S', Name: "s2"}, &pgproto3.Sync{})
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != "26000" {
		t.Fatalf("s2 should not exist, got %+v", er)
	}
	c.recv()
	c.send(&pgproto3.Describe{ObjectType: 'S', Name: "s1"}, &pgproto3.Flush{})
	c.recv()
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("s1 should survive")
	}
	c.send(&pgproto3.Sync{})
	c.recv()
	c.send(&pgproto3.FunctionCall{Function: 1})
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != CodeFeatureNotSupported {
		t.Fatalf("function call: %+v", er)
	}
	c.recv()

	conn, err := pgxConnect(t, ts.addr, "alice", "x", "")
	if err != nil {
		t.Fatal(err)
	}
	selectOne(t, conn)
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if conn.PgConn().TxStatus() != 'T' {
		t.Fatalf("tx status %c", conn.PgConn().TxStatus())
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMultiStatementRefusal(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1; select 1"})
	er, ok := c.recv().(*pgproto3.ErrorResponse)
	if !ok || er.Code != CodeFeatureNotSupported || er.Message != "multi-statement simple queries are not supported" {
		t.Fatalf("got %+v", er)
	}
	c.recv()
	c.send(&pgproto3.Query{String: "select 'unterminated; select 1"})
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != CodeSyntaxError {
		t.Fatalf("got %+v", er)
	}
	c.recv()
	// A single statement full of embedded semicolons reaches the executor.
	c.send(&pgproto3.Query{String: "select $q$; select 2; $q$ -- ; trailing\n /* ; */"})
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != CodeSyntaxError || !strings.Contains(er.Message, "fake executor") {
		t.Fatalf("dollar-quoted statement should reach the executor, got %+v", er)
	}
	c.recv()
	c.send(&pgproto3.Query{String: "select 1 ;"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("trailing semicolon should be a single statement")
	}
}

func TestCopyInPlumbing(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "copy fake from stdin"})
	if _, ok := c.recv().(*pgproto3.CopyInResponse); !ok {
		t.Fatal("expected CopyInResponse")
	}
	c.send(&pgproto3.CopyData{Data: []byte("a\nb\n")}, &pgproto3.CopyData{Data: []byte("c\n")}, &pgproto3.CopyDone{})
	if cc, ok := c.recv().(*pgproto3.CommandComplete); !ok || string(cc.CommandTag) != "COPY 3" {
		t.Fatalf("got %+v", cc)
	}
	c.recv()
	c.send(&pgproto3.Query{String: "copy fake from stdin"})
	c.recv()
	c.send(&pgproto3.CopyFail{Message: "nope"})
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != "57014" {
		t.Fatalf("got %+v", er)
	}
	c.recv()
	// Stray CopyData outside COPY is ignored, and the session keeps working.
	c.send(&pgproto3.CopyData{Data: []byte("x")}, &pgproto3.CopyDone{}, &pgproto3.Query{String: "select 1"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("session should survive stray copy messages")
	}
}

func TestCancelRequest(t *testing.T) {
	ts := startServer(t, Config{})
	started := make(chan struct{}, 1)
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return e, nil
	}
	for _, version := range []uint32{ProtocolVersion30, ProtocolVersion32} {
		c := dialRaw(t, ts.addr)
		res := c.startup(version)
		c.send(&pgproto3.Query{String: "select 1"})
		<-started
		// A wrong key must not cancel.
		bad := append([]byte(nil), res.key.SecretKey...)
		bad[len(bad)-1] ^= 0xff
		if ts.CancelLocal(CancelKey{PID: res.key.ProcessID, Secret: bad}) {
			t.Fatal("wrong key was accepted")
		}
		canceller := dialRaw(t, ts.addr)
		canceller.send(&pgproto3.CancelRequest{ProcessID: res.key.ProcessID, SecretKey: res.key.SecretKey})
		if _, err := canceller.r.ReadByte(); err == nil {
			t.Fatal("cancel connection should be closed without a reply")
		}
		if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != CodeQueryCanceled {
			t.Fatalf("got %+v", er)
		}
		if rq := c.recv().(*pgproto3.ReadyForQuery); rq.TxStatus != 'I' {
			t.Fatalf("tx = %c", rq.TxStatus)
		}
	}
	// A custom handler receives keys instead.
	got := make(chan CancelKey, 1)
	ts2 := startServer(t, Config{CancelHandler: func(_ context.Context, k CancelKey) { got <- k }})
	c := dialRaw(t, ts2.addr)
	c.send(&pgproto3.CancelRequest{ProcessID: 7, SecretKey: []byte("secret")})
	select {
	case k := <-got:
		if k.PID != 7 || string(k.Secret) != "secret" {
			t.Fatalf("key = %+v", k)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
	if !ts2.OwnsCancelKey(CancelKey{Secret: []byte("1234")}) {
		t.Fatal("3.0 keys are always local")
	}
	foreign := make([]byte, CancelKeyLen32)
	binary.BigEndian.PutUint32(foreign, ts2.InstanceID()+1)
	if ts2.OwnsCancelKey(CancelKey{Secret: foreign}) {
		t.Fatal("foreign prefix reported as owned")
	}
}

func TestShutdownTerminatesIdleAndWaitsForActive(t *testing.T) {
	ts := startServer(t, Config{})
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return e, nil
	}
	idle := dialRaw(t, ts.addr)
	idle.startup(ProtocolVersion30)
	active := dialRaw(t, ts.addr)
	active.startup(ProtocolVersion30)
	active.send(&pgproto3.Query{String: "select 1"})
	<-started

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		shutdownDone <- ts.Shutdown(ctx)
	}()
	// This has failed in CI as a bare "i/o timeout" on the receive, which
	// says only that the terminate never arrived. Report what each session
	// looked like so an occurrence names the invariant that broke instead
	// of costing another 50-run matrix.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		t.Log("session states at failure:\n" + ts.sessionStates())
	})
	er, ok := idle.recv().(*pgproto3.ErrorResponse)
	if !ok || er.Code != CodeAdminShutdown || er.Severity != "FATAL" {
		t.Fatalf("idle session got %+v", er)
	}
	if _, err := idle.fe.Receive(); err == nil {
		t.Fatal("idle connection should be closed")
	}
	if _, err := net.Dial("tcp", ts.addr); err == nil {
		t.Fatal("listener should be closed")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned %v while a query was active", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if _, ok := active.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("active query should complete")
	}
	active.recv()
	active.recv()
	active.recv()
	if er, ok := active.recv().(*pgproto3.ErrorResponse); !ok || er.Code != CodeAdminShutdown {
		t.Fatalf("active session should be terminated after its query, got %+v", er)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if ts.Sessions() != 0 {
		t.Fatalf("sessions = %d", ts.Sessions())
	}
	if err := ts.Serve(nil); err == nil {
		t.Fatal("Serve after Shutdown must fail")
	}
}

func TestShutdownDeadlineForcesClose(t *testing.T) {
	ts := startServer(t, Config{})
	started := make(chan struct{}, 1)
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return e, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1"})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := ts.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown err = %v", err)
	}
	// The deadline forced a close; the client observes it on its next read,
	// which may lag the Shutdown return under load, so poll for the error.
	closed := false
	for i := 0; i < 200; i++ {
		if _, err := c.fe.Receive(); err != nil {
			closed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !closed {
		t.Fatal("connection should have been force-closed")
	}
	waitNoSessions(t, ts.Server)
}

func TestNewServerValidation(t *testing.T) {
	if _, err := NewServer(Config{}); err == nil {
		t.Fatal("missing authenticator accepted")
	}
	if _, err := NewServer(Config{Authenticator: TrustAuthenticator{}}); err == nil {
		t.Fatal("missing executor factory accepted")
	}
	s, err := NewServer(Config{Authenticator: TrustAuthenticator{}, NewExecutor: func(SessionInfo) (Executor, error) { return nil, nil }, InstanceID: 42})
	if err != nil || s.InstanceID() != 42 {
		t.Fatalf("%v %v", s, err)
	}
}

func TestExecutorFactoryErrorIsFatal(t *testing.T) {
	ts := startServer(t, Config{Authenticator: TrustAuthenticator{}, NewExecutor: func(SessionInfo) (Executor, error) {
		return nil, Errorf("53300", "too many connections")
	}})
	c := dialRaw(t, ts.addr)
	if res := c.startup(ProtocolVersion30); res.errResp == nil || res.errResp.Code != "53300" || res.errResp.Severity != "FATAL" {
		t.Fatalf("got %+v", res.errResp)
	}
}

func TestShutdownLetsOpenTransactionFinish(t *testing.T) {
	ts := startServer(t, Config{})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "begin"})
	c.recv() // CommandComplete
	if rfq, ok := c.recv().(*pgproto3.ReadyForQuery); !ok || rfq.TxStatus != 'T' {
		t.Fatalf("expected in-block ReadyForQuery, got %+v", rfq)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		shutdownDone <- ts.Shutdown(ctx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", ts.addr)
		if err != nil {
			break
		}
		_ = conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned %v while a transaction was open", err)
	case <-time.After(200 * time.Millisecond):
	}

	c.send(&pgproto3.Query{String: "select 1"})
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("statement inside an open transaction must still run during drain")
	}
	c.recv()
	c.recv()
	c.recv()
	c.send(&pgproto3.Query{String: "commit"})
	if cc, ok := c.recv().(*pgproto3.CommandComplete); !ok || string(cc.CommandTag) != "COMMIT" {
		t.Fatalf("commit during drain got %+v", cc)
	}
	c.recv()
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok || er.Code != CodeAdminShutdown {
		t.Fatalf("session should be terminated once the transaction ends, got %+v", er)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestLookupRefusalIsRelayedAs28000(t *testing.T) {
	auth := SCRAMAuthenticator{Lookup: func(_ context.Context, user string) (string, error) {
		return "", Errorf(CodeInvalidAuthorization, "role %q is not permitted to log in", user)
	}}
	ts := startServer(t, Config{Authenticator: auth})
	_, err := pgxConnect(t, ts.addr, "batch", "pw", "")
	assertAuthFailure(t, err, CodeInvalidAuthorization)
}

func TestStartupConnCapRefusesPolitely(t *testing.T) {
	ts := startServer(t, Config{MaxStartupConns: 1, StartupTimeout: time.Minute})
	stalled, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stalled.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for len(ts.startupSem) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("stalled connection never claimed the startup slot")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c := dialRaw(t, ts.addr)
	msg, ok := c.recv().(*pgproto3.ErrorResponse)
	if !ok || msg.Code != CodeTooManyConnections {
		t.Fatalf("got %+v, want SQLSTATE %s", msg, CodeTooManyConnections)
	}
}

func TestStartupTimeoutClosesHalfOpenConns(t *testing.T) {
	ts := startServer(t, Config{StartupTimeout: 100 * time.Millisecond})
	conn, err := net.Dial("tcp", ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("half-open startup was not closed by the server: %v", err)
	}
	waitNoSessions(t, ts.Server)
}

func TestStartupDeadlineCancelsStalledLookup(t *testing.T) {
	lookupCancelled := make(chan error, 1)
	auth := CleartextAuthenticator{Lookup: func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		lookupCancelled <- ctx.Err()
		return "", ctx.Err()
	}}
	ts := startServer(t, Config{Authenticator: auth, StartupTimeout: 200 * time.Millisecond, MaxStartupConns: 1})
	c := dialRaw(t, ts.addr)
	c.send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "alice"}})
	if _, ok := c.recv().(*pgproto3.AuthenticationCleartextPassword); !ok {
		t.Fatal("expected cleartext password request")
	}
	c.send(&pgproto3.PasswordMessage{Password: "pw"})
	select {
	case err := <-lookupCancelled:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lookup context ended with %v, want deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled Lookup was never cancelled by the startup deadline")
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(ts.startupSem) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("startup slot was never released after the stalled Lookup")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestPlaintextStartupRefusedWhenTLSIsConfigured: a client that simply
// omits SSLRequest must not be served in the clear by a deployment that
// configured a certificate, or the SCRAM exchange and everything after it
// is downgraded silently.
func TestPlaintextStartupRefusedWhenTLSIsConfigured(t *testing.T) {
	ts := startServer(t, Config{TLSConfig: selfSigned(t)})
	c := dialRaw(t, ts.addr)
	res := c.startup(ProtocolVersion30)
	if res.errResp == nil || res.errResp.Code != CodeInvalidAuthorization {
		t.Fatalf("plaintext startup against a TLS server: %+v", res)
	}
	if !strings.Contains(res.errResp.Message, "requires TLS") {
		t.Fatalf("refusal must say why: %q", res.errResp.Message)
	}
}

// TestPlaintextStartupAllowedByTheDevelopmentOptOut keeps the escape hatch
// honest: it exists, and it is the only thing that lets a plaintext
// startup through a server holding a certificate.
func TestPlaintextStartupAllowedByTheDevelopmentOptOut(t *testing.T) {
	ts := startServer(t, Config{TLSConfig: selfSigned(t), AllowPlaintext: true})
	c := dialRaw(t, ts.addr)
	if res := c.startup(ProtocolVersion30); res.ready == nil {
		t.Fatalf("opt-out must still serve plaintext: %+v", res)
	}
}

// TestTerminateUserEndsOnlyThatRolesSessions: revoking a role has to reach
// the sessions it already holds, including one sitting inside an open
// transaction - which is exactly where a client that just lost its access
// would sit - and must not disturb anyone else's.
func TestTerminateUserEndsOnlyThatRolesSessions(t *testing.T) {
	ts := startServer(t, Config{})
	revoked := dialRaw(t, ts.addr)
	if res := revoked.startupAs(ProtocolVersion30, "revoked"); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}
	// Hold a transaction open: a drain would wait for this forever.
	revoked.send(&pgproto3.Query{String: "begin"})
	revoked.recv()
	revoked.recv()
	inTxn := dialRaw(t, ts.addr)
	if res := inTxn.startupAs(ProtocolVersion30, "revoked"); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}
	kept := dialRaw(t, ts.addr)
	if res := kept.startupAs(ProtocolVersion30, "kept"); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}
	if n := ts.TerminateUser("revoked"); n != 2 {
		t.Fatalf("terminated %d sessions, want 2", n)
	}
	// Both revoked sessions must receive a FATAL and then end.
	for _, c := range []*rawClient{revoked, inTxn} {
		sawFatal, closed := false, false
		for range 20 {
			msg, err := c.fe.Receive()
			if err != nil {
				closed = true
				break
			}
			if e, ok := msg.(*pgproto3.ErrorResponse); ok && e.Severity == "FATAL" {
				sawFatal = true
			}
		}
		if !sawFatal {
			t.Fatal("a revoked session must be told why it ended")
		}
		if !closed {
			t.Fatal("a revoked session must actually be closed, not left open")
		}
	}
	kept.send(&pgproto3.Query{String: "select 1"})
	if msg := kept.recv(); msg == nil {
		t.Fatal("an unrelated role's session must keep working")
	}
}

// TestRevokingASessionStopsAnExtendedProtocolBatch: Sync is where an
// extended-protocol batch actually runs. If it is not cancellable a
// revocation waits for the statement instead of stopping it, holding up
// every later session in the same pass.
func TestRevokingASessionStopsAnExtendedProtocolBatch(t *testing.T) {
	running := make(chan struct{})
	ts := startServer(t, Config{})
	ts.newExec = func(SessionInfo) (Executor, error) {
		f := NewFakeExecutor()
		f.SyncDelay = func(ctx context.Context) error {
			close(running)
			<-ctx.Done()
			return ctx.Err()
		}
		return f, nil
	}
	c := dialRaw(t, ts.addr)
	if res := c.startupAs(ProtocolVersion30, "revoked"); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}
	c.send(&pgproto3.Parse{Query: "select 1"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("the batch never reached Sync")
	}
	if n := ts.TerminateUser("revoked"); n != 1 {
		t.Fatalf("terminated %d sessions, want 1", n)
	}
	// The batch must be cancelled and the connection ended, not waited on.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 20 {
			if _, err := c.fe.Receive(); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("revocation waited for the running batch instead of stopping it")
	}
}

// TestOneStalledClientDoesNotDelayRevokingTheRest: the courtesy FATAL can
// wait on a client that has stopped reading, but latching is what actually
// stops a session executing. A sweep must latch every session first, or the
// ones behind an unresponsive client keep working for as long as it takes.
func TestOneStalledClientDoesNotDelayRevokingTheRest(t *testing.T) {
	ts := startServer(t, Config{})
	running := make(chan struct{})
	cancelled := make(chan struct{})
	ts.newExec = func(info SessionInfo) (Executor, error) {
		f := NewFakeExecutor()
		if info.Database == "busy" {
			f.SyncDelay = func(ctx context.Context) error {
				close(running)
				<-ctx.Done()
				close(cancelled)
				return ctx.Err()
			}
		}
		return f, nil
	}
	// Two sessions of the same role; the second is mid-batch.
	idle := dialRaw(t, ts.addr)
	idle.rawStartup(ProtocolVersion30, map[string]string{"user": "revoked", "database": "db"})
	if res := idle.readStartup(); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}
	busy := dialRaw(t, ts.addr)
	busy.rawStartup(ProtocolVersion30, map[string]string{"user": "revoked", "database": "busy"})
	if res := busy.readStartup(); res.ready == nil {
		t.Fatalf("startup: %+v", res)
	}
	busy.send(&pgproto3.Parse{Query: "select 1"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("the batch never reached Sync")
	}

	if n := ts.TerminateUser("revoked"); n != 2 {
		t.Fatalf("terminated %d sessions, want 2", n)
	}
	// Whatever order the sweep took, the running batch was cancelled.
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("a session was left executing while the sweep dealt with another")
	}
}

// TestOversizedMessageIsRefusedBeforeAllocation: the message body length
// comes from a five-byte header and pgproto3 allocates the whole declared
// length before any body byte arrives, so the ceiling is what one
// authenticated session can make the router allocate for free. It was 1 GiB
// -- PostgreSQL's own limit, but a router never needs to hold a 1 GiB Bind.
func TestOversizedMessageIsRefusedBeforeAllocation(t *testing.T) {
	ts := startServer(t, Config{MaxMessageBodyLen: 1 << 20})
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)

	// A Query header declaring a body far larger than the cap, and no body.
	var hdr [5]byte
	hdr[0] = 'Q'
	binary.BigEndian.PutUint32(hdr[1:], uint32(64<<20))
	if _, err := c.conn.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	// The refusal must be prompt and explicit. A deadline is essential to
	// this test: without the cap the router accepts the length, allocates
	// for it, and blocks waiting for a body that never comes -- which looks
	// like a pass to any assertion that tolerates a read error.
	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	msg, err := c.fe.Receive()
	if err != nil {
		t.Fatalf("no refusal within 2s: the router accepted the declared length and waited for the body (%v)", err)
	}
	er, ok := msg.(*pgproto3.ErrorResponse)
	if !ok {
		t.Fatalf("a message declaring more than the cap was accepted: got %T", msg)
	}
	if er.Severity != "FATAL" {
		t.Fatalf("oversized message refused with severity %q, want FATAL", er.Severity)
	}
}

func TestDefaultMessageBodyCapIsFarBelowAGigabyte(t *testing.T) {
	if DefaultMaxMessageBodyLen >= 1<<30 {
		t.Fatalf("default cap %d is at PostgreSQL's 1 GiB backend limit; a router does not need it", DefaultMaxMessageBodyLen)
	}
	if DefaultMaxMessageBodyLen < 1<<20 {
		t.Fatalf("default cap %d is below 1 MiB and would refuse ordinary large statements", DefaultMaxMessageBodyLen)
	}
}

// TestRevocationInTheExecutorInstallGap: between deciding a session is not
// revoked and installing its executor, NewExecutor runs. A revocation landing
// in that window must still take effect, and the client must not be able to
// tell it apart from an ordinary authentication failure -- otherwise the
// error itself reveals that the role exists and was revoked.
func TestRevocationInTheExecutorInstallGap(t *testing.T) {
	ts := startServer(t, Config{})
	building := make(chan struct{})
	release := make(chan struct{})
	ts.newExec = func(info SessionInfo) (Executor, error) {
		if info.User == "revoked" {
			close(building)
			<-release
		}
		return NewFakeExecutor(), nil
	}

	c := dialRaw(t, ts.addr)
	go func() {
		c.rawStartup(ProtocolVersion30, map[string]string{"user": "revoked", "database": "db"})
	}()

	// The session is registered and its role is known, but its executor is
	// not installed yet: this is the gap.
	<-building
	if n := ts.TerminateUser("revoked"); n != 1 {
		t.Fatalf("revoked %d sessions in the install gap, want 1", n)
	}
	close(release)

	if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var sawError bool
	for {
		msg, err := c.fe.Receive()
		if err != nil {
			break
		}
		if er, ok := msg.(*pgproto3.ErrorResponse); ok {
			sawError = true
			// Indistinguishable from any other authentication failure: a
			// distinct code or message would disclose that the role exists.
			if er.Code != CodeInvalidPassword {
				t.Fatalf("revocation in the install gap reported %s (%q); it must look like an ordinary authentication failure",
					er.Code, er.Message)
			}
			break
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			t.Fatal("the session became usable despite being revoked before its executor was installed")
		}
	}
	if !sawError {
		t.Fatal("no error reported to a session revoked in the install gap")
	}
}

// sessionStates renders every registered session's drain-relevant flags.
// drain() decides from exactly these, so a shutdown that failed to
// terminate one is explained by whichever of them is not what it should
// be -- and a session missing from this list altogether is its own answer.
func (ts *testServer) sessionStates() string {
	var b strings.Builder
	for _, sess := range ts.liveSessions() {
		sess.mu.Lock()
		saw := sess.drainSaw
		if saw == "" {
			saw = "drain never ran"
		}
		fmt.Fprintf(&b, "  id=%d serving=%v active=%v inTxn=%v closed=%v draining=%v revoked=%v | drain saw: %s\n",
			sess.id, sess.serving, sess.active, sess.inTxn, sess.closed, sess.draining, sess.revoked, saw)
		sess.mu.Unlock()
	}
	if b.Len() == 0 {
		return "  (no sessions registered)\n"
	}
	return b.String()
}

// liveSessions is the server's registered sessions. testServer has a mutex
// of its own, which shadows the embedded server's, so the lock -- and only
// the lock -- has to name it; this keeps that asymmetry in one place.
func (ts *testServer) liveSessions() []*session {
	ts.Server.mu.Lock()
	defer ts.Server.mu.Unlock()
	out := make([]*session, 0, len(ts.sessions))
	for _, sess := range ts.sessions {
		out = append(out, sess)
	}
	return out
}

// TestDrainDuringStartupNeverLeavesTheClientWaiting: a session is on the
// server's registry before startup runs, so a drain can reach one still
// authenticating. Writing the FATAL straight to its socket then put a
// second writer on a connection the startup path was still sending
// through, and the client read the two interleaved -- the terminate landed
// inside its own handshake and was consumed there, so the client waited
// out its deadline for a message that had already gone past.
//
// Either outcome is fine: told why, or the connection closed. What must
// never happen is the client left waiting on a connection still open.
func TestDrainDuringStartupNeverLeavesTheClientWaiting(t *testing.T) {
	for i := 0; i < 30; i++ {
		ts := startServer(t, Config{})
		conn, err := net.Dial("tcp", ts.addr)
		if err != nil {
			t.Fatal(err)
		}
		var all []*session
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			if all = ts.liveSessions(); len(all) > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if len(all) != 1 {
			t.Fatalf("run %d: %d sessions registered, want the one still starting up", i, len(all))
		}
		for _, sess := range all {
			sess.drain()
		}
		fe := pgproto3.NewFrontend(conn, conn)
		fe.Send(&pgproto3.StartupMessage{ProtocolVersion: ProtocolVersion30,
			Parameters: map[string]string{"user": "u", "database": "d"}})
		if err := fe.Flush(); err != nil {
			_ = conn.Close()
			continue // the server had already gone: nothing was left waiting
		}
		// Read until the connection ends or the terminate arrives. A
		// deadline reached with the connection still open is the bug.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			msg, err := fe.Receive()
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					t.Fatalf("run %d: client left waiting on an open connection", i)
				}
				break // closed, which tells the client the server is gone
			}
			if er, ok := msg.(*pgproto3.ErrorResponse); ok {
				if er.Code != CodeAdminShutdown || er.Severity != "FATAL" {
					t.Fatalf("run %d: got %s %s, want a FATAL %s", i, er.Severity, er.Code, CodeAdminShutdown)
				}
				break
			}
		}
		_ = conn.Close()
	}
}

// TestDrainBetweenServingAndIdleStillTerminates: the whole of startup
// counts as one message, so a session stays marked active until run()
// clears it -- while serving is set inside startup, at its end. A drain
// landing in that window sees a session both serving and active and takes
// neither branch: not the direct write, because it looks busy, and not the
// end-of-startup check, because that has already run. The message loop
// would not look again until the client sent something, and an idle client
// never does, so the session waited for a terminate that never came.
//
// The client's startup returns on its ReadyForQuery, which the server
// flushes before either of those two happen -- so draining right then is
// the window.
func TestDrainBetweenServingAndIdleStillTerminates(t *testing.T) {
	for i := 0; i < 25; i++ {
		ts := startServer(t, Config{})
		c := dialRaw(t, ts.addr)
		c.startup(ProtocolVersion30)
		for _, sess := range ts.liveSessions() {
			sess.drain()
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		msg, err := c.fe.Receive()
		if err != nil {
			t.Fatalf("run %d: drained session never answered: %v", i, err)
		}
		er, ok := msg.(*pgproto3.ErrorResponse)
		if !ok || er.Code != CodeAdminShutdown || er.Severity != "FATAL" {
			t.Fatalf("run %d: got %T %+v, want the admin shutdown FATAL", i, msg, msg)
		}
		_ = c.conn.Close()
	}
}

// TestCopyOutReachesTheClientBeforeTheQueryEnds: COPY data was written into
// the backend buffer and not flushed, so an export sat in the router until
// ReadyForQuery -- memory proportional to the export, and a client whose
// first byte waited for its last.
func TestCopyOutReachesTheClientBeforeTheQueryEnds(t *testing.T) {
	release := make(chan struct{})
	sent := make(chan struct{})
	ts := startServer(t, Config{})
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.CopyOutFn = func(w ResultWriter) error {
			if err := w.CopyOut(0, nil); err != nil {
				return err
			}
			chunk := bytes.Repeat([]byte("x"), 8<<10)
			for range 16 { // 128 KiB, past the flush threshold
				if err := w.CopyData(chunk); err != nil {
					return err
				}
			}
			close(sent)
			// Hold the query open: anything the client has by now was
			// flushed as the copy ran, not at ReadyForQuery.
			<-release
			if err := w.CopyDone(); err != nil {
				return err
			}
			return w.CommandComplete("COPY 16")
		}
		return e, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "copy t to stdout"})
	<-sent

	_ = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var copied int
	for copied < 8<<10 {
		msg, err := c.fe.Receive()
		if err != nil {
			close(release)
			t.Fatalf("the export was still in the router when the query had produced 128 KiB: %v", err)
		}
		if d, ok := msg.(*pgproto3.CopyData); ok {
			copied += len(d.Data)
		}
	}
	close(release)
}
