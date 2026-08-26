package pgparser

import (
	"context"
	"errors"
	"fmt"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const pg18Image = "ghcr.io/andrew01234567890/pgshard-postgres:18"

// differentialCorpus mixes valid statements (some semantically wrong on
// purpose: missing tables, bad types) with grammar errors. Only the syntax
// verdict is compared with the server: 42601 from PostgreSQL must mean a
// parse error here, and vice versa.
var differentialCorpus = []string{
	"SELECT 1",
	"SELECT a, b FROM no_such_table WHERE a = 1",
	"INSERT INTO t (a) VALUES (1) RETURNING OLD.*, NEW.*",
	"UPDATE t SET a = 1 WHERE b = 2 RETURNING WITH (OLD AS o, NEW AS n) o.a, n.a",
	"ALTER TABLE t ADD CONSTRAINT c NOT NULL x NOT VALID",
	"CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL)",
	"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES u NOT ENFORCED",
	"COPY t FROM STDIN (ON_ERROR ignore, REJECT_LIMIT 5)",
	"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING merge_action(), t.*",
	"WITH x AS (SELECT 1) SELECT * FROM x",
	"SELECT * FROM t TABLESAMPLE SYSTEM (10)",
	"SELECT a FROM t GROUP BY GROUPING SETS ((a), ())",
	"SELECT json_table(x, '$' COLUMNS (a int)) FROM t",
	"SELECT * FROM t FOR UPDATE SKIP LOCKED",
	"CREATE INDEX i ON t (a) WITH (fillfactor = 70)",
	"CREATE TABLE t (a int) PARTITION BY HASH (a)",
	"CREATE TEMP TABLE tt (a text COLLATE \"C\")",
	"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$",
	"CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END",
	"DROP TABLE IF EXISTS t",
	"TRUNCATE t CASCADE",
	"EXPLAIN ANALYZE SELECT 1",
	"PREPARE p AS SELECT $1::int",
	"DEALLOCATE ALL",
	"SET search_path TO a, b",
	"SHOW all",
	"VACUUM (ANALYZE) t",
	"BEGIN; SELECT 1; COMMIT",
	"SELECT 'abc'::nonexistent_type",
	"SELECT 1 + 'x'",
	"SELECT * FROM t WHERE a = ANY($1)",
	"SELECT 1 FROM t LIMIT 1 x",
	"SELECT ) 1",
	"SELECT 1 FROM",
	"CREATE TABLE t (a int GENERATED ALWAYS AS (1) STORED VIRTUAL)",
	"INSERT INTO t VALUES (1) RETURNING OLD",
	"COPY t FROM STDIN (ON_ERROR)",
	"ALTER TABLE t ADD CONSTRAINT c NOT NULL",
	"UPDATE t SET a = 1 RETURNING WITH (OLD o) *",
	"SELEC 1",
	"SELECT * FROM t WHERE",
	"CREATE TABLE (a int)",
	"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED DELETE",
	"SELECT 1;; SELECT 2",
	"",
}

func TestDifferentialAgainstPostgres18(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	dsn := startPostgres18(t)
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	syntaxErrors, agreements := 0, 0
	for _, sql := range differentialCorpus {
		ours := ourVerdict(sql)
		theirs := serverVerdict(t, conn, sql)
		if ours != theirs {
			t.Errorf("%q: pgparser=%v postgres=%v", sql, ours, theirs)
			continue
		}
		agreements++
		if !ours {
			syntaxErrors++
		}
	}
	if syntaxErrors < 10 || agreements-syntaxErrors < 25 {
		t.Fatalf("corpus too thin to be meaningful: %d syntax errors, %d accepted", syntaxErrors, agreements-syntaxErrors)
	}
}

// ourVerdict reports whether the local grammar accepts sql. Any non-syntax
// failure is a bug and fails the test.
func ourVerdict(sql string) bool {
	_, err := Parse(sql)
	if err == nil {
		return true
	}
	var pe *Error
	if errors.As(err, &pe) && pe.SQLState == SyntaxErrorSQLState {
		return false
	}
	panic(fmt.Sprintf("unexpected error for %q: %v", sql, err))
}

// serverVerdict runs sql in a rolled-back transaction and reports whether
// the server got past the grammar (any SQLSTATE other than 42601).
func serverVerdict(t *testing.T, conn *pgx.Conn, sql string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO pg_temp"); err != nil {
		t.Fatal(err)
	}
	// Simple-protocol Exec keeps multi-statement strings and empty input legal.
	_, err = tx.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	if err == nil {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("%q: non-PostgreSQL error %v", sql, err)
	}
	if strings.EqualFold(pgErr.Code, SyntaxErrorSQLState) {
		return false
	}
	return true
}

func startPostgres18(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		dockertest.Unavailable(t, "docker not on PATH; skipping differential test")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker daemon unavailable; skipping differential test")
	}
	if exec.Command("docker", "image", "inspect", pg18Image).Run() != nil {
		if out, err := exec.Command("docker", "pull", pg18Image).CombinedOutput(); err != nil {
			dockertest.Unavailable(t, "image %s unavailable: %v: %s", pg18Image, err, strings.TrimSpace(string(out)))
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "--entrypoint", "sh", pg18Image, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", id).CombinedOutput()
	t.Fatalf("postgres did not become ready:\n%s", logs)
	return ""
}
