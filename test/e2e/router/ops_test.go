//go:build integration

package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func readyz(t *testing.T, addr string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(body))
}

func sendCancel(t *testing.T, addr string, pid uint32, secret []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 12+len(secret))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	binary.BigEndian.PutUint32(buf[4:8], 80877102)
	binary.BigEndian.PutUint32(buf[8:12], pid)
	copy(buf[12:], secret)
	if _, err := conn.Write(buf); err != nil {
		t.Fatal(err)
	}
}

func TestRouterOps(t *testing.T) {
	s := startStack(t)
	ctx := context.Background()
	if code, _ := readyz(t, s.healthAddr); code != http.StatusOK {
		t.Fatalf("router A readyz %d", code)
	}
	routerB := s.startRouter(t, 2, map[int]string{1: s.peerAddr})
	connA := s.connect(t)
	if _, err := connA.Exec(ctx, "create table ops (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}

	t.Run("cancel_forwarded_between_routers", func(t *testing.T) {
		pid, secret := connA.PgConn().PID(), connA.PgConn().SecretKey()
		go func() {
			time.Sleep(500 * time.Millisecond)
			sendCancel(t, routerB.addr, pid, secret)
		}()
		start := time.Now()
		_, err := connA.Exec(ctx, "select pg_sleep(20)")
		if sqlstate(err) != "57014" {
			t.Fatalf("cancel via router B: %v\nrouter A log:\n%s\nrouter B log:\n%s", err, s.routerLog.String(), routerB.log.String())
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("cancel took %s", time.Since(start))
		}
		var n int
		if err := connA.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
			t.Fatalf("after cancel: %v", err)
		}
	})

	t.Run("drain_on_sigterm", func(t *testing.T) {
		routerC := s.startRouter(t, 3, nil)
		conn := s.connectTo(t, routerC.addr)
		if _, err := conn.Exec(ctx, "begin"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "insert into ops values (1, 'in-flight')"); err != nil {
			t.Fatal(err)
		}
		idle := s.connectTo(t, routerC.addr)
		if err := routerC.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		signalled := time.Now()
		deadline := time.Now().Add(3 * time.Second)
		for {
			code, _ := readyz(t, routerC.healthAddr)
			if code == http.StatusServiceUnavailable {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("readyz stayed %d after SIGTERM", code)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if c, err := net.DialTimeout("tcp", routerC.addr, time.Second); err != nil {
			t.Fatal("listener must stay open during --drain-delay")
		} else {
			_ = c.Close()
		}
		time.Sleep(1500 * time.Millisecond)
		if _, err := idle.Exec(ctx, "select 1"); err == nil {
			t.Fatal("idle session must be terminated once the listener closes")
		} else if code := sqlstate(err); code != "" && code != "57P01" {
			t.Fatalf("idle session terminated with %s, want 57P01", code)
		}
		if _, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&connect_timeout=2", appRole, appPassword, routerC.addr, appDatabase)); err == nil {
			t.Fatal("new connections must be refused once the listener is closed")
		}
		var v string
		if err := conn.QueryRow(ctx, "select v from ops where id = 1").Scan(&v); err != nil || v != "in-flight" {
			t.Fatalf("statement inside the open transaction during drain: %q %v", v, err)
		}
		if _, err := conn.Exec(ctx, "commit"); err != nil {
			t.Fatalf("commit during drain: %v", err)
		}
		select {
		case <-routerC.exited:
		case <-time.After(8 * time.Second):
			t.Fatalf("router did not exit within the drain timeout:\n%s", routerC.log.String())
		}
		if !routerC.cmd.ProcessState.Success() {
			t.Fatalf("router exit: %v", routerC.cmd.ProcessState)
		}
		if time.Since(signalled) > 7*time.Second {
			t.Fatalf("exit took %s", time.Since(signalled))
		}
		if err := connA.QueryRow(ctx, "select v from ops where id = 1").Scan(&v); err != nil || v != "in-flight" {
			t.Fatalf("committed row not visible: %q %v", v, err)
		}
	})

	t.Run("failover_buffering", func(t *testing.T) {
		admin, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = admin.Close(ctx) }()
		setState := func(state string) {
			if _, err := admin.Exec(ctx, "update pgshard.shard_status set serving_state = $1 where shard_set = 'default' and shard_id = 0", state); err != nil {
				t.Fatalf("set serving_state %s: %v", state, err)
			}
		}
		txConn := s.connect(t)
		if _, err := txConn.Exec(ctx, "begin"); err != nil {
			t.Fatal(err)
		}
		if _, err := txConn.Exec(ctx, "insert into ops values (2, 'doomed')"); err != nil {
			t.Fatal(err)
		}
		setState("fenced")
		defer setState("serving")
		time.Sleep(500 * time.Millisecond)

		if _, err := txConn.Exec(ctx, "select 1"); sqlstate(err) != "40001" {
			t.Fatalf("statement inside a transaction during a fence: %v", err)
		}
		if txConn.PgConn().TxStatus() != 'I' {
			t.Fatalf("session must be idle after a failover error, status %c", txConn.PgConn().TxStatus())
		}

		go func() {
			time.Sleep(1500 * time.Millisecond)
			setState("serving")
		}()
		start := time.Now()
		var n int
		if err := connA.QueryRow(ctx, "select count(*) from ops").Scan(&n); err != nil {
			t.Fatalf("select during fence: %v\nrouter log:\n%s", err, s.routerLog.String())
		}
		if d := time.Since(start); d < time.Second || d > 8*time.Second {
			t.Fatalf("select during fence took %s, want the fence duration", d)
		}
	})
}
