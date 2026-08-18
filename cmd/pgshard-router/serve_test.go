package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestServeUsageErrors(t *testing.T) {
	cases := [][]string{
		{"--bogus"},
		{"extra"},
		{"--tls-cert", "x"},
		{"--tls-key", "x"},
		{"--tls-cert", "/nonexistent/c", "--tls-key", "/nonexistent/k"},
	}
	for _, args := range cases {
		var out, errb bytes.Buffer
		if code := runServe(context.Background(), args, &out, &errb); code != 2 {
			t.Errorf("serve %v: code %d, stderr %q", args, code, errb.String())
		}
	}
	var out, errb bytes.Buffer
	if code := runServe(context.Background(), []string{"--help"}, &out, &errb); code != 0 || !strings.Contains(errb.String(), "-listen") {
		t.Fatalf("--help: code %d, %q", code, errb.String())
	}
	if code := runServe(context.Background(), []string{"--listen", "127.0.0.1:1"}, &out, &errb); code != 1 {
		t.Fatalf("privileged port: code %d", code)
	}
}

func startServe(t *testing.T, args ...string) (addr string, done <-chan int, cancel context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := &syncBuffer{}
	errb := &syncBuffer{}
	codes := make(chan int, 1)
	go func() {
		codes <- runServe(ctx, append([]string{"--listen", "127.0.0.1:0", "--drain-timeout", "5s"}, args...), out, errb)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, after, ok := strings.Cut(out.String(), "listening on "); ok {
			addr, _, _ = strings.Cut(after, " ")
			break
		}
		select {
		case code := <-codes:
			t.Fatalf("serve exited early with %d: %s", code, errb.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not report its address: %q %q", out.String(), errb.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	return addr, codes, cancel
}

func TestServeAnswersSelectOneAndDrains(t *testing.T) {
	addr, done, cancel := startServe(t)
	conn, err := pgx.Connect(context.Background(), "postgres://alice@"+addr+"/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := conn.QueryRow(context.Background(), "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("select 1: %d %v", n, err)
	}
	if _, err := pgx.Connect(context.Background(), "postgres://alice@"+addr+"/db?sslmode=require"); err == nil {
		t.Fatal("TLS should be unavailable without --tls-cert")
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
	}
	if err := conn.Ping(context.Background()); err == nil {
		t.Fatal("idle connection should have been terminated on shutdown")
	}
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Fatal("listener should be closed")
	}
}

func TestServeWithTLS(t *testing.T) {
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "router"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	addr, done, cancel := startServe(t, "--tls-cert", certPath, "--tls-key", keyPath)
	defer func() { cancel(); <-done }()
	conn, err := pgx.Connect(context.Background(), "postgres://alice@"+addr+"/db?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var n int
	if err := conn.QueryRow(context.Background(), "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("select 1 over TLS: %d %v", n, err)
	}
}
