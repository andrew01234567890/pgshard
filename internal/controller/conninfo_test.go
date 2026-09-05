package controller

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestConnInfoKeepsEveryConnectionOption.
//
// ConnInfo used to REBUILD the DSN from the parsed host, port, user,
// password and database, which silently dropped everything else. A DSN
// configured with sslmode=verify-full and a private CA produced a conninfo
// with no sslmode at all, so the subscription fell back to libpq's default
// and connected without verifying anything -- over a string that carries a
// shard superuser credential. sslcert/sslkey went the same way, so a
// certificate-authenticated subscription could not connect at all.
//
// A tls.Config cannot be turned back into sslrootcert, which is why
// reconstruction cannot be made safe: the only way to keep an option is not
// to drop it.

// writeCerts makes a real CA and keypair, because pgx READS sslrootcert,
// sslcert and sslkey while parsing: a made-up path does not fail at connect
// time, it fails to parse, which is also why ConnInfo cannot be given one.
func writeCerts(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pgshard-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	caPath = filepath.Join(dir, "ca.crt")
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	for path, data := range map[string][]byte{caPath: certPEM, certPath: certPEM, keyPath: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return caPath, certPath, keyPath
}

func TestConnInfoKeepsEveryConnectionOption(t *testing.T) {
	ca, crt, key := writeCerts(t)
	for _, c := range []struct {
		name string
		dsn  string
		want []string
	}{
		{
			"keyword/value with verification and a private CA",
			"host=shard-1-rw port=5432 user=postgres password=s3cret dbname=postgres " +
				"sslmode=verify-full sslrootcert=" + ca,
			[]string{"sslmode=verify-full", "sslrootcert=" + ca},
		},
		{
			"keyword/value with a client certificate",
			"host=shard-1-rw user=postgres dbname=postgres sslmode=verify-ca " +
				"sslcert=" + crt + " sslkey=" + key + " sslrootcert=" + ca,
			[]string{"sslcert=" + crt, "sslkey=" + key, "sslmode=verify-ca"},
		},
		{
			"other options that are not TLS at all",
			"host=shard-1-rw user=postgres dbname=postgres connect_timeout=7 application_name=pgshard",
			[]string{"connect_timeout=7", "application_name=pgshard"},
		},
		{
			"URL form",
			"postgres://postgres:s3cret@shard-1-rw:5432/postgres?sslmode=verify-full&sslrootcert=" + url.QueryEscape(ca),
			[]string{"sslmode=verify-full", url.QueryEscape(ca)},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ConnInfo(c.dsn, "reports")
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range c.want {
				if !strings.Contains(got, want) {
					t.Errorf("%q dropped from the conninfo:\n  %s", want, got)
				}
			}
			// The database still changes, which is the whole point of the
			// function, and the rest of the configuration still parses.
			cfg, err := pgx.ParseConfig(got)
			if err != nil {
				t.Fatalf("%s: %v", got, err)
			}
			if cfg.Database != "reports" {
				t.Fatalf("database %q, want reports", cfg.Database)
			}
			if cfg.Host != "shard-1-rw" || cfg.User != "postgres" {
				t.Fatalf("host=%s user=%s", cfg.Host, cfg.User)
			}
		})
	}
}

// The TLS the caller asked for has to reach the connection, not merely
// appear in the string: pgx resolves sslmode and the certificate paths into
// a tls.Config, and a dropped sslmode shows up here as a nil one.
func TestConnInfoTLSSurvivesAsAResolvedConfig(t *testing.T) {
	const dsn = "host=shard-1-rw user=postgres dbname=postgres sslmode=require"
	before, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if before.TLSConfig == nil {
		t.Fatal("the fixture DSN must itself resolve to TLS, or this test proves nothing")
	}
	got, err := ConnInfo(dsn, "reports")
	if err != nil {
		t.Fatal(err)
	}
	after, err := pgx.ParseConfig(got)
	if err != nil {
		t.Fatal(err)
	}
	if after.TLSConfig == nil {
		t.Fatalf("TLS was configured and the conninfo connects without it: %s", got)
	}
	if after.TLSConfig.InsecureSkipVerify != before.TLSConfig.InsecureSkipVerify {
		t.Fatalf("verification changed: before=%v after=%v",
			before.TLSConfig.InsecureSkipVerify, after.TLSConfig.InsecureSkipVerify)
	}
}

// A DSN with no TLS keeps having none: the old code appended
// sslmode=disable for that case and the behaviour must not change.
func TestConnInfoLeavesAPlaintextDSNPlaintext(t *testing.T) {
	const dsn = "host=shard-1-rw user=postgres dbname=postgres sslmode=disable"
	got, err := ConnInfo(dsn, "reports")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(got)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLSConfig != nil {
		t.Fatalf("a plaintext DSN gained TLS: %s", got)
	}
}

// TestWithPasswordSplicesOnlyWhereItMust: the superuser password is kept out
// of every template, argument list and environment until a CREATE
// SUBSCRIPTION is rendered, because PostgreSQL on the target opens that
// connection. Both libpq forms have to carry it, and neither may lose the
// options around it.
func TestWithPasswordSplicesOnlyWhereItMust(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		pw   string
		want []string
		skip []string
	}{
		{"keyword value", "host=src port=5432 user=postgres dbname=app sslmode=verify-full", "s3cr3t",
			[]string{"password='s3cr3t'", "sslmode=verify-full", "dbname=app"}, nil},
		{"a quote in the password", "host=src dbname=app", "p'w\\x", []string{`password='p\'w\\x'`}, nil},
		{"url", "postgres://postgres@src:5432/app?sslmode=require", "s3cr3t",
			[]string{"postgres:s3cr3t@", "sslmode=require", "/app"}, nil},
		{"url with a password already in the query", "postgres://postgres@src/app?password=old", "new",
			[]string{"password=new"}, []string{"password=old"}},
		{"no password leaves the string alone", "host=src dbname=app", "", nil, []string{"password"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := WithPassword(c.dsn, c.pw)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range c.want {
				if !strings.Contains(got, want) {
					t.Errorf("%q does not carry %q", got, want)
				}
			}
			for _, skip := range c.skip {
				if strings.Contains(got, skip) {
					t.Errorf("%q still carries %q", got, skip)
				}
			}
			if _, err := pgx.ParseConfig(got); err != nil {
				t.Fatalf("result is not a connection string: %v", err)
			}
			cfg, err := pgx.ParseConfig(got)
			if err == nil && c.pw != "" && cfg.Password != c.pw {
				t.Errorf("libpq reads the password as %q, want %q", cfg.Password, c.pw)
			}
		})
	}
}
