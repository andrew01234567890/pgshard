package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/cli"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/metrics"
	"github.com/andrew01234567890/pgshard/internal/pki"
	"github.com/andrew01234567890/pgshard/internal/pooler"
	"github.com/andrew01234567890/pgshard/internal/pprofserve"
)

func run(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runPooler(ctx, args, stdout, stderr)
}

// runPooler serves the Pooler gRPC service until ctx is cancelled, then
// drains and returns.
func runPooler(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-pooler run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "127.0.0.1:15432", "gRPC address to listen on")
	socketDir := fs.String("pg-socket-dir", "", "PostgreSQL unix socket directory (preferred over --pg-host)")
	pgHost := fs.String("pg-host", "127.0.0.1", "PostgreSQL host")
	pgPort := fs.Int("pg-port", 5432, "PostgreSQL port")
	database := fs.String("pg-database", "postgres", "default PostgreSQL database for sessions that do not name one")
	pgSSLMode := fs.String("pg-sslmode", "disable", "TLS to PostgreSQL over TCP: disable, require (encrypts but does not authenticate the server), or verify-full (encrypts and authenticates); unix sockets are never upgraded")
	pgSSLRootCert := fs.String("pg-sslrootcert", "", "CA bundle the PostgreSQL server certificate must chain to (verify-full)")
	certFile := fs.String("tls-cert", "", "server certificate for the gRPC listener (mTLS)")
	keyFile := fs.String("tls-key", "", "server private key")
	caFile := fs.String("tls-ca", "", "CA bundle that client (router) certificates must chain to")
	authorizeCallers := fs.Bool("tls-authorize-callers", false, "refuse callers whose certificate does not carry a pgshard identity allowed to call this listener; needs certificates the operator issued")
	insecureDev := fs.Bool("insecure-dev", false, "serve plaintext gRPC without client authentication (development only)")
	catalogDSN := fs.String("catalog-dsn", "", "catalog DSN; when set, generation and epoch come from the catalog")
	catalogPasswordFile := fs.String("catalog-password-file", "", "file holding the password for --catalog-dsn; the environment's PGPASSWORD is left for the local server")
	shardSet := fs.String("shard-set", "", "shard set of this shard (with --catalog-dsn)")
	shardID := fs.Int("shard-id", 0, "shard id of this shard (with --catalog-dsn)")
	generation := fs.Uint64("generation", 0, "static shard-map generation (without --catalog-dsn)")
	epoch := fs.Uint64("epoch", 0, "static primary epoch")
	maxBackends := fs.Int("max-backends", 100, "backend budget for the shard")
	maxPerRole := fs.Int("max-per-role", 0, "backend budget per role (0 = same as --max-backends)")
	maxLifetime := fs.Duration("backend-max-lifetime", time.Hour, "retire backends older than this")
	maxIdle := fs.Duration("backend-max-idle", 10*time.Minute, "close backends idle longer than this")
	reserveTimeout := fs.Duration("reserve-timeout", 5*time.Minute, "release a reserved session whose Execute stream has been gone this long")
	drain := fs.Duration("drain-timeout", 30*time.Second, "time to let in-flight transactions finish on shutdown")
	streamDSN := fs.String("stream-dsn", "", "superuser DSN for change-stream replication connections (enables Stream)")
	streamShard := fs.String("stream-shard", "", "group name used in stream slot names (default derived from --shard-set/--shard-id)")
	metricsListen := fs.String("metrics-listen", "", "HTTP address for /metrics (empty disables)")
	pprofListen := fs.String("pprof-listen", "", "HTTP address for /debug/pprof (empty disables; profiling runs only)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "pgshard-pooler run: unexpected argument %q\n", fs.Arg(0))
		return cli.ExitUsage
	}
	creds, err := grpccreds.Listener(*certFile, *keyFile, *caFile, *insecureDev, authorize(*authorizeCallers, pki.RolePooler)...)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
		return cli.ExitUsage
	}
	if (*catalogDSN != "") != (*shardSet != "") {
		fmt.Fprintln(stderr, "pgshard-pooler run: --catalog-dsn and --shard-set must be given together")
		return cli.ExitUsage
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	addr := net.JoinHostPort(*pgHost, strconv.Itoa(*pgPort))
	if *socketDir != "" {
		addr = filepath.Join(*socketDir, ".s.PGSQL."+strconv.Itoa(*pgPort))
	}
	backendTLS, err := backendTLSConfig(*pgSSLMode, *pgSSLRootCert, *pgHost)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
		return cli.ExitUsage
	}
	dialer := pooler.Dialer{Address: addr, Timeout: 5 * time.Second, TLS: backendTLS}
	base := pooler.View{Generation: *generation, Epoch: *epoch, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Serving: true}
	var source pooler.Source = pooler.NewStaticSource(base)
	snapshotAge := func() float64 { return -1 }
	if *catalogDSN != "" {
		// The catalog login is its own credential. PGPASSWORD is the
		// superuser's, for the local socket that reads replication slots,
		// and libpq would apply it to both connections -- so this one
		// carries its password in the DSN, read from a file rather than
		// written into a pod spec.
		dsn, err := withPasswordFile(*catalogDSN, *catalogPasswordFile)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
			return cli.ExitUsage
		}
		*catalogDSN = dsn
		// ServingOnly: the pooler enforces the shard-map generation and its own
		// shard's epoch, and reads nothing else out of a snapshot. There is one
		// pooler per member, so loading the whole catalog on every notification
		// would grow the catalog's read load as pooler count times catalog size.
		w := snapshot.NewWatcher(*catalogDSN, snapshot.Options{ServingOnly: true,
			Logf: func(f string, a ...any) { logger.Info(fmt.Sprintf(f, a...)) }})
		go func() {
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("catalog watcher stopped", "err", err)
			}
		}()
		source = &pooler.SnapshotSource{Watcher: w, Shard: snapshot.ShardKey{ShardSet: *shardSet, ShardID: int32(*shardID)}, Base: base}
		snapshotAge = func() float64 { return w.AgeSeconds(time.Now()) }
	}
	reg := metrics.NewRegistry("pooler")
	var pm *metrics.Pooler
	poolCfg := pooler.PoolConfig{MaxBackends: *maxBackends, MaxPerRole: *maxPerRole,
		MaxLifetime: *maxLifetime, MaxIdleTime: *maxIdle}
	var pool *pooler.Pool
	pm = metrics.NewPooler(reg,
		func() float64 { live, _ := pool.Stats(); return float64(live) },
		func() float64 { _, idle := pool.Stats(); return float64(idle) },
		snapshotAge)
	poolCfg.OnDial = func(err error) {
		result := "ok"
		if err != nil {
			result = "error"
		}
		pm.BackendDials.WithLabelValues(result).Inc()
	}
	poolCfg.OnWait = func() { pm.PoolWaits.Inc() }
	pool = pooler.NewPool(poolCfg, dialer)
	if *pprofListen != "" {
		pa, err := pprofserve.Serve(ctx, *pprofListen)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
			return cli.ExitNotReady
		}
		fmt.Fprintf(stdout, "pgshard-pooler run: pprof on %s\n", pa)
	}
	if *streamShard == "" {
		set := *shardSet
		if set == "" {
			set = "default"
		}
		*streamShard = catalog.GroupName(set, int32(*shardID))
	}
	srv := pooler.NewServer(pooler.Config{Pool: pool, Source: source, Dialer: dialer, Database: *database, Logger: logger, ReserveTimeout: *reserveTimeout,
		Stream: pooler.StreamConfig{DSN: *streamDSN, Shard: *streamShard}, Metrics: pm})

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
		return cli.ExitNotReady
	}
	// Explicitly what grpc-go would default to, so the limit is pgshard's
	// and testable rather than a dependency's.
	g := grpc.NewServer(grpc.Creds(creds),
		grpc.MaxRecvMsgSize(pooler.MaxMessageBytes), grpc.MaxSendMsgSize(pooler.MaxMessageBytes))
	srv.Register(g)
	mode := "mTLS"
	if *insecureDev {
		mode = "INSECURE plaintext"
	}
	fmt.Fprintf(stdout, "pgshard-pooler run: listening on %s (%s), postgres at %s\n", l.Addr(), mode, addr)
	errc := make(chan error, 1)
	go func() { errc <- g.Serve(l) }()
	// After the gRPC listener, never before: /healthz on this port is the
	// pooler's readiness probe, and a member NetworkPolicy leaves the
	// metrics port open to the kubelet while the gRPC port stays closed to
	// everything but the cluster. Answering it while nothing is serving
	// would report a pooler that cannot take a query as Ready.
	if *metricsListen != "" {
		go func() {
			if err := metrics.Serve(ctx, *metricsListen, reg); err != nil && ctx.Err() == nil {
				// Fatal, not logged: /healthz on this listener is the
				// readiness probe, so a pooler that cannot serve it is a
				// pod that never becomes Ready however well it runs.
				errc <- fmt.Errorf("metrics listener: %w", err)
			}
		}()
	}
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
		return cli.ExitNotReady
	case <-ctx.Done():
	}
	dctx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	if err := srv.Drain(dctx); err != nil {
		fmt.Fprintf(stderr, "pgshard-pooler run: %v\n", err)
	}
	g.Stop()
	<-errc
	return cli.ExitOK
}

// backendTLSConfig maps libpq-style sslmode onto a client TLS config: nil
// for disable, an unverified handshake for require, and a config that
// verifies the chain and host name for verify-full.
func backendTLSConfig(mode, rootCert, host string) (*tls.Config, error) {
	switch mode {
	case "disable":
		return nil, nil
	case "require":
		return &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, nil //nolint:gosec // sslmode=require verifies nothing by definition
	case "verify-full":
		if rootCert == "" {
			return nil, errors.New("--pg-sslmode verify-full requires --pg-sslrootcert")
		}
		pem, err := os.ReadFile(rootCert)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: no certificates found", rootCert)
		}
		return &tls.Config{RootCAs: pool, ServerName: host, MinVersion: tls.VersionTLS12}, nil
	}
	return nil, fmt.Errorf("--pg-sslmode %q: want disable, require or verify-full", mode)
}

// withPasswordFile adds the password in path to a libpq keyword/value DSN.
// An empty path leaves the DSN alone, so a caller that has PGPASSWORD for
// this connection keeps working.
func withPasswordFile(dsn, path string) (string, error) {
	if path == "" {
		return dsn, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("catalog password file: %w", err)
	}
	pw := strings.TrimRight(string(b), "\r\n")
	if pw == "" {
		return "", fmt.Errorf("catalog password file %s is empty", path)
	}
	// libpq quoting: single quotes around the value, backslash before a
	// quote or a backslash inside it.
	esc := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(pw)
	return dsn + " password='" + esc + "'", nil
}

// authorize turns the flag into the option grpccreds takes. Off it is
// nothing at all, so a cluster whose certificates were supplied rather
// than issued keeps working: those certificates carry no pgshard identity,
// and a fail-closed check would refuse every caller.
func authorize(on bool, role string) []grpccreds.Option {
	if !on {
		return nil
	}
	allow, ok := pki.AllowedCallers(role)
	if !ok {
		return nil
	}
	return []grpccreds.Option{grpccreds.Authorize(allow)}
}
