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
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/cli"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/metrics"
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
	pgSSLMode := fs.String("pg-sslmode", "disable", "TLS to PostgreSQL over TCP: disable, require or verify-full (unix sockets are never upgraded)")
	pgSSLRootCert := fs.String("pg-sslrootcert", "", "CA bundle the PostgreSQL server certificate must chain to (verify-full)")
	certFile := fs.String("tls-cert", "", "server certificate for the gRPC listener (mTLS)")
	keyFile := fs.String("tls-key", "", "server private key")
	caFile := fs.String("tls-ca", "", "CA bundle that client (router) certificates must chain to")
	insecureDev := fs.Bool("insecure-dev", false, "serve plaintext gRPC without client authentication (development only)")
	catalogDSN := fs.String("catalog-dsn", "", "catalog DSN; when set, generation and epoch come from the catalog")
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
	creds, err := listenerCredentials(*certFile, *keyFile, *caFile, *insecureDev)
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
	if *catalogDSN != "" {
		w := snapshot.NewWatcher(*catalogDSN, snapshot.Options{Logf: func(f string, a ...any) { logger.Info(fmt.Sprintf(f, a...)) }})
		go func() {
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("catalog watcher stopped", "err", err)
			}
		}()
		source = &pooler.SnapshotSource{Watcher: w, Shard: snapshot.ShardKey{ShardSet: *shardSet, ShardID: int32(*shardID)}, Base: base}
	}
	reg := metrics.NewRegistry("pooler")
	var pm *metrics.Pooler
	poolCfg := pooler.PoolConfig{MaxBackends: *maxBackends, MaxPerRole: *maxPerRole,
		MaxLifetime: *maxLifetime, MaxIdleTime: *maxIdle}
	var pool *pooler.Pool
	pm = metrics.NewPooler(reg,
		func() float64 { live, _ := pool.Stats(); return float64(live) },
		func() float64 { _, idle := pool.Stats(); return float64(idle) })
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
	if *metricsListen != "" {
		go func() {
			if err := metrics.Serve(ctx, *metricsListen, reg); err != nil {
				logger.Error("metrics listener stopped", "err", err)
			}
		}()
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
	g := grpc.NewServer(grpc.Creds(creds))
	srv.Register(g)
	mode := "mTLS"
	if *insecureDev {
		mode = "INSECURE plaintext"
	}
	fmt.Fprintf(stdout, "pgshard-pooler run: listening on %s (%s), postgres at %s\n", l.Addr(), mode, addr)
	errc := make(chan error, 1)
	go func() { errc <- g.Serve(l) }()
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

func listenerCredentials(certFile, keyFile, caFile string, insecureDev bool) (credentials.TransportCredentials, error) {
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("--insecure-dev cannot be combined with TLS flags")
		}
		return insecure.NewCredentials(), nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("--tls-cert, --tls-key and --tls-ca are required (or --insecure-dev)")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no certificates found", caFile)
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}), nil
}
