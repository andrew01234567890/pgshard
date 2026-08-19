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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// serve runs the router: a pgwire front end whose sessions are authenticated
// against the catalog and executed through per-shard poolers.
func serve(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runServe(ctx, args, stdout, stderr)
}

type poolerFlags map[router.Shard]string

func (p poolerFlags) String() string { return fmt.Sprint(map[router.Shard]string(p)) }

// Set accepts "ID=host:port" (shard set "default") or "SET/ID=host:port".
func (p poolerFlags) Set(v string) error {
	key, addr, ok := strings.Cut(v, "=")
	if !ok || addr == "" {
		return fmt.Errorf("want [SET/]ID=host:port, got %q", v)
	}
	set, id, ok := strings.Cut(key, "/")
	if !ok {
		set, id = router.DefaultShardSet, key
	}
	n, err := strconv.ParseInt(id, 10, 32)
	if err != nil || set == "" {
		return fmt.Errorf("want [SET/]ID=host:port, got %q", v)
	}
	p[router.Shard{Set: set, ID: int32(n)}] = addr
	return nil
}

// runServe listens until ctx is cancelled, then drains and returns.
func runServe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-router serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "127.0.0.1:5432", "address to listen on")
	certFile := fs.String("tls-cert", "", "TLS certificate file for clients (enables SSLRequest)")
	keyFile := fs.String("tls-key", "", "TLS private key file for clients")
	catalogDSN := fs.String("catalog-dsn", "", "catalog DSN with access to pgshard.roles verifiers (required)")
	poolers := poolerFlags{}
	fs.Var(poolers, "pooler", "static pooler endpoint [SET/]ID=host:port (repeatable; default: shard_status.primary_endpoint)")
	catalogPooler := fs.String("catalog-pooler", "", "pooler endpoint fronting the catalog database (shard set catalog)")
	poolerCert := fs.String("pooler-tls-cert", "", "client certificate for pooler mTLS")
	poolerKey := fs.String("pooler-tls-key", "", "client key for pooler mTLS")
	poolerCA := fs.String("pooler-tls-ca", "", "CA bundle pooler server certificates must chain to")
	insecureDev := fs.Bool("insecure-dev", false, "talk plaintext gRPC to poolers (development only)")
	rolesTTL := fs.Duration("roles-ttl", 5*time.Second, "how long catalog role verifiers are cached")
	snapshotWait := fs.Duration("snapshot-wait", 30*time.Second, "time to wait for the first catalog snapshot")
	drain := fs.Duration("drain-timeout", 30*time.Second, "time to wait for active queries on shutdown")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "pgshard-router serve: unexpected argument %q\n", fs.Arg(0))
		return cli.ExitUsage
	}
	if (*certFile == "") != (*keyFile == "") {
		fmt.Fprintln(stderr, "pgshard-router serve: --tls-cert and --tls-key must be given together")
		return cli.ExitUsage
	}
	if *catalogDSN == "" {
		fmt.Fprintln(stderr, "pgshard-router serve: --catalog-dsn is required")
		return cli.ExitUsage
	}
	creds, err := poolerCredentials(*poolerCert, *poolerKey, *poolerCA, *insecureDev)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitUsage
	}
	var tlsCfg *tls.Config
	if *certFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitUsage
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	if *catalogPooler != "" {
		poolers[router.Shard{Set: router.CatalogShardSet, ID: 0}] = *catalogPooler
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	pool, err := pgxpool.New(ctx, *catalogDSN)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: catalog: %v\n", err)
		return cli.ExitUsage
	}
	defer pool.Close()
	roles := router.NewRoleCache(pool, *rolesTTL)
	if _, err := roles.Lookup(ctx, ""); err != nil && !errors.Is(err, router.ErrUnknownRole) {
		fmt.Fprintf(stderr, "pgshard-router serve: catalog roles: %v\n", err)
		return cli.ExitNotReady
	}

	watchCtx, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()
	w := snapshot.NewWatcher(*catalogDSN, snapshot.Options{Logf: func(f string, a ...any) { logger.Info(fmt.Sprintf(f, a...)) }})
	go func() {
		if err := w.Run(watchCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("catalog watcher stopped", "err", err)
		}
	}()
	if !waitSnapshot(ctx, w, *snapshotWait) {
		fmt.Fprintln(stderr, "pgshard-router serve: no catalog snapshot within --snapshot-wait")
		return cli.ExitNotReady
	}
	rt, err := router.New(router.Config{
		Snapshot: w.Current,
		Poolers:  router.NewPoolers(poolers, w.Current, creds),
		Logger:   logger,
	})
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitUsage
	}
	var srv *pgwire.Server
	cfg := pgwire.Config{
		Authenticator: pgwire.SCRAMAuthenticator{Lookup: roles.Lookup},
		NewExecutor:   rt.NewExecutor,
		CancelHandler: func(ctx context.Context, key pgwire.CancelKey) { rt.CancelHandler(srv)(ctx, key) },
		TLSConfig:     tlsCfg,
		ServerVersion: "18.6 (pgshard)",
		Logger:        logger,
	}
	srv, err = pgwire.NewServer(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitUsage
	}
	l, err := net.Listen(listenNetwork(*listen), *listen)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitNotReady
	}
	mode := "pooler mTLS"
	if *insecureDev {
		mode = "INSECURE plaintext to poolers"
	}
	fmt.Fprintf(stdout, "pgshard-router serve: listening on %s (%s)\n", l.Addr(), mode)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(l) }()
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitNotReady
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: shutdown: %v\n", err)
	}
	<-errc
	return cli.ExitOK
}

// listenNetwork keeps an IPv4 literal on an IPv4-only socket instead of a
// dual-stack one, which some port forwarders (Docker Desktop's host-gateway)
// do not publish.
func listenNetwork(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		if ip := net.ParseIP(host); ip != nil && ip.To4() != nil {
			return "tcp4"
		}
	}
	return "tcp"
}

func waitSnapshot(ctx context.Context, w *snapshot.Watcher, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for w.Current() == nil {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

func poolerCredentials(certFile, keyFile, caFile string, insecureDev bool) (credentials.TransportCredentials, error) {
	if insecureDev {
		if certFile != "" || keyFile != "" || caFile != "" {
			return nil, errors.New("--insecure-dev cannot be combined with --pooler-tls-* flags")
		}
		return insecure.NewCredentials(), nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("--pooler-tls-cert, --pooler-tls-key and --pooler-tls-ca are required (or --insecure-dev)")
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
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS13}), nil
}
