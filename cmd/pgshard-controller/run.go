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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/controller"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func run(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runController(ctx, args, stdout, stderr)
}

// runController campaigns for leadership through a catalog advisory lock,
// reconciles while leader and serves the Controller gRPC service until ctx
// is cancelled.
func runController(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-controller run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	catalogDSN := fs.String("catalog-dsn", "", "catalog DSN with pgshard_system privileges (required)")
	listen := fs.String("listen", "127.0.0.1:15500", "gRPC address for the Controller service (empty disables)")
	certFile := fs.String("tls-cert", "", "server certificate for the gRPC listener (mTLS)")
	keyFile := fs.String("tls-key", "", "server private key")
	caFile := fs.String("tls-ca", "", "CA bundle that client certificates must chain to")
	insecureDev := fs.Bool("insecure-dev", false, "serve plaintext gRPC without client authentication (development only)")
	interval := fs.Duration("reconcile-interval", 30*time.Second, "longest time between reconcile passes without a catalog notification")
	retry := fs.Duration("election-retry", 5*time.Second, "time between leadership attempts")
	lockKey := fs.Int64("leader-lock-key", controller.LeaderLockKey, "pg_advisory_lock key that elects the leader")
	resolveEvery := fs.Duration("resolve-interval", 5*time.Second, "time between in-doubt transaction resolution passes")
	applyEvery := fs.Duration("apply-interval", time.Second, "time between DDL migration applier passes while leader")
	shardDSNTemplate := fs.String("shard-dsn-template", "", "superuser DSN for shard primaries with {set}, {id} and {group} placeholders (enables the resolver)")
	barrierDrain := fs.Duration("barrier-drain-timeout", controller.DefaultDrainTimeout, "how long a barrier waits for in-flight two-phase commits")
	barrierArchive := fs.Duration("barrier-archive-timeout", controller.DefaultArchiveTimeout, "how long a barrier waits for every group's restore point to be archived")
	var shardDSNs shardDSNFlag
	fs.Var(&shardDSNs, "shard-dsn", "explicit shard DSN as <set>/<id>=<dsn>; repeatable")
	ddlRole := fs.String("ddl-role", controller.DefaultDDLRole, "non-superuser login the applier provisions on every shard and runs client DDL through")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "pgshard-controller run: unexpected argument %q\n", fs.Arg(0))
		return cli.ExitUsage
	}
	if *catalogDSN == "" {
		fmt.Fprintln(stderr, "pgshard-controller run: --catalog-dsn is required")
		return cli.ExitUsage
	}
	var creds credentials.TransportCredentials
	if *listen != "" {
		var err error
		if creds, err = listenerCredentials(*certFile, *keyFile, *caFile, *insecureDev); err != nil {
			fmt.Fprintf(stderr, "pgshard-controller run: %v\n", err)
			return cli.ExitUsage
		}
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	pool, err := pgxpool.New(ctx, *catalogDSN)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-controller run: catalog: %v\n", err)
		return cli.ExitUsage
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintf(stderr, "pgshard-controller run: catalog: %v\n", err)
		return cli.ExitNotReady
	}

	var leader atomic.Bool
	rec := &controller.Reconciler{DSN: *catalogDSN, Logger: logger, LockKey: *lockKey, Interval: *interval, RetryInterval: *retry,
		OnLeader: leader.Store}
	go func() { _ = rec.Run(ctx) }()
	var resolver *controller.Resolver
	var barrier *controller.Barrier
	if *shardDSNTemplate != "" || len(shardDSNs) > 0 {
		dialer := &controller.PgxShardDialer{Pool: pool, DSNs: shardDSNs, Template: *shardDSNTemplate}
		resolver = &controller.Resolver{Pool: pool, Logger: logger, Shards: dialer}
		go resolver.Run(ctx, *resolveEvery)
		barrier = &controller.Barrier{Store: &controller.PGBarrierStore{Pool: pool}, Groups: &controller.SQLBarrierGroups{Pool: pool, Shards: dialer},
			Resolver: resolver, Logger: logger, DrainTimeout: *barrierDrain, ArchiveTimeout: *barrierArchive}
		applier := &controller.Applier{Store: &controller.PGMigrationStore{Pool: pool}, Logger: logger, Shards: dialer, DDLRole: *ddlRole}
		go applier.Run(ctx, *applyEvery, leader.Load)
	}

	if *listen == "" {
		fmt.Fprintln(stdout, "pgshard-controller run: reconciling without a gRPC listener")
		<-ctx.Done()
		return cli.ExitOK
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-controller run: %v\n", err)
		return cli.ExitNotReady
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pgshardv1.RegisterControllerServer(g, &controller.Server{Pool: pool, Resolver: resolver, Barrier: barrier})
	mode := "mTLS"
	if *insecureDev {
		mode = "INSECURE plaintext"
	}
	fmt.Fprintf(stdout, "pgshard-controller run: listening on %s (%s)\n", l.Addr(), mode)
	errc := make(chan error, 1)
	go func() { errc <- g.Serve(l) }()
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "pgshard-controller run: %v\n", err)
		return cli.ExitNotReady
	case <-ctx.Done():
	}
	g.Stop()
	<-errc
	return cli.ExitOK
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

// shardDSNFlag collects --shard-dsn <set>/<id>=<dsn> values.
type shardDSNFlag map[controller.ShardRef]string

func (f *shardDSNFlag) String() string { return fmt.Sprint(len(*f), " shard DSNs") }

func (f *shardDSNFlag) Set(v string) error {
	key, dsn, ok := strings.Cut(v, "=")
	if !ok {
		return errors.New("expected <set>/<id>=<dsn>")
	}
	set, idText, ok := strings.Cut(key, "/")
	if !ok {
		return errors.New("expected <set>/<id>=<dsn>")
	}
	id, err := strconv.ParseInt(idText, 10, 32)
	if err != nil {
		return fmt.Errorf("shard id %q: %w", idText, err)
	}
	if *f == nil {
		*f = shardDSNFlag{}
	}
	(*f)[controller.ShardRef{Set: set, ID: int32(id)}] = dsn
	return nil
}
