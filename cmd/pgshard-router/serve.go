package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/cli"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/pprofserve"
	"github.com/andrew01234567890/pgshard/internal/router"
	"github.com/andrew01234567890/pgshard/internal/router/cancelpeer"
	"github.com/andrew01234567890/pgshard/internal/router/vstream"
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
	certFile := fs.String("tls-cert", "", "TLS certificate file for clients; also requires TLS unless --allow-plaintext")
	keyFile := fs.String("tls-key", "", "TLS private key file for clients")
	allowPlaintext := fs.Bool("allow-plaintext", false, "serve clients that never requested TLS even though --tls-cert is set (development only)")
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
	startupTimeout := fs.Duration("startup-timeout", 10*time.Second, "time a connection may spend before authentication completes")
	// Compiled in, this was wrong for every PostgreSQL minor after the one
	// it was written against, and wrong by a whole major on a cluster
	// serving 19 -- which reports itself to clients as 18.6. Deriving it
	// from what the shards actually run needs the router to learn their
	// version, and during a rolling major upgrade to decide which of two
	// answers is the cluster's; that is PGS-471. Until then it is at least
	// correctable without a rebuild.
	serverVersion := fs.String("server-version", "18.6 (pgshard)", "value reported as the server_version parameter to clients")
	maxStartupConns := fs.Int("max-startup-conns", 100, "concurrent connections allowed in the pre-authentication phase (refused with 53300 past the cap)")
	maxSessions := fs.Int("max-sessions", router.DefaultMaxSessions, "authenticated sessions this router holds at once, whatever role they belong to (refused with 53300 past the cap; negative means no cap)")
	maxMessageBody := fs.Int("max-message-body", pgwire.DefaultMaxMessageBodyLen, "largest frontend message body accepted, in bytes; the buffer is allocated from the message header before the body arrives")
	drain := fs.Duration("drain-timeout", 30*time.Second, "time to wait for open transactions and active queries on shutdown")
	drainDelay := fs.Duration("drain-delay", 5*time.Second, "time between readiness turning false and closing the listener")
	healthListen := fs.String("health-listen", "", "HTTP address for /readyz and /healthz (empty disables)")
	pprofListen := fs.String("pprof-listen", "", "HTTP address for /debug/pprof (empty disables; profiling runs only)")
	instanceID := fs.Uint("instance-id", 0, "router instance id embedded in protocol 3.2 cancel keys (0 draws a random one)")
	peerListen := fs.String("peer-cancel-listen", "", "gRPC address for cancels forwarded by peer routers (empty disables)")
	peers := peerFlags{}
	fs.Var(peers, "peer", "static peer router ID=host:port (repeatable)")
	peerService := fs.String("peer-service", "", "host:port whose DNS records enumerate peer routers (headless Service)")
	peerRate := fs.Float64("peer-cancel-rate", 50, "forwarded cancels per second (burst equal)")
	bufferWindow := fs.Duration("buffer-window", 10*time.Second, "how long a statement waits for a shard failover")
	bufferTransportWindow := fs.Duration("buffer-transport-window", time.Second, "how long a statement waits after a pooler refused the connection while the shard is still serving")
	bufferCap := fs.Int("buffer-cap", 256, "statements buffered per shard during failover")
	scatterMaxShards := fs.Int("scatter-max-shards", 0, "most shards one SELECT may fan out to (0 = all)")
	scatterMaxStreams := fs.Int("scatter-max-streams", 4096, "shard streams open for multi-shard reads across this router")
	scatterMaxWait := fs.Duration("scatter-max-wait", 30*time.Second, "longest a multi-shard read waits for stream capacity before 53300; negative waits for ever")
	crossShardLockTimeout := fs.Duration("cross-shard-lock-timeout", router.DefaultCrossShardLockTimeout, "bound a lock wait once a transaction spans shards; no shard's deadlock detector can see a cycle that crosses them. Negative waits for ever")
	vstreamListen := fs.String("vstream-listen", "", "gRPC address for the VStream change-stream service (empty disables)")
	controllerAddr := fs.String("controller", "", "controller gRPC endpoint used to create and drop streams (same credentials as poolers)")
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
	if *instanceID > math.MaxUint32 {
		fmt.Fprintln(stderr, "pgshard-router serve: --instance-id must fit in 32 bits")
		return cli.ExitUsage
	}
	if (len(peers) > 0 || *peerService != "") && *peerListen == "" {
		fmt.Fprintln(stderr, "pgshard-router serve: --peer or --peer-service require --peer-cancel-listen")
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
	if *rolesTTL <= 0 {
		*rolesTTL = 5 * time.Second
	}
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
	if *instanceID == 0 {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitNotReady
		}
		*instanceID = uint(binary.BigEndian.Uint32(b[:]) | 1)
		if *peerListen != "" {
			logger.Warn("random instance id; peers cannot address this instance statically", "instance_id", *instanceID)
		}
	}
	var srv *pgwire.Server
	srvCfg := pgwire.Config{
		Authenticator:     pgwire.SCRAMAuthenticator{Lookup: roles.Lookup},
		TLSConfig:         tlsCfg,
		AllowPlaintext:    *allowPlaintext,
		ServerVersion:     *serverVersion,
		InstanceID:        uint32(*instanceID),
		StartupTimeout:    *startupTimeout,
		MaxStartupConns:   *maxStartupConns,
		MaxMessageBodyLen: *maxMessageBody,
		Logger:            logger,
	}
	var forwarder *cancelpeer.Forwarder
	if *peerListen != "" {
		forwarder, err = cancelpeer.New(cancelpeer.Config{Self: uint32(*instanceID), Static: peers, Service: *peerService,
			Creds: creds, Rate: *peerRate, Burst: int(*peerRate), Logger: logger})
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitUsage
		}
		defer forwarder.Close()
	}
	poolerClients := router.NewPoolers(poolers, w.Current, creds)
	rt, err := router.New(router.Config{
		Snapshot:              w.Current,
		Poolers:               poolerClients,
		Logger:                logger,
		Peers:                 peersOrNil(forwarder),
		Buffering:             router.Buffering{Window: *bufferWindow, TransportWindow: *bufferTransportWindow, PerShardCap: *bufferCap, Changes: w.Subscribe},
		Scatter:               router.ScatterConfig{MaxShards: *scatterMaxShards, MaxStreams: *scatterMaxStreams, MaxWait: *scatterMaxWait},
		CrossShardLockTimeout: *crossShardLockTimeout,
		Decisions:             &router.PGDecisionLog{Pool: pool},
		Sequences:             router.NewSequenceAllocator(&router.PGBlockSource{Pool: pool}),
		Migrations:            &router.PGMigrationQueue{Pool: pool},
		RoleLimits:            roles,
		MaxSessions:           *maxSessions,
	})
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitUsage
	}
	srvCfg.NewExecutor = rt.NewExecutor
	srvCfg.CancelHandler = func(ctx context.Context, key pgwire.CancelKey) { rt.CancelHandler(srv)(ctx, key) }
	srv, err = pgwire.NewServer(srvCfg)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitUsage
	}
	l, err := net.Listen(listenNetwork(*listen), *listen)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitNotReady
	}
	defer func() { _ = l.Close() }()
	// Every endpoint reports here. A router whose peer-cancel, VStream or
	// health listener has stopped keeps accepting sessions and keeps
	// answering readiness, so those exits end the process rather than being
	// discarded: cancellation would be delivered nowhere, a consumer's
	// stream would never reconnect, and the Service would go on sending
	// traffic to a router that cannot say it is unwell.
	errc := make(chan error, 4)
	if *peerListen != "" {
		serverCreds, err := peerCredentials(*poolerCert, *poolerKey, *poolerCA, *insecureDev)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitUsage
		}
		pl, err := net.Listen(listenNetwork(*peerListen), *peerListen)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitNotReady
		}
		g := grpc.NewServer(grpc.Creds(serverCreds))
		pgshardv1.RegisterRouterPeerServer(g, &cancelpeer.Server{Local: func(ctx context.Context, key pgwire.CancelKey) bool {
			return rt.CancelLocal(ctx, srv, key)
		}})
		serveAux(ctx, errc, "peer cancels", func() error { return g.Serve(pl) })
		defer g.Stop()
		fmt.Fprintf(stdout, "pgshard-router serve: peer cancels on %s (instance %d)\n", pl.Addr(), srv.InstanceID())
	}
	if *vstreamListen != "" {
		serverCreds, err := peerCredentials(*poolerCert, *poolerKey, *poolerCA, *insecureDev)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitUsage
		}
		vl, err := net.Listen(listenNetwork(*vstreamListen), *vstreamListen)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitNotReady
		}
		vs := &vstream.Server{Topology: vstream.SnapshotTopology{Snapshot: w.Current, Poolers: poolerClients},
			Catalog: vstream.PGCatalog{Pool: pool}, Logger: logger}
		if *controllerAddr != "" {
			cc, err := grpc.NewClient(*controllerAddr, grpc.WithTransportCredentials(creds))
			if err != nil {
				fmt.Fprintf(stderr, "pgshard-router serve: controller: %v\n", err)
				return cli.ExitUsage
			}
			defer func() { _ = cc.Close() }()
			vs.Controller = pgshardv1.NewControllerClient(cc)
		}
		g := grpc.NewServer(grpc.Creds(serverCreds))
		pgshardv1.RegisterVStreamServer(g, vs)
		serveAux(ctx, errc, "vstream", func() error { return g.Serve(vl) })
		defer g.Stop()
		fmt.Fprintf(stdout, "pgshard-router serve: vstream on %s\n", vl.Addr())
	}
	if *pprofListen != "" {
		pa, err := pprofserve.Serve(ctx, *pprofListen)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitNotReady
		}
		fmt.Fprintf(stdout, "pgshard-router serve: pprof on %s\n", pa)
	}
	drainer := router.NewDrainer(srv, *drainDelay, *drain)
	// A router with no catalog snapshot stamps every request with generation
	// zero and has each one refused, so it must not be sent traffic.
	// A router whose reloads are failing keeps its last snapshot, so
	// without the age it would stay in the Service advertising a view of
	// the catalog it has already stopped planning against.
	drainer.Routable = func() bool { return w.Fresh(time.Now()) }
	if *healthListen != "" {
		hl, err := net.Listen(listenNetwork(*healthListen), *healthListen)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitNotReady
		}
		mux := http.NewServeMux()
		mux.Handle("/", drainer.Handler())
		mux.Handle("/metrics", rt.MetricsHandler())
		hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		serveAux(ctx, errc, "health", func() error { return hs.Serve(hl) })
		defer func() { _ = hs.Close() }()
		fmt.Fprintf(stdout, "pgshard-router serve: health on %s\n", hl.Addr())
	}
	mode := "pooler mTLS"
	if *insecureDev {
		mode = "INSECURE plaintext to poolers"
	}
	fmt.Fprintf(stdout, "pgshard-router serve: listening on %s (%s)\n", l.Addr(), mode)
	// Authentication only gates new connections, so a role that is dropped,
	// set NOLOGIN or expired would keep the sessions it already holds.
	go func() {
		t := time.NewTicker(*rolesTTL)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			rctx, cancel := context.WithTimeout(ctx, *rolesTTL)
			err := roles.Refresh(rctx)
			cancel()
			if err != nil {
				logger.Warn("role refresh", "error", err)
				continue
			}
			if n := srv.TerminateWhere(func(user string) bool { return !roles.MayLogIn(user) }); n > 0 {
				logger.Info("terminated sessions of roles that may no longer log in", "sessions", n)
			}
		}
	}()
	go func() { errc <- srv.Serve(l) }()
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitNotReady
	case <-ctx.Done():
	}
	fmt.Fprintf(stdout, "pgshard-router serve: draining (delay %s, timeout %s)\n", *drainDelay, *drain)
	if err := drainer.Drain(context.Background()); err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: shutdown: %v\n", err)
	}
	<-errc
	return cli.ExitOK
}

func peersOrNil(f *cancelpeer.Forwarder) router.CancelForwarder {
	if f == nil {
		return nil
	}
	return f
}

type peerFlags map[uint32]string

func (p peerFlags) String() string { return fmt.Sprint(map[uint32]string(p)) }

// Set accepts "ID=host:port".
func (p peerFlags) Set(v string) error {
	id, addr, ok := strings.Cut(v, "=")
	n, err := strconv.ParseUint(id, 10, 32)
	if !ok || addr == "" || err != nil || n == 0 {
		return fmt.Errorf("want ID=host:port with a non-zero 32-bit ID, got %q", v)
	}
	p[uint32(n)] = addr
	return nil
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

// peerCredentials secures the RouterPeer listener with the same certificate
// the router presents to poolers; peers must chain to the pooler CA.
func peerCredentials(certFile, keyFile, caFile string, insecureDev bool) (credentials.TransportCredentials, error) {
	if insecureDev {
		return insecure.NewCredentials(), nil
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

// serveAux runs one of the router's auxiliary listeners and reports its
// exit on errc. An auxiliary endpoint is not optional once it is bound:
// the router goes on accepting sessions whatever happens to it, so an exit
// nobody hears leaves the process serving with a piece of itself missing.
// An exit after the context ends is the shutdown, and says nothing.
func serveAux(ctx context.Context, errc chan<- error, name string, serve func() error) {
	go func() {
		err := serve()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("stopped serving")
		}
		select {
		case errc <- fmt.Errorf("%s listener: %w", name, err):
		default:
		}
	}()
}
