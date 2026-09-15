package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/andrew01234567890/pgshard/internal/agentauth"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// Run bootstraps and supervises the instance until ctx ends or a fatal
// condition (lease loss, postgres exit) occurs. It returns nil after a
// clean shutdown.
func Run(ctx context.Context, cfg *Config, log *slog.Logger) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	sup := NewSupervisor(cfg.BinDir, cfg.PGData, log)
	go sup.ReapOrphans(ctx)

	epoch, err := OpenEpochStoreAt(cfg.PGData, cfg.EpochFile)
	if err != nil {
		return err
	}
	inst := NewInstance(cfg, sup, epoch, log)

	var lease *Lease
	if cfg.Lease.Enabled {
		lease, err = NewLease(cfg, log)
		if err != nil {
			return fmt.Errorf("lease: %w", err)
		}
		if lease == nil {
			return errors.New("lease.enabled=true but no in-cluster kube API is configured")
		}
	} else {
		log.Warn("primary lease disabled: no kube API configured (lease.enabled=false)")
	}

	fatal := func(err error) { cancel(err) }
	var rollback startupRollback
	defer rollback.run()

	// The probes answer BEFORE the data directory is built, not after.
	// Bootstrap can take an hour on a large shard -- a clone, a rejoin, a
	// restore from the repository -- and with nothing listening the startup
	// probe failed for all of it, ran out of budget, and the kubelet killed
	// the container. The restart cleared the data directory and copied
	// again, and was killed again: a permanent failure that got worse as
	// the data grew. What the probes say while this runs is in
	// Probes.Bootstrapping.
	reg := metrics.NewRegistry("agent")
	am := metrics.NewAgent(reg,
		func() float64 {
			if primary, err := inst.IsPrimary(); err == nil && primary {
				return 1
			}
			return 0
		},
		func() float64 {
			if primary, err := inst.IsPrimary(); err != nil || primary {
				return 0
			}
			lctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			lag, err := inst.ReplayLagBytes(lctx)
			if err != nil {
				return -1
			}
			return float64(lag)
		})
	probes := &Probes{Health: inst, MaxLagBytes: cfg.MaxLagBytes, Peers: cfg.PeerFailsafeURLs,
		IsolationGrace: time.Duration(cfg.IsolationGrace), Bootstrapping: inst.BootstrapState,
		Fenced: func() {
			am.FenceEvents.Inc()
			fatal(errors.New("primary isolated: self-fencing"))
		}}
	if lease != nil {
		probes.KubeReachable = lease.Reachable
		probes.LeaseStale = lease.Stale
	}
	mux := http.NewServeMux()
	mux.Handle("/", probes.Handler())
	mux.Handle("/metrics", metrics.Handler(reg))
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	httpLn, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	rollback.push(func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = httpSrv.Shutdown(shutCtx)
	})
	go func() {
		if err := httpSrv.Serve(httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(err)
		}
	}()

	go inst.WatchBootstrap(ctx)

	if err := inst.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	go pollMetrics(ctx, inst, am)
	sup.OnUnexpectedExit = fatal
	srv := NewServer(inst, epoch, lease, log, fatal)
	srv.bgCtx = ctx
	standby, err := inst.IsStandby()
	if err != nil {
		return fmt.Errorf("reading the instance role: %w", err)
	}
	// Checked before anything is acquired: an agent whose control-plane
	// token cannot be read will refuse every RPC, and finding that out
	// after taking the lease and starting PostgreSQL means unwinding both.
	if cfg.AuthTokenFile != "" {
		b, err := os.ReadFile(cfg.AuthTokenFile)
		if err != nil {
			return fmt.Errorf("agent auth token: %w", err)
		}
		if strings.TrimSpace(string(b)) == "" {
			return fmt.Errorf("agent auth token: %s is empty", cfg.AuthTokenFile)
		}
	}

	// Startup takes a lease, a PostgreSQL process and two listeners, and
	// steps after the first of them can still fail. Without this, Run
	// returned with PostgreSQL serving, HTTP answering and the lease
	// renewing: the caller exits, and the lease is left to expire on its
	// own, during which nothing else may promote.

	if !standby && lease != nil {
		if err := lease.Acquire(ctx); err != nil {
			return fmt.Errorf("primary cannot start without the lease: %w", err)
		}
		srv.startHold()
		rollback.push(func() {
			relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer relCancel()
			srv.releaseLease(relCtx)
		})
	}
	startCtx, startCancel := context.WithTimeout(ctx, 10*time.Minute)
	err = inst.Start(startCtx)
	startCancel()
	if err != nil {
		return err
	}
	rollback.push(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Duration(cfg.ShutdownTimeout))
		defer stopCancel()
		_ = sup.Stop(stopCtx, ShutdownFast, time.Duration(cfg.ShutdownTimeout))
	})
	inst.startStanzaWorker(ctx, stanzaRetry)
	// A primary holding an ACTIVE failover slot writes a running-xacts
	// record on a clock of its own. A standby's copy of that slot, if it
	// was synced after the record the slot's own creation wrote, cannot
	// persist until the primary's slot advances past what the standby
	// reserved -- and on a quiet cluster nothing else makes it. See
	// needsStandbySnapshot for what this does and does not rescue.
	go srv.runStandbySnapshots(ctx, standbySnapshotEvery)

	// Both tokens, re-read on every call so a rotated Secret is honoured
	// without an agent restart.
	//
	// The token is the one the operator generates and mounts into every
	// member, and it is the only one accepted. Agents used to also accept a
	// token derived from the superuser password, so a cluster could be
	// rolled onto the mounted one a member at a time; that path is gone
	// (PGS-572), and with it a credential that let anything holding the
	// superuser password call Promote, Demote, Rewind and Reclone.
	//
	// Both interceptors read the same tokens. The service is unary
	// throughout, so the streaming one gates nothing today -- it is
	// registered so that adding a streaming method cannot quietly add an
	// unauthenticated one.
	// An agent with no token file accepts nothing: authorized() skips empty
	// tokens and answers false on an empty list, so a misconfigured agent
	// refuses every call rather than serving them unauthenticated.
	agentTokens := func() ([]string, error) {
		if cfg.AuthTokenFile == "" {
			return nil, nil
		}
		b, err := os.ReadFile(cfg.AuthTokenFile)
		if err != nil {
			return nil, err
		}
		return []string{strings.TrimSpace(string(b))}, nil
	}
	// Transport security is opt-in and off by default. Both callers can dial
	// with credentials, but each decides per member from
	// spec.internalTLS.agentMTLS, so an agent that listens for TLS before its
	// member carries the flag refuses every handshake it is sent. The flag is
	// what turns this on; enabling it here alone takes the agent off the air.
	// grpccreds.Listener is the same hardened definition the pooler and
	// controller listen with: client certificates required and verified
	// against a named CA.
	//
	// Until it is configured the bearer token travels in clear, which is
	// what PGS-235 and PGS-421 are about.
	if cfg.GRPCTLS.AcceptPlaintext {
		log.Warn("agent gRPC also accepts PLAINTEXT callers while the cluster moves to mutual TLS",
			"until", "the operator renders the member without acceptPlaintext")
	}
	plaintext := cfg.GRPCTLS.Plaintext()
	if plaintext {
		// Said once, loudly, at the one moment an operator is looking:
		// internalTLS.issue mounts the certificates on every member
		// without requiring them, so a cluster can carry a full internal
		// PKI and still serve Promote, Demote, SetWriteFence and Reclone
		// behind a bearer token in clear. Nothing else says so -- the
		// listener starts, the RPCs work, and the only evidence is a
		// field that was not set.
		log.Warn("agent gRPC is PLAINTEXT: Promote, Demote, SetWriteFence, Reclone and DropSlot are authorised by a bearer token sent in clear",
			"enable", "spec.internalTLS.agentMTLS", "reachable_by", "every peer spec.networkPolicy.clients admits to the agent port")
	}
	grpcCreds, err := cfg.GRPCTLS.listenerCredentials()
	if err != nil {
		return fmt.Errorf("agent gRPC credentials: %w", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.Creds(grpcCreds),
		grpc.UnaryInterceptor(agentauth.AnyOfUnaryServerInterceptor(agentTokens)),
		grpc.StreamInterceptor(agentauth.AnyOfStreamServerInterceptor(agentTokens)),
	)
	pgshardv1.RegisterAgentServer(grpcSrv, srv)
	grpcLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	rollback.push(grpcSrv.Stop)
	go func() {
		if err := grpcSrv.Serve(grpcLn); err != nil {
			fatal(err)
		}
	}()
	log.Info("agent ready", "http", httpLn.Addr().String(), "grpc", grpcLn.Addr().String(), "standby", standby)

	// Past here the steady-state path below owns the shutdown.
	rollback.succeed()

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)

	var runErr error
	select {
	case sig := <-sigs:
		log.Info("signal received; shutting down", "signal", sig)
	case <-ctx.Done():
		runErr = context.Cause(ctx)
		if errors.Is(runErr, context.Canceled) {
			runErr = nil
		}
	}
	grpcSrv.Stop()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), terminationHTTPTimeout)
	_ = httpSrv.Shutdown(shutCtx)
	shutCancel()

	// Fast, straight away, for ShutdownTimeout. A smart shutdown waits for
	// every client to disconnect, and a member always has clients -- the
	// pooler's -- so the smart phase preserved nothing and only spent time
	// the Pod's grace could have given a fast shutdown instead. Fast ends the
	// sessions (prepared transactions stay prepared, and walsenders still
	// drain to their standbys) and writes the shutdown checkpoint that lets
	// the member rejoin by pg_rewind without crash recovery.
	//
	// When ShutdownTimeout runs out the stop gives up and the agent exits;
	// whatever of postgres is still running dies with the container. There
	// is no immediate phase worth budgeting for: a checkpointer stuck in
	// fsync -- the usual reason a fast shutdown overruns -- cannot act on
	// SIGQUIT either, and the postmaster itself waits seconds for children.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeout))
	defer stopCancel()
	if err := sup.Stop(stopCtx, ShutdownFast, time.Duration(cfg.ShutdownTimeout)); err != nil {
		log.Warn("postgres did not finish a fast shutdown in time; exiting, which kills it", "err", err)
		// The Lease is left for the operator, which fences it itself when it
		// promotes; releasing it here would only race that.
		return errors.Join(runErr, err)
	}
	leaseCtx, leaseCancel := context.WithTimeout(context.Background(), terminationLeaseTimeout)
	defer leaseCancel()
	srv.releaseLease(leaseCtx)
	return runErr
}

// terminationHTTPTimeout and terminationLeaseTimeout are the parts of the
// SIGTERM stop that are not postgres's; TerminationOverhead is their sum.
const (
	terminationHTTPTimeout  = 1 * time.Second
	terminationLeaseTimeout = 1 * time.Second
)

// TerminationOverhead is what the agent's stop on SIGTERM takes beyond the
// fast shutdown itself. The longest the agent takes to exit is
// ShutdownTimeout plus this, and whoever deletes its Pod has to allow that
// much grace or the shutdown is SIGKILLed partway through.
const TerminationOverhead = terminationHTTPTimeout + terminationLeaseTimeout
