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

	epoch, err := OpenEpochStore(cfg.PGData)
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

	if err := inst.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	fatal := func(err error) { cancel(err) }
	sup.OnUnexpectedExit = fatal
	srv := NewServer(inst, epoch, lease, log, fatal)
	srv.bgCtx = ctx
	standby, err := inst.IsStandby()
	if err != nil {
		return fmt.Errorf("reading the instance role: %w", err)
	}
	// Checked before anything is acquired: an agent that cannot derive its
	// control-plane token will not serve, and finding that out after taking
	// the lease and starting PostgreSQL means unwinding both.
	password, err := inst.password()
	if err != nil {
		return fmt.Errorf("agent auth token: %w", err)
	}
	if _, err := agentauth.Token(password); err != nil {
		return fmt.Errorf("agent auth token: %w", err)
	}

	// Startup takes a lease, a PostgreSQL process and two listeners, and
	// steps after the first of them can still fail. Without this, Run
	// returned with PostgreSQL serving, HTTP answering and the lease
	// renewing: the caller exits, and the lease is left to expire on its
	// own, during which nothing else may promote.
	var rollback startupRollback
	defer rollback.run()

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
	go pollMetrics(ctx, inst, am)
	probes := &Probes{Health: inst, MaxLagBytes: cfg.MaxLagBytes, Peers: cfg.PeerFailsafeURLs,
		IsolationGrace: time.Duration(cfg.IsolationGrace),
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

	// Both tokens, re-read on every call so a rotated Secret is honoured
	// without an agent restart.
	//
	// The mounted one is this cluster's own. The derived one is what agents
	// used before that Secret existed, and is still accepted so a cluster
	// can be rolled onto the new token a member at a time -- callers send
	// both, so an old agent and a new one are both reachable throughout.
	//
	// REMOVE the derived token once every caller sends the mounted one --
	// PGS-572. The controller still derives its own from the catalog
	// password, so until that changes, anything holding the superuser
	// password also holds a token that unlocks Promote, Demote, Rewind and
	// Reclone.
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(agentauth.AnyOfUnaryServerInterceptor(func() ([]string, error) {
		var tokens []string
		if cfg.AuthTokenFile != "" {
			b, err := os.ReadFile(cfg.AuthTokenFile)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, strings.TrimSpace(string(b)))
		}
		pw, err := inst.password()
		if err != nil {
			return nil, err
		}
		derived, err := agentauth.Token(pw)
		if err != nil {
			return nil, err
		}
		return append(tokens, derived), nil
	})))
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
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = httpSrv.Shutdown(shutCtx)
	shutCancel()

	mode := ShutdownSmart
	if runErr != nil {
		mode = ShutdownFast
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Duration(cfg.ShutdownTimeout))
	defer stopCancel()
	if err := sup.Stop(stopCtx, mode, time.Duration(cfg.ShutdownTimeout)); err != nil {
		return errors.Join(runErr, err)
	}
	srv.releaseLease(stopCtx)
	return runErr
}
