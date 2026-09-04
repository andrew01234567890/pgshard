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
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/metrics"
	"github.com/andrew01234567890/pgshard/internal/pki"
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
	var authz []grpccreds.Option
	if cfg.GRPCTLS.AuthorizeCallers {
		if allow, ok := pki.AllowedCallers(pki.RoleAgent); ok {
			authz = append(authz, grpccreds.Authorize(allow))
		}
	}
	grpcCreds, err := grpccreds.Listener(cfg.GRPCTLS.CertFile, cfg.GRPCTLS.KeyFile, cfg.GRPCTLS.CAFile,
		cfg.GRPCTLS.CertFile == "" && cfg.GRPCTLS.KeyFile == "" && cfg.GRPCTLS.CAFile == "", authz...)
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
