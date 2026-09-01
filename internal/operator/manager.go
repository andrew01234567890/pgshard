package operator

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// Options configures the operator manager.
type Options struct {
	MetricsAddr     string
	ProbeAddr       string
	LeaderElect     bool
	LeaderElectID   string
	AdminImage      string
	ControllerImage string
	// ControllerPlacementDropOldAfter is passed through to every cluster's
	// controller; zero leaves the controller's own default.
	ControllerPlacementDropOldAfter time.Duration
	RouterImage                     string
	Development                     bool
	// ControllerTLSCert, ControllerTLSKey and ControllerTLSCA are the
	// client certificate and CA the operator presents to cluster
	// controllers (scheduled barriers); unset dials plaintext.
	ControllerTLSCert string
	ControllerTLSKey  string
	ControllerTLSCA   string
}

// ParseFlags parses the `run` subcommand's flags.
func ParseFlags(args []string, stderr io.Writer) (Options, error) {
	fs := flag.NewFlagSet("pgshard-operator run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o Options
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, or 0 to disable")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", ":8081", "health/readiness probe address")
	fs.BoolVar(&o.LeaderElect, "leader-elect", false, "enable leader election")
	fs.StringVar(&o.LeaderElectID, "leader-election-id", "pgshard-operator.pgshard.io", "leader election lease name")
	fs.StringVar(&o.AdminImage, "admin-image", DefaultAdminImage, "image of the admin UI deployed for clusters with spec.admin.enabled")
	fs.StringVar(&o.ControllerImage, "controller-image", DefaultControllerImage, "image of the controller deployed for every cluster: two-phase resolution, DDL, workflows and barriers")
	fs.DurationVar(&o.ControllerPlacementDropOldAfter, "controller-placement-drop-old-after", 0, "grace before a placement workflow drops the tables it replaced (0 keeps the controller's default)")
	fs.StringVar(&o.RouterImage, "router-image", DefaultRouterImage, "image of the router Deployment created for every cluster")
	fs.BoolVar(&o.Development, "development", false, "human-readable logs")
	fs.StringVar(&o.ControllerTLSCert, "controller-tls-cert", "", "client certificate for Controller gRPC calls (scheduled barriers); unset dials plaintext")
	fs.StringVar(&o.ControllerTLSKey, "controller-tls-key", "", "client private key for Controller gRPC calls")
	fs.StringVar(&o.ControllerTLSCA, "controller-tls-ca", "", "CA bundle controller certificates must chain to")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return o, nil
}

// Run starts the controller manager and blocks until ctx is done.
func Run(ctx context.Context, o Options) error {
	ctrl.SetLogger(zap.New(zap.UseDevMode(o.Development)))
	scheme, err := NewScheme()
	if err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: o.MetricsAddr},
		HealthProbeBindAddress: o.ProbeAddr,
		LeaderElection:         o.LeaderElect,
		LeaderElectionID:       o.LeaderElectID,
	})
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}
	// One client for the process: it keeps a connection per agent address,
	// so the reconcilers share them rather than each dialling per call.
	agents := NewGRPCAgentClient()
	defer agents.Close()
	r := &ClusterReconciler{Client: mgr.GetClient(), Renderer: Renderer{AdminImage: o.AdminImage, RouterImage: o.RouterImage, ControllerImage: o.ControllerImage, ControllerPlacementDropOldAfter: o.ControllerPlacementDropOldAfter}, Prober: boundedProber{Inner: PgxProber{}}, Agents: agents,
		Metrics: metrics.NewOperator(ctrlmetrics.Registry)}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup reconciler: %w", err)
	}
	if err := (&BackupReconciler{Client: mgr.GetClient(), Agents: agents}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup backup reconciler: %w", err)
	}
	barriers, err := NewGRPCBarrierClient(o.ControllerTLSCert, o.ControllerTLSKey, o.ControllerTLSCA)
	if err != nil {
		return fmt.Errorf("controller credentials: %w", err)
	}
	scheduler := NewBackupScheduler(mgr.GetClient())
	scheduler.Barriers = barriers
	if err := (&BackupPolicyReconciler{Client: mgr.GetClient(), Scheduler: scheduler}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup backup policy reconciler: %w", err)
	}
	if err := (&RestoreReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Agents: agents, TwoPC: agents, Barriers: PgxProber{}}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup restore reconciler: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	return mgr.Start(ctx)
}
