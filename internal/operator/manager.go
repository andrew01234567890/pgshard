package operator

import (
	"context"
	"flag"
	"fmt"
	"io"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options configures the operator manager.
type Options struct {
	MetricsAddr   string
	ProbeAddr     string
	LeaderElect   bool
	LeaderElectID string
	AdminImage    string
	RouterImage   string
	Development   bool
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
	fs.StringVar(&o.RouterImage, "router-image", DefaultRouterImage, "image of the router Deployment created for every cluster")
	fs.BoolVar(&o.Development, "development", false, "human-readable logs")
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
	r := &ClusterReconciler{Client: mgr.GetClient(), Renderer: Renderer{AdminImage: o.AdminImage, RouterImage: o.RouterImage}, Prober: PgxProber{}, Agents: GRPCAgentClient{}}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup reconciler: %w", err)
	}
	if err := (&BackupReconciler{Client: mgr.GetClient(), Agents: GRPCAgentClient{}}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup backup reconciler: %w", err)
	}
	if err := (&BackupPolicyReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup backup policy reconciler: %w", err)
	}
	if err := (&RestoreReconciler{Client: mgr.GetClient(), Agents: GRPCAgentClient{}, TwoPC: GRPCAgentClient{}}).SetupWithManager(mgr); err != nil {
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
