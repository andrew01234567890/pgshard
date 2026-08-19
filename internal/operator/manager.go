package operator

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

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
	MemberCommand []string
	Development   bool
}

// ParseFlags parses the `run` subcommand's flags.
func ParseFlags(args []string, stderr io.Writer) (Options, error) {
	fs := flag.NewFlagSet("pgshard-operator run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o Options
	var cmd string
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", ":8080", "metrics endpoint address, or 0 to disable")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", ":8081", "health/readiness probe address")
	fs.BoolVar(&o.LeaderElect, "leader-elect", false, "enable leader election")
	fs.StringVar(&o.LeaderElectID, "leader-election-id", "pgshard-operator.pgshard.io", "leader election lease name")
	fs.StringVar(&cmd, "member-command", "", "comma-separated container command for group members; empty runs the interim bootstrap shell")
	fs.BoolVar(&o.Development, "development", false, "human-readable logs")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if cmd != "" {
		o.MemberCommand = strings.Split(cmd, ",")
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
	r := &ClusterReconciler{Client: mgr.GetClient(), Renderer: Renderer{MemberCommand: o.MemberCommand}, Prober: PgxProber{}}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup reconciler: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	return mgr.Start(ctx)
}
