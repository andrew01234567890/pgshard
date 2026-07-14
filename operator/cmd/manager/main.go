package main

import (
	"crypto/tls"
	"flag"
	"os"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/controller"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(pgshardv1alpha1.AddToScheme(scheme))
}

type commandOptions struct {
	metricsAddress string
	probeAddress   string
	leaderElection bool
	secureMetrics  bool
	webhookEnabled bool
	images         owned.Images
}

func bindCommandFlags(flags *flag.FlagSet) *commandOptions {
	options := &commandOptions{images: owned.DefaultImages()}
	flags.StringVar(&options.metricsAddress, "metrics-bind-address", ":8443", "metrics endpoint bind address; set to 0 to disable")
	flags.StringVar(&options.probeAddress, "health-probe-bind-address", ":8081", "health probe bind address")
	flags.BoolVar(&options.leaderElection, "leader-elect", true, "enable Kubernetes leader election")
	flags.BoolVar(&options.secureMetrics, "metrics-secure", true, "serve metrics over TLS")
	flags.BoolVar(&options.webhookEnabled, "webhook-enabled", true, "register admission webhooks; serving certificates are required")
	flags.StringVar(&options.images.Etcd, "etcd-image", options.images.Etcd, "etcd image reference")
	flags.StringVar(&options.images.Orchestrator, "orchestrator-image", options.images.Orchestrator, "pgshard orchestrator image reference")
	flags.StringVar(&options.images.Pooler, "pooler-image", options.images.Pooler, "pgshard pooler image reference")
	return options
}

func main() {
	options := bindCommandFlags(flag.CommandLine)
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))

	managerOptions := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   options.metricsAddress,
			SecureServing: options.secureMetrics,
			TLSOpts: []func(*tls.Config){func(config *tls.Config) {
				config.MinVersion = tls.VersionTLS13
			}},
		},
		HealthProbeBindAddress:        options.probeAddress,
		LeaderElection:                options.leaderElection,
		LeaderElectionID:              "operator.pgshard.io",
		LeaderElectionReleaseOnCancel: true,
	}
	if options.webhookEnabled {
		managerOptions.WebhookServer = webhook.NewServer(webhook.Options{
			TLSOpts: []func(*tls.Config){func(config *tls.Config) {
				config.MinVersion = tls.VersionTLS13
			}},
		})
	}
	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), managerOptions)
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := (&controller.PgShardClusterReconciler{
		Client:    manager.GetClient(),
		APIReader: manager.GetAPIReader(),
		Images:    options.images,
	}).SetupWithManager(manager); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PgShardCluster")
		os.Exit(1)
	}
	if options.webhookEnabled {
		if err := ctrl.NewWebhookManagedBy(manager, &pgshardv1alpha1.PgShardCluster{}).
			WithDefaulter(&pgshardv1alpha1.PgShardClusterDefaulter{}).
			WithValidator(&pgshardv1alpha1.PgShardClusterValidator{}).
			Complete(); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "PgShardCluster")
			os.Exit(1)
		}
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add readiness check")
		os.Exit(1)
	}

	setupLog.Info("starting manager", "postgresqlMajor", pgshardv1alpha1.PostgreSQLMajor18, "serviceModes", []string{"rw", "ro", "r"}, "webhookEnabled", options.webhookEnabled)
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager stopped with an error")
		os.Exit(1)
	}
}
