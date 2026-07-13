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

func main() {
	var metricsAddress string
	var probeAddress string
	var leaderElection bool
	var secureMetrics bool
	images := owned.DefaultImages()
	flag.StringVar(&metricsAddress, "metrics-bind-address", ":8443", "metrics endpoint bind address; set to 0 to disable")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "health probe bind address")
	flag.BoolVar(&leaderElection, "leader-elect", true, "enable Kubernetes leader election")
	flag.BoolVar(&secureMetrics, "metrics-secure", true, "serve metrics over TLS")
	flag.StringVar(&images.Etcd, "etcd-image", images.Etcd, "etcd image reference")
	flag.StringVar(&images.Orchestrator, "orchestrator-image", images.Orchestrator, "pgshard orchestrator image reference")
	flag.StringVar(&images.Pooler, "pooler-image", images.Pooler, "pgshard pooler image reference")
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: []func(*tls.Config){func(config *tls.Config) {
			config.MinVersion = tls.VersionTLS13
		}},
	})
	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddress,
			SecureServing: secureMetrics,
			TLSOpts: []func(*tls.Config){func(config *tls.Config) {
				config.MinVersion = tls.VersionTLS13
			}},
		},
		WebhookServer:                 webhookServer,
		HealthProbeBindAddress:        probeAddress,
		LeaderElection:                leaderElection,
		LeaderElectionID:              "operator.pgshard.io",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := (&controller.PgShardClusterReconciler{
		Client:    manager.GetClient(),
		APIReader: manager.GetAPIReader(),
		Images:    images,
	}).SetupWithManager(manager); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PgShardCluster")
		os.Exit(1)
	}
	if err := ctrl.NewWebhookManagedBy(manager, &pgshardv1alpha1.PgShardCluster{}).
		WithDefaulter(&pgshardv1alpha1.PgShardClusterDefaulter{}).
		WithValidator(&pgshardv1alpha1.PgShardClusterValidator{}).
		Complete(); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "PgShardCluster")
		os.Exit(1)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add readiness check")
		os.Exit(1)
	}

	setupLog.Info("starting manager", "postgresqlMajor", pgshardv1alpha1.PostgreSQLMajor18, "serviceModes", []string{"rw", "ro", "r"})
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager stopped with an error")
		os.Exit(1)
	}
}
