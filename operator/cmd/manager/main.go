package main

import (
	"crypto/tls"
	"flag"
	"os"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/controller"
	"github.com/andrew01234567890/pgshard/operator/internal/pki"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"net/http"

	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
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
	metricsAddress         string
	probeAddress           string
	leaderElection         bool
	secureMetrics          bool
	webhookEnabled         bool
	attestedRequestTimeout time.Duration
	webhook                webhookCommandOptions
	images                 owned.Images
}

type webhookCommandOptions struct {
	namespace                   string
	serviceName                 string
	caSecretName                string
	servingSecretName           string
	fencingKeySecretName        string
	mutatingConfigurationName   string
	validatingConfigurationName string
	certificateDirectory        string
	statefulSetControllerName   string
	replicaSetControllerName    string
	deploymentControllerName    string
	hpaControllerName           string
}

func bindCommandFlags(flags *flag.FlagSet) *commandOptions {
	options := &commandOptions{images: owned.DefaultImages()}
	flags.StringVar(&options.metricsAddress, "metrics-bind-address", ":8443", "metrics endpoint bind address; set to 0 to disable")
	flags.StringVar(&options.probeAddress, "health-probe-bind-address", ":8081", "health probe bind address")
	flags.BoolVar(&options.leaderElection, "leader-elect", true, "enable Kubernetes leader election")
	flags.BoolVar(&options.secureMetrics, "metrics-secure", true, "serve metrics over TLS")
	flags.BoolVar(&options.webhookEnabled, "webhook-enabled", true, "register admission webhooks; serving certificates are required")
	flags.StringVar(&options.webhook.namespace, "webhook-namespace", "pgshard-system", "namespace containing the webhook Service and managed Secrets")
	flags.StringVar(&options.webhook.serviceName, "webhook-service-name", "pgshard-webhook-service", "webhook Service name")
	flags.StringVar(&options.webhook.caSecretName, "webhook-ca-secret-name", "pgshard-webhook-ca", "pre-created webhook CA Secret name")
	flags.StringVar(&options.webhook.servingSecretName, "webhook-serving-secret-name", "pgshard-webhook-certificate", "pre-created webhook serving Secret name")
	flags.StringVar(&options.webhook.fencingKeySecretName, "webhook-fencing-key-secret-name", "pgshard-webhook-fencing-key", "pre-created immutable Pod fencing key Secret name")
	flags.StringVar(&options.webhook.mutatingConfigurationName, "webhook-mutating-configuration-name", "pgshard-mutating-webhook-configuration", "mutating webhook configuration name")
	flags.StringVar(&options.webhook.validatingConfigurationName, "webhook-validating-configuration-name", "pgshard-validating-webhook-configuration", "validating webhook configuration name")
	flags.StringVar(&options.webhook.certificateDirectory, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "private directory for generated webhook certificate files")
	flags.StringVar(&options.webhook.statefulSetControllerName, "statefulset-controller-identity", "system:serviceaccount:kube-system:statefulset-controller", "authenticated username of the built-in StatefulSet controller that creates managed member pods")
	flags.StringVar(&options.webhook.replicaSetControllerName, "replicaset-controller-identity", "system:serviceaccount:kube-system:replicaset-controller", "authenticated username of the built-in ReplicaSet controller that creates managed supporting pods")
	flags.StringVar(&options.webhook.deploymentControllerName, "deployment-controller-identity", "system:serviceaccount:kube-system:deployment-controller", "authenticated username of the built-in Deployment controller that creates managed supporting ReplicaSets")
	flags.StringVar(&options.webhook.hpaControllerName, "horizontalpodautoscaler-controller-identity", "system:serviceaccount:kube-system:horizontal-pod-autoscaler", "authenticated username of the built-in HorizontalPodAutoscaler controller, verified by the activation identity probe")
	flags.DurationVar(&options.attestedRequestTimeout, "attested-max-request-timeout", 0, "installation-attested maximum whole-request lifetime across every API server (the effective --request-timeout ceiling); zero means unattested, which withholds isolation activation")
	flags.StringVar(&options.images.Orchestrator, "orchestrator-image", options.images.Orchestrator, "pgshard orchestrator image reference")
	flags.StringVar(&options.images.Pooler, "pooler-image", options.images.Pooler, "pgshard pooler image reference")
	flags.StringVar(&options.images.PostgreSQL, "postgresql-image", options.images.PostgreSQL, "PostgreSQL 18 image reference")
	flags.StringVar(&options.images.PostgreSQLBootstrap, "postgresql-bootstrap-image", options.images.PostgreSQLBootstrap, "digest-pinned pgshard PostgreSQL 18 bootstrap image required for single-member clusters and multi-member agent-quarantine composition; pgshard/postgres-agent:dev is local-only")
	flags.Var(&options.images.PostgreSQLRuntime, "postgresql-runtime", "creation-time PostgreSQL process composition: direct or the explicit non-serving agent-quarantine integration mode; existing workload changes are rejected")
	return options
}

func main() {
	options := bindCommandFlags(flag.CommandLine)
	zapOptions := zap.Options{Development: false}
	zapOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))

	restConfig := ctrl.GetConfigOrDie()
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
	var webhookServer webhook.Server
	if options.webhookEnabled {
		webhookServer = webhook.NewServer(webhook.Options{
			CertDir: options.webhook.certificateDirectory,
			TLSOpts: []func(*tls.Config){func(config *tls.Config) {
				config.MinVersion = tls.VersionTLS13
			}},
		})
		managerOptions.WebhookServer = webhookServer
	}
	manager, err := ctrl.NewManager(restConfig, managerOptions)
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	receiptKey := podfence.SecretReceiptKeyRef{
		Secret:           client.ObjectKey{Namespace: options.webhook.namespace, Name: options.webhook.fencingKeySecretName},
		DataKey:          pki.PodFencingKeyKey,
		AnchorSecret:     client.ObjectKey{Namespace: options.webhook.namespace, Name: options.webhook.caSecretName},
		AnchorAnnotation: pki.PodFencingKeyFingerprintAnnotation,
	}
	controllerIdentities := podfence.ControllerIdentities{
		Operator:                          "system:serviceaccount:" + options.webhook.namespace + ":pgshard-controller-manager",
		StatefulSetController:             options.webhook.statefulSetControllerName,
		ReplicaSetController:              options.webhook.replicaSetControllerName,
		DeploymentController:              options.webhook.deploymentControllerName,
		HorizontalPodAutoscalerController: options.webhook.hpaControllerName,
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		setupLog.Error(err, "unable to build discovery client for the activation preflight")
		os.Exit(1)
	}
	identityProbeStore := podfence.NewIdentityObservationStore()
	if err := (&controller.PgShardClusterReconciler{
		Client:                      manager.GetClient(),
		APIReader:                   manager.GetAPIReader(),
		Images:                      options.images,
		PodFencingReceiptKey:        receiptKey,
		ControllerIdents:            controllerIdentities,
		DispatchProber:              controller.NewServerDispatchProber(manager.GetAPIReader(), restConfig, options.webhook.namespace, options.webhook.validatingConfigurationName),
		MinorGate:                   controller.NewServerVersionGate(discoveryClient),
		IdentityProber:              controller.NewServerControllerIdentityProber(manager.GetClient(), controllerIdentities, identityProbeStore),
		AttestedRequestTimeout:      options.attestedRequestTimeout,
		ValidatingWebhookConfigName: options.webhook.validatingConfigurationName,
	}).SetupWithManager(manager); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PgShardCluster")
		os.Exit(1)
	}
	if err := (&controller.PgShardRestoreReconciler{
		Client:    manager.GetClient(),
		APIReader: manager.GetAPIReader(),
	}).SetupWithManager(manager); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PgShardRestore")
		os.Exit(1)
	}
	if options.webhookEnabled {
		handshakeCodec := podfence.NewSecretHandshakeCodec(manager.GetAPIReader(), receiptKey)
		if err := ctrl.NewWebhookManagedBy(manager, &pgshardv1alpha1.PgShardCluster{}).
			WithDefaulter(&pgshardv1alpha1.PgShardClusterDefaulter{}).
			WithValidator(&pgshardv1alpha1.PgShardClusterValidator{
				FencingReceiptVerifier:    handshakeCodec,
				FencingControllerUsername: "system:serviceaccount:" + options.webhook.namespace + ":pgshard-controller-manager",
				NamespaceStateReader:      manager.GetAPIReader(),
			}).
			Complete(); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "PgShardCluster")
			os.Exit(1)
		}
		if err := ctrl.NewWebhookManagedBy(manager, &pgshardv1alpha1.PgShardRestore{}).
			WithValidator(&pgshardv1alpha1.PgShardRestoreValidator{}).
			Complete(); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "PgShardRestore")
			os.Exit(1)
		}
		if err := ctrl.NewWebhookManagedBy(manager, &pgshardv1alpha1.PgShardCatalogActivation{}).
			WithValidator(&pgshardv1alpha1.PgShardCatalogActivationValidator{
				ControllerUsername: "system:serviceaccount:" + options.webhook.namespace + ":pgshard-controller-manager",
			}).
			Complete(); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "PgShardCatalogActivation")
			os.Exit(1)
		}
		webhookServer.Register(podfence.BindingWebhookPath, &admission.Webhook{
			Handler: podfence.NewBindingAttestor(manager.GetAPIReader(), scheme),
		})
		webhookServer.Register(podfence.BindingValidationWebhookPath, &admission.Webhook{
			Handler: podfence.NewBindingValidator(manager.GetAPIReader(), scheme),
		})
		webhookServer.Register(podfence.StatusWebhookPath, &admission.Webhook{
			Handler: podfence.NewStatusAttestor(manager.GetAPIReader(), handshakeCodec, scheme),
		})
		webhookServer.Register(podfence.HandshakeWebhookPath, &admission.Webhook{
			Handler: podfence.NewHandshakeAttestor(handshakeCodec, scheme),
		})
		webhookServer.Register(podfence.MetadataWebhookPath, &admission.Webhook{
			Handler: podfence.NewMetadataValidator(handshakeCodec, scheme),
		})
		webhookServer.Register(podfence.PodCreateWebhookPath, &admission.Webhook{
			Handler: podfence.NewPodCreateValidator(manager.GetAPIReader(), controllerIdentities, scheme).WithIdentityProbeStore(identityProbeStore),
		})
		webhookServer.Register(podfence.WorkloadWebhookPath, &admission.Webhook{
			Handler: podfence.NewWorkloadIntegrityValidator(manager.GetAPIReader(), controllerIdentities, scheme).WithIdentityProbeStore(identityProbeStore),
		})
		webhookServer.Register(podfence.PodConnectWebhookPath, &admission.Webhook{
			Handler: podfence.NewPodConnectDenyValidator(manager.GetAPIReader(), options.webhook.namespace),
		})
		webhookServer.Register(podfence.LimitRangeWebhookPath, &admission.Webhook{
			Handler: podfence.NewLimitRangeValidator(),
		})
		webhookServer.Register(podfence.StatusValidationWebhookPath, &admission.Webhook{
			Handler: podfence.NewStatusValidator(manager.GetAPIReader(), handshakeCodec, scheme),
		})
		webhookServer.Register(podfence.NamespaceWebhookPath, &admission.Webhook{
			Handler: podfence.NewNamespaceValidator(scheme),
		})
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add readiness check")
		os.Exit(1)
	}

	managerContext := ctrl.SetupSignalHandler()
	if options.webhookEnabled {
		directClient, err := client.New(restConfig, client.Options{Scheme: scheme})
		if err != nil {
			setupLog.Error(err, "unable to create direct webhook certificate client")
			os.Exit(1)
		}
		certificateProvisioner, err := pki.New(pki.Config{
			Client:                      directClient,
			Namespace:                   options.webhook.namespace,
			ServiceName:                 options.webhook.serviceName,
			CASecretName:                options.webhook.caSecretName,
			ServingSecretName:           options.webhook.servingSecretName,
			FencingKeySecretName:        options.webhook.fencingKeySecretName,
			MutatingConfigurationName:   options.webhook.mutatingConfigurationName,
			ValidatingConfigurationName: options.webhook.validatingConfigurationName,
			CertificateDirectory:        options.webhook.certificateDirectory,
			Logger:                      setupLog.WithName("webhook-pki"),
		})
		if err != nil {
			setupLog.Error(err, "invalid webhook certificate configuration")
			os.Exit(1)
		}
		if err := certificateProvisioner.Bootstrap(managerContext); err != nil {
			setupLog.Error(err, "unable to bootstrap webhook certificates")
			os.Exit(1)
		}
		if err := manager.Add(certificateProvisioner); err != nil {
			setupLog.Error(err, "unable to schedule webhook certificate maintenance")
			os.Exit(1)
		}
		if err := manager.AddReadyzCheck("webhook-certificate", certificateProvisioner.Checker); err != nil {
			setupLog.Error(err, "unable to add webhook certificate readiness check")
			os.Exit(1)
		}
		if err := manager.AddReadyzCheck("webhook-server", webhookServer.StartedChecker()); err != nil {
			setupLog.Error(err, "unable to add webhook server readiness check")
			os.Exit(1)
		}
		// The isolation webhooks read the durable receipt authoritatively per
		// request; report not-ready until an uncached API read succeeds, so a
		// freshly restarted manager never serves admission before it can load the
		// durable isolation state.
		apiReader := manager.GetAPIReader()
		if err := manager.AddReadyzCheck("isolation-state-load", func(request *http.Request) error {
			clusters := &pgshardv1alpha1.PgShardClusterList{}
			return apiReader.List(request.Context(), clusters, client.Limit(1))
		}); err != nil {
			setupLog.Error(err, "unable to add isolation state readiness check")
			os.Exit(1)
		}
	}

	setupLog.Info("starting manager", "postgresqlMajor", pgshardv1alpha1.PostgreSQLMajor18, "serviceModes", []string{"rw", "ro", "r"}, "webhookEnabled", options.webhookEnabled)
	if err := manager.Start(managerContext); err != nil {
		setupLog.Error(err, "manager stopped with an error")
		os.Exit(1)
	}
}
