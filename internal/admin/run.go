package admin

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/operator"
)

// Options configures the `serve` subcommand.
type Options struct {
	Listen     string
	Kubeconfig string
	Namespace  string
	CatalogDSN string
}

// ParseFlags parses the `serve` subcommand's flags.
func ParseFlags(args []string, stderr io.Writer) (Options, error) {
	fs := flag.NewFlagSet("pgshard-admin serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o Options
	fs.StringVar(&o.Listen, "listen", ":8081", "HTTP listen address")
	fs.StringVar(&o.Kubeconfig, "kubeconfig", "", "path to a kubeconfig; empty uses the in-cluster configuration")
	fs.StringVar(&o.Namespace, "namespace", "", "namespace to watch; empty watches all namespaces")
	fs.StringVar(&o.CatalogDSN, "catalog-dsn", "", "optional PostgreSQL DSN of the catalog database for the shard status snapshot")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return o, nil
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return ctrl.GetConfig()
}

// Run serves the admin UI until ctx is done, then shuts the listener down gracefully.
func Run(ctx context.Context, o Options) error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctrl.SetLogger(zap.New())
	cfg, err := restConfig(o.Kubeconfig)
	if err != nil {
		return fmt.Errorf("kubernetes config: %w", err)
	}
	scheme, err := operator.NewScheme()
	if err != nil {
		return err
	}
	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	}
	if o.Namespace != "" {
		mgrOpts.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{o.Namespace: {}}}
	}
	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}
	notifier := NewNotifier()
	if err := RegisterWatches(mgr, notifier); err != nil {
		return err
	}
	var catalogSrc CatalogSource
	if o.CatalogDSN != "" {
		catalogSrc = PgxCatalog{DSN: o.CatalogDSN}
	}
	srv, err := NewServer(mgr.GetClient(), catalogSrc, notifier, o.Namespace, logger)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		errc := make(chan error, 1)
		go func() { errc <- httpSrv.Serve(ln) }()
		logger.Info("admin listening", "addr", ln.Addr().String())
		select {
		case err := <-errc:
			return err
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := httpSrv.Shutdown(shutdownCtx); err != nil {
				return err
			}
			if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		}
	})); err != nil {
		return err
	}
	return mgr.Start(ctx)
}

// RegisterWatches wires informers on PgShardCluster, PgShardGroup, the backup
// objects and Pod so
// every create/update/delete calls notifier.Notify.
func RegisterWatches(mgr ctrl.Manager, notifier *Notifier) error {
	_, err := ctrl.NewControllerManagedBy(mgr).
		Named("admin-topology").
		WithOptions(controller.Options{SkipNameValidation: &skipNameValidation}).
		WatchesRawSource(source.Kind(mgr.GetCache(), &pgshardv1alpha1.PgShardCluster{}, notifyHandler[*pgshardv1alpha1.PgShardCluster]())).
		WatchesRawSource(source.Kind(mgr.GetCache(), &pgshardv1alpha1.PgShardGroup{}, notifyHandler[*pgshardv1alpha1.PgShardGroup]())).
		WatchesRawSource(source.Kind(mgr.GetCache(), &pgshardv1alpha1.PgShardBackupPolicy{}, notifyHandler[*pgshardv1alpha1.PgShardBackupPolicy]())).
		WatchesRawSource(source.Kind(mgr.GetCache(), &pgshardv1alpha1.PgShardBackup{}, notifyHandler[*pgshardv1alpha1.PgShardBackup]())).
		WatchesRawSource(source.Kind(mgr.GetCache(), &pgshardv1alpha1.PgShardRestore{}, notifyHandler[*pgshardv1alpha1.PgShardRestore]())).
		WatchesRawSource(source.Kind(mgr.GetCache(), &corev1.Pod{}, notifyHandler[*corev1.Pod](), predicate.NewTypedPredicateFuncs(func(p *corev1.Pod) bool {
			_, ok := p.Labels[operator.LabelCluster]
			return ok
		}))).
		Build(reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			notifier.Notify()
			return reconcile.Result{}, nil
		}))
	return err
}

var skipNameValidation = true

func notifyHandler[T client.Object]() handler.TypedEventHandler[T, reconcile.Request] {
	return handler.TypedEnqueueRequestsFromMapFunc[T](func(_ context.Context, obj T) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(obj)}}
	})
}
