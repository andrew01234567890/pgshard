package operator

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// DefaultControllerImage is the controller image used when the operator is
// started without --controller-image.
const DefaultControllerImage = "ghcr.io/andrew01234567890/pgshard-controller:latest"

const (
	controllerPort      = 15500
	controllerHTTPPort  = 8082
	controllerComponent = "controller"
)

// ControllerName names every controller object of the cluster. It is also
// the host in DefaultControllerEndpoint, which is what a backup policy's
// barrier and the router's workflow calls already expect to find.
func ControllerName(cluster string) string { return cluster + "-controller" }

func controllerLabels(c *pgshardv1alpha1.PgShardCluster) map[string]string {
	return map[string]string{LabelCluster: c.Name, LabelComponent: controllerComponent}
}

func controllerMeta(c *pgshardv1alpha1.PgShardCluster) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: ControllerName(c.Name), Namespace: c.Namespace, Labels: controllerLabels(c)}
}

// ControllerDeployment renders the controller: the process that resolves
// in-doubt two-phase transactions, applies queued DDL, drives reshard,
// upgrade and placement workflows, and certifies barriers.
//
// One replica. It elects a leader through a catalog advisory lock, so a
// second would idle waiting for the first to let go, and the useful
// redundancy is Kubernetes restarting the one that stopped.
func (r Renderer) ControllerDeployment(c *pgshardv1alpha1.PgShardCluster) *appsv1.Deployment {
	image := r.ControllerImage
	if image == "" {
		image = DefaultControllerImage
	}
	labels := controllerLabels(c)
	args := []string{"run",
		"--catalog-dsn=" + CatalogDSN(c),
		fmt.Sprintf("--listen=:%d", controllerPort),
		fmt.Sprintf("--metrics-listen=:%d", controllerHTTPPort),
		// Without the shard template the resolver does not run at all, so a
		// controller given only the catalog is a controller that cannot
		// finish an in-doubt transaction -- the one job nothing else does.
		"--shard-dsn-template=" + ShardDSNTemplate(c),
		// PostgreSQL on the target opens this connection, not the
		// controller, so this one carries the password rather than reading
		// PGPASSWORD. Kubernetes expands $(PGPASSWORD) from the container's
		// environment, so the secret stays out of the manifest.
		"--subscription-dsn-template=" + SubscriptionDSNTemplate(c),
	}
	if d := r.ControllerPlacementDropOldAfter; d > 0 {
		args = append(args, "--placement-drop-old-after="+d.String())
	}
	var mounts []corev1.VolumeMount
	var volumes []corev1.Volume
	// The cluster's own agent token, and the only one the controller has:
	// agent RPCs no longer depend on the superuser password, and a
	// controller started without this file refuses to materialise rather
	// than falling back to deriving one.
	args = append(args, "--agent-token-file="+agentTokenDir+"/"+agentTokenKey)
	mounts = append(mounts, corev1.VolumeMount{Name: agentTokenVolume, MountPath: agentTokenDir, ReadOnly: true})
	volumes = append(volumes, corev1.Volume{Name: agentTokenVolume, VolumeSource: corev1.VolumeSource{
		Secret: &corev1.SecretVolumeSource{SecretName: AgentSecretName(c.Name)}}})
	if ref := internalTLSRefFor(c, pki.RoleController); ref != nil {
		args = append(args,
			"--tls-cert="+internalTLSMountPath+"/tls.crt",
			"--tls-key="+internalTLSMountPath+"/tls.key",
			"--tls-ca="+internalTLSMountPath+"/ca.crt")
		if c.Spec.InternalTLS.Issue {
			args = append(args, "--tls-authorize-callers")
		}
		mounts = append(mounts, corev1.VolumeMount{Name: internalTLSVolume, MountPath: internalTLSMountPath, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: internalTLSVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.Name}}})
	} else if c.Spec.InternalTLS.Insecure {
		args = append(args, "--insecure-dev")
	}
	// Placement (spec.placement, PGS-497) is not applied here: that field
	// is on another branch. It wants adding when both have landed, so the
	// controller spreads like the routers do.
	dep := &appsv1.Deployment{
		ObjectMeta: controllerMeta(c),
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{
					AnnotationScrape: "true", AnnotationScrapePort: strconv.Itoa(controllerHTTPPort), AnnotationScrapePath: "/metrics"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: ControllerName(c.Name),
					SecurityContext:    &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name:            controllerComponent,
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            args,
						// The catalog superuser: the controller applies DDL
						// on every shard, mutates replication, and finishes
						// prepared transactions other sessions started.
						Env: []corev1.EnvVar{{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(c.Name)}, Key: secretKey}}}},
						Ports: []corev1.ContainerPort{
							{Name: "grpc", ContainerPort: controllerPort},
							{Name: "http", ContainerPort: controllerHTTPPort}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: mounts,
					}},
					Volumes: volumes,
				},
			},
		},
	}
	return dep
}

// ShardDSNTemplate is the superuser DSN of any shard group's primary, with
// the {group} placeholder the controller expands. The password comes from
// PGPASSWORD in the controller's environment.
func ShardDSNTemplate(c *pgshardv1alpha1.PgShardCluster) string {
	return fmt.Sprintf("host=%s-{group}-rw.%s.svc port=%d user=%s dbname=postgres", c.Name, c.Namespace, postgresPort, superuserName)
}

// SubscriptionDSNTemplate is what a target's PostgreSQL uses to subscribe to
// a source database: {group} names the source group and {db} the database.
func SubscriptionDSNTemplate(c *pgshardv1alpha1.PgShardCluster) string {
	return fmt.Sprintf("host=%s-{group}-rw.%s.svc port=%d user=%s password=$(PGPASSWORD) dbname={db}", c.Name, c.Namespace, postgresPort, superuserName)
}

// ControllerService is the address DefaultControllerEndpoint names.
func (Renderer) ControllerService(c *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: controllerMeta(c),
		Spec: corev1.ServiceSpec{
			Selector: controllerLabels(c),
			Ports: []corev1.ServicePort{
				{Name: "grpc", Port: controllerPort, TargetPort: intstr.FromString("grpc")},
				{Name: "http", Port: controllerHTTPPort, TargetPort: intstr.FromString("http")},
			},
		},
	}
}

// reconcileController creates or updates the controller ServiceAccount,
// Deployment and Service.
//
// Without it a cluster serves SQL while nothing resolves in-doubt
// transactions, applies queued DDL, drives a reshard or certifies a
// barrier: everything looks healthy and the control plane is not running.
func (r *ClusterReconciler) reconcileController(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	sa := &corev1.ServiceAccount{ObjectMeta: controllerMeta(c)}
	if err := r.ensureOwned(ctx, c, sa, func() error { sa.Labels = controllerLabels(c); return nil }); err != nil {
		return err
	}
	desired := r.Renderer.ControllerDeployment(c)
	dep := &appsv1.Deployment{ObjectMeta: controllerMeta(c)}
	if err := r.ensureOwned(ctx, c, dep, func() error {
		dep.Labels = desired.Labels
		dep.Spec = desired.Spec
		return nil
	}); err != nil {
		return err
	}
	desiredSvc := r.Renderer.ControllerService(c)
	svc := &corev1.Service{ObjectMeta: controllerMeta(c)}
	return r.ensureOwned(ctx, c, svc, func() error {
		svc.Labels = desiredSvc.Labels
		svc.Spec.Selector = desiredSvc.Spec.Selector
		svc.Spec.Ports = desiredSvc.Spec.Ports
		svc.Spec.Type = desiredSvc.Spec.Type
		return nil
	})
}
