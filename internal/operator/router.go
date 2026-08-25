package operator

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// DefaultRouterImage is the router image used when the operator is started
// without --router-image.
const DefaultRouterImage = "ghcr.io/andrew01234567890/pgshard-router:latest"

const (
	routerComponent   = "router"
	routerTLSMountDir = "/etc/pgshard-tls"
	// Defaults mirror the CRD defaults for clusters created before the
	// router fields were defaulted.
	defaultRouterMinReplicas    = 2
	defaultRouterMaxReplicas    = 10
	defaultRouterCPUUtilization = 70
)

// RouterName names every router object of the cluster.
func RouterName(cluster string) string { return cluster + "-router" }

func routerLabels(c *pgshardv1alpha1.PgShardCluster) map[string]string {
	return map[string]string{LabelCluster: c.Name, LabelComponent: routerComponent}
}

func routerMeta(c *pgshardv1alpha1.PgShardCluster) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: RouterName(c.Name), Namespace: c.Namespace, Labels: routerLabels(c)}
}

// RouterReplicas returns the effective min and max replicas of the router.
func RouterReplicas(c *pgshardv1alpha1.PgShardCluster) (minReplicas, maxReplicas int32) {
	minReplicas, maxReplicas = int32(c.Spec.Router.MinReplicas), int32(c.Spec.Router.MaxReplicas)
	if minReplicas < 1 {
		minReplicas = defaultRouterMinReplicas
	}
	if maxReplicas < minReplicas {
		maxReplicas = max(minReplicas, defaultRouterMaxReplicas)
	}
	return minReplicas, maxReplicas
}

// CatalogDSN is the libpq connection string the router uses to reach the
// catalog primary; the password comes from PGPASSWORD.
func CatalogDSN(c *pgshardv1alpha1.PgShardCluster) string {
	return fmt.Sprintf("host=%s.%s.svc port=%d user=%s dbname=postgres", CatalogServiceRW(c.Name), c.Namespace, postgresPort, superuserName)
}

// RouterDeployment renders the router Deployment; the HPA owns the replica
// count above minReplicas.
func (r Renderer) RouterDeployment(c *pgshardv1alpha1.PgShardCluster) *appsv1.Deployment {
	image := r.RouterImage
	if image == "" {
		image = DefaultRouterImage
	}
	labels := routerLabels(c)
	minReplicas, _ := RouterReplicas(c)
	args := []string{"serve", fmt.Sprintf("--listen=:%d", postgresPort), fmt.Sprintf("--health-listen=:%d", routerHTTPPort), "--catalog-dsn=" + CatalogDSN(c), "--catalog-pooler=" + CatalogPoolerEndpoint(c)}
	var mounts []corev1.VolumeMount
	var volumes []corev1.Volume
	if ref := internalTLSRef(c); ref != nil {
		args = append(args,
			"--pooler-tls-cert="+internalTLSMountPath+"/tls.crt",
			"--pooler-tls-key="+internalTLSMountPath+"/tls.key",
			"--pooler-tls-ca="+internalTLSMountPath+"/ca.crt")
		mounts = append(mounts, corev1.VolumeMount{Name: internalTLSVolume, MountPath: internalTLSMountPath, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: internalTLSVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.Name}}})
	} else if c.Spec.InternalTLS.Insecure {
		args = append(args, "--insecure-dev")
	}
	if ref := c.Spec.Router.TLS.SecretRef; ref != nil && ref.Name != "" {
		args = append(args, "--tls-cert="+routerTLSMountDir+"/tls.crt", "--tls-key="+routerTLSMountDir+"/tls.key")
		mounts = append(mounts, corev1.VolumeMount{Name: "tls", MountPath: routerTLSMountDir, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.Name}}})
	}
	return &appsv1.Deployment{
		ObjectMeta: routerMeta(c),
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(minReplicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{
					AnnotationScrape: "true", AnnotationScrapePort: strconv.Itoa(routerHTTPPort), AnnotationScrapePath: "/metrics"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: RouterName(c.Name),
					SecurityContext:    &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name:            routerComponent,
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            args,
						Env: []corev1.EnvVar{{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(c.Name)}, Key: secretKey}}}},
						Ports: []corev1.ContainerPort{{Name: "postgres", ContainerPort: postgresPort}, {Name: "http", ContainerPort: routerHTTPPort}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("postgres")}},
							PeriodSeconds: 10,
						},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}},
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
}

// RouterService renders the ClusterIP Service applications connect to.
func (Renderer) RouterService(c *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: routerMeta(c),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: routerLabels(c),
			Ports:    []corev1.ServicePort{{Name: "postgres", Port: postgresPort, TargetPort: intstr.FromString("postgres")}},
		},
	}
}

// RouterHPA renders the CPU-utilization HPA between min and max replicas.
func (Renderer) RouterHPA(c *pgshardv1alpha1.PgShardCluster) *autoscalingv2.HorizontalPodAutoscaler {
	minReplicas, maxReplicas := RouterReplicas(c)
	cpu := int32(c.Spec.Router.HPA.CPUUtilization)
	if cpu < 1 {
		cpu = defaultRouterCPUUtilization
	}
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: routerMeta(c),
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: RouterName(c.Name)},
			MinReplicas:    ptr.To(minReplicas),
			MaxReplicas:    maxReplicas,
			Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name:   corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To(cpu)},
				},
			}},
		},
	}
}

// RouterPDB keeps at least one router serving during voluntary disruptions.
func (Renderer) RouterPDB(c *pgshardv1alpha1.PgShardCluster) *policyv1.PodDisruptionBudget {
	one := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: routerMeta(c),
		Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &one, Selector: &metav1.LabelSelector{MatchLabels: routerLabels(c)}},
	}
}

// AnnotationInternalTLSChecksum on the router pod template digests the
// internal TLS Secret's content so certificate rotation rolls the routers.
const AnnotationInternalTLSChecksum = "pgshard.io/internal-tls-checksum"

// internalTLSChecksum digests the referenced internal TLS Secret; empty
// when the cluster runs with the explicit insecure override.
func (r *ClusterReconciler) internalTLSChecksum(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (string, error) {
	ref := internalTLSRef(c)
	if ref == nil {
		return "", nil
	}
	var sec corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: ref.Name}, &sec); err != nil {
		return "", fmt.Errorf("internal TLS secret %q: %w", ref.Name, err)
	}
	return internalTLSDataChecksum(sec.Data), nil
}

// reconcileRouter creates or updates the router ServiceAccount, Deployment,
// Service, PDB and HPA.
func (r *ClusterReconciler) reconcileRouter(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	sa := &corev1.ServiceAccount{ObjectMeta: routerMeta(c)}
	if err := r.ensureOwned(ctx, c, sa, func() error { sa.Labels = routerLabels(c); return nil }); err != nil {
		return err
	}
	desiredDep := r.Renderer.RouterDeployment(c)
	if sum, err := r.internalTLSChecksum(ctx, c); err != nil {
		return err
	} else if sum != "" {
		if desiredDep.Spec.Template.Annotations == nil {
			desiredDep.Spec.Template.Annotations = map[string]string{}
		}
		desiredDep.Spec.Template.Annotations[AnnotationInternalTLSChecksum] = sum
	}
	dep := &appsv1.Deployment{ObjectMeta: routerMeta(c)}
	if err := r.ensureOwned(ctx, c, dep, func() error {
		dep.Labels = desiredDep.Labels
		// The HPA owns the replica count once it exceeds minReplicas.
		if dep.Spec.Replicas == nil || *dep.Spec.Replicas < *desiredDep.Spec.Replicas {
			dep.Spec.Replicas = desiredDep.Spec.Replicas
		}
		dep.Spec.Selector = desiredDep.Spec.Selector
		dep.Spec.Template = desiredDep.Spec.Template
		return nil
	}); err != nil {
		return err
	}
	desiredSvc := r.Renderer.RouterService(c)
	svc := &corev1.Service{ObjectMeta: routerMeta(c)}
	if err := r.ensureOwned(ctx, c, svc, func() error {
		svc.Labels = desiredSvc.Labels
		svc.Spec.Type = desiredSvc.Spec.Type
		svc.Spec.Selector = desiredSvc.Spec.Selector
		svc.Spec.Ports = desiredSvc.Spec.Ports
		return nil
	}); err != nil {
		return err
	}
	desiredPDB := r.Renderer.RouterPDB(c)
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: routerMeta(c)}
	if err := r.ensureOwned(ctx, c, pdb, func() error {
		pdb.Labels = desiredPDB.Labels
		pdb.Spec = desiredPDB.Spec
		return nil
	}); err != nil {
		return err
	}
	desiredHPA := r.Renderer.RouterHPA(c)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: routerMeta(c)}
	return r.ensureOwned(ctx, c, hpa, func() error {
		hpa.Labels = desiredHPA.Labels
		hpa.Spec = desiredHPA.Spec
		return nil
	})
}

// CatalogPoolerEndpoint is the pooler fronting the catalog group, reached
// through its -rw Service.
func CatalogPoolerEndpoint(c *pgshardv1alpha1.PgShardCluster) string {
	return fmt.Sprintf("%s.%s.svc:%d", Groups(c)[0].ServiceRW(), c.Namespace, poolerGRPCPort)
}
