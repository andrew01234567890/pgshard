package operator

import (
	"context"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// DefaultAdminImage is the admin UI image used when the operator is started
// without --admin-image.
const DefaultAdminImage = "ghcr.io/andrew01234567890/pgshard-admin:latest"

const (
	adminPort      = 8081
	adminComponent = "admin"
	// LabelComponent marks the operator-managed admin objects.
	LabelComponent = "pgshard.io/component"
)

// AdminEnabled reports whether spec.admin.enabled is set or defaulted to true.
func AdminEnabled(c *pgshardv1alpha1.PgShardCluster) bool {
	return c.Spec.Admin.Enabled == nil || *c.Spec.Admin.Enabled
}

// AdminName names every admin object of the cluster.
func AdminName(cluster string) string { return cluster + "-admin" }

func adminLabels(c *pgshardv1alpha1.PgShardCluster) map[string]string {
	return map[string]string{LabelCluster: c.Name, LabelComponent: adminComponent}
}

func adminMeta(c *pgshardv1alpha1.PgShardCluster) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: AdminName(c.Name), Namespace: c.Namespace, Labels: adminLabels(c)}
}

// AdminRules are the read-only permissions the admin UI needs in the cluster's namespace.
var AdminRules = []rbacv1.PolicyRule{
	{APIGroups: []string{pgshardv1alpha1.GroupVersion.Group}, Resources: []string{"pgshardclusters", "pgshardgroups", "pgshardbackuppolicies", "pgshardbackups", "pgshardrestores", "pgshardreshards"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{""}, Resources: []string{"pods", "persistentvolumeclaims", "services"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch"}},
}

// AdminDeployment renders the admin UI Deployment scoped to the cluster's namespace.
// AdminDeployment renders the admin UI. tokenKey is the key of the admin
// Secret holding the credential; it is mounted under the name the admin
// expects whatever the Secret calls it.
func (r Renderer) AdminDeployment(c *pgshardv1alpha1.PgShardCluster, tokenKey string) *appsv1.Deployment {
	image := r.AdminImage
	if image == "" {
		image = DefaultAdminImage
	}
	labels := adminLabels(c)
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: adminMeta(c),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{
					AnnotationScrape: "true", AnnotationScrapePort: strconv.Itoa(adminPort), AnnotationScrapePath: "/metrics"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: AdminName(c.Name),
					SecurityContext:    &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name:            adminComponent,
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            adminArgs(c),
						Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: adminPort}},
						VolumeMounts:    adminMounts(c),
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")}},
							PeriodSeconds: 10,
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
					Volumes: adminVolumes(c, tokenKey),
				},
			},
		},
	}
}

const (
	adminTokenDir = "/etc/pgshard-admin"
	// adminSecretKey is the key of the admin credential, and the file name
	// it is mounted under.
	adminSecretKey = "token"
)

// adminArgs credentials the admin unless the spec asks for it to be open.
func adminArgs(c *pgshardv1alpha1.PgShardCluster) []string {
	args := []string{"serve", "--listen=:8081", "--namespace=" + c.Namespace}
	if c.Spec.Admin.InsecureNoAuth {
		return append(args, "--insecure-no-auth")
	}
	return append(args, "--token-file="+adminTokenDir+"/"+adminSecretKey)
}

func adminMounts(c *pgshardv1alpha1.PgShardCluster) []corev1.VolumeMount {
	if c.Spec.Admin.InsecureNoAuth {
		return nil
	}
	return []corev1.VolumeMount{{Name: "token", ReadOnly: true, MountPath: adminTokenDir}}
}

func adminVolumes(c *pgshardv1alpha1.PgShardCluster, tokenKey string) []corev1.Volume {
	if c.Spec.Admin.InsecureNoAuth {
		return nil
	}
	if tokenKey == "" {
		tokenKey = adminSecretKey
	}
	return []corev1.Volume{{Name: "token", VolumeSource: corev1.VolumeSource{
		Secret: &corev1.SecretVolumeSource{
			SecretName: AdminSecretName(c.Name),
			Items:      []corev1.KeyToPath{{Key: tokenKey, Path: adminSecretKey}},
		},
	}}}
}

// AdminService renders the ClusterIP Service in front of the admin UI.
func (Renderer) AdminService(c *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: adminMeta(c),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: adminLabels(c),
			Ports:    []corev1.ServicePort{{Name: "http", Port: adminPort, TargetPort: intstr.FromString("http")}},
		},
	}
}

// reconcileAdmin creates or updates the admin ServiceAccount, Role,
// RoleBinding, Deployment and Service, or deletes them when the admin UI is
// disabled.
func (r *ClusterReconciler) reconcileAdmin(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	sa := &corev1.ServiceAccount{ObjectMeta: adminMeta(c)}
	role := &rbacv1.Role{ObjectMeta: adminMeta(c)}
	rb := &rbacv1.RoleBinding{ObjectMeta: adminMeta(c)}
	dep := &appsv1.Deployment{ObjectMeta: adminMeta(c)}
	svc := &corev1.Service{ObjectMeta: adminMeta(c)}
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: AdminSecretName(c.Name), Namespace: c.Namespace}}
	// A credential nothing mounts is a credential somebody can still read,
	// so it goes when the admin does -- and when the admin is deliberately
	// open.
	if !AdminEnabled(c) || c.Spec.Admin.InsecureNoAuth {
		gone := []client.Object{sec}
		if !AdminEnabled(c) {
			gone = []client.Object{dep, svc, rb, role, sa, sec}
		}
		for _, obj := range gone {
			if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		if !AdminEnabled(c) {
			return nil
		}
	}
	tokenKey := adminSecretKey
	if !c.Spec.Admin.InsecureNoAuth {
		var err error
		if tokenKey, err = r.ensureAdminSecret(ctx, c); err != nil {
			return err
		}
	}
	if err := r.ensureOwned(ctx, c, sa, func() error { sa.Labels = adminLabels(c); return nil }); err != nil {
		return err
	}
	if err := r.ensureOwned(ctx, c, role, func() error {
		role.Labels = adminLabels(c)
		role.Rules = AdminRules
		return nil
	}); err != nil {
		return err
	}
	if err := r.ensureOwned(ctx, c, rb, func() error {
		rb.Labels = adminLabels(c)
		rb.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: AdminName(c.Name)}
		rb.Subjects = []rbacv1.Subject{{Kind: "ServiceAccount", Name: AdminName(c.Name), Namespace: c.Namespace}}
		return nil
	}); err != nil {
		return err
	}
	desiredDep := r.Renderer.AdminDeployment(c, tokenKey)
	if err := r.ensureOwned(ctx, c, dep, func() error {
		dep.Labels = desiredDep.Labels
		dep.Spec.Replicas = desiredDep.Spec.Replicas
		dep.Spec.Selector = desiredDep.Spec.Selector
		dep.Spec.Template = desiredDep.Spec.Template
		return nil
	}); err != nil {
		return err
	}
	desiredSvc := r.Renderer.AdminService(c)
	return r.ensureOwned(ctx, c, svc, func() error {
		svc.Labels = desiredSvc.Labels
		svc.Spec.Type = desiredSvc.Spec.Type
		svc.Spec.Selector = desiredSvc.Spec.Selector
		svc.Spec.Ports = desiredSvc.Spec.Ports
		return nil
	})
}
