package operator

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func TestAdminEnabledDefaultsTrue(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{}
	if !AdminEnabled(c) {
		t.Fatal("nil enabled must mean enabled")
	}
	f := false
	c.Spec.Admin.Enabled = &f
	if AdminEnabled(c) {
		t.Fatal("false must disable")
	}
}

func TestAdminDeploymentAndService(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns1"}}
	dep := Renderer{}.AdminDeployment(c)
	if dep.Name != "demo-admin" || dep.Namespace != "ns1" {
		t.Errorf("meta %s/%s", dep.Namespace, dep.Name)
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
	if ctr.Image != DefaultAdminImage {
		t.Errorf("default image %q", ctr.Image)
	}
	if got := (Renderer{AdminImage: "custom:1"}).AdminDeployment(c).Spec.Template.Spec.Containers[0].Image; got != "custom:1" {
		t.Errorf("custom image %q", got)
	}
	wantArgs := []string{"serve", "--listen=:8081", "--namespace=ns1"}
	for i, a := range wantArgs {
		if ctr.Args[i] != a {
			t.Fatalf("args %v", ctr.Args)
		}
	}
	if dep.Spec.Template.Spec.ServiceAccountName != "demo-admin" {
		t.Errorf("service account %q", dep.Spec.Template.Spec.ServiceAccountName)
	}
	if ctr.SecurityContext == nil || ctr.SecurityContext.ReadOnlyRootFilesystem == nil || !*ctr.SecurityContext.ReadOnlyRootFilesystem {
		t.Error("container must run on a read-only root filesystem")
	}
	svc := Renderer{}.AdminService(c)
	if svc.Spec.Ports[0].Port != 8081 || svc.Spec.Selector[LabelComponent] != "admin" || svc.Spec.Selector[LabelCluster] != "demo" {
		t.Errorf("service %+v", svc.Spec)
	}
	if dep.Spec.Selector.MatchLabels[LabelComponent] != "admin" || dep.Spec.Template.Labels[LabelCluster] != "demo" {
		t.Errorf("selector %+v", dep.Spec.Selector)
	}
	for _, rule := range AdminRules {
		for _, v := range rule.Verbs {
			if v != "get" && v != "list" && v != "watch" {
				t.Errorf("admin role grants %q", v)
			}
		}
	}
}

func TestReconcileAdminObjects(t *testing.T) {
	r, _, c := setup(t, "admin")
	reconcile(t, r, c)
	var dep appsv1.Deployment
	get(t, "admin-admin", &dep)
	if dep.Spec.Template.Spec.Containers[0].Image != DefaultAdminImage || len(dep.OwnerReferences) != 1 {
		t.Errorf("deployment %+v", dep.Spec.Template.Spec.Containers[0])
	}
	var svc corev1.Service
	get(t, "admin-admin", &svc)
	var role rbacv1.Role
	get(t, "admin-admin", &role)
	if len(role.Rules) != len(AdminRules) {
		t.Errorf("role rules %+v", role.Rules)
	}
	var rb rbacv1.RoleBinding
	get(t, "admin-admin", &rb)
	if rb.RoleRef.Name != "admin-admin" || rb.Subjects[0].Name != "admin-admin" {
		t.Errorf("binding %+v", rb)
	}
	var sa corev1.ServiceAccount
	get(t, "admin-admin", &sa)

	get(t, "admin", c)
	disabled := false
	c.Spec.Admin.Enabled = &disabled
	if err := k8sClient.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	for _, obj := range []client.Object{&appsv1.Deployment{}, &corev1.Service{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}, &corev1.ServiceAccount{}} {
		err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "admin-admin"}, obj)
		if !apierrors.IsNotFound(err) {
			t.Errorf("%T still present after disabling admin: %v", obj, err)
		}
	}
}
