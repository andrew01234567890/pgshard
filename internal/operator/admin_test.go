package operator

import (
	"context"
	"slices"
	"strings"
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
	dep := Renderer{}.AdminDeployment(c, adminSecretKey)
	if dep.Name != "demo-admin" || dep.Namespace != "ns1" {
		t.Errorf("meta %s/%s", dep.Namespace, dep.Name)
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
	if ctr.Image != DefaultAdminImage {
		t.Errorf("default image %q", ctr.Image)
	}
	if got := (Renderer{AdminImage: "custom:1"}).AdminDeployment(c, adminSecretKey).Spec.Template.Spec.Containers[0].Image; got != "custom:1" {
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

// TestAdminDeploymentCarriesItsCredential: the admin is rendered with the
// token file its API requires, and mounts the Secret holding it. The open
// mode has to be asked for in the spec, and then says so on the command
// line rather than silently serving to anyone.
func TestAdminDeploymentCarriesItsCredential(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "bank", Namespace: "prod"}}
	dep := Renderer{}.AdminDeployment(c, adminSecretKey)
	ctr := dep.Spec.Template.Spec.Containers[0]
	if !slices.Contains(ctr.Args, "--token-file=/etc/pgshard-admin/token") {
		t.Errorf("args %v, want the token file", ctr.Args)
	}
	if len(ctr.VolumeMounts) != 1 || ctr.VolumeMounts[0].MountPath != "/etc/pgshard-admin" || !ctr.VolumeMounts[0].ReadOnly {
		t.Errorf("mounts %+v", ctr.VolumeMounts)
	}
	vols := dep.Spec.Template.Spec.Volumes
	if len(vols) != 1 || vols[0].Secret == nil || vols[0].Secret.SecretName != "bank-admin" {
		t.Errorf("volumes %+v, want the admin secret", vols)
	}
	// A Secret somebody else made follows the basic-auth convention, so
	// the key is mapped onto the file name the admin expects.
	byPassword := Renderer{}.AdminDeployment(c, "password").Spec.Template.Spec.Volumes
	if len(byPassword) != 1 || len(byPassword[0].Secret.Items) != 1 ||
		byPassword[0].Secret.Items[0].Key != "password" || byPassword[0].Secret.Items[0].Path != "token" {
		t.Errorf("a password-keyed secret must still mount as token: %+v", byPassword)
	}

	c.Spec.Admin.InsecureNoAuth = true
	dep = Renderer{}.AdminDeployment(c, adminSecretKey)
	ctr = dep.Spec.Template.Spec.Containers[0]
	if !slices.Contains(ctr.Args, "--insecure-no-auth") {
		t.Errorf("args %v, want the open mode said out loud", ctr.Args)
	}
	if len(ctr.VolumeMounts) != 0 || len(dep.Spec.Template.Spec.Volumes) != 0 {
		t.Errorf("nothing to mount without a credential: %+v %+v", ctr.VolumeMounts, dep.Spec.Template.Spec.Volumes)
	}
}

// TestAdminSecretIsGeneratedAndKept: the credential is the cluster's own,
// generated once and left alone afterwards, so restarting the admin does
// not lock out whoever already has it.
func TestAdminSecretIsGeneratedAndKept(t *testing.T) {
	requireEnvtest(t)
	r, _, c := setup(t, "adm")
	ctx := context.Background()
	reconcile(t, r, c)

	var sec corev1.Secret
	get(t, AdminSecretName(c.Name), &sec)
	ownedBy(t, &sec, c)
	first := string(sec.Data["token"])
	if len(first) < 32 {
		t.Fatalf("token is %d characters, want something worth guarding", len(first))
	}
	if string(sec.Data["username"]) != "admin" {
		t.Errorf("username %q", sec.Data["username"])
	}

	reconcile(t, r, c)
	get(t, AdminSecretName(c.Name), &sec)
	if got := string(sec.Data["token"]); got != first {
		t.Error("the credential must survive a reconcile: it was regenerated")
	}

	// The superuser password is a different credential: reading the admin
	// is not being able to write to PostgreSQL.
	var su corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: SecretName(c.Name)}, &su); err != nil {
		t.Fatal(err)
	}
	if string(su.Data["password"]) == first {
		t.Error("the admin token must not be the superuser password")
	}
}

// TestAdminReadsTheCatalogAsItsOwnLogin (PGS-902): the operator never gave
// the admin a catalog connection, so every catalog-backed page -- the
// migrations panel, the workflow rows, the operation queue -- was empty in
// a cluster it deployed. It reads as pgshard_admin_ui, whose password
// reaches the pod in the environment and appears in no argument.
func TestAdminReadsTheCatalogAsItsOwnLogin(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns1"}}
	dep := Renderer{}.AdminDeployment(c, adminSecretKey)
	ctr := dep.Spec.Template.Spec.Containers[0]
	want := "--catalog-dsn=host=" + CatalogServiceRW(c.Name) + "." + c.Namespace + ".svc port=5432 user=pgshard_admin_ui dbname=postgres application_name=pgshard-admin connect_timeout=5"
	if !slices.Contains(ctr.Args, want) {
		t.Fatalf("args %q, want %q", ctr.Args, want)
	}
	var env *corev1.EnvVar
	for i := range ctr.Env {
		if ctr.Env[i].Name == "PGPASSWORD" {
			env = &ctr.Env[i]
		}
	}
	if env == nil || env.Value != "" || env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil ||
		env.ValueFrom.SecretKeyRef.Name != AdminCatalogSecretName(c.Name) || env.ValueFrom.SecretKeyRef.Key != "password" {
		t.Fatalf("PGPASSWORD %+v, want it read from the %s secret", env, AdminCatalogSecretName(c.Name))
	}
	for _, arg := range ctr.Args {
		if strings.Contains(strings.ToLower(arg), "password") {
			t.Errorf("an argument carries a password: %q", arg)
		}
	}
}

// TestAdminCatalogSecretIsMadeBeforeAnythingMountsIt (PGS-902): the
// Deployment names the Secret holding the catalog password, so a pod that
// starts before it exists never starts at all. It goes with the admin.
func TestAdminCatalogSecretIsMadeBeforeAnythingMountsIt(t *testing.T) {
	requireEnvtest(t)
	r, _, c := setup(t, "admcat")
	ctx := context.Background()
	reconcile(t, r, c)

	var sec corev1.Secret
	get(t, AdminCatalogSecretName(c.Name), &sec)
	ownedBy(t, &sec, c)
	if string(sec.Data["username"]) != "pgshard_admin_ui" || len(sec.Data["password"]) < 32 {
		t.Fatalf("catalog login secret %q with a %d-character password", sec.Data["username"], len(sec.Data["password"]))
	}
	first := string(sec.Data["password"])
	reconcile(t, r, c)
	get(t, AdminCatalogSecretName(c.Name), &sec)
	if string(sec.Data["password"]) != first {
		t.Error("the catalog password must survive a reconcile: it was regenerated")
	}

	disabled := false
	get(t, c.Name, c)
	c.Spec.Admin.Enabled = &disabled
	if err := r.Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)
	if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: AdminCatalogSecretName(c.Name)}, &sec); !apierrors.IsNotFound(err) {
		t.Errorf("a disabled admin left its catalog credential behind: %v", err)
	}
}
