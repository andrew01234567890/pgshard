package operator

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func controllerCluster() *pgshardv1alpha1.PgShardCluster {
	c := &pgshardv1alpha1.PgShardCluster{}
	c.Name, c.Namespace = "demo", "ns"
	c.Spec.InternalTLS.Insecure = true
	return c
}

func argOf(args []string, prefix string) string {
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return strings.TrimPrefix(a, prefix)
		}
	}
	return ""
}

// TestTheControllerIsReachableWhereEverythingLooksForIt: barriers, the
// router's workflow calls and a backup policy all resolve the controller
// through DefaultControllerEndpoint, "{cluster}-controller.{namespace}
// .svc:15500". Rendering it under any other name or port would leave every
// one of those callers dialling nothing.
func TestTheControllerIsReachableWhereEverythingLooksForIt(t *testing.T) {
	c := controllerCluster()
	svc := Renderer{}.ControllerService(c)
	want := ControllerEndpoint("", c.Name, c.Namespace)
	if got := svc.Name + "." + svc.Namespace + ".svc:15500"; got != want {
		t.Fatalf("service reachable at %s, but callers dial %s", got, want)
	}
	var grpc *corev1.ServicePort
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].Name == "grpc" {
			grpc = &svc.Spec.Ports[i]
		}
	}
	if grpc == nil || grpc.Port != controllerPort {
		t.Fatalf("grpc port %+v, want %d", grpc, controllerPort)
	}
	dep := Renderer{}.ControllerDeployment(c)
	if got := dep.Spec.Selector.MatchLabels; got[LabelComponent] != controllerComponent || got[LabelCluster] != c.Name {
		t.Fatalf("selector %v does not name this cluster's controller", got)
	}
	for k, v := range svc.Spec.Selector {
		if dep.Spec.Template.Labels[k] != v {
			t.Fatalf("the service selects %s=%s, which the pods do not carry", k, v)
		}
	}
}

// TestTheControllerRunsAsTheSuperuser: it applies DDL on every shard,
// mutates replication and finishes prepared transactions other sessions
// started, so it holds the catalog superuser rather than the router's
// least-privilege role.
func TestTheControllerRunsAsTheSuperuser(t *testing.T) {
	c := controllerCluster()
	dep := Renderer{}.ControllerDeployment(c)
	ct := dep.Spec.Template.Spec.Containers[0]
	if got := argOf(ct.Args, "--catalog-dsn="); got != CatalogDSN(c) {
		t.Fatalf("catalog dsn %q, want %q", got, CatalogDSN(c))
	}
	if len(ct.Env) != 1 || ct.Env[0].Name != "PGPASSWORD" ||
		ct.Env[0].ValueFrom.SecretKeyRef.Name != SecretName(c.Name) {
		t.Fatalf("password env %+v, want the cluster superuser secret", ct.Env)
	}
}

// TestOneController: leadership is a catalog advisory lock, so a second
// replica would idle waiting for the first to let go. The redundancy that
// helps is Kubernetes restarting the one that stopped.
func TestOneController(t *testing.T) {
	dep := Renderer{}.ControllerDeployment(controllerCluster())
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("replicas %v, want 1", dep.Spec.Replicas)
	}
}

// TestTheControllerSpeaksTheClusterSTLS: it serves the same mTLS the rest
// of the internal plane uses, or --insecure-dev when the cluster has said
// so; never a plaintext listener that a peer expecting mTLS would refuse.
func TestTheControllerSpeaksTheClusterSTLS(t *testing.T) {
	insecure := Renderer{}.ControllerDeployment(controllerCluster())
	if got := insecure.Spec.Template.Spec.Containers[0].Args; !strings.Contains(strings.Join(got, " "), "--insecure-dev") {
		t.Fatalf("args %v, want --insecure-dev for a cluster with internalTLS.insecure", got)
	}

	secure := controllerCluster()
	secure.Spec.InternalTLS.Insecure = false
	secure.Spec.InternalTLS.SecretRef = &corev1.LocalObjectReference{Name: "internal-tls"}
	dep := Renderer{}.ControllerDeployment(secure)
	ct := dep.Spec.Template.Spec.Containers[0]
	for _, want := range []string{"--tls-cert=", "--tls-key=", "--tls-ca="} {
		if argOf(ct.Args, want) == "" {
			t.Fatalf("args %v, want %s", ct.Args, want)
		}
	}
	if len(ct.VolumeMounts) == 0 || len(dep.Spec.Template.Spec.Volumes) == 0 {
		t.Fatal("the certificate files are named but not mounted")
	}
}
