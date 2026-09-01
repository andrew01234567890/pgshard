package operator

import (
	"strings"
	"testing"
	"time"

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

// TestTheRouterCanReachTheStreamAndTheController: the merged change stream
// needs both ends. Without --vstream-listen a consumer has nothing to
// dial; without --controller the router can serve an existing stream but
// not create or drop one, so Create answers Unimplemented on a cluster
// whose controller is running right there.
func TestTheRouterCanReachTheStreamAndTheController(t *testing.T) {
	c := controllerCluster()
	args := Renderer{}.RouterDeployment(c).Spec.Template.Spec.Containers[0].Args
	if got := argOf(args, "--vstream-listen="); got == "" {
		t.Fatalf("args %v: no VStream listener, so nothing can consume the stream", args)
	}
	// The address the router is told is the Service this operator renders.
	want := ControllerName(c.Name) + "." + c.Namespace + ".svc:15500"
	if got := argOf(args, "--controller="); got != want {
		t.Fatalf("controller endpoint %q, want %q", got, want)
	}
	if got := ControllerEndpoint("", c.Name, c.Namespace); got != want {
		t.Fatalf("the router dials %q while barriers resolve %q", want, got)
	}

	// And a consumer outside the cluster reaches it through the router
	// Service, so the port has to be published, not merely listened on.
	svc := Renderer{}.RouterService(c)
	var found bool
	for _, p := range svc.Spec.Ports {
		if p.Name == "vstream" {
			found = true
		}
	}
	if !found {
		t.Fatalf("router service ports %+v: the stream is listened on but not published", svc.Spec.Ports)
	}
}

// TestTheControllerCanReachTheShards: without a shard DSN template the
// controller does not start its resolver at all, so a cluster would have a
// controller that cannot finish an in-doubt transaction -- the one job
// nothing else in the system does. Without the subscription template a
// reshard cannot copy: the target's PostgreSQL has nothing to subscribe to.
func TestTheControllerCanReachTheShards(t *testing.T) {
	dep := Renderer{}.ControllerDeployment(controllerCluster())
	args := dep.Spec.Template.Spec.Containers[0].Args

	shard := argOf(args, "--shard-dsn-template=")
	if !strings.Contains(shard, "demo-{group}-rw.ns.svc") || !strings.Contains(shard, "dbname=postgres") {
		t.Errorf("shard DSN template = %q, want the group's rw Service with a {group} placeholder", shard)
	}
	sub := argOf(args, "--subscription-dsn-template=")
	if !strings.Contains(sub, "demo-{group}-rw.ns.svc") || !strings.Contains(sub, "dbname={db}") {
		t.Errorf("subscription DSN template = %q, want the source group's rw Service and a {db} placeholder", sub)
	}
	// PostgreSQL on the target opens this one, not the controller, so it
	// cannot pick the password up from the controller's environment.
	if !strings.Contains(sub, "password=$(PGPASSWORD)") {
		t.Errorf("subscription DSN template = %q, want the password Kubernetes expands from the container's environment", sub)
	}
	if strings.Contains(shard, "password=") {
		t.Errorf("shard DSN template must leave the password to PGPASSWORD, got %q", shard)
	}
}

func TestThePlacementGraceIsOnlyPassedWhenSet(t *testing.T) {
	if got := argOf(Renderer{}.ControllerDeployment(controllerCluster()).Spec.Template.Spec.Containers[0].Args, "--placement-drop-old-after="); got != "" {
		t.Errorf("unset grace must leave the controller's own default, got %q", got)
	}
	r := Renderer{ControllerPlacementDropOldAfter: 10 * time.Second}
	if got := argOf(r.ControllerDeployment(controllerCluster()).Spec.Template.Spec.Containers[0].Args, "--placement-drop-old-after="); got != "10s" {
		t.Errorf("grace = %q, want 10s", got)
	}
}
