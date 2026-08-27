package operator

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func routerCluster() *pgshardv1alpha1.PgShardCluster {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns1"}}
	c.Spec.Router = pgshardv1alpha1.RouterSpec{MinReplicas: 3, MaxReplicas: 7, HPA: pgshardv1alpha1.HPASpec{CPUUtilization: 55}}
	c.Spec.InternalTLS.Insecure = true
	return c
}

func TestRouterReplicasDefaults(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{}
	if mn, mx := RouterReplicas(c); mn != 2 || mx != 10 {
		t.Errorf("zero spec: %d/%d", mn, mx)
	}
	c.Spec.Router.MinReplicas = 20
	if mn, mx := RouterReplicas(c); mn != 20 || mx != 20 {
		t.Errorf("max must never be below min: %d/%d", mn, mx)
	}
	c.Spec.Router.MaxReplicas = 25
	if mn, mx := RouterReplicas(c); mn != 20 || mx != 25 {
		t.Errorf("explicit: %d/%d", mn, mx)
	}
}

func TestRouterDeployment(t *testing.T) {
	c := routerCluster()
	dep := Renderer{}.RouterDeployment(c)
	if dep.Name != "demo-router" || dep.Namespace != "ns1" || *dep.Spec.Replicas != 3 {
		t.Errorf("meta/replicas: %s/%s %d", dep.Namespace, dep.Name, *dep.Spec.Replicas)
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
	if ctr.Image != DefaultRouterImage {
		t.Errorf("default image %q", ctr.Image)
	}
	if got := (Renderer{RouterImage: "r:1"}).RouterDeployment(c).Spec.Template.Spec.Containers[0].Image; got != "r:1" {
		t.Errorf("custom image %q", got)
	}
	args := strings.Join(ctr.Args, " ")
	want := "serve --listen=:5432 --health-listen=:8080 --catalog-dsn=host=demo-catalog-rw.ns1.svc port=5432 user=postgres dbname=postgres --catalog-pooler=demo-catalog-rw.ns1.svc:9091 " +
		"--peer-cancel-listen=:9090 --peer-service=demo-router-peers.ns1.svc:9090 --insecure-dev"
	if args != want {
		t.Errorf("args\n got %q\nwant %q", args, want)
	}
	if len(ctr.Env) != 1 || ctr.Env[0].Name != "PGPASSWORD" || ctr.Env[0].ValueFrom.SecretKeyRef.Name != "demo-superuser" || ctr.Env[0].ValueFrom.SecretKeyRef.Key != "password" {
		t.Errorf("env %+v", ctr.Env)
	}
	if len(dep.Spec.Template.Spec.Volumes) != 0 || len(ctr.VolumeMounts) != 0 {
		t.Errorf("no TLS volume expected: %+v", dep.Spec.Template.Spec.Volumes)
	}
	if dep.Spec.Template.Spec.ServiceAccountName != "demo-router" || dep.Spec.Selector.MatchLabels[LabelComponent] != "router" {
		t.Errorf("sa/selector: %s %v", dep.Spec.Template.Spec.ServiceAccountName, dep.Spec.Selector)
	}
	if ctr.Resources.Requests.Cpu().IsZero() {
		t.Error("HPA on CPU needs a CPU request")
	}
	if ctr.ReadinessProbe == nil || ctr.ReadinessProbe.TCPSocket == nil || ctr.ReadinessProbe.TCPSocket.Port.String() != "postgres" {
		t.Errorf("readiness %+v", ctr.ReadinessProbe)
	}
}

func TestRouterDeploymentMountsTLSSecret(t *testing.T) {
	c := routerCluster()
	c.Spec.Router.TLS.SecretRef = &corev1.LocalObjectReference{Name: "router-tls"}
	dep := Renderer{}.RouterDeployment(c)
	ctr := dep.Spec.Template.Spec.Containers[0]
	if ctr.Args[len(ctr.Args)-2] != "--tls-cert=/etc/pgshard-tls/tls.crt" || ctr.Args[len(ctr.Args)-1] != "--tls-key=/etc/pgshard-tls/tls.key" {
		t.Errorf("args %v", ctr.Args)
	}
	if len(ctr.VolumeMounts) != 1 || ctr.VolumeMounts[0].MountPath != "/etc/pgshard-tls" || !ctr.VolumeMounts[0].ReadOnly {
		t.Errorf("mounts %+v", ctr.VolumeMounts)
	}
	if v := dep.Spec.Template.Spec.Volumes; len(v) != 1 || v[0].Secret == nil || v[0].Secret.SecretName != "router-tls" {
		t.Errorf("volumes %+v", v)
	}
}

func TestRouterServiceHPAAndPDB(t *testing.T) {
	c := routerCluster()
	svc := Renderer{}.RouterService(c)
	if svc.Spec.Ports[0].Port != 5432 || svc.Spec.Ports[0].TargetPort.String() != "postgres" || svc.Spec.Selector[LabelComponent] != "router" || svc.Spec.Selector[LabelCluster] != "demo" {
		t.Errorf("service %+v", svc.Spec)
	}
	hpa := Renderer{}.RouterHPA(c)
	if *hpa.Spec.MinReplicas != 3 || hpa.Spec.MaxReplicas != 7 {
		t.Errorf("hpa bounds %d/%d", *hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
	}
	if hpa.Spec.ScaleTargetRef.Kind != "Deployment" || hpa.Spec.ScaleTargetRef.Name != "demo-router" || hpa.Spec.ScaleTargetRef.APIVersion != "apps/v1" {
		t.Errorf("target %+v", hpa.Spec.ScaleTargetRef)
	}
	m := hpa.Spec.Metrics[0]
	if m.Resource == nil || m.Resource.Name != corev1.ResourceCPU || *m.Resource.Target.AverageUtilization != 55 {
		t.Errorf("metric %+v", m)
	}
	c.Spec.Router.HPA.CPUUtilization = 0
	if got := *(Renderer{}).RouterHPA(c).Spec.Metrics[0].Resource.Target.AverageUtilization; got != 70 {
		t.Errorf("default cpu %d", got)
	}
	pdb := Renderer{}.RouterPDB(c)
	if pdb.Spec.MinAvailable.IntValue() != 1 || pdb.Spec.Selector.MatchLabels[LabelComponent] != "router" {
		t.Errorf("pdb %+v", pdb.Spec)
	}
}

func TestPoolerSidecarInMemberPod(t *testing.T) {
	c := routerCluster()
	c.Spec.PostgreSQL.Major = 18
	g := Groups(c)[0]
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, Group{}, nil, nil))
	if len(pod.Spec.Containers) != 2 {
		t.Fatalf("containers %d", len(pod.Spec.Containers))
	}
	agent, pooler := pod.Spec.Containers[0], pod.Spec.Containers[1]
	if pooler.Name != "pooler" || pooler.Image != agent.Image {
		t.Errorf("pooler %s %s", pooler.Name, pooler.Image)
	}
	got := strings.Join(append(pooler.Command, pooler.Args...), " ")
	// Without --stream-dsn the pooler refuses every Stream and CopyTables
	// call, so a change stream fails on its first request in any
	// operator-deployed cluster. The database comes from the request, so
	// the DSN only has to reach the local server.
	for _, want := range []string{"pgshard-pooler run", "--listen :9091", "--pg-socket-dir /tmp", "--catalog-dsn ", "--shard-set catalog", "--shard-id 0",
		"--stream-dsn host=/tmp user=postgres dbname=postgres", "--insecure-dev"} {
		if !strings.Contains(got, want) {
			t.Errorf("command %q lacks %q", got, want)
		}
	}
	if strings.Contains(got, "--http") {
		t.Errorf("command %q still passes the removed --http flag", got)
	}
	if pooler.ReadinessProbe == nil || pooler.ReadinessProbe.TCPSocket == nil || pooler.ReadinessProbe.TCPSocket.Port.IntValue() != 9091 {
		t.Errorf("readiness %+v", pooler.ReadinessProbe)
	}
	if len(pooler.Env) != 1 || pooler.Env[0].Name != "PGPASSWORD" || pooler.Env[0].ValueFrom == nil {
		t.Errorf("pooler must read the catalog password from the secret: %+v", pooler.Env)
	}
	socketMount := func(ctr corev1.Container) string {
		for _, m := range ctr.VolumeMounts {
			if m.Name == "pg-socket" {
				return m.MountPath
			}
		}
		return ""
	}
	if socketMount(agent) != "/tmp" || socketMount(pooler) != "/tmp" {
		t.Errorf("both containers must share the socket dir: agent=%q pooler=%q", socketMount(agent), socketMount(pooler))
	}
	found := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "pg-socket" && v.EmptyDir != nil {
			found = true
		}
	}
	if !found {
		t.Error("pg-socket emptyDir volume missing")
	}
	if pod.Spec.Volumes[0].Name != "data" || pod.Spec.Volumes[0].PersistentVolumeClaim == nil {
		t.Errorf("data volume position changed: %+v", pod.Spec.Volumes)
	}
}

func TestInternalTLSEnablesRouterPoolerMTLS(t *testing.T) {
	c := routerCluster()
	c.Spec.PostgreSQL.Major = 18
	c.Spec.InternalTLS.SecretRef = &corev1.LocalObjectReference{Name: "internal-tls"}

	dep := Renderer{}.RouterDeployment(c)
	ctr := dep.Spec.Template.Spec.Containers[0]
	args := strings.Join(ctr.Args, " ")
	if strings.Contains(args, "--insecure-dev") {
		t.Errorf("router args still plaintext: %q", args)
	}
	for _, want := range []string{
		"--pooler-tls-cert=/etc/pgshard-internal-tls/tls.crt",
		"--pooler-tls-key=/etc/pgshard-internal-tls/tls.key",
		"--pooler-tls-ca=/etc/pgshard-internal-tls/ca.crt",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("router args %q lack %q", args, want)
		}
	}
	if v := dep.Spec.Template.Spec.Volumes; len(v) != 1 || v[0].Secret == nil || v[0].Secret.SecretName != "internal-tls" {
		t.Errorf("router volumes %+v", v)
	}

	g := Groups(c)[0]
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, Group{}, nil, nil))
	pooler := pod.Spec.Containers[1]
	pargs := strings.Join(pooler.Args, " ")
	if strings.Contains(pargs, "--insecure-dev") {
		t.Errorf("pooler args still plaintext: %q", pargs)
	}
	for _, want := range []string{
		"--tls-cert /etc/pgshard-internal-tls/tls.crt",
		"--tls-key /etc/pgshard-internal-tls/tls.key",
		"--tls-ca /etc/pgshard-internal-tls/ca.crt",
	} {
		if !strings.Contains(pargs, want) {
			t.Errorf("pooler args %q lack %q", pargs, want)
		}
	}
	var mounted, volumed bool
	for _, m := range pooler.VolumeMounts {
		if m.Name == "internal-tls" && m.MountPath == "/etc/pgshard-internal-tls" && m.ReadOnly {
			mounted = true
		}
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name == "internal-tls" && v.Secret != nil && v.Secret.SecretName == "internal-tls" {
			volumed = true
		}
	}
	if !mounted || !volumed {
		t.Errorf("pooler TLS not mounted (mount=%v volume=%v)", mounted, volumed)
	}
}

func TestInternalTLSFailsClosedWithoutExplicitInsecure(t *testing.T) {
	c := routerCluster()
	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{}
	dep := Renderer{}.RouterDeployment(c)
	args := strings.Join(dep.Spec.Template.Spec.Containers[0].Args, " ")
	if strings.Contains(args, "--insecure-dev") {
		t.Errorf("router args must not fall back to --insecure-dev: %q", args)
	}
	if strings.Contains(args, "--pooler-tls-cert") {
		t.Errorf("router args must not claim TLS material that was never referenced: %q", args)
	}
	c.Spec.PostgreSQL.Major = 18
	g := Groups(c)[0]
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, Group{}, nil, nil))
	got := strings.Join(pod.Spec.Containers[1].Args, " ")
	if strings.Contains(got, "--insecure-dev") || strings.Contains(got, "--tls-cert") {
		t.Errorf("pooler args must carry neither plaintext nor phantom TLS flags: %q", got)
	}

	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{Insecure: true}
	args = strings.Join(Renderer{}.RouterDeployment(c).Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--insecure-dev") {
		t.Errorf("explicit insecure opt-in must render --insecure-dev: %q", args)
	}
}

func TestMemberTemplateHashTracksInternalTLS(t *testing.T) {
	c := routerCluster()
	g := Groups(c)[0]
	insecure := Template(c, g, nil, nil).Hash()

	c.Spec.InternalTLS = pgshardv1alpha1.InternalTLSSpec{SecretRef: &corev1.LocalObjectReference{Name: "internal-tls"}}
	secure := Template(c, g, nil, nil)
	if secure.Hash() == insecure {
		t.Fatal("enabling internal TLS must change the member template hash")
	}

	v1, v2 := secure, secure
	v1.InternalTLS += ":" + internalTLSDataChecksum(map[string][]byte{"tls.crt": []byte("cert-a"), "tls.key": []byte("key-a")})
	v2.InternalTLS += ":" + internalTLSDataChecksum(map[string][]byte{"tls.crt": []byte("cert-b"), "tls.key": []byte("key-a")})
	if v1.Hash() == secure.Hash() {
		t.Fatal("the secret checksum must be part of the member template hash")
	}
	if v1.Hash() == v2.Hash() {
		t.Fatal("rotating the secret content must change the member template hash")
	}
	same := secure
	same.InternalTLS += ":" + internalTLSDataChecksum(map[string][]byte{"tls.key": []byte("key-a"), "tls.crt": []byte("cert-a")})
	if same.Hash() != v1.Hash() {
		t.Fatal("the checksum must not depend on map iteration order")
	}
}

// TestPoolerSidecarCarriesItsOwnShardSet: a pooler reads its epoch and its
// migration state under the shard set it is told it belongs to. Every
// non-catalog group was told "default", so after a reshard promoted g2 its
// poolers were still watching the retired set's epoch - which matches only
// until the epochs diverge, and a failover is what makes them diverge.
func TestPoolerSidecarCarriesItsOwnShardSet(t *testing.T) {
	c := routerCluster()
	c.Spec.PostgreSQL.Major = 18
	for _, tc := range []struct {
		group Group
		want  string
	}{
		{Group{Cluster: c.Name, Kind: "shard", ShardID: 1, Generation: 1}, "--shard-set default"},
		{Group{Cluster: c.Name, Kind: "shard", ShardID: 1, Generation: 2}, "--shard-set g2"},
		{Group{Cluster: c.Name, Kind: "shard", ShardID: 0, Generation: 3}, "--shard-set g3"},
		{Group{Cluster: c.Name, Kind: "catalog", Generation: 2}, "--shard-set catalog"},
	} {
		pod := Renderer{}.Pod(c, tc.group, 0, RolePrimary, tc.group.MemberName(0), Template(c, Group{}, nil, nil))
		pooler := pod.Spec.Containers[len(pod.Spec.Containers)-1]
		got := strings.Join(append(pooler.Command, pooler.Args...), " ")
		if !strings.Contains(got, tc.want) {
			t.Errorf("generation %d %s group: command lacks %q\n%s", tc.group.Generation, tc.group.Kind, tc.want, got)
		}
	}
}

// TestRouterPeerCancelIsWired: the default deployment is two replicas
// behind one Service, and a CancelRequest arrives on a new connection, so
// it lands on an arbitrary replica. Without a peer listener and a way to
// find the others, the key is dropped and the query it named runs on.
func TestRouterPeerCancelIsWired(t *testing.T) {
	c := routerCluster()
	if replicas, _ := RouterReplicas(c); replicas < 2 {
		t.Fatalf("default router replicas = %d; peer cancels only matter above one", replicas)
	}
	svc := (Renderer{}).RouterPeerService(c)
	if svc.Name != "demo-router-peers" {
		t.Errorf("peer service name %q", svc.Name)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("the peer service must be headless so DNS enumerates the replicas, got ClusterIP %q", svc.Spec.ClusterIP)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("a draining replica still holds sessions a cancel must reach, so its address must stay in DNS")
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != routerPeerPort || svc.Spec.Ports[0].TargetPort.StrVal != "peer" {
		t.Errorf("peer ports %+v", svc.Spec.Ports)
	}
	if svc.Spec.Selector[LabelComponent] != "router" {
		t.Errorf("peer selector %v", svc.Spec.Selector)
	}
	ctr := (Renderer{}).RouterDeployment(c).Spec.Template.Spec.Containers[0]
	var named bool
	for _, p := range ctr.Ports {
		if p.Name == "peer" && p.ContainerPort == routerPeerPort {
			named = true
		}
	}
	if !named {
		t.Errorf("the deployment must expose the peer port the Service targets: %+v", ctr.Ports)
	}
}
