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
	want := "serve --listen=:5432 --catalog-dsn=host=demo-catalog-rw.ns1.svc port=5432 user=postgres dbname=postgres --catalog-pooler=demo-catalog-rw.ns1.svc:9091 --insecure-dev"
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
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, nil, nil))
	if len(pod.Spec.Containers) != 2 {
		t.Fatalf("containers %d", len(pod.Spec.Containers))
	}
	agent, pooler := pod.Spec.Containers[0], pod.Spec.Containers[1]
	if pooler.Name != "pooler" || pooler.Image != agent.Image {
		t.Errorf("pooler %s %s", pooler.Name, pooler.Image)
	}
	got := strings.Join(append(pooler.Command, pooler.Args...), " ")
	for _, want := range []string{"pgshard-pooler run", "--listen :9091", "--pg-socket-dir /tmp", "--catalog-dsn ", "--shard-set catalog", "--shard-id 0", "--insecure-dev"} {
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
