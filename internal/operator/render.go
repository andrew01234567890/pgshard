package operator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent"
)

const (
	configMountPath = "/etc/pgshard"
	secretMountPath = "/etc/pgshard-secret"
	dataMountPath   = "/var/lib/postgresql/data"
	pgdataPath      = dataMountPath + "/pgdata"
	postgresUID     = int64(999)
	// pgSocketDir is the agent's fixed unix_socket_directories; the pooler
	// sidecar reaches the local server through it over a shared emptyDir.
	pgSocketDir     = "/tmp"
	poolerGRPCPort  = int32(9091)
	poolerContainer = "pooler"
	// agentShutdownTimeout bounds the smart shutdown on SIGTERM so a planned
	// switchover pauses for seconds, not the pod grace period.
	agentShutdownTimeout = 5 * time.Second
)

// Renderer builds the Kubernetes objects for one cluster.
type Renderer struct {
	AdminImage  string
	RouterImage string
}

func objectMeta(g Group, name, namespace string, extra map[string]string) metav1.ObjectMeta {
	labels := g.Labels()
	for k, v := range extra {
		labels[k] = v
	}
	return metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels}
}

// PgShardGroup renders the status-only group object.
func (Renderer) PgShardGroup(c *pgshardv1alpha1.PgShardCluster, g Group) *pgshardv1alpha1.PgShardGroup {
	obj := &pgshardv1alpha1.PgShardGroup{
		ObjectMeta: objectMeta(g, g.Prefix(), c.Namespace, nil),
		Spec:       pgshardv1alpha1.PgShardGroupSpec{ClusterRef: c.Name, Kind: g.Kind},
	}
	if g.Kind == "shard" {
		id := g.ShardID
		obj.Spec.ShardID = &id
	}
	return obj
}

// AgentConfig renders the pgshard-agent JSON config for one member given
// the group's current primary.
func AgentConfig(c *pgshardv1alpha1.PgShardCluster, g Group, member, primary string) agent.Config {
	role := agent.RoleStandby
	if member == primary {
		role = agent.RolePrimary
	}
	var peers []string
	for _, m := range g.MemberNames() {
		if m != member {
			peers = append(peers, fmt.Sprintf("http://%s:%d/failsafe", g.MemberHost(m, c.Namespace), agentHTTPPort))
		}
	}
	return agent.Config{
		Cluster:          c.Name,
		Shard:            g.Name(),
		Member:           member,
		Role:             role,
		PGData:           pgdataPath,
		PasswordFile:     secretMountPath + "/" + secretKey,
		PrimaryConninfo:  fmt.Sprintf("host=%s.%s.svc port=%d user=%s", g.ServiceRW(), c.Namespace, postgresPort, superuserName),
		PodCIDR:          "all",
		PeerFailsafeURLs: peers,
		Port:             postgresPort,
		HTTPAddr:         fmt.Sprintf(":%d", agentHTTPPort),
		GRPCAddr:         fmt.Sprintf(":%d", agentGRPCPort),
		Postgres: agent.PostgresSettings{
			SynchronousStandbyNames: SyncStandbyNames(g, primary, c.Spec.Durability.MinSyncStandbys, nil),
			Parameters:              c.Spec.PostgreSQL.Parameters,
		},
		Lease:           agent.LeaseConfig{Enabled: true, Namespace: c.Namespace},
		ShutdownTimeout: agent.Duration(agentShutdownTimeout),
	}
}

func agentConfigKey(member string) string { return member + ".json" }

// ConfigMap renders the per-member agent configs; primary decides which
// member bootstraps with initdb and which ones clone.
func (Renderer) ConfigMap(c *pgshardv1alpha1.PgShardCluster, g Group, primary string) *corev1.ConfigMap {
	data := map[string]string{}
	for _, m := range g.MemberNames() {
		b, err := json.MarshalIndent(AgentConfig(c, g, m, primary), "", "  ")
		if err != nil {
			panic(err)
		}
		data[agentConfigKey(m)] = string(b) + "\n"
	}
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(g, g.ConfigMapName(), c.Namespace, nil),
		Data:       data,
	}
}

// MemberRBAC renders the ServiceAccount, Role and RoleBinding the member
// agents use to hold their primary Lease and to look up peers.
func (Renderer) MemberRBAC(c *pgshardv1alpha1.PgShardCluster) (*corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding) {
	name := MemberServiceAccount(c.Name)
	labels := map[string]string{LabelCluster: c.Name}
	meta := metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels}
	sa := &corev1.ServiceAccount{ObjectMeta: meta}
	role := &rbacv1.Role{ObjectMeta: meta, Rules: MemberRules()}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: meta,
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: c.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
	}
	return sa, role, rb
}

// MemberRules are the namespace permissions a member agent needs.
func MemberRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
	}
}

func service(c *pgshardv1alpha1.PgShardCluster, g Group, name string, selector map[string]string, headless bool) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: objectMeta(g, name, c.Namespace, nil),
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports:    []corev1.ServicePort{{Name: "postgres", Port: postgresPort, TargetPort: intstr.FromInt32(postgresPort)}},
		},
	}
	if headless {
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
	}
	return svc
}

// Services renders the -rw, -ro and headless peer services.
func (Renderer) Services(c *pgshardv1alpha1.PgShardCluster, g Group) []*corev1.Service {
	rw := g.Labels()
	rw[LabelRole] = RolePrimary
	ro := g.Labels()
	ro[LabelRole] = RoleReplica
	return []*corev1.Service{
		service(c, g, g.ServiceRW(), rw, false),
		service(c, g, g.ServiceRO(), ro, false),
		service(c, g, g.ServiceHeadless(), g.Labels(), true),
	}
}

// PDBs renders the primary PDB and, when the group is large enough, the replica PDB.
func (Renderer) PDBs(c *pgshardv1alpha1.PgShardCluster, g Group) []*policyv1.PodDisruptionBudget {
	primary := g.Labels()
	primary[LabelRole] = RolePrimary
	one := intstr.FromInt32(1)
	out := []*policyv1.PodDisruptionBudget{{
		ObjectMeta: objectMeta(g, g.PDBPrimary(), c.Namespace, nil),
		Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &one, Selector: &metav1.LabelSelector{MatchLabels: primary}},
	}}
	if n := ReplicaMinAvailable(g.Replicas); n > 0 {
		replica := g.Labels()
		replica[LabelRole] = RoleReplica
		minAvail := intstr.FromInt32(int32(n))
		out = append(out, &policyv1.PodDisruptionBudget{
			ObjectMeta: objectMeta(g, g.PDBReplicas(), c.Namespace, nil),
			Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &minAvail, Selector: &metav1.LabelSelector{MatchLabels: replica}},
		})
	}
	return out
}

// PVC renders the member's data volume claim.
func (Renderer) PVC(c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: objectMeta(g, g.MemberName(ordinal), c.Namespace, map[string]string{LabelOrdinal: strconv.Itoa(ordinal)}),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: g.Storage.StorageClassName,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: g.Storage.Size}},
		},
	}
}

func httpProbe(path string, period, failures int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(agentHTTPPort)}},
		PeriodSeconds:    period,
		FailureThreshold: failures,
	}
}

// Pod renders member ordinal of the group with the given role label; the
// agent is PID 1 and reads its config from the group ConfigMap.
func (Renderer) Pod(c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int, role string) *corev1.Pod {
	name := g.MemberName(ordinal)
	uid := postgresUID
	return &corev1.Pod{
		ObjectMeta: objectMeta(g, name, c.Namespace, map[string]string{LabelOrdinal: strconv.Itoa(ordinal), LabelRole: role}),
		Spec: corev1.PodSpec{
			Hostname:           name,
			Subdomain:          g.ServiceHeadless(),
			ServiceAccountName: MemberServiceAccount(c.Name),
			RestartPolicy:      corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: &uid, RunAsGroup: &uid, FSGroup: &uid,
			},
			Containers: []corev1.Container{{
				Name:    "postgres",
				Image:   Image(c),
				Command: []string{"pgshard-agent"},
				Args:    []string{"run", "--config", configMountPath + "/" + agentConfigKey(name)},
				Ports: []corev1.ContainerPort{
					{Name: "postgres", ContainerPort: postgresPort},
					{Name: "http", ContainerPort: agentHTTPPort},
					{Name: "grpc", ContainerPort: agentGRPCPort},
				},
				Resources:      c.Spec.Resources,
				StartupProbe:   httpProbe("/startz", 5, 120),
				ReadinessProbe: httpProbe("/readyz", 5, 1),
				LivenessProbe:  httpProbe("/livez", 10, 3),
				VolumeMounts: []corev1.VolumeMount{
					{Name: "data", MountPath: dataMountPath},
					{Name: "config", MountPath: configMountPath, ReadOnly: true},
					{Name: "secret", MountPath: secretMountPath, ReadOnly: true},
					{Name: "pg-socket", MountPath: pgSocketDir},
				},
			}, poolerSidecar(c, g)},
			Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}},
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: g.ConfigMapName()}}}},
				{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: SecretName(c.Name)}}},
				{Name: "pg-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
}

// poolerSidecar renders the pooler container that fronts the local server
// through its Unix socket. It runs from the postgres image so the two
// binaries always ship together.
func poolerSidecar(c *pgshardv1alpha1.PgShardCluster, g Group) corev1.Container {
	shardSet := "default"
	if g.Kind == "catalog" {
		shardSet = "catalog"
	}
	return corev1.Container{
		Name:    poolerContainer,
		Image:   Image(c),
		Command: []string{"pgshard-pooler"},
		Args: []string{"run",
			"--listen", fmt.Sprintf(":%d", poolerGRPCPort),
			"--pg-socket-dir", pgSocketDir,
			"--catalog-dsn", CatalogDSN(c),
			"--shard-set", shardSet,
			"--shard-id", fmt.Sprint(g.ShardID),
			"--insecure-dev",
		},
		Env: []corev1.EnvVar{{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(c.Name)}, Key: secretKey}}}},
		Ports: []corev1.ContainerPort{
			{Name: "pooler-grpc", ContainerPort: poolerGRPCPort},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(poolerGRPCPort)}},
			PeriodSeconds: 5,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "pg-socket", MountPath: pgSocketDir},
			{Name: "secret", MountPath: secretMountPath, ReadOnly: true},
		},
	}
}
