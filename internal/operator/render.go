package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent"
	"github.com/andrew01234567890/pgshard/internal/pgtune"
)

const (
	configMountPath = "/etc/pgshard"
	secretMountPath = "/etc/pgshard-secret"
	// internalTLSMountPath holds the router<->pooler mTLS material.
	internalTLSMountPath = "/etc/pgshard-internal-tls"
	internalTLSVolume    = "internal-tls"
	dataMountPath        = "/var/lib/postgresql/data"
	pgdataPath           = dataMountPath + "/pgdata"
	postgresUID          = int64(999)
	// pgSocketDir is the agent's fixed unix_socket_directories; the pooler
	// sidecar reaches the local server through it over a shared emptyDir.
	pgSocketDir    = "/tmp"
	poolerGRPCPort = int32(9091)
	// poolerMetricsPort serves the pooler sidecar's /metrics.
	poolerMetricsPort = int32(9127)
	poolerContainer   = "pooler"
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
		obj.Spec.ShardSet = g.ShardSet()
		obj.Spec.NonServing = g.NonServing
	}
	return obj
}

// MemberTemplate is everything about a member pod that a change to the
// spec can alter without changing the member's identity. Its hash is
// stamped on the pod; a different hash means the member must be reloaded or
// restarted to match the spec.
type MemberTemplate struct {
	Image        string                      `json:"image"`
	Resources    corev1.ResourceRequirements `json:"resources"`
	Settings     map[string]string           `json:"settings"`
	RestartToken string                      `json:"restartToken,omitempty"`
	// Backup is the policy the members archive to; it changes the pod
	// (mounted Secrets) and archive_mode, so it is part of the pod hash.
	Backup *pgshardv1alpha1.PgShardBackupPolicySpec `json:"backup,omitempty"`
	// InternalTLS is the router<->pooler transport mode plus, when TLS is
	// on, a checksum of the referenced Secret's content; enabling TLS or
	// rotating the certificate must roll the immutable member pods.
	InternalTLS string `json:"internalTLS,omitempty"`
}

// Template computes the desired member template of a group. tuning is the
// derived override for the group (nil when none applies); pol the backup
// policy bound to the cluster (nil when none).
func Template(c *pgshardv1alpha1.PgShardCluster, g Group, tuning pgtune.Settings, pol *pgshardv1alpha1.PgShardBackupPolicy) MemberTemplate {
	tpl := MemberTemplate{
		Image:        ImageFor(c, g),
		Resources:    c.Spec.Resources,
		Settings:     effectiveSettings(c.Spec.PostgreSQL.Parameters, tuning),
		RestartToken: c.Annotations[AnnotationRestart],
		InternalTLS:  internalTLSMode(c),
	}
	if pol != nil {
		spec := pol.Spec.DeepCopy()
		tpl.Backup = spec
	}
	return tpl
}

// Hash is the pod-shaped part of the template (image, resources, restart
// token) as stamped on pods; a difference always means a pod restart.
func (t MemberTemplate) Hash() string {
	t.Settings = nil
	b, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// SettingsHash identifies the settings alone: it is what the agent echoes
// back from Reload so a live reload can be confirmed applied.
func (t MemberTemplate) SettingsHash() string {
	b, err := json.Marshal(t.Settings)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// AgentConfig renders the pgshard-agent JSON config for one member given
// the group's current primary.
func AgentConfig(c *pgshardv1alpha1.PgShardCluster, g Group, member, primary string) agent.Config {
	return agentConfig(c, g, member, primary, Template(c, g, nil, nil), false, false)
}

func agentConfig(c *pgshardv1alpha1.PgShardCluster, g Group, member, primary string, tpl MemberTemplate, override, repoReady bool) agent.Config {
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
	cfg := agent.Config{
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
		SettingsHash:    tpl.SettingsHash(),
		NonServing:      g.NonServing,
	}
	if override {
		cfg.OverrideFile = configMountPath + "/" + overrideConfKey
	}
	if tpl.Backup != nil {
		bs := BackupSettings(c, g, tpl.Backup)
		cfg.Backup = &bs
		cfg.RecloneFromRepo = repoReady
		if src, ok := RestoreSourceOf(c); ok {
			opts := src.Options(g)
			cfg.Restore = &opts
		}
	}
	return cfg
}

func agentConfigKey(member string) string { return member + ".json" }

// ConfigMap renders the per-member agent configs and the derived override;
// primary decides which member bootstraps with initdb and which ones clone.
func (Renderer) ConfigMap(c *pgshardv1alpha1.PgShardCluster, g Group, primary string, tuning pgtune.Settings, pol *pgshardv1alpha1.PgShardBackupPolicy, repoReady bool) *corev1.ConfigMap {
	tpl := Template(c, g, tuning, pol)
	data := map[string]string{}
	override := OverrideConf(tuning)
	if override != "" {
		data[overrideConfKey] = override
	}
	for _, m := range g.MemberNames() {
		b, err := json.MarshalIndent(agentConfig(c, g, m, primary, tpl, override != "", repoReady), "", "  ")
		if err != nil {
			panic(err)
		}
		data[agentConfigKey(m)] = string(b) + "\n"
	}
	settings, err := json.Marshal(tpl.Settings)
	if err != nil {
		panic(err)
	}
	data[settingsKey] = string(settings)
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(g, g.ConfigMapName(), c.Namespace, nil),
		Data:       data,
	}
}

// settingsKey holds the effective settings map in the ConfigMap so the next
// reconcile can tell which GUCs a spec change touched.
const settingsKey = "settings.json"

// ConfigMapSettings reads the settings map a ConfigMap was rendered with.
func ConfigMapSettings(cm *corev1.ConfigMap) map[string]string {
	out := map[string]string{}
	if cm == nil || cm.Data[settingsKey] == "" {
		return out
	}
	_ = json.Unmarshal([]byte(cm.Data[settingsKey]), &out)
	return out
}

// MemberRBAC renders the ServiceAccount, Role and RoleBinding the member
// agents use to hold their primary Lease and to look up peers.
func (Renderer) MemberRBAC(c *pgshardv1alpha1.PgShardCluster) (*corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding) {
	name := MemberServiceAccount(c.Name)
	labels := map[string]string{LabelCluster: c.Name}
	meta := metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels}
	sa := &corev1.ServiceAccount{ObjectMeta: meta}
	role := &rbacv1.Role{ObjectMeta: meta, Rules: MemberRules(c)}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: meta,
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: c.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
	}
	return sa, role, rb
}

// MemberRules are the namespace permissions a member agent needs. Lease
// writes are pinned to this cluster's own primary Leases so a compromised
// member cannot steal or fence another cluster's primary; create cannot be
// name-scoped in RBAC, so it stays namespace-wide.
//
// Every generation whose pods are running is included, not just the serving
// one. A reshard or upgrade target holds a primary Lease from the moment it
// starts, well before its generation serves, and a retired source keeps holding
// one for the whole rollback window after the cutover has moved on. A rule
// naming only the serving groups leaves each of them unable to renew, and an
// agent that cannot hold its lease refuses to run.
func MemberRules(c *pgshardv1alpha1.PgShardCluster) []rbacv1.PolicyRule {
	groups := Groups(c)
	groups = append(groups, TargetGroups(c)...)
	groups = append(groups, RetiredGroups(c)...)
	if g := CatalogTargetGroup(c); g != nil {
		groups = append(groups, *g)
	}
	if g := RetiredCatalogGroup(c); g != nil {
		groups = append(groups, *g)
	}
	var leases []string
	for _, g := range groups {
		leases = append(leases, g.LeaseName())
	}
	slices.Sort(leases)
	leases = slices.Compact(leases)
	return []rbacv1.PolicyRule{
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "create"}},
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, ResourceNames: leases, Verbs: []string{"get", "update", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
	}
}

func service(c *pgshardv1alpha1.PgShardCluster, g Group, name string, selector map[string]string, headless bool) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: objectMeta(g, name, c.Namespace, nil),
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: "postgres", Port: postgresPort, TargetPort: intstr.FromInt32(postgresPort)},
			},
		},
	}
	if name == g.ServiceRW() {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Name: "pooler-grpc", Port: poolerGRPCPort, TargetPort: intstr.FromInt32(poolerGRPCPort)})
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
	out := []*corev1.Service{
		service(c, g, g.ServiceRO(), ro, false),
		service(c, g, g.ServiceHeadless(), g.Labels(), true),
	}
	// A retired catalog group must not touch its own -rw Service: for the
	// first generation that name IS the stable catalog endpoint, already
	// repointed at the new-major group by the cutover.
	if g.Kind != "catalog" || !g.Retired {
		out = append(out, service(c, g, g.ServiceRW(), rw, false))
	}
	return out
}

// CatalogEndpointService renders the stable catalog endpoint pointing at
// the active catalog group's primary. It shares its name with the first
// catalog generation's own -rw Service, so it only needs rendering
// explicitly once the active generation moved past 1.
func (Renderer) CatalogEndpointService(c *pgshardv1alpha1.PgShardCluster, active Group) *corev1.Service {
	sel := active.Labels()
	sel[LabelRole] = RolePrimary
	return service(c, active, CatalogServiceRW(c.Name), sel, false)
}

// CatalogGenerationService renders a Service that selects one catalog
// generation's primary regardless of which generation is active. Only
// generation 1 needs it - later generations already have an unambiguous
// -rw Service of their own - and only while an upgrade is in flight.
func (Renderer) CatalogGenerationService(c *pgshardv1alpha1.PgShardCluster, g Group) *corev1.Service {
	sel := g.Labels()
	sel[LabelRole] = RolePrimary
	return service(c, g, CatalogGenerationServiceRW(c.Name, g.Generation), sel, false)
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

// PVC renders the member's data volume claim under name (the member name,
// or a -v<n> successor after a storage rebuild).
func (Renderer) PVC(c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: objectMeta(g, name, c.Namespace, map[string]string{LabelOrdinal: strconv.Itoa(ordinal), LabelMember: g.MemberName(ordinal)}),
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

// Pod renders member ordinal of the group with the given role label, bound
// to the pvc claim and stamped with the template hash; the agent is PID 1
// and reads its config from the group ConfigMap.
func (Renderer) Pod(c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int, role, pvc string, tpl MemberTemplate) *corev1.Pod {
	name := g.MemberName(ordinal)
	uid := postgresUID
	meta := objectMeta(g, name, c.Namespace, map[string]string{LabelOrdinal: strconv.Itoa(ordinal), LabelRole: role})
	meta.Annotations = map[string]string{AnnotationTemplateHash: tpl.Hash(), AnnotationSettingsHash: tpl.SettingsHash(),
		AnnotationScrape: "true", AnnotationScrapePort: strconv.Itoa(agentHTTPPort), AnnotationScrapePath: "/metrics"}
	pod := &corev1.Pod{
		ObjectMeta: meta,
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
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc}}},
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: g.ConfigMapName()}}}},
				{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: SecretName(c.Name)}}},
				{Name: "pg-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
	if ref := internalTLSRef(c); ref != nil {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: internalTLSVolume,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ref.Name}}})
	}
	if tpl.Backup != nil {
		mountBackupSecrets(pod, tpl.Backup)
	}
	return pod
}

// poolerSidecar renders the pooler container that fronts the local server
// through its Unix socket. It runs from the postgres image so the two
// binaries always ship together.
func poolerSidecar(c *pgshardv1alpha1.PgShardCluster, g Group) corev1.Container {
	// The pooler's identity is its own group's, not the bootstrap set's: it
	// reads its epoch and its migration state under this name, and a
	// generation-2 pooler watching "default" fences against another set's
	// epoch. Equal epochs hide it until they diverge, which is exactly what
	// a failover does.
	shardSet := g.ShardSet()
	if g.Kind == "catalog" {
		shardSet = "catalog"
	}
	args := []string{"run",
		"--listen", fmt.Sprintf(":%d", poolerGRPCPort),
		"--metrics-listen", fmt.Sprintf(":%d", poolerMetricsPort),
		"--pg-socket-dir", pgSocketDir,
		"--catalog-dsn", CatalogDSN(c),
		"--shard-set", shardSet,
		"--shard-id", fmt.Sprint(g.ShardID),
	}
	mounts := []corev1.VolumeMount{
		{Name: "pg-socket", MountPath: pgSocketDir},
		{Name: "secret", MountPath: secretMountPath, ReadOnly: true},
	}
	if internalTLSRef(c) != nil {
		args = append(args,
			"--tls-cert", internalTLSMountPath+"/tls.crt",
			"--tls-key", internalTLSMountPath+"/tls.key",
			"--tls-ca", internalTLSMountPath+"/ca.crt")
		mounts = append(mounts, corev1.VolumeMount{Name: internalTLSVolume, MountPath: internalTLSMountPath, ReadOnly: true})
	} else if c.Spec.InternalTLS.Insecure {
		args = append(args, "--insecure-dev")
	}
	return corev1.Container{
		Name:    poolerContainer,
		Image:   Image(c),
		Command: []string{"pgshard-pooler"},
		Args:    args,
		Env: []corev1.EnvVar{{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(c.Name)}, Key: secretKey}}}},
		Ports: []corev1.ContainerPort{
			{Name: "pooler-grpc", ContainerPort: poolerGRPCPort},
			{Name: "pooler-metrics", ContainerPort: poolerMetricsPort},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(poolerGRPCPort)}},
			PeriodSeconds: 5,
		},
		VolumeMounts: mounts,
	}
}

// internalTLSMode names the router<->pooler transport the spec asks for.
func internalTLSMode(c *pgshardv1alpha1.PgShardCluster) string {
	if ref := internalTLSRef(c); ref != nil {
		return "secret:" + ref.Name
	}
	if c.Spec.InternalTLS.Insecure {
		return "insecure"
	}
	return ""
}

// internalTLSDataChecksum digests a TLS Secret's content so certificate
// rotation changes the member template hash.
func internalTLSDataChecksum(data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write(data[k])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// internalTLSRef returns the router/pooler mTLS secret reference, if any.
func internalTLSRef(c *pgshardv1alpha1.PgShardCluster) *corev1.LocalObjectReference {
	if ref := c.Spec.InternalTLS.SecretRef; ref != nil && ref.Name != "" {
		return ref
	}
	return nil
}
