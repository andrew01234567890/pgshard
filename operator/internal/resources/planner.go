// Package resources produces the Kubernetes resources owned by a PgShardCluster.
// Planning is deliberately pure: the controller can test and diff a complete,
// deterministic desired state before it writes anything to the API server.
package resources

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ManagedByLabel = "app.kubernetes.io/managed-by"
	InstanceLabel  = "app.kubernetes.io/instance"
	ComponentLabel = "app.kubernetes.io/component"
	ClusterLabel   = "pgshard.io/cluster"
	ShardLabel     = "pgshard.io/shard"

	ManagedByValue = "pgshard-operator"

	PostgreSQLConfigSuffix = "-postgresql-config"
	TopologyConfigSuffix   = "-topology"
	EtcdSuffix             = "-etcd"
	OrchestratorSuffix     = "-orchestrator"
	PoolerSuffix           = "-pooler"

	PostgreSQLPort int32 = 5432
	PoolerRWPort   int32 = 5432
	PoolerROPort   int32 = 5433
	PoolerRPort    int32 = 5434
	EtcdClientPort int32 = 2379
	EtcdPeerPort   int32 = 2380
	HTTPPort       int32 = 8080

	etcdExecutable   = "/usr/local/bin/etcd"
	defaultEtcdImage = "registry.k8s.io/etcd:3.6.5-0@sha256:042ef9c02799eb9303abf1aa99b09f09d94b8ee3ba0c2dd3f42dc4e1d3dce534"

	configHashAnnotation = "pgshard.io/config-hash"
	ScaleOwnerAnnotation = "pgshard.io/hpa-scale-handed-off"
)

// Images contains the deployable images used by the supporting workloads.
// Image references are controller configuration, not part of the cluster API,
// so changing a controller release does not mutate the user's database spec.
type Images struct {
	Etcd         string
	Orchestrator string
	Pooler       string
}

// DefaultImages are development-channel references. The controller never uses
// their availability as evidence that PostgreSQL lifecycle or HA is complete.
func DefaultImages() Images {
	return Images{
		Etcd:         defaultEtcdImage,
		Orchestrator: "ghcr.io/andrew01234567890/pgshard-orch:main",
		Pooler:       "ghcr.io/andrew01234567890/pgshard-pooler:main",
	}
}

// Plan returns the complete set of safe-to-create resources for cluster. It
// intentionally does not create PostgreSQL Pods: bootstrap, replication,
// fencing integration, promotion, and recovery are not implemented yet.
func Plan(cluster *pgshardv1alpha1.PgShardCluster, images Images) ([]client.Object, error) {
	if cluster == nil {
		return nil, fmt.Errorf("cluster is nil")
	}
	if messages := validation.IsDNS1123Label(cluster.Name); len(messages) != 0 {
		return nil, fmt.Errorf("cluster name %q cannot be used for owned Services: %s", cluster.Name, messages[0])
	}
	if len(cluster.Name) > pgshardv1alpha1.MaximumClusterNameLength {
		return nil, fmt.Errorf("cluster name %q is too long: at most %d characters are supported", cluster.Name, pgshardv1alpha1.MaximumClusterNameLength)
	}
	if cluster.Namespace == "" {
		return nil, fmt.Errorf("cluster namespace is empty")
	}
	if cluster.UID == "" {
		return nil, fmt.Errorf("cluster UID is empty")
	}
	if cluster.Spec.Shards < 1 || cluster.Spec.Shards > pgshardv1alpha1.MaximumShards {
		return nil, fmt.Errorf("shards must be between 1 and %d", pgshardv1alpha1.MaximumShards)
	}
	if strings.TrimSpace(images.Etcd) == "" || strings.TrimSpace(images.Orchestrator) == "" || strings.TrimSpace(images.Pooler) == "" {
		return nil, fmt.Errorf("etcd, orchestrator, and pooler images must all be configured")
	}
	if err := pgshardv1alpha1.ValidateClusterForReconciliation(cluster); err != nil {
		return nil, fmt.Errorf("cluster fails safety validation: %w", err)
	}
	if endpoint := cluster.Spec.Observability.OpenTelemetryEndpoint; endpoint != "" {
		if err := pgshardv1alpha1.ValidateOpenTelemetryEndpoint(endpoint); err != nil {
			return nil, fmt.Errorf("invalid OpenTelemetry endpoint: %w", err)
		}
	}
	if repository := cluster.Spec.Backup.Repository; repository.S3 != nil {
		if err := pgshardv1alpha1.ValidateObjectReferenceName(repository.S3.CredentialsSecretRef.Name); err != nil {
			return nil, fmt.Errorf("invalid S3 credential Secret reference: %w", err)
		}
		if repository.S3.Endpoint != "" {
			if err := pgshardv1alpha1.ValidateCredentialFreeHTTPSEndpoint(repository.S3.Endpoint); err != nil {
				return nil, fmt.Errorf("invalid S3 endpoint: %w", err)
			}
		}
	}
	if repository := cluster.Spec.Backup.Repository; repository.Filesystem != nil {
		if err := pgshardv1alpha1.ValidateObjectReferenceName(repository.Filesystem.PersistentVolumeClaimName); err != nil {
			return nil, fmt.Errorf("invalid backup PVC reference: %w", err)
		}
	}

	settings, err := cluster.ResolvedPostgreSQLSettings()
	if err != nil {
		return nil, fmt.Errorf("resolve PostgreSQL settings: %w", err)
	}
	postgresqlConfig := renderPostgreSQLConfig(settings)
	topologyConfig, err := renderTopology(cluster)
	if err != nil {
		return nil, err
	}
	topologyHash := configHash(topologyConfig)
	poolerHash := configHash(postgresqlConfig, topologyConfig)

	objects := make([]client.Object, 0, 16+cluster.Spec.Shards)
	objects = append(objects,
		configMap(cluster, cluster.Name+PostgreSQLConfigSuffix, map[string]string{"postgresql.conf": postgresqlConfig}),
		configMap(cluster, cluster.Name+TopologyConfigSuffix, map[string]string{"cluster.json": topologyConfig}),
		applicationService(cluster, "rw", cluster.Spec.Services.ReadWrite, PoolerRWPort),
		applicationService(cluster, "ro", cluster.Spec.Services.ReadOnly, PoolerROPort),
		applicationService(cluster, "r", cluster.Spec.Services.Read, PoolerRPort),
		etcdService(cluster),
		orchestratorService(cluster),
		poolerService(cluster),
		etcdNetworkPolicy(cluster),
	)
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		objects = append(objects, shardService(cluster, shard))
	}

	objects = append(objects,
		etcdStatefulSet(cluster, images.Etcd),
		orchestratorDeployment(cluster, images.Orchestrator, topologyHash),
		poolerDeployment(cluster, images.Pooler, poolerHash),
		podDisruptionBudget(cluster, "etcd", 1),
		podDisruptionBudget(cluster, "orchestrator", 1),
		podDisruptionBudget(cluster, "pooler", 1),
	)
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingHPA {
		objects = append(objects, poolerHPA(cluster))
	}
	return objects, nil
}

func renderPostgreSQLConfig(settings map[string]string) string {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var output strings.Builder
	output.WriteString("# Generated by pgshard-operator. Manual edits are overwritten.\n")
	for _, key := range keys {
		fmt.Fprintf(&output, "%s = %s\n", key, settings[key])
	}
	return output.String()
}

type topologyDocument struct {
	Cluster         string                `json:"cluster"`
	Namespace       string                `json:"namespace"`
	Durability      string                `json:"durability"`
	MembersPerShard int32                 `json:"membersPerShard"`
	Listeners       []topologyListener    `json:"listeners"`
	Shards          []topologyShard       `json:"shards"`
	Databases       []string              `json:"databases,omitempty"`
	Backup          topologyBackup        `json:"backup"`
	Observability   topologyObservability `json:"observability"`
}

type topologyListener struct {
	Mode       string `json:"mode"`
	Service    string `json:"service"`
	TargetPort int32  `json:"targetPort"`
}

type topologyShard struct {
	ID      int32  `json:"id"`
	Service string `json:"service"`
}

type topologyBackup struct {
	Type                  string `json:"type"`
	Bucket                string `json:"bucket,omitempty"`
	Endpoint              string `json:"endpoint,omitempty"`
	Region                string `json:"region,omitempty"`
	Prefix                string `json:"prefix,omitempty"`
	CredentialsSecret     string `json:"credentialsSecret,omitempty"`
	PersistentVolumeClaim string `json:"persistentVolumeClaim,omitempty"`
}

type topologyObservability struct {
	Prometheus            bool   `json:"prometheus"`
	ServiceMonitor        bool   `json:"serviceMonitorRequested"`
	OpenTelemetryEndpoint string `json:"openTelemetryEndpoint,omitempty"`
}

func renderTopology(cluster *pgshardv1alpha1.PgShardCluster) (string, error) {
	document := topologyDocument{
		Cluster:         cluster.Name,
		Namespace:       cluster.Namespace,
		Durability:      string(cluster.Spec.Durability),
		MembersPerShard: cluster.Spec.MembersPerShard,
		Listeners: []topologyListener{
			{Mode: "rw", Service: cluster.Name + "-rw", TargetPort: PoolerRWPort},
			{Mode: "ro", Service: cluster.Name + "-ro", TargetPort: PoolerROPort},
			{Mode: "r", Service: cluster.Name + "-r", TargetPort: PoolerRPort},
		},
		Shards: make([]topologyShard, 0, cluster.Spec.Shards),
		Backup: topologyBackup{Type: string(cluster.Spec.Backup.Repository.Type)},
		Observability: topologyObservability{
			Prometheus:            cluster.Spec.Observability.Prometheus != nil && *cluster.Spec.Observability.Prometheus,
			ServiceMonitor:        cluster.Spec.Observability.ServiceMonitor,
			OpenTelemetryEndpoint: cluster.Spec.Observability.OpenTelemetryEndpoint,
		},
	}
	for shard := int32(0); shard < cluster.Spec.Shards; shard++ {
		document.Shards = append(document.Shards, topologyShard{ID: shard, Service: shardName(cluster.Name, shard)})
	}
	for _, database := range cluster.Spec.Databases {
		document.Databases = append(document.Databases, database.Name)
	}
	sort.Strings(document.Databases)
	if repository := cluster.Spec.Backup.Repository; repository.S3 != nil {
		document.Backup.Bucket = repository.S3.Bucket
		document.Backup.Endpoint = repository.S3.Endpoint
		document.Backup.Region = repository.S3.Region
		document.Backup.Prefix = repository.S3.Prefix
		document.Backup.CredentialsSecret = repository.S3.CredentialsSecretRef.Name
	}
	if repository := cluster.Spec.Backup.Repository; repository.Filesystem != nil {
		document.Backup.PersistentVolumeClaim = repository.Filesystem.PersistentVolumeClaimName
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return "", fmt.Errorf("render topology: %w", err)
	}
	return string(encoded) + "\n", nil
}

func configHash(configs ...string) string {
	hash := sha256.New()
	for _, config := range configs {
		hash.Write([]byte(config))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func configMap(cluster *pgshardv1alpha1.PgShardCluster, name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: ownedMeta(cluster, name, "configuration", nil),
		Data:       data,
	}
}

func applicationService(cluster *pgshardv1alpha1.PgShardCluster, mode string, template pgshardv1alpha1.ServiceTemplate, targetPort int32) *corev1.Service {
	appProtocol := "postgresql"
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+"-"+mode, "pooler", template.Annotations),
		Spec: corev1.ServiceSpec{
			Type:     template.Type,
			Selector: componentSelector(cluster, "pooler"),
			Ports: []corev1.ServicePort{{
				Name:        "postgresql",
				Protocol:    corev1.ProtocolTCP,
				AppProtocol: &appProtocol,
				Port:        PostgreSQLPort,
				TargetPort:  intstr.FromString("pooler-" + mode),
			}},
		},
	}
}

func shardService(cluster *pgshardv1alpha1.PgShardCluster, shard int32) *corev1.Service {
	selector := componentSelector(cluster, "postgresql")
	selector[ShardLabel] = shardLabel(shard)
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, shardName(cluster.Name, shard), "postgresql", nil),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selector,
			Ports: []corev1.ServicePort{
				{Name: "postgresql", Protocol: corev1.ProtocolTCP, Port: PostgreSQLPort, TargetPort: intstr.FromString("postgresql")},
				{Name: "agent-http", Protocol: corev1.ProtocolTCP, Port: HTTPPort, TargetPort: intstr.FromString("agent-http")},
			},
		},
	}
}

func etcdService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+EtcdSuffix, "etcd", nil),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 componentSelector(cluster, "etcd"),
			Ports: []corev1.ServicePort{
				{Name: "client", Protocol: corev1.ProtocolTCP, Port: EtcdClientPort, TargetPort: intstr.FromString("client")},
				{Name: "peer", Protocol: corev1.ProtocolTCP, Port: EtcdPeerPort, TargetPort: intstr.FromString("peer")},
			},
		},
	}
}

func orchestratorService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+OrchestratorSuffix, "orchestrator", nil),
		Spec: corev1.ServiceSpec{
			Selector: componentSelector(cluster, "orchestrator"),
			Ports:    []corev1.ServicePort{{Name: "http", Protocol: corev1.ProtocolTCP, Port: HTTPPort, TargetPort: intstr.FromString("http")}},
		},
	}
}

func poolerService(cluster *pgshardv1alpha1.PgShardCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: ownedMeta(cluster, cluster.Name+PoolerSuffix, "pooler", nil),
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			PublishNotReadyAddresses: true,
			Selector:                 componentSelector(cluster, "pooler"),
			Ports:                    []corev1.ServicePort{{Name: "http", Protocol: corev1.ProtocolTCP, Port: HTTPPort, TargetPort: intstr.FromString("http")}},
		},
	}
}

func etcdNetworkPolicy(cluster *pgshardv1alpha1.PgShardCluster) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP
	clientPort := intstr.FromInt32(EtcdClientPort)
	peerPort := intstr.FromInt32(EtcdPeerPort)
	return &networkingv1.NetworkPolicy{
		ObjectMeta: ownedMeta(cluster, cluster.Name+EtcdSuffix, "etcd", nil),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: componentSelector(cluster, "etcd")},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{
					From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{ClusterLabel: cluster.Name},
						MatchExpressions: []metav1.LabelSelectorRequirement{{
							Key: ComponentLabel, Operator: metav1.LabelSelectorOpIn, Values: []string{"orchestrator", "pooler", "postgresql"},
						}},
					}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &clientPort}},
				},
				{
					From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: componentSelector(cluster, "etcd")}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &peerPort}},
				},
			},
		},
	}
}

func etcdStatefulSet(cluster *pgshardv1alpha1.PgShardCluster, image string) *appsv1.StatefulSet {
	const replicas int32 = 3
	var storageClassName *string
	if cluster.Spec.Storage.StorageClassName != nil {
		storageClassName = ptr(*cluster.Spec.Storage.StorageClassName)
	}
	name := cluster.Name + EtcdSuffix
	selector := componentSelector(cluster, "etcd")
	claimMetadata := ownedMeta(cluster, "data", "etcd", nil)
	// The namespace is assigned when the StatefulSet creates each claim. A
	// direct CR controller reference lets our finalizer UID-safely wait for PVC
	// deletion instead of racing a same-name cluster replacement.
	claimMetadata.Namespace = ""
	initialCluster := make([]string, 0, replicas)
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		pod := fmt.Sprintf("%s-%d", name, ordinal)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=http://%s.%s.%s.svc:%d", pod, pod, name, cluster.Namespace, EtcdPeerPort))
	}
	return &appsv1.StatefulSet{
		ObjectMeta: ownedMeta(cluster, name, "etcd", nil),
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr(replicas),
			ServiceName:         name,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: selector},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec: securePodSpec(selector, []corev1.Container{{
					Name:            "etcd",
					Image:           image,
					ImagePullPolicy: imagePullPolicy(image),
					Command:         []string{etcdExecutable},
					Args: []string{
						"--name=$(POD_NAME)",
						"--data-dir=/var/lib/etcd",
						"--listen-client-urls=http://0.0.0.0:2379",
						"--advertise-client-urls=http://$(POD_NAME)." + name + "." + cluster.Namespace + ".svc:2379",
						"--listen-peer-urls=http://0.0.0.0:2380",
						"--initial-advertise-peer-urls=http://$(POD_NAME)." + name + "." + cluster.Namespace + ".svc:2380",
						"--initial-cluster=" + strings.Join(initialCluster, ","),
						"--initial-cluster-state=new",
						"--initial-cluster-token=" + cluster.Name,
						"--quota-backend-bytes=805306368",
						"--max-wals=2",
						"--max-snapshots=2",
						"--auto-compaction-mode=periodic",
						"--auto-compaction-retention=1h",
					},
					Env: []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}},
					Ports: []corev1.ContainerPort{
						{Name: "client", ContainerPort: EtcdClientPort, Protocol: corev1.ProtocolTCP},
						{Name: "peer", ContainerPort: EtcdPeerPort, Protocol: corev1.ProtocolTCP},
					},
					Resources:      resources("100m", "128Mi", "1", "512Mi"),
					ReadinessProbe: httpReadinessProbe("/readyz", "client"),
					LivenessProbe:  httpLivenessProbe("/livez", "client"),
					VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/etcd"}},
				}}),
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: claimMetadata,
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: storageClassName,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}},
				},
			}},
		},
	}
}

func orchestratorDeployment(cluster *pgshardv1alpha1.PgShardCluster, image, hash string) *appsv1.Deployment {
	const replicas int32 = 3
	selector := componentSelector(cluster, "orchestrator")
	env := []corev1.EnvVar{
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_ORCH_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
		{Name: "PGSHARD_ETCD_ENDPOINTS", Value: etcdEndpoints(cluster)},
	}
	if cluster.Spec.Observability.OpenTelemetryEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cluster.Spec.Observability.OpenTelemetryEndpoint})
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: ownedMeta(cluster, cluster.Name+OrchestratorSuffix, "orchestrator", nil),
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType, RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: intOrStringPtr(intstr.FromInt32(1)), MaxSurge: intOrStringPtr(intstr.FromInt32(1))}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: map[string]string{configHashAnnotation: hash}},
				Spec: securePodSpec(selector, []corev1.Container{{
					Name:            "orchestrator",
					Image:           image,
					ImagePullPolicy: imagePullPolicy(image),
					Env:             env,
					Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP}},
					Resources:       resources("100m", "128Mi", "1", "512Mi"),
					ReadinessProbe:  httpReadinessProbe("/readyz", "http"),
					LivenessProbe:   httpLivenessProbe("/healthz", "http"),
					VolumeMounts:    []corev1.VolumeMount{{Name: "topology", MountPath: "/etc/pgshard", ReadOnly: true}},
				}}),
			},
		},
	}
	deployment.Spec.Template.Spec.Volumes = []corev1.Volume{{
		Name: "topology",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + TopologyConfigSuffix},
		}},
	}}
	return deployment
}

func poolerDeployment(cluster *pgshardv1alpha1.PgShardCluster, image, hash string) *appsv1.Deployment {
	replicas := poolerReplicas(cluster)
	var desiredReplicas *int32
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		desiredReplicas = ptr(replicas)
	}
	selector := componentSelector(cluster, "pooler")
	env := []corev1.EnvVar{
		{Name: "PGSHARD_CLUSTER_ID", Value: cluster.Name},
		{Name: "PGSHARD_TOPOLOGY_FILE", Value: "/etc/pgshard/topology/cluster.json"},
		{Name: "PGSHARD_HTTP_BIND", Value: "0.0.0.0:8080"},
		{Name: "PGSHARD_RW_BIND", Value: "0.0.0.0:5432"},
		{Name: "PGSHARD_RO_BIND", Value: "0.0.0.0:5433"},
		{Name: "PGSHARD_R_BIND", Value: "0.0.0.0:5434"},
		{Name: "PGSHARD_CATALOG_MODE", Value: "bootstrap-unavailable"},
		{Name: "PGSHARD_ETCD_ENDPOINTS", Value: etcdEndpoints(cluster)},
	}
	if cluster.Spec.Observability.OpenTelemetryEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cluster.Spec.Observability.OpenTelemetryEndpoint})
	}
	podSpec := securePodSpec(selector, []corev1.Container{{
		Name:            "pooler",
		Image:           image,
		ImagePullPolicy: imagePullPolicy(image),
		Env:             env,
		Ports: []corev1.ContainerPort{
			{Name: "pooler-rw", ContainerPort: PoolerRWPort, Protocol: corev1.ProtocolTCP},
			{Name: "pooler-ro", ContainerPort: PoolerROPort, Protocol: corev1.ProtocolTCP},
			{Name: "pooler-r", ContainerPort: PoolerRPort, Protocol: corev1.ProtocolTCP},
			{Name: "http", ContainerPort: HTTPPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:      resources("250m", "256Mi", "2", "1Gi"),
		ReadinessProbe: httpReadinessProbe("/readyz", "http"),
		LivenessProbe:  httpLivenessProbe("/healthz", "http"),
		Lifecycle:      &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 10}}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "topology", MountPath: "/etc/pgshard/topology", ReadOnly: true},
			{Name: "postgresql-config", MountPath: "/etc/pgshard/postgresql", ReadOnly: true},
		},
	}})
	podSpec.TerminationGracePeriodSeconds = ptr(int64(60))
	podSpec.Volumes = []corev1.Volume{
		{Name: "topology", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + TopologyConfigSuffix}}}},
		{Name: "postgresql-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + PostgreSQLConfigSuffix}}}},
	}
	metadata := ownedMeta(cluster, cluster.Name+PoolerSuffix, "pooler", nil)
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingHPA {
		metadata.Annotations = map[string]string{ScaleOwnerAnnotation: "true"}
	}
	return &appsv1.Deployment{
		ObjectMeta: metadata,
		Spec: appsv1.DeploymentSpec{
			Replicas: desiredReplicas,
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType, RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: intOrStringPtr(intstr.FromInt32(1)), MaxSurge: intOrStringPtr(intstr.FromInt32(1))}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: selector, Annotations: map[string]string{configHashAnnotation: hash}}, Spec: podSpec},
		},
	}
}

func poolerHPA(cluster *pgshardv1alpha1.PgShardCluster) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := cluster.Spec.Pooler.Scaling.HPA
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: ownedMeta(cluster, cluster.Name+PoolerSuffix, "pooler", nil),
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: cluster.Name + PoolerSuffix},
			MinReplicas:    ptr(hpa.MinReplicas),
			MaxReplicas:    hpa.MaxReplicas,
			Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name:   corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr(hpa.TargetCPUUtilizationPercentage)},
				},
			}},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp:   &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: ptr(int32(30)), SelectPolicy: scalingPolicyPtr(autoscalingv2.MaxChangePolicySelect), Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 60}}},
				ScaleDown: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: ptr(int32(300)), SelectPolicy: scalingPolicyPtr(autoscalingv2.MaxChangePolicySelect), Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 25, PeriodSeconds: 60}}},
			},
		},
	}
}

func podDisruptionBudget(cluster *pgshardv1alpha1.PgShardCluster, component string, maxUnavailable int32) *policyv1.PodDisruptionBudget {
	value := intstr.FromInt32(maxUnavailable)
	budget := &policyv1.PodDisruptionBudget{
		ObjectMeta: ownedMeta(cluster, cluster.Name+"-"+component, component, nil),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable:             &value,
			Selector:                   &metav1.LabelSelector{MatchLabels: componentSelector(cluster, component)},
			UnhealthyPodEvictionPolicy: unhealthyEvictionPolicyPtr(policyv1.AlwaysAllow),
		},
	}
	if component == "pooler" {
		budget.Spec.MaxUnavailable = nil
		budget.Spec.MinAvailable = &value
	}
	return budget
}

func securePodSpec(selector map[string]string, containers []corev1.Container) corev1.PodSpec {
	runAsNonRoot := true
	runAsUser := int64(10001)
	runAsGroup := int64(10001)
	fsGroup := int64(10001)
	fsGroupChangePolicy := corev1.FSGroupChangeOnRootMismatch
	seccomp := corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	for index := range containers {
		containers[index].SecurityContext = &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr(false),
			ReadOnlyRootFilesystem:   ptr(true),
			RunAsNonRoot:             &runAsNonRoot,
			RunAsUser:                &runAsUser,
			RunAsGroup:               &runAsGroup,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		}
	}
	automount := false
	enableServiceLinks := false
	return corev1.PodSpec{
		AutomountServiceAccountToken: &automount,
		EnableServiceLinks:           &enableServiceLinks,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:        &runAsNonRoot,
			RunAsUser:           &runAsUser,
			RunAsGroup:          &runAsGroup,
			FSGroup:             &fsGroup,
			FSGroupChangePolicy: &fsGroupChangePolicy,
			SeccompProfile:      &seccomp,
		},
		Containers: containers,
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
			{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: &metav1.LabelSelector{MatchLabels: selector}},
			{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: &metav1.LabelSelector{MatchLabels: selector}},
		},
	}
}

func ownedMeta(cluster *pgshardv1alpha1.PgShardCluster, name, component string, annotations map[string]string) metav1.ObjectMeta {
	controller := true
	blockDeletion := true
	return metav1.ObjectMeta{
		Name:        name,
		Namespace:   cluster.Namespace,
		Labels:      labels(cluster, component),
		Annotations: cloneMap(annotations),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion:         pgshardv1alpha1.GroupVersion.String(),
			Kind:               "PgShardCluster",
			Name:               cluster.Name,
			UID:                cluster.UID,
			Controller:         &controller,
			BlockOwnerDeletion: &blockDeletion,
		}},
	}
}

func labels(cluster *pgshardv1alpha1.PgShardCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "pgshard",
		ManagedByLabel:           ManagedByValue,
		InstanceLabel:            cluster.Name,
		ComponentLabel:           component,
		ClusterLabel:             cluster.Name,
	}
}

func componentSelector(cluster *pgshardv1alpha1.PgShardCluster, component string) map[string]string {
	return map[string]string{ClusterLabel: cluster.Name, ComponentLabel: component}
}

func shardName(cluster string, shard int32) string {
	return fmt.Sprintf("%s-shard-%04d", cluster, shard)
}

func shardLabel(shard int32) string { return fmt.Sprintf("%04d", shard) }

func etcdEndpoints(cluster *pgshardv1alpha1.PgShardCluster) string {
	name := cluster.Name + EtcdSuffix
	endpoints := make([]string, 0, 3)
	for ordinal := 0; ordinal < 3; ordinal++ {
		endpoints = append(endpoints, fmt.Sprintf("http://%s-%d.%s.%s.svc:%d", name, ordinal, name, cluster.Namespace, EtcdClientPort))
	}
	return strings.Join(endpoints, ",")
}

func poolerReplicas(cluster *pgshardv1alpha1.PgShardCluster) int32 {
	if cluster.Spec.Pooler.Scaling.Mode == pgshardv1alpha1.ScalingFixed {
		return cluster.Spec.Pooler.Scaling.Fixed.Replicas
	}
	return cluster.Spec.Pooler.Scaling.HPA.MinReplicas
}

func resources(requestCPU, requestMemory, limitCPU, limitMemory string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(requestCPU), corev1.ResourceMemory: resource.MustParse(requestMemory)},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(limitCPU), corev1.ResourceMemory: resource.MustParse(limitMemory)},
	}
}

func httpReadinessProbe(path, port string) *corev1.Probe {
	return httpProbe(path, port, 1)
}

func httpLivenessProbe(path, port string) *corev1.Probe {
	return httpProbe(path, port, 3)
}

func httpProbe(path, port string, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString(port), Scheme: corev1.URISchemeHTTP}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
		TimeoutSeconds:      3,
		FailureThreshold:    failureThreshold,
	}
}

func imagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@") {
		return corev1.PullIfNotPresent
	}
	lastComponent := image[strings.LastIndex(image, "/")+1:]
	if !strings.Contains(lastComponent, ":") || strings.HasSuffix(lastComponent, ":latest") || strings.HasSuffix(lastComponent, ":main") {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func cloneMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func ptr[T any](value T) *T { return &value }

func intOrStringPtr(value intstr.IntOrString) *intstr.IntOrString { return &value }

func scalingPolicyPtr(value autoscalingv2.ScalingPolicySelect) *autoscalingv2.ScalingPolicySelect {
	return &value
}

func unhealthyEvictionPolicyPtr(value policyv1.UnhealthyPodEvictionPolicyType) *policyv1.UnhealthyPodEvictionPolicyType {
	return &value
}

// Key identifies an object independently of its in-memory concrete pointer.
func Key(object client.Object) string {
	return fmt.Sprintf("%T/%s/%s", object, object.GetNamespace(), object.GetName())
}
