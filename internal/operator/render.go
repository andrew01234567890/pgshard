package operator

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

const (
	configMountPath = "/etc/pgshard"
	dataMountPath   = "/var/lib/postgresql/data"
	pgdataPath      = dataMountPath + "/pgdata"
	postgresUID     = int64(999)
)

// DefaultMemberCommand runs the interim bootstrap shell shipped in the group
// ConfigMap. The agent replaces it once it lives in the image.
var DefaultMemberCommand = []string{"bash", configMountPath + "/bootstrap.sh"}

// Renderer builds the Kubernetes objects for one cluster.
type Renderer struct {
	MemberCommand []string
}

func (r Renderer) command() []string {
	if len(r.MemberCommand) > 0 {
		return r.MemberCommand
	}
	return DefaultMemberCommand
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

// FixedSettings are the postgresql.conf settings pgshard owns.
func FixedSettings(c *pgshardv1alpha1.PgShardCluster) map[string]string {
	sc := c.Spec.Durability.SynchronousCommit
	if sc == "" {
		sc = "on"
	}
	return map[string]string{
		"listen_addresses":          "'*'",
		"port":                      strconv.Itoa(postgresPort),
		"wal_level":                 "replica",
		"hot_standby":               "on",
		"max_wal_senders":           "10",
		"max_replication_slots":     "10",
		"wal_keep_size":             "'256MB'",
		"fsync":                     "on",
		"full_page_writes":          "on",
		"synchronous_commit":        sc,
		"max_prepared_transactions": "100",
		"ssl":                       "off",
		"password_encryption":       "scram-sha-256",
	}
}

func renderConf(fixed, user map[string]string) string {
	var b strings.Builder
	b.WriteString("# Managed by pgshard-operator; do not edit.\n")
	for _, k := range sortedKeys(fixed) {
		fmt.Fprintf(&b, "%s = %s\n", k, fixed[k])
	}
	if len(user) > 0 {
		b.WriteString("# spec.postgresql.parameters\n")
		for _, k := range sortedKeys(user) {
			fmt.Fprintf(&b, "%s = '%s'\n", k, strings.ReplaceAll(user[k], "'", "''"))
		}
	}
	return b.String()
}

const bootstrapScript = `#!/usr/bin/env bash
# Interim bootstrap: initdb (member 0) or pg_basebackup (standbys), then postgres.
set -euo pipefail
export PGDATA="${PGDATA:?}"
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  if [ "$PGSHARD_ORDINAL" = "0" ]; then
    pw="$(mktemp)"; printf '%s' "$PGPASSWORD" > "$pw"
    initdb -D "$PGDATA" --username=postgres --pwfile="$pw" --auth-local=trust --auth-host=scram-sha-256 --encoding=UTF8
    rm -f "$pw"
  else
    until pg_basebackup -D "$PGDATA" -R -X stream -C -S "slot_${PGSHARD_ORDINAL}" -c fast \
        -d "host=${PGSHARD_RW_HOST} port=5432 user=postgres application_name=${PGSHARD_MEMBER}"; do
      echo "pg_basebackup from ${PGSHARD_RW_HOST} failed; retrying"; rm -rf "${PGDATA:?}"/*; sleep 5
    done
  fi
fi
conf="$PGDATA/postgresql.conf"
grep -q "pgshard.conf" "$conf" || {
  echo "include '/etc/pgshard/pgshard.conf'" >> "$conf"
  echo "include_if_exists '/etc/pgshard/pgshard.override.conf'" >> "$conf"
}
hba="$PGDATA/pg_hba.conf"
grep -q "pgshard-managed" "$hba" || cat >> "$hba" <<'HBA'
# pgshard-managed
host all         all all scram-sha-256
host replication all all scram-sha-256
HBA
exec postgres -D "$PGDATA"
`

// ConfigMap renders the group's postgresql.conf fragments and bootstrap script.
func (Renderer) ConfigMap(c *pgshardv1alpha1.PgShardCluster, g Group) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: objectMeta(g, g.ConfigMapName(), c.Namespace, nil),
		Data: map[string]string{
			"pgshard.conf":          renderConf(FixedSettings(c), c.Spec.PostgreSQL.Parameters),
			"pgshard.override.conf": "# Tuning overrides land here; empty until the tuning layer exists.\n",
			"bootstrap.sh":          bootstrapScript,
		},
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

// Pod renders member ordinal of the group. Member 0 is the primary at epoch 0.
func (r Renderer) Pod(c *pgshardv1alpha1.PgShardCluster, g Group, ordinal int) *corev1.Pod {
	role := RoleReplica
	if ordinal == 0 {
		role = RolePrimary
	}
	name := g.MemberName(ordinal)
	uid := postgresUID
	env := []corev1.EnvVar{
		{Name: "PGDATA", Value: pgdataPath},
		{Name: "PGSHARD_CLUSTER", Value: c.Name},
		{Name: "PGSHARD_GROUP", Value: g.Name()},
		{Name: "PGSHARD_MEMBER", Value: name},
		{Name: "PGSHARD_ORDINAL", Value: strconv.Itoa(ordinal)},
		{Name: "PGSHARD_RW_HOST", Value: g.ServiceRW()},
		{Name: "PGUSER", Value: superuserName},
		{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: SecretName(c.Name)}, Key: secretKey}}},
	}
	return &corev1.Pod{
		ObjectMeta: objectMeta(g, name, c.Namespace, map[string]string{LabelOrdinal: strconv.Itoa(ordinal), LabelRole: role}),
		Spec: corev1.PodSpec{
			Hostname:      name,
			Subdomain:     g.ServiceHeadless(),
			RestartPolicy: corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: &uid, RunAsGroup: &uid, FSGroup: &uid,
			},
			Containers: []corev1.Container{{
				Name:      "postgres",
				Image:     Image(c),
				Command:   r.command(),
				Env:       env,
				Ports:     []corev1.ContainerPort{{Name: "postgres", ContainerPort: postgresPort}},
				Resources: c.Spec.Resources,
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"pg_isready", "-U", superuserName}}},
					PeriodSeconds:       5,
					InitialDelaySeconds: 5,
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "data", MountPath: dataMountPath},
					{Name: "config", MountPath: configMountPath, ReadOnly: true},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}},
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: g.ConfigMapName()}}}},
			},
		},
	}
}
