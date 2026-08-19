package operator

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

// Mount points of the backup Secrets inside member pods.
const (
	backupCredentialsMountPath = "/etc/pgshard-backup/credentials"
	backupEncryptionMountPath  = "/etc/pgshard-backup/encryption"
	backupCredentialsVolume    = "backup-credentials"
	backupEncryptionVolume     = "backup-encryption"
	// LabelBackupPolicy marks PgShardBackup objects the scheduler created.
	LabelBackupPolicy = "pgshard.io/backup-policy"
	// LabelBackupType is the backup type a scheduled PgShardBackup carries.
	LabelBackupType = "pgshard.io/backup-type"
)

// BackupSettings translates the policy into the agent's pgbackrest settings
// for one group.
func BackupSettings(c *pgshardv1alpha1.PgShardCluster, g Group, spec *pgshardv1alpha1.PgShardBackupPolicySpec) backup.Settings {
	st := spec.ObjectStore
	s := backup.Settings{
		Stanza:        backup.StanzaName(c.Name, g.Name(), c.Spec.PostgreSQL.Major),
		RetentionFull: spec.Retention.Full,
		RetentionDiff: spec.Retention.Differential,
		LogLevel:      spec.LogLevel,
		ProcessMax:    spec.ProcessMax,
		Repo: backup.Repo{
			Type:      st.Type,
			Bucket:    st.Bucket,
			Endpoint:  st.Endpoint,
			Region:    st.Region,
			Path:      st.Prefix,
			URIStyle:  st.URIStyle,
			VerifyTLS: st.VerifyTLS,
			KeyType:   st.CredentialType,
		},
	}
	if st.Type == backup.TypeAzure {
		s.Repo.Bucket = st.Container
	}
	if s.Repo.Path != "" && s.Repo.Path[0] != '/' {
		s.Repo.Path = "/" + s.Repo.Path
	}
	if st.Credentials.SecretRef != nil {
		s.Repo.CredentialsDir = backupCredentialsMountPath
	}
	if st.Encryption.SecretRef != nil {
		s.CipherPassFile = backupEncryptionMountPath + "/" + backup.CredPassphrase
	}
	if st.SFTP != nil {
		s.Repo.Host = st.SFTP.Host
		s.Repo.HostUser = st.SFTP.User
		s.Repo.HostPort = st.SFTP.Port
		s.Repo.HostKeyCheck = st.SFTP.HostKeyCheck
	}
	return s
}

// mountBackupSecrets adds the credential and encryption Secret volumes to
// the postgres container.
func mountBackupSecrets(pod *corev1.Pod, spec *pgshardv1alpha1.PgShardBackupPolicySpec) {
	mode := int32(0o400)
	add := func(volume, secret, path string) {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: volume, VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: secret, DefaultMode: &mode}}})
		pg := &pod.Spec.Containers[0]
		pg.VolumeMounts = append(pg.VolumeMounts, corev1.VolumeMount{Name: volume, MountPath: path, ReadOnly: true})
	}
	if ref := spec.ObjectStore.Credentials.SecretRef; ref != nil {
		add(backupCredentialsVolume, ref.Name, backupCredentialsMountPath)
	}
	if ref := spec.ObjectStore.Encryption.SecretRef; ref != nil {
		add(backupEncryptionVolume, ref.Name, backupEncryptionMountPath)
	}
}

// ErrBackupPolicyMissing reports a spec.backup.policyRef that names no policy.
var ErrBackupPolicyMissing = errors.New("backup policy not found")

// findBackupPolicy resolves the cluster's spec.backup.policyRef: nil when the
// cluster has none, ErrBackupPolicyMissing when the named policy is absent.
func findBackupPolicy(ctx context.Context, cl client.Client, c *pgshardv1alpha1.PgShardCluster) (*pgshardv1alpha1.PgShardBackupPolicy, error) {
	ref := c.Spec.Backup.PolicyRef
	if ref == "" {
		return nil, nil
	}
	var pol pgshardv1alpha1.PgShardBackupPolicy
	if err := cl.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: ref}, &pol); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrBackupPolicyMissing, c.Namespace, ref)
		}
		return nil, err
	}
	if !pol.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("%w: %s/%s is being deleted", ErrBackupPolicyMissing, c.Namespace, ref)
	}
	return &pol, nil
}

// clustersOfPolicy lists the clusters in the policy's namespace bound to it.
func clustersOfPolicy(ctx context.Context, cl client.Client, key client.ObjectKey) ([]pgshardv1alpha1.PgShardCluster, error) {
	var list pgshardv1alpha1.PgShardClusterList
	if err := cl.List(ctx, &list, client.InNamespace(key.Namespace)); err != nil {
		return nil, err
	}
	var out []pgshardv1alpha1.PgShardCluster
	for _, c := range list.Items {
		if c.Spec.Backup.PolicyRef == key.Name && c.DeletionTimestamp.IsZero() {
			out = append(out, c)
		}
	}
	return out, nil
}

// policyToClusters enqueues every cluster bound to a changed policy.
func (r *ClusterReconciler) policyToClusters(ctx context.Context, obj client.Object) []ctrl.Request {
	p, ok := obj.(*pgshardv1alpha1.PgShardBackupPolicy)
	if !ok {
		return nil
	}
	clusters, err := clustersOfPolicy(ctx, r.Client, client.ObjectKeyFromObject(p))
	if err != nil {
		return nil
	}
	var reqs []ctrl.Request
	for i := range clusters {
		reqs = append(reqs, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&clusters[i])})
	}
	return reqs
}

// backupToCluster enqueues the cluster a backup belongs to.
func backupToCluster(_ context.Context, obj client.Object) []ctrl.Request {
	b, ok := obj.(*pgshardv1alpha1.PgShardBackup)
	if !ok || b.Spec.ClusterName == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: b.Namespace, Name: b.Spec.ClusterName}}}
}

// backupState resolves the cluster's policy and derives its BackupHealthy
// condition from the completed PgShardBackups. repoReady reports that the
// repository holds a completed backup, so members may be rebuilt from it.
func (r *ClusterReconciler) backupState(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (policy *pgshardv1alpha1.PgShardBackupPolicy, repoReady bool, cond metav1.Condition, err error) {
	cond = metav1.Condition{Type: pgshardv1alpha1.ConditionBackupHealthy, Status: metav1.ConditionFalse, ObservedGeneration: c.Generation}
	policy, err = findBackupPolicy(ctx, r.Client, c)
	switch {
	case errors.Is(err, ErrBackupPolicyMissing):
		cond.Reason, cond.Message = "PolicyMissing", err.Error()
		return nil, false, cond, nil
	case err != nil:
		return nil, false, cond, err
	case policy == nil:
		cond.Reason, cond.Message = "NoPolicy", "spec.backup.policyRef is empty; WAL is not archived"
		return nil, false, cond, nil
	}
	backups, err := backupsOfCluster(ctx, r.Client, c.Namespace, c.Name)
	if err != nil {
		return nil, false, cond, err
	}
	health := BackupHealth(r.now(), policy.Spec.Schedules, lastSuccessful(backups))
	cond.Status, cond.Reason = health.Status, health.Reason
	cond.Message = fmt.Sprintf("policy %s (%s): %s", policy.Name, policy.Spec.ObjectStore.Type, health.Message)
	return policy, len(lastSuccessful(backups)) > 0, cond, nil
}

// backupsOfCluster lists the PgShardBackups naming the cluster.
func backupsOfCluster(ctx context.Context, cl client.Client, namespace, cluster string) ([]pgshardv1alpha1.PgShardBackup, error) {
	var list pgshardv1alpha1.PgShardBackupList
	if err := cl.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var out []pgshardv1alpha1.PgShardBackup
	for _, b := range list.Items {
		if b.Spec.ClusterName == cluster {
			out = append(out, b)
		}
	}
	return out, nil
}
