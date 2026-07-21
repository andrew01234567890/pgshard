package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	owned "github.com/andrew01234567890/pgshard/operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type activationPublicationFixture struct {
	client        client.Client
	verifier      *CatalogActivationPublicationVerifier
	cluster       *pgshardv1alpha1.PgShardCluster
	oldActivation *pgshardv1alpha1.PgShardCatalogActivation
	newActivation *pgshardv1alpha1.PgShardCatalogActivation
}

func TestCatalogActivationPublicationVerifierAcceptsExactLivePublications(t *testing.T) {
	fixture := newActivationPublicationFixture(t)
	if err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation); err != nil {
		t.Fatalf("exact live publication rejected: %v", err)
	}
}

func TestCatalogActivationPublicationVerifierAcceptsInjectedServiceAccountProjection(t *testing.T) {
	fixture := newActivationPublicationFixture(t)
	if err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation); err != nil {
		t.Fatalf("Kubernetes-injected service-account projection rejected: %v", err)
	}
}

func TestCatalogActivationPublicationVerifierAcceptsBuiltInTemplateDefaults(t *testing.T) {
	fixture := newActivationPublicationFixture(t)
	deployment := &appsv1.Deployment{}
	mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.cluster.Name+owned.OrchestratorSuffix, deployment)
	if deployment.Spec.RevisionHistoryLimit == nil || *deployment.Spec.RevisionHistoryLimit != 10 || deployment.Spec.Template.Spec.DNSPolicy != corev1.DNSClusterFirst || deployment.Spec.Template.Spec.Containers[0].TerminationMessagePath != corev1.TerminationMessagePathDefault {
		t.Fatalf("dispatcher Deployment lacks realistic API defaults: %#v", deployment.Spec)
	}
	statefulSet := activationFixtureStatefulSet(t, fixture, 0)
	if statefulSet.Spec.PersistentVolumeClaimRetentionPolicy == nil || statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || statefulSet.Spec.Template.Spec.SchedulerName != corev1.DefaultSchedulerName {
		t.Fatalf("source StatefulSet lacks realistic API defaults: %#v", statefulSet.Spec)
	}
	if err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation); err != nil {
		t.Fatalf("built-in Kubernetes template defaults rejected: %v", err)
	}
}

func TestCatalogActivationPublicationVerifierBindsConfiguredImages(t *testing.T) {
	fixture := newActivationPublicationFixture(t)
	images := fixture.verifier.images
	images.Orchestrator = "foreign.example/orchestrator:latest"
	fixture.verifier = NewCatalogActivationPublicationVerifier(fixture.client, images)
	err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation)
	if err == nil || !strings.Contains(err.Error(), "exact configured workload plan") {
		t.Fatalf("configured image drift error = %v, want planned-workload rejection", err)
	}
}

func TestCatalogActivationStatusDigestMatchesSerdeJSON(t *testing.T) {
	rawStatus := map[string]any{
		"condition": "<ready>&\u2028\u2029",
		"literal":   `\u003c\u2028`,
	}
	encoded, err := json.Marshal(rawStatus)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"condition":"\u003cready\u003e\u0026\u2028\u2029","literal":"\\u003c\\u2028"}`; got != want {
		t.Fatalf("canonical status JSON = %q, want %q", got, want)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(catalogActivationStatusDigestDomain))
	_, _ = hash.Write(encoded)
	if got, want := hex.EncodeToString(hash.Sum(nil)), "cbe68afa073340a29cc0a806ed69aba0ecd325f6d7b50df8c06f29b76e0f4471"; got != want {
		t.Fatalf("status digest = %s, want serde_json golden %s", got, want)
	}
}

func TestCatalogActivationStatusDigestRejectsNonIntegerNumbers(t *testing.T) {
	for name, value := range map[string]any{
		"floating point": map[string]any{"value": 1.0},
		"nested decimal": map[string]any{"value": []any{json.Number("1.25")}},
		"exponent":       map[string]any{"value": json.Number("1e3")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateCatalogActivationStatusJSONValue(value); err == nil || !strings.Contains(err.Error(), "non-integer JSON number") {
				t.Fatalf("status JSON domain error = %v, want non-integer rejection", err)
			}
		})
	}
	if err := validateCatalogActivationStatusJSONValue(map[string]any{
		"null": nil, "bool": true, "string": "value", "signed": int64(-1), "unsigned": uint64(1), "nested": []any{map[string]any{"integer": json.Number("2")}},
	}); err != nil {
		t.Fatalf("integer-only status JSON rejected: %v", err)
	}
}

func TestCatalogActivationPublicationVerifierRejectsDriftAndReadFailures(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *activationPublicationFixture)
		want   string
	}{
		{name: "cluster status digest", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Cluster.StatusSHA256 = strings.Repeat("f", 64)
		}, want: "status digest"},
		{name: "cluster deletion", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			setObjectDeleting(t, fixture.client, fixture.cluster)
		}, want: "identity differs"},
		{name: "carrier UID", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Carrier.UID = "recreated-carrier"
		}, want: "carrier"},
		{name: "candidate resourceVersion", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Candidate.ResourceVersion = "stale"
		}, want: "candidate ConfigMap identity"},
		{name: "candidate owner", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			request := fixture.newActivation.Spec.Request
			object := &corev1.ConfigMap{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, request.Candidate.Name, object)
			object.OwnerReferences = nil
			mustUpdate(t, fixture.client, object)
			mustGet(t, fixture.client, fixture.cluster.Namespace, object.Name, object)
			fixture.newActivation.Spec.Request.Candidate.ResourceVersion = object.ResourceVersion
		}, want: "metadata is not bound"},
		{name: "candidate mutability", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			object := &corev1.ConfigMap{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.newActivation.Spec.Request.Candidate.Name, object)
			mutable := false
			object.Immutable = &mutable
			mustUpdate(t, fixture.client, object)
			mustGet(t, fixture.client, fixture.cluster.Namespace, object.Name, object)
			fixture.newActivation.Spec.Request.Candidate.ResourceVersion = object.ResourceVersion
		}, want: "differs from its immutable"},
		{name: "dispatcher Pod UID", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Dispatcher.PodUID = "recreated-dispatcher"
		}, want: "dispatcher Pod identity"},
		{name: "dispatcher owner chain", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			request := fixture.newActivation.Spec.Request
			object := &corev1.Pod{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, request.Dispatcher.PodName, object)
			object.OwnerReferences = nil
			mustUpdate(t, fixture.client, object)
		}, want: "ReplicaSet owner"},
		{name: "orchestrator Lease resourceVersion", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Dispatcher.LeaseResourceVersion = "stale"
		}, want: "orchestrator Lease identity"},
		{name: "source Pod UID", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Source.PodUID = "recreated-source"
		}, want: "PostgreSQL member Pod"},
		{name: "source StatefulSet owner", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			object := &appsv1.StatefulSet{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, owned.PostgreSQLMemberStatefulSetName(fixture.cluster.Name, 0, 0), object)
			object.OwnerReferences = nil
			mustUpdate(t, fixture.client, object)
		}, want: "StatefulSet"},
		{name: "witness Pod UID", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.RemoteApplyWitness.PodUID = "recreated-witness"
		}, want: "PostgreSQL member Pod"},
		{name: "writable Lease holder", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			request := fixture.newActivation.Spec.Request
			lease := &coordinationv1.Lease{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, request.WritableTerm.Name, lease)
			foreign := "foreign/foreign/0123456789abcdef01234567"
			lease.Spec.HolderIdentity = &foreign
			mustUpdate(t, fixture.client, lease)
		}, want: "writable Lease term"},
		{name: "bootstrap Secret owner", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			request := fixture.newActivation.Spec.Request
			secret := &corev1.Secret{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, request.Bootstrap.Secret.Name, secret)
			secret.OwnerReferences = nil
			mustUpdate(t, fixture.client, secret)
		}, want: "bootstrap Secret identity"},
		{name: "bootstrap PVC deletion", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			claim := &corev1.PersistentVolumeClaim{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.newActivation.Spec.Request.Bootstrap.PVC.Name, claim)
			setObjectDeleting(t, fixture.client, claim)
		}, want: "bootstrap PVC identity"},
		{name: "bootstrap PVC finalizer", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			claim := &corev1.PersistentVolumeClaim{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.newActivation.Spec.Request.Bootstrap.PVC.Name, claim)
			claim.Finalizers = nil
			mustUpdate(t, fixture.client, claim)
		}, want: "protection finalizer"},
		{name: "replication digest", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			secret := &corev1.Secret{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.newActivation.Spec.Request.Materials.Replication.Name, secret)
			mutateCanonicalHex(secret.Data[owned.PostgreSQLReplicationPasswordKey])
			mustUpdate(t, fixture.client, secret)
		}, want: "material differs"},
		{name: "replication metadata", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			secret := &corev1.Secret{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.newActivation.Spec.Request.Materials.Replication.Name, secret)
			secret.Annotations[owned.PostgreSQLReplicationClusterUIDAnnotation] = "foreign"
			mustUpdate(t, fixture.client, secret)
		}, want: "metadata is not bound"},
		{name: "catalog digest", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Materials.Catalog.ClientSHA256 = strings.Repeat("e", 64)
		}, want: "material checkpoints"},
		{name: "operation writer digest", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			secret := &corev1.Secret{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.newActivation.Spec.Request.Materials.OperationWriter.Name, secret)
			mutateCanonicalHex(secret.Data[owned.OperationWriterPasswordKey])
			mustUpdate(t, fixture.client, secret)
		}, want: "material differs"},
		{name: "configuration UID", mutate: func(_ *testing.T, fixture *activationPublicationFixture) {
			fixture.newActivation.Spec.Request.Materials.PostgreSQLConfiguration.UID = "recreated-configuration"
		}, want: "material checkpoints"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newActivationPublicationFixture(t)
			test.mutate(t, fixture)
			err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verification error = %v, want %q", err, test.want)
			}
		})
	}

	fixture := newActivationPublicationFixture(t)
	fixture.verifier.Reader = &failingActivationPublicationReader{Reader: fixture.client, name: fixture.newActivation.Spec.Request.Materials.OperationWriter.Name}
	err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation)
	if err == nil || !strings.Contains(err.Error(), "injected read failure") {
		t.Fatalf("read failure error = %v", err)
	}

	fixture = newActivationPublicationFixture(t)
	fixture.verifier.Reader = &secondLeaseReadDriftReader{Reader: fixture.client, name: fixture.newActivation.Spec.Request.WritableTerm.Name}
	err = fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation)
	if err == nil || !strings.Contains(err.Error(), "changed during activation publication verification") {
		t.Fatalf("final-fence drift error = %v", err)
	}

}

func TestCatalogActivationPublicationVerifierRejectsSecretAuthorityDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *activationPublicationFixture)
		want   string
	}{
		{name: "source full catalog Secret", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			activationVolume(t, &pod.Spec, "catalog-activation-tls").Secret.Items = nil
			mustUpdate(t, fixture.client, pod)
		}, want: "exact key and mode allowlist"},
		{name: "source StatefulSet full catalog Secret", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			statefulSet := activationFixtureStatefulSet(t, fixture, 0)
			activationVolume(t, &statefulSet.Spec.Template.Spec, "catalog-activation-tls").Secret.Items = nil
			mustUpdate(t, fixture.client, statefulSet)
		}, want: "exact configured workload plan"},
		{name: "source operation writer volume", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			pod.Spec.Volumes = append(pod.Spec.Volumes, activationDirectSecretVolume("operation-writer-extra", fixture.newActivation.Spec.Request.Materials.OperationWriter.Name))
			mustUpdate(t, fixture.client, pod)
		}, want: "unauthorized direct Secret"},
		{name: "source projected Secret", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			pod.Spec.Volumes = append(pod.Spec.Volumes, activationProjectedSecretVolume("projected-writer", fixture.newActivation.Spec.Request.Materials.OperationWriter.Name))
			mustUpdate(t, fixture.client, pod)
		}, want: "unauthorized projected Secret"},
		{name: "source token PodCertificate", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			volume := activationVolume(t, &pod.Spec, "kubernetes-api")
			volume.Projected.Sources = append(volume.Projected.Sources, corev1.VolumeProjection{PodCertificate: &corev1.PodCertificateProjection{}})
			mustUpdate(t, fixture.client, pod)
		}, want: "unauthorized token or private-key"},
		{name: "source token init exposure", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			container := activationInitContainer(t, &pod.Spec, "bootstrap-postgresql")
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "kubernetes-api", MountPath: serviceAccountMountPath, ReadOnly: true})
			mustUpdate(t, fixture.client, pod)
		}, want: "exact PostgreSQL agent token"},
		{name: "source catalog mount in bootstrap container", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			container := activationInitContainer(t, &pod.Spec, "bootstrap-postgresql")
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "catalog-activation-tls", MountPath: catalogActivationTLSMountPath, ReadOnly: true})
			mustUpdate(t, fixture.client, pod)
		}, want: "outside its exact least-authority container"},
		{name: "source bootstrap mount in postgresql container", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			container := activationContainer(t, &pod.Spec, "postgresql")
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "bootstrap-secret", MountPath: "/etc/pgshard/bootstrap", ReadOnly: true})
			mustUpdate(t, fixture.client, pod)
		}, want: "outside its exact least-authority container"},
		{name: "source replication mount wrong path", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			mount := activationVolumeMount(t, &activationInitContainer(t, &pod.Spec, "bootstrap-postgresql").VolumeMounts, "replication-credential")
			mount.MountPath = "/tmp/replication"
			mustUpdate(t, fixture.client, pod)
		}, want: "outside its exact least-authority container"},
		{name: "witness operation writer volume", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.RemoteApplyWitness.PodName)
			pod.Spec.Volumes = append(pod.Spec.Volumes, activationDirectSecretVolume("operation-writer-extra", fixture.newActivation.Spec.Request.Materials.OperationWriter.Name))
			mustUpdate(t, fixture.client, pod)
		}, want: "unauthorized direct Secret"},
		{name: "witness StatefulSet projected Secret", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			statefulSet := activationFixtureStatefulSet(t, fixture, fixture.newActivation.Spec.Request.RemoteApplyWitness.Member)
			statefulSet.Spec.Template.Spec.Volumes = append(statefulSet.Spec.Template.Spec.Volumes, activationProjectedSecretVolume("projected-writer", fixture.newActivation.Spec.Request.Materials.OperationWriter.Name))
			mustUpdate(t, fixture.client, statefulSet)
		}, want: "exact configured workload plan"},
		{name: "witness explicit token", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.RemoteApplyWitness.PodName)
			expiration := int64(600)
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "foreign-token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}}}}}})
			mustUpdate(t, fixture.client, pod)
		}, want: "unauthorized token or private-key"},
		{name: "dispatcher mounted Secret", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
			pod.Spec.Volumes = append(pod.Spec.Volumes, activationDirectSecretVolume("mounted-writer", fixture.newActivation.Spec.Request.Materials.OperationWriter.Name))
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "mounted-writer", MountPath: "/tmp/writer", ReadOnly: true})
			mustUpdate(t, fixture.client, pod)
		}, want: "unauthorized direct Secret"},
		{name: "dispatcher CA wrong mode", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
			mode := int32(0o444)
			activationVolume(t, &pod.Spec, "catalog-activation-ca").Secret.DefaultMode = &mode
			mustUpdate(t, fixture.client, pod)
		}, want: "exact key and mode allowlist"},
		{name: "dispatcher CA wrong mount", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
			mount := activationVolumeMount(t, &pod.Spec.Containers[0].VolumeMounts, "catalog-activation-ca")
			mount.MountPath = "/tmp/catalog-ca"
			mustUpdate(t, fixture.client, pod)
		}, want: "outside its exact least-authority container"},
		{name: "dispatcher CA wrong environment", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
			for index := range pod.Spec.Containers[0].Env {
				if pod.Spec.Containers[0].Env[index].Name == catalogActivationCAEnvironment {
					pod.Spec.Containers[0].Env[index].Value = "/tmp/catalog-ca"
				}
			}
			mustUpdate(t, fixture.client, pod)
		}, want: "CA environment"},
		{name: "dispatcher ReplicaSet projected Secret", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			replicaSet := activationFixtureDispatcherReplicaSet(t, fixture)
			replicaSet.Spec.Template.Spec.Volumes = append(replicaSet.Spec.Template.Spec.Volumes, activationProjectedSecretVolume("projected-writer", fixture.newActivation.Spec.Request.Materials.OperationWriter.Name))
			mustUpdate(t, fixture.client, replicaSet)
		}, want: "unauthorized projected Secret"},
		{name: "dispatcher Deployment SecretKeyRef", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			deployment := &appsv1.Deployment{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.cluster.Name+owned.OrchestratorSuffix, deployment)
			deployment.Spec.Template.Spec.Containers[0].Env = append(deployment.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "WRITER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: fixture.newActivation.Spec.Request.Materials.OperationWriter.Name}, Key: owned.OperationWriterPasswordKey}}})
			mustUpdate(t, fixture.client, deployment)
		}, want: "exact configured workload plan"},
		{name: "dispatcher Pod security drift", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
			privileged := true
			pod.Spec.Containers[0].SecurityContext.Privileged = &privileged
			mustUpdate(t, fixture.client, pod)
		}, want: "security-relevant spec"},
		{name: "dispatcher ReplicaSet command drift", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			replicaSet := activationFixtureDispatcherReplicaSet(t, fixture)
			replicaSet.Spec.Template.Spec.Containers[0].Command = []string{"/bin/foreign"}
			mustUpdate(t, fixture.client, replicaSet)
		}, want: "security-relevant spec"},
		{name: "source Pod security drift", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			privileged := true
			pod.Spec.Containers[0].SecurityContext.Privileged = &privileged
			mustUpdate(t, fixture.client, pod)
		}, want: "security-relevant spec"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newActivationPublicationFixture(t)
			test.mutate(t, fixture)
			err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verification error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestActivationPodSecretBoundaryRejectsEveryVolumeSecretReference(t *testing.T) {
	secret := &corev1.LocalObjectReference{Name: "forbidden"}
	tests := map[string]corev1.VolumeSource{
		"direct":      {Secret: &corev1.SecretVolumeSource{SecretName: "forbidden"}},
		"projected":   {Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: *secret}}}}},
		"iscsi":       {ISCSI: &corev1.ISCSIVolumeSource{SecretRef: secret}},
		"rbd":         {RBD: &corev1.RBDVolumeSource{SecretRef: secret}},
		"flex volume": {FlexVolume: &corev1.FlexVolumeSource{SecretRef: secret}},
		"cinder":      {Cinder: &corev1.CinderVolumeSource{SecretRef: secret}},
		"cephfs":      {CephFS: &corev1.CephFSVolumeSource{SecretRef: secret}},
		"azure file":  {AzureFile: &corev1.AzureFileVolumeSource{SecretName: "forbidden"}},
		"scaleio":     {ScaleIO: &corev1.ScaleIOVolumeSource{SecretRef: secret}},
		"storageos":   {StorageOS: &corev1.StorageOSVolumeSource{SecretRef: secret}},
		"csi":         {CSI: &corev1.CSIVolumeSource{NodePublishSecretRef: secret}},
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			spec := corev1.PodSpec{Volumes: []corev1.Volume{{Name: "forbidden", VolumeSource: source}}}
			if err := validateActivationPodSecretBoundary("test Pod", spec, nil); err == nil || !strings.Contains(err.Error(), "Secret") {
				t.Fatalf("Secret-bearing %s volume error = %v, want rejection", name, err)
			}
		})
	}
}

func TestActivationPodSecretBoundaryRejectsEveryContainerSecretReference(t *testing.T) {
	tests := map[string]corev1.PodSpec{
		"image pull": {ImagePullSecrets: []corev1.LocalObjectReference{{Name: "forbidden"}}},
		"envFrom": {Containers: []corev1.Container{{Name: "orchestrator", EnvFrom: []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "forbidden"}},
		}}}}},
		"env": {InitContainers: []corev1.Container{{Name: "init", Env: []corev1.EnvVar{{Name: "SECRET", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "forbidden"}, Key: "key"},
		}}}}}},
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateActivationPodSecretBoundary("test Pod", spec, nil); err == nil || !strings.Contains(err.Error(), "Secret") {
				t.Fatalf("%s error = %v, want Secret rejection", name, err)
			}
		})
	}
}

func TestActivationServiceAccountProjectionIsExact(t *testing.T) {
	automount := true
	parent := corev1.PodSpec{AutomountServiceAccountToken: &automount, Containers: []corev1.Container{{Name: "orchestrator"}}}
	child := *parent.DeepCopy()
	addExactServiceAccountInjection(&child)
	if !activationDispatcherPodSpecMatches(parent, child) {
		t.Fatal("exact built-in service-account projection was rejected")
	}
	tests := map[string]func(*corev1.PodSpec){
		"pod certificate source": func(spec *corev1.PodSpec) {
			spec.Volumes[len(spec.Volumes)-1].Projected.Sources = append(spec.Volumes[len(spec.Volumes)-1].Projected.Sources, corev1.VolumeProjection{PodCertificate: &corev1.PodCertificateProjection{}})
		},
		"extra token source": func(spec *corev1.PodSpec) {
			spec.Volumes[len(spec.Volumes)-1].Projected.Sources = append(spec.Volumes[len(spec.Volumes)-1].Projected.Sources, spec.Volumes[len(spec.Volumes)-1].Projected.Sources[0])
		},
		"wrong default mode": func(spec *corev1.PodSpec) {
			mode := int32(0o440)
			spec.Volumes[len(spec.Volumes)-1].Projected.DefaultMode = &mode
		},
		"alternate mount path": func(spec *corev1.PodSpec) {
			spec.Containers[0].VolumeMounts[len(spec.Containers[0].VolumeMounts)-1].MountPath = "/tmp/token"
		},
		"subpath mount": func(spec *corev1.PodSpec) {
			spec.Containers[0].VolumeMounts[len(spec.Containers[0].VolumeMounts)-1].SubPath = "token"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			drifted := child.DeepCopy()
			mutate(drifted)
			if activationDispatcherPodSpecMatches(parent, *drifted) {
				t.Fatalf("%s was accepted", name)
			}
		})
	}
}

func TestActivationResourceDefaultingIsExact(t *testing.T) {
	parent := corev1.PodSpec{Containers: []corev1.Container{{Name: "orchestrator", Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}}}}
	child := *parent.DeepCopy()
	child.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	if !normalizeActivationContainerResourceDefaults(parent, &child) || !apiequality.Semantic.DeepEqual(&parent, &child) {
		t.Fatal("exact request-from-limit API default was rejected")
	}
	for name, requests := range map[string]corev1.ResourceList{
		"different request":  {corev1.ResourceCPU: resource.MustParse("500m")},
		"LimitRange request": {corev1.ResourceMemory: resource.MustParse("64Mi")},
	} {
		t.Run(name, func(t *testing.T) {
			drifted := *parent.DeepCopy()
			drifted.Containers[0].Resources.Requests = requests
			if !normalizeActivationContainerResourceDefaults(parent, &drifted) {
				t.Fatal("resource normalization rejected structurally matching containers")
			}
			if apiequality.Semantic.DeepEqual(&parent, &drifted) {
				t.Fatalf("%s was normalized away", name)
			}
		})
	}
}

func TestCatalogActivationPublicationVerifierRejectsControllerMetadataDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *activationPublicationFixture)
	}{
		{name: "Deployment extra annotation", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			deployment := &appsv1.Deployment{}
			mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.cluster.Name+owned.OrchestratorSuffix, deployment)
			deployment.Annotations["foreign.example/annotation"] = "value"
			mustUpdate(t, fixture.client, deployment)
		}},
		{name: "ReplicaSet stale revision", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			replicaSet := activationFixtureDispatcherReplicaSet(t, fixture)
			replicaSet.Annotations[deploymentRevisionAnnotation] = "2"
			mustUpdate(t, fixture.client, replicaSet)
		}},
		{name: "ReplicaSet desired replicas", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			replicaSet := activationFixtureDispatcherReplicaSet(t, fixture)
			replicaSet.Annotations[deploymentDesiredReplicasAnnotation] = "4"
			mustUpdate(t, fixture.client, replicaSet)
		}},
		{name: "dispatcher Pod extra label", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
			pod.Labels["foreign.example/label"] = "value"
			mustUpdate(t, fixture.client, pod)
		}},
		{name: "StatefulSet Pod extra annotation", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			pod.Annotations["foreign.example/annotation"] = "value"
			mustUpdate(t, fixture.client, pod)
		}},
		{name: "StatefulSet Pod hostname", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			pod.Spec.Hostname = "foreign"
			mustUpdate(t, fixture.client, pod)
		}},
		{name: "StatefulSet Pod index", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			pod.Labels[appsv1.PodIndexLabel] = "1"
			mustUpdate(t, fixture.client, pod)
		}},
		{name: "StatefulSet Pod controller revision", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Source.PodName)
			pod.Labels[appsv1.ControllerRevisionHashLabelKey] = "foreign"
			mustUpdate(t, fixture.client, pod)
		}},
		{name: "StatefulSet service name", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			statefulSet := activationFixtureStatefulSet(t, fixture, 0)
			statefulSet.Spec.ServiceName = "foreign"
			mustUpdate(t, fixture.client, statefulSet)
		}},
		{name: "StatefulSet mixed revision", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			statefulSet := activationFixtureStatefulSet(t, fixture, 0)
			statefulSet.Status.UpdateRevision = "new-revision"
			if err := fixture.client.Status().Update(context.Background(), statefulSet); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newActivationPublicationFixture(t)
			test.mutate(t, fixture)
			if err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation); err == nil {
				t.Fatalf("%s was accepted", test.name)
			}
		})
	}
}

func TestCatalogActivationPublicationVerifierRejectsCoordinatedWorkloadDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *activationPublicationFixture)
	}{
		{name: "dispatcher image", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			mutateActivationDispatcherChain(t, fixture, func(spec *corev1.PodSpec) { spec.Containers[0].Image = "foreign.example/orchestrator:latest" })
		}},
		{name: "source command", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			mutateActivationStatefulSetChain(t, fixture, 0, func(spec *corev1.PodSpec) { spec.Containers[0].Command = []string{"/bin/foreign"} })
		}},
		{name: "source security context", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			mutateActivationStatefulSetChain(t, fixture, 0, func(spec *corev1.PodSpec) {
				privileged := true
				spec.Containers[0].SecurityContext.Privileged = &privileged
			})
		}},
		{name: "source sidecar", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			mutateActivationStatefulSetChain(t, fixture, 0, func(spec *corev1.PodSpec) {
				spec.Containers = append(spec.Containers, corev1.Container{Name: "foreign-sidecar", Image: "foreign.example/sidecar:latest"})
			})
		}},
		{name: "source hostPath", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			mutateActivationStatefulSetChain(t, fixture, 0, func(spec *corev1.PodSpec) {
				typeDirectory := corev1.HostPathDirectory
				spec.Volumes = append(spec.Volumes, corev1.Volume{Name: "foreign-host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/", Type: &typeDirectory}}})
			})
		}},
		{name: "witness PVC", mutate: func(t *testing.T, fixture *activationPublicationFixture) {
			member := fixture.newActivation.Spec.Request.RemoteApplyWitness.Member
			mutateActivationStatefulSetChain(t, fixture, member, func(spec *corev1.PodSpec) {
				activationVolume(t, spec, "data").PersistentVolumeClaim.ClaimName = "foreign-pvc"
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newActivationPublicationFixture(t)
			test.mutate(t, fixture)
			err := fixture.verifier.VerifyPublication(context.Background(), fixture.oldActivation, fixture.newActivation)
			if err == nil || !strings.Contains(err.Error(), "exact configured workload plan") {
				t.Fatalf("coordinated drift error = %v, want exact configured workload plan rejection", err)
			}
		})
	}
}

type failingActivationPublicationReader struct {
	client.Reader
	name string
}

type secondLeaseReadDriftReader struct {
	client.Reader
	name  string
	reads int
}

func (reader *secondLeaseReadDriftReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := reader.Reader.Get(ctx, key, object, options...); err != nil {
		return err
	}
	if key.Name == reader.name {
		reader.reads++
		if reader.reads == 2 {
			object.SetResourceVersion("concurrently-changed")
		}
	}
	return nil
}

func (reader *failingActivationPublicationReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if key.Name == reader.name {
		return errors.New("injected read failure")
	}
	return reader.Reader.Get(ctx, key, object, options...)
}

func newActivationPublicationFixture(t *testing.T) *activationPublicationFixture {
	t.Helper()
	ctx := context.Background()
	cluster := validCluster()
	cluster.Spec.Shards = 1
	cluster.Spec.MembersPerShard = 3
	base := newFakeClient(t, cluster)
	reconciler := developmentReconciler(base, nil)
	reconciler.Images.PostgreSQLRuntime = owned.PostgreSQLRuntimeAgentQuarantine
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	markSupportingWorkloadsAvailable(t, ctx, base, cluster)
	if _, err := reconciler.Reconcile(ctx, requestFor(cluster)); err != nil {
		t.Fatal(err)
	}
	cluster = getCluster(t, ctx, base, cluster)

	sourcePod := createStatefulSetPod(t, base, cluster.Namespace, owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 0))
	witnessPod := createStatefulSetPod(t, base, cluster.Namespace, owned.PostgreSQLMemberStatefulSetName(cluster.Name, 0, 1))
	setPodNodeIdentity(t, base, sourcePod, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	setPodNodeIdentity(t, base, witnessPod, "ffffffff-1111-2222-3333-444444444444")
	mustGet(t, base, cluster.Namespace, sourcePod.Name, sourcePod)
	mustGet(t, base, cluster.Namespace, witnessPod.Name, witnessPod)
	dispatcherPod := createDispatcherPod(t, base, cluster)

	dispatcherHolder := fmt.Sprintf("%s/%s/11111111-2222-4333-8444-555555555555", dispatcherPod.Name, dispatcherPod.UID)
	dispatcherLease := activateLease(t, base, cluster.Namespace, cluster.Name+owned.OrchestratorLeaseSuffix, dispatcherHolder)
	sourceHolder := fmt.Sprintf("%s/%s/0123456789abcdef01234567", sourcePod.Name, sourcePod.UID)
	writableLease := activateLease(t, base, cluster.Namespace, owned.PostgreSQLWritableLeaseName(cluster.Name, 0), sourceHolder)
	cluster = getCluster(t, ctx, base, cluster)

	carrier := &pgshardv1alpha1.PgShardCatalogActivation{}
	mustGet(t, base, cluster.Namespace, pgshardv1alpha1.CatalogActivationName(cluster.Name), carrier)
	candidateCheckpoint := cluster.Status.PostgreSQLCatalogCandidates[0]
	candidate := &corev1.ConfigMap{}
	mustGet(t, base, cluster.Namespace, candidateCheckpoint.ConfigMapName, candidate)
	document := &activationCandidateDocument{}
	if err := json.Unmarshal([]byte(candidate.Data[catalogCandidatePayloadKey]), document); err != nil {
		t.Fatal(err)
	}

	statusMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cluster.Status)
	if err != nil {
		t.Fatal(err)
	}
	statusJSON, err := json.Marshal(statusMap)
	if err != nil {
		t.Fatal(err)
	}
	statusHash := sha256.New()
	_, _ = statusHash.Write([]byte(catalogActivationStatusDigestDomain))
	_, _ = statusHash.Write(statusJSON)
	request := &pgshardv1alpha1.CatalogActivationRequest{
		SchemaVersion: pgshardv1alpha1.CatalogActivationRequestVersion,
		Carrier:       pgshardv1alpha1.CatalogActivationObjectIdentity{Name: carrier.Name, UID: carrier.UID},
		Cluster:       pgshardv1alpha1.CatalogActivationCluster{CatalogActivationObjectIdentity: pgshardv1alpha1.CatalogActivationObjectIdentity{Name: cluster.Name, UID: cluster.UID}, Namespace: cluster.Namespace, Generation: fmt.Sprintf("%d", cluster.Generation), ResourceVersion: cluster.ResourceVersion, StatusSHA256: hex.EncodeToString(statusHash.Sum(nil))},
		Dispatcher:    pgshardv1alpha1.CatalogActivationDispatcher{PodName: dispatcherPod.Name, PodUID: dispatcherPod.UID, LeaseName: dispatcherLease.Name, LeaseUID: dispatcherLease.UID, LeaseResourceVersion: dispatcherLease.ResourceVersion, LeaseHolder: dispatcherHolder},
		Candidate:     pgshardv1alpha1.CatalogActivationCandidate{CatalogActivationObjectIdentity: pgshardv1alpha1.CatalogActivationObjectIdentity{Name: candidate.Name, UID: candidate.UID}, ResourceVersion: candidate.ResourceVersion, PayloadSHA256: owned.PostgreSQLCatalogCandidatePayloadSHA256(candidate)},
		Bootstrap:     pgshardv1alpha1.CatalogActivationBootstrap{Secret: activationAPIObject(document.Bootstrap.Secret), PVC: activationAPIObject(document.Bootstrap.PVC)},
		WritableTerm:  pgshardv1alpha1.CatalogActivationWritableTerm{CatalogActivationObjectIdentity: pgshardv1alpha1.CatalogActivationObjectIdentity{Name: writableLease.Name, UID: writableLease.UID}, ResourceVersion: writableLease.ResourceVersion, Holder: sourceHolder, Generation: fmt.Sprintf("%d", *writableLease.Spec.LeaseTransitions)},
		Materials: pgshardv1alpha1.CatalogActivationMaterials{
			Replication: activationAPIMaterial(document.Replication), Catalog: activationAPICatalog(document.Catalog), OperationWriter: activationAPIMaterial(document.Materialization.OperationWriterAccess),
			PostgreSQLConfiguration: pgshardv1alpha1.CatalogActivationMaterialIdentity{CatalogActivationObjectIdentity: pgshardv1alpha1.CatalogActivationObjectIdentity{Name: document.Materialization.PostgreSQLConfiguration.Name, UID: document.Materialization.PostgreSQLConfiguration.UID}, MaterialSHA256: document.Materialization.PostgreSQLConfiguration.DataSHA256},
			MigrationSHA256:         document.Materialization.ShardschemaMigration.SHA256, GenesisSHA256: document.Materialization.DatabaseGenesis.SHA256, PreflightSHA256: document.Materialization.DatabaseTopologyPreflight.SHA256,
			ServingHBAVersion: document.Materialization.ServingHBA.Version, ServingHBASHA256: document.Materialization.ServingHBA.SHA256, TargetTemplateSHA256: document.Materialization.TargetPodTemplate.SHA256,
		},
		Source:             pgshardv1alpha1.CatalogActivationSource{ClusterName: cluster.Name, ClusterUID: cluster.UID, PodName: sourcePod.Name, PodUID: sourcePod.UID, Shard: 0, Member: 0, InstanceID: sourcePod.Name, BootID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", PostmasterPID: 100, SystemIdentifier: "12345678901234567890", Timeline: 3, GenerationBarrierLSN: "4294967296", TargetFenceAcknowledgement: pgshardv1alpha1.CatalogActivationTargetFenceAcknowledgement{ObservedAtUnixMS: "1700000000000", DeadlineBoottimeNS: "9000000000", RemainingValidityAtAckMS: "5000", RemainingValidityAtReportMS: "4500", ControlBackendPID: 101}},
		RemoteApplyWitness: pgshardv1alpha1.CatalogActivationRemoteApplyWitness{ClusterName: cluster.Name, ClusterUID: cluster.UID, PodName: witnessPod.Name, PodUID: witnessPod.UID, Shard: 0, Member: 1, InstanceID: witnessPod.Name, BootID: "ffffffff-1111-2222-3333-444444444444", PostmasterPID: 200, MemberSlotName: "pgshard_member_0001", SystemIdentifier: "12345678901234567890", Timeline: 3, GenerationBarrierLSN: "4294967296", ReceiveLSN: "4294967396", ReplayLSN: "4294967396"},
	}
	generationIdentity := fmt.Sprintf("format=1\ncluster_name=%s\ncluster_uid=%s\nshard=0\nlease_namespace=%s\nlease_name=%s\nlease_uid=%s\nholder=%s\nterm=%d\n", cluster.Name, cluster.UID, cluster.Namespace, writableLease.Name, writableLease.UID, sourceHolder, *writableLease.Spec.LeaseTransitions)
	request.Source.GenerationIdentity = generationIdentity
	request.RemoteApplyWitness.GenerationIdentity = generationIdentity
	digest, err := request.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	newActivation := carrier.DeepCopy()
	newActivation.Spec.Request = request
	newActivation.Spec.RequestSHA256 = digest
	return &activationPublicationFixture{client: base, verifier: NewCatalogActivationPublicationVerifier(base, reconciler.Images), cluster: cluster, oldActivation: carrier, newActivation: newActivation}
}

func createStatefulSetPod(t *testing.T, base client.Client, namespace, name string) *corev1.Pod {
	t.Helper()
	statefulSet := &appsv1.StatefulSet{}
	mustGet(t, base, namespace, name, statefulSet)
	defaultActivationStatefulSet(statefulSet)
	if statefulSet.UID == "" {
		statefulSet.UID = types.UID(name + "-uid")
	}
	mustUpdate(t, base, statefulSet)
	statefulSet.Status.CurrentRevision = name + "-revision"
	statefulSet.Status.UpdateRevision = statefulSet.Status.CurrentRevision
	if err := base.Status().Update(context.Background(), statefulSet); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: *statefulSet.Spec.Template.ObjectMeta.DeepCopy(), Spec: *statefulSet.Spec.Template.Spec.DeepCopy()}
	pod.Name, pod.Namespace = name+"-0", namespace
	pod.Labels[appsv1.StatefulSetPodNameLabel] = pod.Name
	pod.Labels[appsv1.PodIndexLabel] = "0"
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = statefulSet.Status.CurrentRevision
	pod.Spec.Hostname, pod.Spec.Subdomain = pod.Name, statefulSet.Spec.ServiceName
	addExactPodAPIDefaults(&pod.Spec)
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}
	if err := base.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	mustGet(t, base, namespace, pod.Name, pod)
	return pod
}

func createDispatcherPod(t *testing.T, base client.Client, cluster *pgshardv1alpha1.PgShardCluster) *corev1.Pod {
	t.Helper()
	deployment := &appsv1.Deployment{}
	mustGet(t, base, cluster.Namespace, cluster.Name+owned.OrchestratorSuffix, deployment)
	defaultActivationDeployment(deployment)
	if deployment.UID == "" {
		deployment.UID = "orchestrator-deployment-uid"
	}
	if deployment.Annotations == nil {
		deployment.Annotations = map[string]string{}
	}
	deployment.Annotations[deploymentRevisionAnnotation] = "1"
	mustUpdate(t, base, deployment)
	hash := "abc123def0"
	replicaSetLabels := maps.Clone(deployment.Spec.Template.Labels)
	replicaSetLabels[appsv1.DefaultDeploymentUniqueLabelKey] = hash
	replicaSetSelector := deployment.Spec.Selector.DeepCopy()
	replicaSetSelector.MatchLabels[appsv1.DefaultDeploymentUniqueLabelKey] = hash
	replicaSetTemplate := *deployment.Spec.Template.DeepCopy()
	replicaSetTemplate.Labels = maps.Clone(replicaSetLabels)
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: deployment.Name + "-" + hash, Namespace: cluster.Namespace, Labels: maps.Clone(replicaSetLabels),
			Annotations: map[string]string{
				owned.ApplyOwnershipAnnotation: owned.ApplyOwnershipVersion,
				deploymentRevisionAnnotation:   "1", deploymentDesiredReplicasAnnotation: "3", deploymentMaxReplicasAnnotation: "4",
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(deployment, appsv1.SchemeGroupVersion.WithKind("Deployment"))},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: deployment.Spec.Replicas, Selector: replicaSetSelector, Template: replicaSetTemplate},
	}
	if err := base.Create(context.Background(), replicaSet); err != nil {
		t.Fatal(err)
	}
	mustGet(t, base, cluster.Namespace, replicaSet.Name, replicaSet)
	pod := &corev1.Pod{ObjectMeta: *replicaSet.Spec.Template.ObjectMeta.DeepCopy(), Spec: *replicaSet.Spec.Template.Spec.DeepCopy()}
	pod.Name, pod.Namespace = replicaSet.Name+"-12345", cluster.Namespace
	pod.GenerateName = replicaSet.Name + "-"
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(replicaSet, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}
	addExactServiceAccountInjection(&pod.Spec)
	addExactPodAPIDefaults(&pod.Spec)
	if err := base.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	mustGet(t, base, cluster.Namespace, pod.Name, pod)
	return pod
}

func addExactServiceAccountInjection(spec *corev1.PodSpec) {
	mode, expiration := int32(0o644), int64(3_607)
	name := "kube-api-access-abcde"
	spec.Volumes = append(spec.Volumes, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		DefaultMode: &mode,
		Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
		},
	}}})
	spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: name, MountPath: serviceAccountMountPath, ReadOnly: true})
}

func addExactPodAPIDefaults(spec *corev1.PodSpec) {
	priority := int32(0)
	preemption := corev1.PreemptLowerPriority
	seconds := int64(300)
	spec.NodeName = "worker-1"
	spec.DeprecatedServiceAccount = spec.ServiceAccountName
	spec.Priority = &priority
	spec.PreemptionPolicy = &preemption
	spec.Tolerations = append(spec.Tolerations,
		corev1.Toleration{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		corev1.Toleration{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	)
}

func activateLease(t *testing.T, base client.Client, namespace, name, holder string) *coordinationv1.Lease {
	t.Helper()
	lease := &coordinationv1.Lease{}
	mustGet(t, base, namespace, name, lease)
	duration, transitions, now := int32(6), int32(1), metav1.NowMicro()
	lease.Spec = coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &now, LeaseTransitions: &transitions}
	mustUpdate(t, base, lease)
	mustGet(t, base, namespace, name, lease)
	return lease
}

func setPodNodeIdentity(t *testing.T, base client.Client, pod *corev1.Pod, bootID string) {
	t.Helper()
	pod.Annotations[podfence.NodeUIDAnnotation] = "node-uid"
	pod.Annotations[podfence.NodeBootIDAnnotation] = bootID
	mustUpdate(t, base, pod)
}

func activationAPIObject(object activationCandidateObject) pgshardv1alpha1.CatalogActivationObjectIdentity {
	return pgshardv1alpha1.CatalogActivationObjectIdentity{Name: object.Name, UID: object.UID}
}
func activationAPIMaterial(material activationCandidateMaterial) pgshardv1alpha1.CatalogActivationMaterialIdentity {
	return pgshardv1alpha1.CatalogActivationMaterialIdentity{CatalogActivationObjectIdentity: activationAPIObject(material.activationCandidateObject), MaterialSHA256: material.MaterialSHA256}
}
func activationAPICatalog(material activationCandidateCatalog) pgshardv1alpha1.CatalogActivationCatalogMaterialIdentity {
	return pgshardv1alpha1.CatalogActivationCatalogMaterialIdentity{CatalogActivationObjectIdentity: activationAPIObject(material.activationCandidateObject), ClientSHA256: material.ClientSHA256, ServerSHA256: material.ServerSHA256}
}

func mutateCanonicalHex(value []byte) {
	if value[0] == '0' {
		value[0] = '1'
	} else {
		value[0] = '0'
	}
}

func activationFixturePod(t *testing.T, fixture *activationPublicationFixture, name string) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	mustGet(t, fixture.client, fixture.cluster.Namespace, name, pod)
	return pod
}

func activationFixtureStatefulSet(t *testing.T, fixture *activationPublicationFixture, member int32) *appsv1.StatefulSet {
	t.Helper()
	statefulSet := &appsv1.StatefulSet{}
	mustGet(t, fixture.client, fixture.cluster.Namespace, owned.PostgreSQLMemberStatefulSetName(fixture.cluster.Name, 0, member), statefulSet)
	return statefulSet
}

func activationFixtureDispatcherReplicaSet(t *testing.T, fixture *activationPublicationFixture) *appsv1.ReplicaSet {
	t.Helper()
	pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.Kind != "ReplicaSet" {
		t.Fatalf("dispatcher Pod owner = %#v", owner)
	}
	replicaSet := &appsv1.ReplicaSet{}
	mustGet(t, fixture.client, fixture.cluster.Namespace, owner.Name, replicaSet)
	return replicaSet
}

func mutateActivationDispatcherChain(t *testing.T, fixture *activationPublicationFixture, mutate func(*corev1.PodSpec)) {
	t.Helper()
	deployment := &appsv1.Deployment{}
	mustGet(t, fixture.client, fixture.cluster.Namespace, fixture.cluster.Name+owned.OrchestratorSuffix, deployment)
	replicaSet := activationFixtureDispatcherReplicaSet(t, fixture)
	pod := activationFixturePod(t, fixture, fixture.newActivation.Spec.Request.Dispatcher.PodName)
	mutate(&deployment.Spec.Template.Spec)
	mutate(&replicaSet.Spec.Template.Spec)
	mutate(&pod.Spec)
	mustUpdate(t, fixture.client, deployment)
	mustUpdate(t, fixture.client, replicaSet)
	mustUpdate(t, fixture.client, pod)
}

func mutateActivationStatefulSetChain(t *testing.T, fixture *activationPublicationFixture, member int32, mutate func(*corev1.PodSpec)) {
	t.Helper()
	statefulSet := activationFixtureStatefulSet(t, fixture, member)
	podName := owned.PostgreSQLMemberStatefulSetName(fixture.cluster.Name, 0, member) + "-0"
	pod := activationFixturePod(t, fixture, podName)
	mutate(&statefulSet.Spec.Template.Spec)
	mutate(&pod.Spec)
	mustUpdate(t, fixture.client, statefulSet)
	mustUpdate(t, fixture.client, pod)
}

func activationDirectSecretVolume(name, secretName string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName, DefaultMode: activationMode0440()}}}
}

func activationProjectedSecretVolume(name, secretName string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: secretName}}}}}}}
}

func activationVolume(t *testing.T, spec *corev1.PodSpec, name string) *corev1.Volume {
	t.Helper()
	for index := range spec.Volumes {
		if spec.Volumes[index].Name == name {
			return &spec.Volumes[index]
		}
	}
	t.Fatalf("volume %s not found", name)
	return nil
}

func activationInitContainer(t *testing.T, spec *corev1.PodSpec, name string) *corev1.Container {
	t.Helper()
	for index := range spec.InitContainers {
		if spec.InitContainers[index].Name == name {
			return &spec.InitContainers[index]
		}
	}
	t.Fatalf("init container %s not found", name)
	return nil
}

func activationContainer(t *testing.T, spec *corev1.PodSpec, name string) *corev1.Container {
	t.Helper()
	for index := range spec.Containers {
		if spec.Containers[index].Name == name {
			return &spec.Containers[index]
		}
	}
	t.Fatalf("container %s not found", name)
	return nil
}

func activationVolumeMount(t *testing.T, mounts *[]corev1.VolumeMount, name string) *corev1.VolumeMount {
	t.Helper()
	for index := range *mounts {
		if (*mounts)[index].Name == name {
			return &(*mounts)[index]
		}
	}
	t.Fatalf("volume mount %s not found", name)
	return nil
}

func mustGet(t *testing.T, reader client.Reader, namespace, name string, object client.Object) {
	t.Helper()
	if err := reader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, object); err != nil {
		t.Fatal(err)
	}
}
func mustUpdate(t *testing.T, writer client.Writer, object client.Object) {
	t.Helper()
	if err := writer.Update(context.Background(), object); err != nil {
		t.Fatal(err)
	}
}
func setObjectDeleting(t *testing.T, writer client.Client, object client.Object) {
	t.Helper()
	if err := writer.Delete(context.Background(), object); err != nil {
		t.Fatal(err)
	}
}
