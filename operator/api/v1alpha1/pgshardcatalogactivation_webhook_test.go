package v1alpha1

import (
	"context"
	"fmt"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const catalogActivationControllerUsername = "system:serviceaccount:pgshard-system:pgshard-controller-manager"
const activationSourceHolder = "demo-shard-0000-member-0000-0/source-pod-uid/0123456789abcdef01234567"
const activationDispatcherHolder = "demo-orchestrator-0/dispatcher-uid/11111111-2222-4333-8444-555555555555"

func catalogActivationAdmissionContext(username string) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		UserInfo: authenticationv1.UserInfo{Username: username},
	}})
}

func catalogActivationAdmissionContextForPod(username, podName, podUID string) context.Context {
	return admission.NewContextWithRequest(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		UserInfo: authenticationv1.UserInfo{
			Username: username,
			Extra: map[string]authenticationv1.ExtraValue{
				"authentication.kubernetes.io/pod-name": {podName},
				"authentication.kubernetes.io/pod-uid":  {podUID},
			},
		},
	}})
}

func activationDigest(value byte) string { return strings.Repeat(fmt.Sprintf("%02x", value), 32) }

func activationGenerationIdentityForHolder(clusterName, leaseName, holder string) string {
	return "format=1\n" +
		"cluster_name=" + clusterName + "\n" +
		"cluster_uid=cluster-uid\n" +
		"shard=0\n" +
		"lease_namespace=database\n" +
		"lease_name=" + leaseName + "\n" +
		"lease_uid=writable-lease-uid\n" +
		"holder=" + holder + "\n" +
		"term=9\n"
}

func activationGenerationIdentityFor(clusterName, leaseName string) string {
	return activationGenerationIdentityForHolder(clusterName, leaseName, activationSourceHolder)
}

func activationGenerationIdentity() string {
	return activationGenerationIdentityFor("demo", "demo-shard-0000-term")
}

func validCatalogActivationRequest() CatalogActivationRequest {
	return CatalogActivationRequest{
		SchemaVersion: CatalogActivationRequestVersion,
		Carrier:       CatalogActivationObjectIdentity{Name: "demo-catalog-activation", UID: "carrier-uid"},
		Cluster: CatalogActivationCluster{
			CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "demo", UID: "cluster-uid"},
			Namespace:                       "database", Generation: "7", ResourceVersion: "101", StatusSHA256: activationDigest(1),
		},
		Dispatcher: CatalogActivationDispatcher{
			PodName: "demo-orchestrator-0", PodUID: "dispatcher-uid", LeaseName: "demo-orch-lease",
			LeaseUID: "orchestrator-lease-uid", LeaseResourceVersion: "102", LeaseHolder: activationDispatcherHolder,
		},
		Candidate: CatalogActivationCandidate{
			CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "demo-s0-m0000-cfg-00112233445566778899aabbccddeeff", UID: "candidate-uid"},
			ResourceVersion:                 "103", PayloadSHA256: activationDigest(2),
		},
		Bootstrap: CatalogActivationBootstrap{
			Secret: CatalogActivationObjectIdentity{Name: "bootstrap-secret", UID: "bootstrap-secret-uid"},
			PVC:    CatalogActivationObjectIdentity{Name: "bootstrap-pvc", UID: "bootstrap-pvc-uid"},
		},
		WritableTerm: CatalogActivationWritableTerm{
			CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "demo-shard-0000-term", UID: "writable-lease-uid"},
			ResourceVersion:                 "104", Holder: activationSourceHolder, Generation: "9",
		},
		Materials: CatalogActivationMaterials{
			Replication:             CatalogActivationMaterialIdentity{CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "replication", UID: "replication-uid"}, MaterialSHA256: activationDigest(3)},
			Catalog:                 CatalogActivationCatalogMaterialIdentity{CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "catalog", UID: "catalog-uid"}, ClientSHA256: activationDigest(4), ServerSHA256: activationDigest(5)},
			OperationWriter:         CatalogActivationMaterialIdentity{CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "writer", UID: "writer-uid"}, MaterialSHA256: activationDigest(6)},
			PostgreSQLConfiguration: CatalogActivationMaterialIdentity{CatalogActivationObjectIdentity: CatalogActivationObjectIdentity{Name: "configuration", UID: "configuration-uid"}, MaterialSHA256: activationDigest(7)},
			MigrationSHA256:         activationDigest(8), GenesisSHA256: activationDigest(9), PreflightSHA256: activationDigest(10),
			ServingHBAVersion: "pgshard.catalog-serving-hba.v1", ServingHBASHA256: activationDigest(11), TargetTemplateSHA256: activationDigest(12),
		},
		Source: CatalogActivationSource{
			ClusterName: "demo", ClusterUID: "cluster-uid",
			PodName: "demo-shard-0000-member-0000-0", PodUID: "source-pod-uid", Shard: 0, Member: 0,
			InstanceID: "demo-shard-0000-member-0000-0", BootID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", PostmasterPID: 100,
			SystemIdentifier: "12345678901234567890", Timeline: 3, GenerationIdentity: activationGenerationIdentity(), GenerationBarrierLSN: "4294967296",
			TargetFenceAcknowledgement: CatalogActivationTargetFenceAcknowledgement{
				ObservedAtUnixMS: "1700000000000", DeadlineBoottimeNS: "9000000000", RemainingValidityAtAckMS: "5000",
				RemainingValidityAtReportMS: "4500", ControlBackendPID: 101,
			},
		},
		RemoteApplyWitness: CatalogActivationRemoteApplyWitness{
			ClusterName: "demo", ClusterUID: "cluster-uid",
			PodName: "demo-shard-0000-member-0001-0", PodUID: "witness-pod-uid", Shard: 0, Member: 1,
			InstanceID: "demo-shard-0000-member-0001-0", BootID: "ffffffff-1111-2222-3333-444444444444", PostmasterPID: 200,
			MemberSlotName: "pgshard_member_0001", SystemIdentifier: "12345678901234567890", Timeline: 3,
			GenerationIdentity: activationGenerationIdentity(), GenerationBarrierLSN: "4294967296", ReceiveLSN: "4294967396", ReplayLSN: "4294967396",
		},
	}
}

func validCatalogActivationRequestForCluster(clusterName string) CatalogActivationRequest {
	request := validCatalogActivationRequest()
	leaseName := PostgreSQLWritableLeaseName(clusterName, 0)
	request.Carrier.Name = CatalogActivationName(clusterName)
	request.Cluster.Name = clusterName
	request.Dispatcher.LeaseName = clusterName + "-orch-lease"
	request.WritableTerm.Name = leaseName
	request.Source.ClusterName = clusterName
	request.RemoteApplyWitness.ClusterName = clusterName
	request.Source.GenerationIdentity = activationGenerationIdentityFor(clusterName, leaseName)
	request.RemoteApplyWitness.GenerationIdentity = request.Source.GenerationIdentity
	return request
}

func activationCarrier() *PgShardCatalogActivation {
	return activationCarrierForCluster("demo")
}

func activationCarrierForCluster(clusterName string) *PgShardCatalogActivation {
	cluster := &PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: "database", UID: "cluster-uid"}}
	activation := EmptyCatalogActivation(cluster)
	activation.UID = "carrier-uid"
	activation.ResourceVersion = "1"
	return activation
}

func TestCatalogActivationDigestMatchesRustGolden(t *testing.T) {
	request := validCatalogActivationRequest()
	digest, err := request.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	if digest != "2272dfe2f91126128f51746efed94637f326ea31fa8e83f1dff0e90be5d2f3aa" {
		t.Fatalf("catalog activation request digest = %s", digest)
	}
	changed := validCatalogActivationRequest()
	changed.RemoteApplyWitness.ReceiveLSN = "4294967397"
	changed.RemoteApplyWitness.ReplayLSN = "4294967397"
	changedDigest, err := changed.SHA256()
	if err != nil || changedDigest == digest {
		t.Fatalf("changed request digest = %q, error %v", changedDigest, err)
	}
	foreignGeneration := validCatalogActivationRequest()
	foreignGeneration.Source.GenerationIdentity = strings.Replace(foreignGeneration.Source.GenerationIdentity, "lease_namespace=database", "lease_namespace=other", 1)
	foreignGeneration.RemoteApplyWitness.GenerationIdentity = foreignGeneration.Source.GenerationIdentity
	if _, err := foreignGeneration.SHA256(); err == nil {
		t.Fatal("foreign writable-generation namespace was accepted")
	}
	laggingWitness := validCatalogActivationRequest()
	laggingWitness.RemoteApplyWitness.ReplayLSN = "4294967295"
	if _, err := laggingWitness.SHA256(); err == nil {
		t.Fatal("remote witness below the generation barrier was accepted")
	}
	foreignHolder := validCatalogActivationRequest()
	foreignHolder.WritableTerm.Holder = "demo-shard-0000-member-0000-0/witness-pod-uid/0123456789abcdef01234567"
	foreignHolder.Source.GenerationIdentity = activationGenerationIdentityForHolder("demo", foreignHolder.WritableTerm.Name, foreignHolder.WritableTerm.Holder)
	foreignHolder.RemoteApplyWitness.GenerationIdentity = foreignHolder.Source.GenerationIdentity
	if _, err := foreignHolder.SHA256(); err == nil {
		t.Fatal("writable generation held by a different Pod was accepted for the source")
	}
	malformedHolder := validCatalogActivationRequest()
	malformedHolder.WritableTerm.Holder = "demo-shard-0000-member-0000-0/source-pod-uid/ABCDEFABCDEFABCDEFABCDEF"
	malformedHolder.Source.GenerationIdentity = activationGenerationIdentityForHolder("demo", malformedHolder.WritableTerm.Name, malformedHolder.WritableTerm.Holder)
	malformedHolder.RemoteApplyWitness.GenerationIdentity = malformedHolder.Source.GenerationIdentity
	if _, err := malformedHolder.SHA256(); err == nil {
		t.Fatal("noncanonical process incarnation was accepted in the writable holder")
	}
	foreignDispatcher := validCatalogActivationRequest()
	foreignDispatcher.Dispatcher.LeaseHolder = "demo-orchestrator-1/other-dispatcher-uid/11111111-2222-4333-8444-555555555555"
	if _, err := foreignDispatcher.SHA256(); err == nil {
		t.Fatal("orchestrator Lease held by a different Pod was accepted for the dispatcher")
	}
	malformedDispatcher := validCatalogActivationRequest()
	malformedDispatcher.Dispatcher.LeaseHolder = "demo-orchestrator-0/dispatcher-uid/11111111-2222-1333-8444-555555555555"
	if _, err := malformedDispatcher.SHA256(); err == nil {
		t.Fatal("non-v4 orchestrator process incarnation was accepted")
	}
	longRequest := validCatalogActivationRequestForCluster(strings.Repeat("a", MaximumClusterNameLength))
	longDigest, err := longRequest.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	if longDigest != "3c747fc699f1711f61e5f2be413de395a1eb3e081bc7b1eaa25455eb2a881809" {
		t.Fatalf("long-name catalog activation request digest = %s", longDigest)
	}
}

func TestPostgreSQLActivationNamesMatchWorkloadBoundaries(t *testing.T) {
	tests := []struct {
		length        int
		expectedTerm  string
		expectedAgent string
	}{
		{41, strings.Repeat("a", 41) + "-shard-0000-term", strings.Repeat("a", 41) + "-shard-0000-agent"},
		{42, "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f-shard-0000-term", "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f-shard-0000-agent"},
		{MaximumClusterNameLength, "aaaaaaaaaaaaaaaaa-160b4e433e384e05e537dc59-shard-0000-term", "aaaaaaaaaaaaaaaaa-160b4e433e384e05e537dc59-shard-0000-agent"},
	}
	for _, test := range tests {
		clusterName := strings.Repeat("a", test.length)
		if got := PostgreSQLWritableLeaseName(clusterName, 0); got != test.expectedTerm {
			t.Errorf("%d-byte cluster writable Lease = %q, want %q", test.length, got, test.expectedTerm)
		}
		if got := PostgreSQLAgentServiceAccountName(clusterName, 0); got != test.expectedAgent {
			t.Errorf("%d-byte cluster agent ServiceAccount = %q, want %q", test.length, got, test.expectedAgent)
		}
	}
}

func TestCatalogActivationCreateIsControllerOwnedAndEmpty(t *testing.T) {
	validator := &PgShardCatalogActivationValidator{ControllerUsername: catalogActivationControllerUsername}
	carrier := activationCarrier()
	carrier.UID = ""
	carrier.ResourceVersion = ""
	if _, err := validator.ValidateCreate(catalogActivationAdmissionContext(catalogActivationControllerUsername), carrier); err != nil {
		t.Fatalf("exact empty carrier rejected: %v", err)
	}
	if _, err := validator.ValidateCreate(catalogActivationAdmissionContext("system:admin"), carrier); err == nil {
		t.Fatal("non-controller carrier creation was accepted")
	}
	nonEmpty := carrier.DeepCopy()
	request := validCatalogActivationRequest()
	nonEmpty.Spec.Request = &request
	nonEmpty.Spec.RequestSHA256 = activationDigest(1)
	if _, err := validator.ValidateCreate(catalogActivationAdmissionContext(catalogActivationControllerUsername), nonEmpty); err == nil {
		t.Fatal("controller-created non-empty carrier was accepted")
	}
}

func TestCatalogActivationRequestAndAcceptanceAreOwnedSetOnce(t *testing.T) {
	validator := &PgShardCatalogActivationValidator{}
	oldCarrier := activationCarrier()
	published := oldCarrier.DeepCopy()
	request := validCatalogActivationRequest()
	digest, err := request.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	published.Spec.Request = &request
	published.Spec.RequestSHA256 = digest
	published.Generation = 2
	publisher := "system:serviceaccount:database:demo-orchestrator"
	publisherContext := catalogActivationAdmissionContextForPod(publisher, request.Dispatcher.PodName, string(request.Dispatcher.PodUID))
	if _, err := validator.ValidateUpdate(publisherContext, oldCarrier, published); err != nil {
		t.Fatalf("exact orchestrator request rejected: %v", err)
	}
	if _, err := validator.ValidateUpdate(catalogActivationAdmissionContext(publisher), oldCarrier, published); err == nil {
		t.Fatal("orchestrator request without Pod-bound token extras was accepted")
	}
	siblingPublisher := catalogActivationAdmissionContextForPod(publisher, "demo-orchestrator-1", "sibling-dispatcher-uid")
	if _, err := validator.ValidateUpdate(siblingPublisher, oldCarrier, published); err == nil {
		t.Fatal("sibling orchestrator Pod was accepted for another dispatcher's request")
	}
	foreignNamespace := oldCarrier.DeepCopy()
	foreignRequest := validCatalogActivationRequest()
	foreignRequest.Cluster.Namespace = "other"
	foreignRequest.Source.GenerationIdentity = strings.Replace(foreignRequest.Source.GenerationIdentity, "lease_namespace=database", "lease_namespace=other", 1)
	foreignRequest.RemoteApplyWitness.GenerationIdentity = foreignRequest.Source.GenerationIdentity
	foreignDigest, err := foreignRequest.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	foreignNamespace.Spec.Request = &foreignRequest
	foreignNamespace.Spec.RequestSHA256 = foreignDigest
	if _, err := validator.ValidateUpdate(publisherContext, oldCarrier, foreignNamespace); err == nil {
		t.Fatal("request bound to a foreign namespace was accepted")
	}
	if _, err := validator.ValidateUpdate(catalogActivationAdmissionContext("system:admin"), oldCarrier, published); err == nil {
		t.Fatal("foreign request publisher was accepted")
	}

	accepted := published.DeepCopy()
	accepted.Status.Acceptance = &CatalogActivationAcceptance{
		SchemaVersion: CatalogActivationAcceptanceVersion, CarrierUID: "carrier-uid", RequestSHA256: digest,
		TargetPodName: request.Source.PodName, TargetPodUID: request.Source.PodUID,
		Persistence: CatalogActivationPersistenceFsync, PersistedAtUnixMS: "1700000000100",
	}
	agent := "system:serviceaccount:database:demo-shard-0000-agent"
	agentContext := catalogActivationAdmissionContextForPod(agent, request.Source.PodName, string(request.Source.PodUID))
	if _, err := validator.ValidateUpdate(agentContext, published, accepted); err != nil {
		t.Fatalf("exact agent acceptance rejected: %v", err)
	}
	if _, err := validator.ValidateUpdate(catalogActivationAdmissionContext(agent), published, accepted); err == nil {
		t.Fatal("agent acceptance without Pod-bound token extras was accepted")
	}
	siblingAgent := catalogActivationAdmissionContextForPod(agent, request.RemoteApplyWitness.PodName, string(request.RemoteApplyWitness.PodUID))
	if _, err := validator.ValidateUpdate(siblingAgent, published, accepted); err == nil {
		t.Fatal("sibling shard agent Pod was accepted for another source's request")
	}
	mutated := accepted.DeepCopy()
	mutated.Status.Acceptance.PersistedAtUnixMS = "1700000000101"
	if _, err := validator.ValidateUpdate(agentContext, accepted, mutated); err == nil {
		t.Fatal("accepted status mutation was allowed")
	}
	metadataMutation := published.DeepCopy()
	metadataMutation.Labels["foreign"] = "value"
	if _, err := validator.ValidateUpdate(catalogActivationAdmissionContext("system:serviceaccount:database:demo-orchestrator"), published, metadataMutation); err == nil {
		t.Fatal("carrier metadata mutation was allowed")
	}
}

func TestCatalogActivationLongNameAcceptanceUsesBoundedAgentIdentity(t *testing.T) {
	validator := &PgShardCatalogActivationValidator{}
	clusterName := strings.Repeat("a", MaximumClusterNameLength)
	oldCarrier := activationCarrierForCluster(clusterName)
	published := oldCarrier.DeepCopy()
	request := validCatalogActivationRequestForCluster(clusterName)
	digest, err := request.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	published.Spec.Request = &request
	published.Spec.RequestSHA256 = digest
	published.Generation = 2
	orchestrator := "system:serviceaccount:database:" + clusterName + "-orchestrator"
	longPublisherContext := catalogActivationAdmissionContextForPod(orchestrator, request.Dispatcher.PodName, string(request.Dispatcher.PodUID))
	if _, err := validator.ValidateUpdate(longPublisherContext, oldCarrier, published); err != nil {
		t.Fatalf("long-name orchestrator request rejected: %v", err)
	}
	accepted := published.DeepCopy()
	accepted.Status.Acceptance = &CatalogActivationAcceptance{
		SchemaVersion: CatalogActivationAcceptanceVersion, CarrierUID: "carrier-uid", RequestSHA256: digest,
		TargetPodName: request.Source.PodName, TargetPodUID: request.Source.PodUID,
		Persistence: CatalogActivationPersistenceFsync, PersistedAtUnixMS: "1700000000100",
	}
	boundedAgent := "system:serviceaccount:database:" + PostgreSQLAgentServiceAccountName(clusterName, 0)
	longAgentContext := catalogActivationAdmissionContextForPod(boundedAgent, request.Source.PodName, string(request.Source.PodUID))
	if _, err := validator.ValidateUpdate(longAgentContext, published, accepted); err != nil {
		t.Fatalf("bounded long-name agent acceptance rejected: %v", err)
	}
	rawAgent := fmt.Sprintf("system:serviceaccount:database:%s-shard-0000-agent", clusterName)
	if _, err := validator.ValidateUpdate(catalogActivationAdmissionContextForPod(rawAgent, request.Source.PodName, string(request.Source.PodUID)), published, accepted); err == nil {
		t.Fatal("nonexistent raw long-name agent identity was accepted")
	}
}
