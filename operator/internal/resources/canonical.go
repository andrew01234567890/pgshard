package resources

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodClass identifies the kind of protected pod a template produces. It is a
// domain-separation input to the contract hash so two structurally identical
// templates of different classes can never collide.
type PodClass string

const (
	ClassSource       PodClass = "source"
	ClassStandby      PodClass = "standby"
	ClassSingleMember PodClass = "single-member"
	ClassPooler       PodClass = "pooler"
	ClassOrchestrator PodClass = "orchestrator"
)

const (
	// PodContractHashAnnotation carries the reconciler-stamped full-contract
	// hash on a workload's pod template (and therefore on every pod the
	// controller creates from it). It is deliberately EXCLUDED from every
	// canonical normal form (a pod's stamp cannot vouch for itself).
	PodContractHashAnnotation = "pgshard.io/contract-hash"
	// PodSecurityGenerationAnnotation carries the monotonic per-(class,member)
	// security generation the template was stamped at. It is stripped from the
	// normal form; its authoritative value is injected into the hash message.
	PodSecurityGenerationAnnotation = "pgshard.io/security-generation"

	contractHashDomain = "pgshard.pod-contract.v1"

	serviceAccountTokenMountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
	normalizedTokenVolumeName    = "pgshard-normalized-sa-token"

	// StatefulSet/ReplicaSet controller-added pod labels.
	labelControllerRevisionHash = "controller-revision-hash"
	labelStatefulSetPodName     = "statefulset.kubernetes.io/pod-name"
	labelPodIndex               = "apps.kubernetes.io/pod-index"
	labelPodTemplateHash        = "pod-template-hash"
)

// memberControllerLabels are stamped by the StatefulSet controller and must be
// present (and derivable) on a member pod; supporting pods must never carry
// them. supportingControllerLabels is the ReplicaSet controller's equivalent.
var memberControllerLabels = []string{labelControllerRevisionHash, labelStatefulSetPodName, labelPodIndex}

var supportingControllerLabels = []string{labelPodTemplateHash}

// nodeTopologyLabels are copied onto a bound pod by the PodTopologyLabels
// admission plugin. LiveNormalForm validates them against binding evidence and
// strips them; any other binding-copied label is unexpected residue and fails
// comparison.
var nodeTopologyLabels = []string{corev1.LabelTopologyZone, corev1.LabelTopologyRegion}

// canonicalEncMode is a true canonical CBOR encoder: RFC 8949 §4.2.1 Core
// Deterministic Encoding — definite-length (length-framed) items, shortest
// integer forms, and map keys sorted in bytewise lexicographic order.
var canonicalEncMode = mustCanonicalEncMode()

func mustCanonicalEncMode() cbor.EncMode {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(fmt.Sprintf("initialize canonical CBOR encoder: %v", err))
	}
	return mode
}

// BindingEvidence is the authoritative node/topology identity the scheduler and
// binding webhook attest for a bound pod. LiveNormalForm requires the pod's
// binding residue to equal this exactly before stripping it. Zone/Region are
// empty when the bound Node carries no such label; the residue must then be
// absent too.
type BindingEvidence struct {
	NodeName string
	NodeUID  string
	BootID   string
	Zone     string
	Region   string
}

// ControllerEvidence is the authoritative controller-provenance identity for a
// pod's owning parent. The webhook layer supplies it (the live parent UID, and
// for supporting pods the owning ReplicaSet's name and pod-template-hash);
// step-2 callers pass what they can validate. Empty fields are validated
// structurally only.
type ControllerEvidence struct {
	ParentUID              string
	ReplicaSetName         string
	PodTemplateHash        string
	ControllerRevisionHash string
}

// NormContext carries the identity a normal form is derived against. Class,
// Shard, and Member are always required; ClusterName and Namespace bind the
// pod to its cluster/namespace (validated in CREATE/LIVE). Provenance and
// Binding carry the authoritative controller/binding evidence used to validate
// and strip residue.
type NormContext struct {
	Class       PodClass
	ClusterName string
	Namespace   string
	Shard       int32
	Member      int32
	Provenance  *ControllerEvidence
	Binding     *BindingEvidence
}

type normMode int

const (
	modeTemplate normMode = iota // stamp time: strip identity/residue, no pod-side validation
	modeCreate                   // pod CREATE: nodeName empty; validate controller residue
	modeLive                     // running pod: additionally validate+strip binding residue
)

// ComputeContractStamp returns the domain-separated, length-framed full-contract
// hash of a pod template:
//
//	contractHash = HMAC_SHA256(
//	    key = lengthFramed("pgshard.pod-contract.v1" ‖ class ‖ clusterUID ‖ shard ‖ member ‖ securityGeneration),
//	    msg = canonicalCBOR([securityGeneration, templateNormalForm]))
//
// The template normal form applies pinned k8s-1.36 defaulting and strips
// identity/controller residue so that a stamped template and any pod the
// controllers + API server produce from it reduce to the same normal form and
// therefore the same hash. The authoritative security generation is injected
// into the hashed message (not merely the key).
func ComputeContractStamp(class PodClass, clusterUID string, shard, member int32, securityGeneration int64, template *corev1.PodTemplateSpec) (string, error) {
	if template == nil {
		return "", fmt.Errorf("pod template is required to compute a contract stamp")
	}
	tree, err := normalTree(NormContext{Class: class, Shard: shard, Member: member}, template.ObjectMeta, template.Spec, modeTemplate)
	if err != nil {
		return "", err
	}
	return hashNormalizedContract(class, clusterUID, shard, member, securityGeneration, tree)
}

// HashAdmittedPod recomputes the contract hash from an admitted pod's metadata
// and spec, normalized for the given lifecycle stage. A valid pod produces the
// same hash the reconciler stamped onto its parent template.
func HashAdmittedPod(nc NormContext, meta metav1.ObjectMeta, spec corev1.PodSpec, stage PodComparisonStage, clusterUID string, securityGeneration int64) (string, error) {
	mode := modeCreate
	if stage == StageLive {
		mode = modeLive
	}
	tree, err := normalTree(nc, meta, spec, mode)
	if err != nil {
		return "", err
	}
	return hashNormalizedContract(nc.Class, clusterUID, nc.Shard, nc.Member, securityGeneration, tree)
}

func hashNormalizedContract(class PodClass, clusterUID string, shard, member int32, securityGeneration int64, tree any) (string, error) {
	// Inject the authoritative generation into the hashed message as a
	// length-framed CBOR array element, in addition to the HMAC key.
	message, err := canonicalEncMode.Marshal([]any{securityGeneration, tree})
	if err != nil {
		return "", fmt.Errorf("canonical-encode normalized contract: %w", err)
	}
	mac := hmac.New(sha256.New, contractDomainKey(class, clusterUID, shard, member, securityGeneration))
	if _, err := mac.Write(message); err != nil {
		return "", fmt.Errorf("hash normalized contract: %w", err)
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// ApplyContractStamp computes the stamp and writes both stamp annotations onto
// the supplied template (mutating it), returning the hash.
func ApplyContractStamp(template *corev1.PodTemplateSpec, class PodClass, clusterUID string, shard, member int32, securityGeneration int64) (string, error) {
	hash, err := ComputeContractStamp(class, clusterUID, shard, member, securityGeneration, template)
	if err != nil {
		return "", err
	}
	if template.Annotations == nil {
		template.Annotations = make(map[string]string, 2)
	}
	template.Annotations[PodSecurityGenerationAnnotation] = fmt.Sprintf("%d", securityGeneration)
	template.Annotations[PodContractHashAnnotation] = hash
	return hash, nil
}

// contractDomainKey builds an unambiguous, length-framed HMAC key from the
// domain-separation tuple.
func contractDomainKey(class PodClass, clusterUID string, shard, member int32, securityGeneration int64) []byte {
	parts := [][]byte{
		[]byte(contractHashDomain),
		[]byte(class),
		[]byte(clusterUID),
		[]byte(fmt.Sprintf("%d", shard)),
		[]byte(fmt.Sprintf("%d", member)),
		[]byte(fmt.Sprintf("%d", securityGeneration)),
	}
	var buffer bytes.Buffer
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		buffer.Write(length[:])
		buffer.Write(part)
	}
	return buffer.Bytes()
}

func normalTree(nc NormContext, meta metav1.ObjectMeta, spec corev1.PodSpec, mode normMode) (any, error) {
	m := *meta.DeepCopy()
	s := *spec.DeepCopy()
	if err := normalizePodContract(nc, &m, &s, mode); err != nil {
		return nil, err
	}
	return jsonTree(struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
		Spec     corev1.PodSpec    `json:"spec"`
	}{Metadata: m, Spec: s})
}

// ---------------------------------------------------------------------------
// Full-spec normalization (design v7 §A2/§3)
// ---------------------------------------------------------------------------

// CreateNormalForm normalizes a pod as it appears at CREATE (before scheduling
// binds it). The entire remaining structure is compared by the caller — there
// is no capability projection.
func CreateNormalForm(nc NormContext, meta metav1.ObjectMeta, spec corev1.PodSpec) (metav1.ObjectMeta, corev1.PodSpec, error) {
	m := *meta.DeepCopy()
	s := *spec.DeepCopy()
	err := normalizePodContract(nc, &m, &s, modeCreate)
	return m, s, err
}

// LiveNormalForm normalizes a running (bound) pod: it additionally validates
// the scheduler/binding residue against the supplied BindingEvidence — nodeName,
// node-UID/boot-ID annotations, and zone/region topology labels — before
// stripping it, so a bound pod reduces to the same normal form as its template.
func LiveNormalForm(nc NormContext, meta metav1.ObjectMeta, spec corev1.PodSpec) (metav1.ObjectMeta, corev1.PodSpec, error) {
	m := *meta.DeepCopy()
	s := *spec.DeepCopy()
	err := normalizePodContract(nc, &m, &s, modeLive)
	return m, s, err
}

func normalizePodContract(nc NormContext, meta *metav1.ObjectMeta, spec *corev1.PodSpec, mode normMode) error {
	if err := normalizeContractMetadata(nc, meta, mode); err != nil {
		return err
	}
	return normalizeContractSpec(nc, spec, mode)
}

func normalizeContractMetadata(nc NormContext, meta *metav1.ObjectMeta, mode normMode) error {
	if mode != modeTemplate {
		if meta.Namespace != nc.Namespace {
			return fmt.Errorf("managed pod namespace %q does not match its cluster namespace %q", meta.Namespace, nc.Namespace)
		}
		if err := validateControllerOwnerReference(nc, meta); err != nil {
			return err
		}
		if err := validateControllerLabels(nc, meta); err != nil {
			return err
		}
		if mode == modeLive {
			if err := validateBindingLabelsAndAnnotations(nc, meta); err != nil {
				return err
			}
		}
	}

	meta.Name = ""
	meta.GenerateName = ""
	meta.Namespace = ""
	meta.ResourceVersion = ""
	meta.UID = ""
	meta.Generation = 0
	meta.CreationTimestamp = metav1.Time{}
	meta.DeletionTimestamp = nil
	meta.DeletionGracePeriodSeconds = nil
	meta.ManagedFields = nil
	meta.OwnerReferences = nil
	meta.SelfLink = ""

	for _, key := range memberControllerLabels {
		delete(meta.Labels, key)
	}
	for _, key := range supportingControllerLabels {
		delete(meta.Labels, key)
	}
	if mode == modeLive {
		for _, key := range nodeTopologyLabels {
			delete(meta.Labels, key)
		}
	}
	delete(meta.Annotations, PodContractHashAnnotation)
	delete(meta.Annotations, PodSecurityGenerationAnnotation)
	if mode == modeLive {
		delete(meta.Annotations, PostgreSQLNodeUIDAnnotation)
		delete(meta.Annotations, PostgreSQLNodeBootIDAnnotation)
	}
	if len(meta.Labels) == 0 {
		meta.Labels = nil
	}
	if len(meta.Annotations) == 0 {
		meta.Annotations = nil
	}
	return nil
}

func validateControllerOwnerReference(nc NormContext, meta *metav1.ObjectMeta) error {
	controllers := make([]metav1.OwnerReference, 0, 1)
	for _, ref := range meta.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			controllers = append(controllers, ref)
		}
	}
	if len(controllers) != 1 {
		return fmt.Errorf("managed pod must have exactly one controller owner reference, found %d", len(controllers))
	}
	ref := controllers[0]
	if isMemberClass(nc.Class) {
		wantName := PostgreSQLMemberStatefulSetName(nc.ClusterName, nc.Shard, nc.Member)
		if ref.Kind != "StatefulSet" {
			return fmt.Errorf("member pod controller owner reference kind %q, want StatefulSet", ref.Kind)
		}
		if ref.Name != wantName {
			return fmt.Errorf("member pod controller owner reference name %q, want %q", ref.Name, wantName)
		}
	} else {
		if ref.Kind != "ReplicaSet" {
			return fmt.Errorf("supporting pod controller owner reference kind %q, want ReplicaSet", ref.Kind)
		}
		if nc.Provenance != nil && nc.Provenance.ReplicaSetName != "" && ref.Name != nc.Provenance.ReplicaSetName {
			return fmt.Errorf("supporting pod controller owner reference name %q, want %q", ref.Name, nc.Provenance.ReplicaSetName)
		}
		if ref.Name == "" {
			return fmt.Errorf("supporting pod controller owner reference has no name")
		}
	}
	if nc.Provenance != nil && nc.Provenance.ParentUID != "" && string(ref.UID) != nc.Provenance.ParentUID {
		return fmt.Errorf("managed pod controller owner reference UID does not match the live parent")
	}
	return nil
}

func validateControllerLabels(nc NormContext, meta *metav1.ObjectMeta) error {
	labels := meta.Labels
	if isMemberClass(nc.Class) {
		if _, present := labels[labelPodTemplateHash]; present {
			return fmt.Errorf("member pod must not carry the ReplicaSet %q label", labelPodTemplateHash)
		}
		wantPodName := PostgreSQLMemberStatefulSetName(nc.ClusterName, nc.Shard, nc.Member) + "-0"
		if labels[labelStatefulSetPodName] != wantPodName {
			return fmt.Errorf("member pod-name label %q, want %q", labels[labelStatefulSetPodName], wantPodName)
		}
		if labels[labelPodIndex] != "0" {
			return fmt.Errorf("member pod-index label %q, want \"0\"", labels[labelPodIndex])
		}
		revision, present := labels[labelControllerRevisionHash]
		if !present || revision == "" {
			return fmt.Errorf("member pod is missing its %q label", labelControllerRevisionHash)
		}
		if nc.Provenance != nil && nc.Provenance.ControllerRevisionHash != "" && revision != nc.Provenance.ControllerRevisionHash {
			return fmt.Errorf("member pod controller-revision-hash does not match the live StatefulSet revision")
		}
		return nil
	}
	for _, key := range memberControllerLabels {
		if _, present := labels[key]; present {
			return fmt.Errorf("supporting pod must not carry the StatefulSet %q label", key)
		}
	}
	hash, present := labels[labelPodTemplateHash]
	if !present || hash == "" {
		return fmt.Errorf("supporting pod is missing its %q label", labelPodTemplateHash)
	}
	if nc.Provenance != nil && nc.Provenance.PodTemplateHash != "" && hash != nc.Provenance.PodTemplateHash {
		return fmt.Errorf("supporting pod-template-hash does not match the owning ReplicaSet")
	}
	return nil
}

// validateBindingLabelsAndAnnotations requires the pod's node-UID/boot-ID
// annotations and zone/region labels to equal the authoritative binding
// evidence exactly, and forbids any other binding-copied topology label.
func validateBindingLabelsAndAnnotations(nc NormContext, meta *metav1.ObjectMeta) error {
	evidence := nc.Binding
	if evidence == nil {
		return fmt.Errorf("live managed pod requires authoritative binding evidence")
	}
	if meta.Annotations[PostgreSQLNodeUIDAnnotation] != evidence.NodeUID || evidence.NodeUID == "" {
		return fmt.Errorf("bound pod node-UID annotation does not match the authoritative binding evidence")
	}
	if meta.Annotations[PostgreSQLNodeBootIDAnnotation] != evidence.BootID || evidence.BootID == "" {
		return fmt.Errorf("bound pod boot-ID annotation does not match the authoritative binding evidence")
	}
	if err := validateTopologyLabel(meta, corev1.LabelTopologyZone, evidence.Zone); err != nil {
		return err
	}
	if err := validateTopologyLabel(meta, corev1.LabelTopologyRegion, evidence.Region); err != nil {
		return err
	}
	return nil
}

func validateTopologyLabel(meta *metav1.ObjectMeta, key, want string) error {
	value, present := meta.Labels[key]
	if want == "" {
		if present {
			return fmt.Errorf("bound pod carries topology label %q the Node does not have", key)
		}
		return nil
	}
	if !present || value != want {
		return fmt.Errorf("bound pod topology label %q does not match the bound Node", key)
	}
	return nil
}

func normalizeContractSpec(nc NormContext, spec *corev1.PodSpec, mode normMode) error {
	// The API server mirrors serviceAccountName into the deprecated field.
	spec.DeprecatedServiceAccount = ""

	if err := normalizeOrdinalIdentity(nc, spec, mode); err != nil {
		return err
	}
	if err := normalizeNodeName(nc, spec, mode); err != nil {
		return err
	}
	if err := normalizePriorityTuple(spec, mode); err != nil {
		return err
	}
	if err := normalizeInjectedServiceAccountToken(nc.Class, spec, mode); err != nil {
		return err
	}
	// Apply pinned k8s-1.36 defaulting last so the raw template and the
	// API-server-defaulted pod converge to the same normal form.
	applyContractDefaults(spec)
	return nil
}

func isMemberClass(class PodClass) bool {
	return class == ClassSource || class == ClassStandby || class == ClassSingleMember
}

// IsMemberClass reports whether a class is a PostgreSQL member (as opposed to a
// supporting pooler/orchestrator).
func IsMemberClass(class PodClass) bool {
	return isMemberClass(class)
}

// ClassForMember maps a member ordinal to its protected pod class. A
// single-member topology is the serving single-member class; multi-member
// member zero is the replication source, and every nonzero member is a standby.
func ClassForMember(membersPerShard, member int32) PodClass {
	if membersPerShard == 1 {
		return ClassSingleMember
	}
	if member == 0 {
		return ClassSource
	}
	return ClassStandby
}

// ParseIdentityLabel parses a fixed-width zero-padded shard or member ordinal
// label. It returns false for any value that is not exactly four digits of a
// non-negative int32.
func ParseIdentityLabel(value string) (int32, bool) {
	if len(value) != 4 {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 0 {
		return 0, false
	}
	return int32(parsed), true
}

func normalizeOrdinalIdentity(nc NormContext, spec *corev1.PodSpec, mode normMode) error {
	if isMemberClass(nc.Class) {
		if mode != modeTemplate {
			wantHostname := PostgreSQLMemberStatefulSetName(nc.ClusterName, nc.Shard, nc.Member) + "-0"
			wantSubdomain := shardName(nc.ClusterName, nc.Shard)
			if spec.Hostname != wantHostname {
				return fmt.Errorf("member hostname %q does not match the derived ordinal identity %q", spec.Hostname, wantHostname)
			}
			if spec.Subdomain != wantSubdomain {
				return fmt.Errorf("member subdomain %q does not match the shard service %q", spec.Subdomain, wantSubdomain)
			}
		}
	} else if mode != modeTemplate && (spec.Hostname != "" || spec.Subdomain != "") {
		return fmt.Errorf("supporting pod must not set hostname/subdomain")
	}
	spec.Hostname = ""
	spec.Subdomain = ""
	return nil
}

func normalizeNodeName(nc NormContext, spec *corev1.PodSpec, mode normMode) error {
	switch mode {
	case modeTemplate, modeCreate:
		if spec.NodeName != "" {
			return fmt.Errorf("managed pod must be created unassigned; spec.nodeName %q is preset", spec.NodeName)
		}
	case modeLive:
		evidence := nc.Binding
		if evidence == nil {
			return fmt.Errorf("live managed pod requires authoritative binding evidence")
		}
		if spec.NodeName == "" || spec.NodeName != evidence.NodeName {
			return fmt.Errorf("bound pod nodeName %q does not match the authoritative binding evidence", spec.NodeName)
		}
	}
	spec.NodeName = ""
	return nil
}

// normalizePriorityTuple synthesizes the {priority, preemptionPolicy} tuple
// from a pgshard PriorityClass name ONLY in template mode. In CREATE/LIVE modes
// an honest pod always carries the plugin-resolved values, so they are required
// to be present and to equal the pinned resolution; a missing or wrong value
// (e.g. a pod created without Priority admission) is rejected.
func normalizePriorityTuple(spec *corev1.PodSpec, mode normMode) error {
	value, ok := priorityValueForClassName(spec.PriorityClassName)
	if !ok {
		return nil
	}
	if mode == modeTemplate {
		if spec.Priority == nil {
			resolved := value
			spec.Priority = &resolved
		}
		if spec.PreemptionPolicy == nil {
			policy := pgShardPreemptionPolicy
			spec.PreemptionPolicy = &policy
		}
		return nil
	}
	if spec.Priority == nil || *spec.Priority != value {
		return fmt.Errorf("managed pod priority does not match the resolved value for %q", spec.PriorityClassName)
	}
	if spec.PreemptionPolicy == nil || *spec.PreemptionPolicy != pgShardPreemptionPolicy {
		return fmt.Errorf("managed pod preemptionPolicy does not match the resolved policy for %q", spec.PriorityClassName)
	}
	return nil
}

func classExpectsInjectedToken(class PodClass) bool {
	// Only the orchestrator runs with automountServiceAccountToken=true, so it
	// alone receives the ServiceAccount admission plugin's projected token.
	return class == ClassOrchestrator
}

// normalizeInjectedServiceAccountToken canonicalizes the ServiceAccount
// plugin's randomly-named projected token as a relational tuple: the template
// gets the exact expected tuple (fixed placeholder name), and a pod's injected
// `kube-api-access-*` volume is validated against that exact shape and renamed
// to the placeholder. The volume name is the only free variable; any extra
// source, volume, or mount, or a missing/mismatched tuple, fails comparison.
func normalizeInjectedServiceAccountToken(class PodClass, spec *corev1.PodSpec, mode normMode) error {
	if !classExpectsInjectedToken(class) {
		return nil
	}
	expected := expectedInjectedTokenVolumeSource()
	if mode == modeTemplate {
		spec.Volumes = append(spec.Volumes, corev1.Volume{Name: normalizedTokenVolumeName, VolumeSource: corev1.VolumeSource{Projected: expected}})
		mount := corev1.VolumeMount{Name: normalizedTokenVolumeName, ReadOnly: true, MountPath: serviceAccountTokenMountPath}
		for i := range spec.Containers {
			spec.Containers[i].VolumeMounts = append(spec.Containers[i].VolumeMounts, mount)
		}
		for i := range spec.InitContainers {
			spec.InitContainers[i].VolumeMounts = append(spec.InitContainers[i].VolumeMounts, mount)
		}
		return nil
	}

	injectedIndex := -1
	for i := range spec.Volumes {
		if spec.Volumes[i].Projected != nil && equality.Semantic.DeepEqual(spec.Volumes[i].Projected, expected) {
			if injectedIndex >= 0 {
				return fmt.Errorf("more than one projected service-account token volume is present")
			}
			injectedIndex = i
		}
	}
	if injectedIndex < 0 {
		return fmt.Errorf("automounting class %s is missing its exact injected service-account token volume", class)
	}
	injectedName := spec.Volumes[injectedIndex].Name
	spec.Volumes[injectedIndex].Name = normalizedTokenVolumeName

	mounted := false
	renameMount := func(container *corev1.Container) error {
		for mi := range container.VolumeMounts {
			mount := &container.VolumeMounts[mi]
			if mount.Name != injectedName {
				continue
			}
			if mount.MountPath != serviceAccountTokenMountPath || !mount.ReadOnly || mount.SubPath != "" {
				return fmt.Errorf("injected service-account token mount has an unexpected shape")
			}
			mount.Name = normalizedTokenVolumeName
			mounted = true
		}
		return nil
	}
	for i := range spec.Containers {
		if err := renameMount(&spec.Containers[i]); err != nil {
			return err
		}
	}
	for i := range spec.InitContainers {
		if err := renameMount(&spec.InitContainers[i]); err != nil {
			return err
		}
	}
	if !mounted {
		return fmt.Errorf("injected service-account token volume is not mounted read-only at %s", serviceAccountTokenMountPath)
	}
	return nil
}

func expectedInjectedTokenVolumeSource() *corev1.ProjectedVolumeSource {
	expiration := int64(3607)
	mode := corev1.ProjectedVolumeSourceDefaultMode
	return &corev1.ProjectedVolumeSource{
		DefaultMode: &mode,
		Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
		},
	}
}

// ---------------------------------------------------------------------------
// Pinned k8s 1.36 defaulting (design v7 §3, review blocker 1)
//
// Applied to BOTH the raw Plan template and the API-server-defaulted pod so
// they converge. It mirrors the fields SetDefaults_Pod / SetObjectDefaults_*
// add to a stored StatefulSet/Deployment pod template and to a real pod under
// stock Kubernetes 1.36.1. It is idempotent. The real API-server round-trip
// test is the authority that this set matches the target server; a gap
// false-denies (fail closed), never opens.
// ---------------------------------------------------------------------------

func applyContractDefaults(spec *corev1.PodSpec) {
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = corev1.RestartPolicyAlways
	}
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	if spec.TerminationGracePeriodSeconds == nil {
		spec.TerminationGracePeriodSeconds = ptr(int64(corev1.DefaultTerminationGracePeriodSeconds))
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if spec.EnableServiceLinks == nil {
		spec.EnableServiceLinks = ptr(true)
	}
	for i := range spec.InitContainers {
		applyContainerDefaults(&spec.InitContainers[i])
	}
	for i := range spec.Containers {
		applyContainerDefaults(&spec.Containers[i])
	}
	for i := range spec.Volumes {
		applyVolumeDefaults(&spec.Volumes[i])
	}
}

func applyContainerDefaults(container *corev1.Container) {
	if container.TerminationMessagePath == "" {
		container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if container.TerminationMessagePolicy == "" {
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	if container.ImagePullPolicy == "" {
		container.ImagePullPolicy = defaultImagePullPolicy(container.Image)
	}
	for pi := range container.Ports {
		if container.Ports[pi].Protocol == "" {
			container.Ports[pi].Protocol = corev1.ProtocolTCP
		}
	}
	applyProbeDefaults(container.LivenessProbe)
	applyProbeDefaults(container.ReadinessProbe)
	applyProbeDefaults(container.StartupProbe)
	applyResourceRequestDefaults(&container.Resources)
	for ei := range container.Env {
		if container.Env[ei].ValueFrom != nil && container.Env[ei].ValueFrom.FieldRef != nil && container.Env[ei].ValueFrom.FieldRef.APIVersion == "" {
			container.Env[ei].ValueFrom.FieldRef.APIVersion = "v1"
		}
	}
}

func applyProbeDefaults(probe *corev1.Probe) {
	if probe == nil {
		return
	}
	if probe.TimeoutSeconds == 0 {
		probe.TimeoutSeconds = 1
	}
	if probe.PeriodSeconds == 0 {
		probe.PeriodSeconds = 10
	}
	if probe.SuccessThreshold == 0 {
		probe.SuccessThreshold = 1
	}
	if probe.FailureThreshold == 0 {
		probe.FailureThreshold = 3
	}
	if probe.HTTPGet != nil && probe.HTTPGet.Scheme == "" {
		probe.HTTPGet.Scheme = corev1.URISchemeHTTP
	}
}

// applyResourceRequestDefaults mirrors SetDefaults_Pod's copy of each missing
// request from its corresponding limit (which the API server applies only to
// real Pods, not templates) so a limit-only member entry converges.
func applyResourceRequestDefaults(resources *corev1.ResourceRequirements) {
	if len(resources.Limits) == 0 {
		return
	}
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	for name, quantity := range resources.Limits {
		if _, present := resources.Requests[name]; !present {
			resources.Requests[name] = quantity.DeepCopy()
		}
	}
}

func applyVolumeDefaults(volume *corev1.Volume) {
	switch {
	case volume.Secret != nil && volume.Secret.DefaultMode == nil:
		volume.Secret.DefaultMode = ptr(corev1.SecretVolumeSourceDefaultMode)
	case volume.ConfigMap != nil && volume.ConfigMap.DefaultMode == nil:
		volume.ConfigMap.DefaultMode = ptr(corev1.ConfigMapVolumeSourceDefaultMode)
	case volume.DownwardAPI != nil && volume.DownwardAPI.DefaultMode == nil:
		volume.DownwardAPI.DefaultMode = ptr(corev1.DownwardAPIVolumeSourceDefaultMode)
	case volume.Projected != nil && volume.Projected.DefaultMode == nil:
		volume.Projected.DefaultMode = ptr(corev1.ProjectedVolumeSourceDefaultMode)
	}
	if volume.DownwardAPI != nil {
		for i := range volume.DownwardAPI.Items {
			if volume.DownwardAPI.Items[i].FieldRef != nil && volume.DownwardAPI.Items[i].FieldRef.APIVersion == "" {
				volume.DownwardAPI.Items[i].FieldRef.APIVersion = "v1"
			}
		}
	}
	if volume.Projected != nil {
		for si := range volume.Projected.Sources {
			downward := volume.Projected.Sources[si].DownwardAPI
			if downward == nil {
				continue
			}
			for i := range downward.Items {
				if downward.Items[i].FieldRef != nil && downward.Items[i].FieldRef.APIVersion == "" {
					downward.Items[i].FieldRef.APIVersion = "v1"
				}
			}
		}
	}
}

// defaultImagePullPolicy mirrors stock container defaulting: a digest or an
// explicit non-latest tag defaults to IfNotPresent; only an empty tag or
// ":latest" defaults to Always.
func defaultImagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@") {
		return corev1.PullIfNotPresent
	}
	lastComponent := image[strings.LastIndex(image, "/")+1:]
	colon := strings.LastIndex(lastComponent, ":")
	if colon < 0 || lastComponent[colon+1:] == "latest" {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

// ---------------------------------------------------------------------------
// Pre-comparison structural validators (design v7 §10)
// ---------------------------------------------------------------------------

// ValidateNoDuplicateIdentities rejects duplicate list identities on the
// effective admitted object: env names within a container, port names within a
// container, container names across all container lists, and volume names.
func ValidateNoDuplicateIdentities(spec *corev1.PodSpec) error {
	containerNames := map[string]struct{}{}
	checkContainer := func(container corev1.Container) error {
		if _, dup := containerNames[container.Name]; dup {
			return fmt.Errorf("duplicate container name %q", container.Name)
		}
		containerNames[container.Name] = struct{}{}
		envNames := map[string]struct{}{}
		for _, env := range container.Env {
			if _, dup := envNames[env.Name]; dup {
				return fmt.Errorf("container %q has duplicate env name %q", container.Name, env.Name)
			}
			envNames[env.Name] = struct{}{}
		}
		portNames := map[string]struct{}{}
		for _, port := range container.Ports {
			if port.Name == "" {
				continue
			}
			if _, dup := portNames[port.Name]; dup {
				return fmt.Errorf("container %q has duplicate port name %q", container.Name, port.Name)
			}
			portNames[port.Name] = struct{}{}
		}
		return nil
	}
	for _, container := range spec.InitContainers {
		if err := checkContainer(container); err != nil {
			return err
		}
	}
	for _, container := range spec.Containers {
		if err := checkContainer(container); err != nil {
			return err
		}
	}
	for _, ephemeral := range spec.EphemeralContainers {
		if _, dup := containerNames[ephemeral.Name]; dup {
			return fmt.Errorf("duplicate container name %q", ephemeral.Name)
		}
		containerNames[ephemeral.Name] = struct{}{}
	}
	volumeNames := map[string]struct{}{}
	for _, volume := range spec.Volumes {
		if _, dup := volumeNames[volume.Name]; dup {
			return fmt.Errorf("duplicate volume name %q", volume.Name)
		}
		volumeNames[volume.Name] = struct{}{}
	}
	return nil
}

// ImageIsDigestPinned reports whether an image reference pins an exact
// `…@sha256:<64 hex>` digest.
func ImageIsDigestPinned(image string) bool {
	index := strings.Index(image, "@sha256:")
	if index < 0 {
		return false
	}
	digest := image[index+len("@sha256:"):]
	if len(digest) != 64 {
		return false
	}
	for _, r := range digest {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// ValidateProtectedImagesDigestPinned requires every regular and init container
// image to be digest-pinned. It is enforced only when isolation is active
// (production); development/CI with mutable `:main`/`:dev` tags keeps working
// when isolation is off.
func ValidateProtectedImagesDigestPinned(spec *corev1.PodSpec) error {
	for _, container := range spec.InitContainers {
		if !ImageIsDigestPinned(container.Image) {
			return fmt.Errorf("init container %q image %q is not digest-pinned", container.Name, container.Image)
		}
	}
	for _, container := range spec.Containers {
		if !ImageIsDigestPinned(container.Image) {
			return fmt.Errorf("container %q image %q is not digest-pinned", container.Name, container.Image)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Comparison (design v7 §3)
// ---------------------------------------------------------------------------

// PodComparisonStage selects which normal form the pod side uses.
type PodComparisonStage int

const (
	StageCreate PodComparisonStage = iota
	StageLive
)

// ComparePodToStampedTemplate compares an admitted pod against its stamped
// parent template. It rejects duplicate identities, ephemeral containers, and
// (when enforceDigestPin) non-digest images on the raw pod, normalizes both
// sides to the canonical normal form for the given lifecycle stage, and then
// requires the COMPLETE remaining metadata+spec to be semantically equal. A
// mismatch returns the first differing field path.
func ComparePodToStampedTemplate(nc NormContext, podMeta metav1.ObjectMeta, podSpec corev1.PodSpec, templateMeta metav1.ObjectMeta, templateSpec corev1.PodSpec, stage PodComparisonStage, enforceDigestPin bool) error {
	if err := ValidateNoDuplicateIdentities(&podSpec); err != nil {
		return err
	}
	if len(podSpec.EphemeralContainers) != 0 {
		return fmt.Errorf("managed pod must not carry ephemeral containers")
	}
	if enforceDigestPin {
		if err := ValidateProtectedImagesDigestPinned(&podSpec); err != nil {
			return err
		}
	}

	mode := modeCreate
	if stage == StageLive {
		mode = modeLive
	}
	normalizedPodMeta := *podMeta.DeepCopy()
	normalizedPodSpec := *podSpec.DeepCopy()
	if err := normalizePodContract(nc, &normalizedPodMeta, &normalizedPodSpec, mode); err != nil {
		return err
	}
	normalizedTemplateMeta := *templateMeta.DeepCopy()
	normalizedTemplateSpec := *templateSpec.DeepCopy()
	if err := normalizePodContract(nc, &normalizedTemplateMeta, &normalizedTemplateSpec, modeTemplate); err != nil {
		return fmt.Errorf("normalize stamped template: %w", err)
	}

	if !equality.Semantic.DeepEqual(normalizedTemplateMeta, normalizedPodMeta) {
		return firstMismatchError("metadata", normalizedTemplateMeta, normalizedPodMeta)
	}
	if !equality.Semantic.DeepEqual(normalizedTemplateSpec, normalizedPodSpec) {
		return firstMismatchError("spec", normalizedTemplateSpec, normalizedPodSpec)
	}
	return nil
}

func firstMismatchError(root string, template, pod any) error {
	templateTree, err := jsonTree(template)
	if err != nil {
		return fmt.Errorf("managed pod %s does not match its stamped template", root)
	}
	podTree, err := jsonTree(pod)
	if err != nil {
		return fmt.Errorf("managed pod %s does not match its stamped template", root)
	}
	if path, differs := firstDifference(templateTree, podTree, root); differs {
		return fmt.Errorf("managed pod does not match its stamped template at %s", path)
	}
	return fmt.Errorf("managed pod %s does not match its stamped template", root)
}

func firstDifference(template, pod any, path string) (string, bool) {
	switch templateValue := template.(type) {
	case map[string]any:
		podValue, ok := pod.(map[string]any)
		if !ok {
			return path, true
		}
		keys := map[string]struct{}{}
		for key := range templateValue {
			keys[key] = struct{}{}
		}
		for key := range podValue {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		for _, key := range ordered {
			templateChild, templateHas := templateValue[key]
			podChild, podHas := podValue[key]
			if templateHas != podHas {
				return path + "." + key, true
			}
			if p, differs := firstDifference(templateChild, podChild, path+"."+key); differs {
				return p, true
			}
		}
		return "", false
	case []any:
		podValue, ok := pod.([]any)
		if !ok || len(templateValue) != len(podValue) {
			return path + " (length)", true
		}
		for index := range templateValue {
			if p, differs := firstDifference(templateValue[index], podValue[index], fmt.Sprintf("%s[%d]", path, index)); differs {
				return p, true
			}
		}
		return "", false
	default:
		if fmt.Sprint(template) != fmt.Sprint(pod) {
			return path, true
		}
		return "", false
	}
}

// jsonTree round-trips a value through the Kubernetes types' own JSON
// marshaling into a deterministic generic tree (exact integers via UseNumber),
// used as the canonical-CBOR input and for precise mismatch reporting.
func jsonTree(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}
	return tree, nil
}
