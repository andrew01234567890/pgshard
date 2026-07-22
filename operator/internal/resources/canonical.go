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
	// security generation the template was stamped at. Its integrity is bound
	// through the hash's domain key, so it is stripped from the normal form.
	PodSecurityGenerationAnnotation = "pgshard.io/security-generation"

	contractHashDomain = "pgshard.pod-contract.v1"

	serviceAccountTokenMountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
	normalizedTokenVolumeName    = "pgshard-normalized-sa-token"
)

// controllerAddedPodLabels are the labels the StatefulSet and ReplicaSet
// controllers stamp onto a pod that are absent from its template. They are
// stripped from every normal form so a template and its pods compare equal.
var controllerAddedPodLabels = []string{
	"controller-revision-hash",
	"statefulset.kubernetes.io/pod-name",
	"apps.kubernetes.io/pod-index",
	"pod-template-hash",
}

// nodeTopologyLabels are copied onto a bound pod by the PodTopologyLabels
// admission plugin. LiveNormalForm permits and strips exactly these two (they
// are validated against the bound Node by the webhook layer in a later step);
// any other binding-copied label is unexpected residue and fails comparison.
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

// NormContext carries the identity a normal form is derived against. Class,
// Shard, and Member are always required; ClusterName and Namespace are used
// only when normalizing a real pod (CREATE/LIVE) to derive and validate its
// ordinal hostname/subdomain — the template-mode normalization used by the
// stamp only strips those fields.
type NormContext struct {
	Class       PodClass
	ClusterName string
	Namespace   string
	Shard       int32
	Member      int32
}

type normMode int

const (
	modeTemplate normMode = iota // stamp time: strip identity/residue, no pod-side validation
	modeCreate                   // pod CREATE: nodeName must be empty; no binding residue
	modeLive                     // running pod: permit+strip validated binding residue
)

// ComputeContractStamp returns the domain-separated, length-framed full-contract
// hash of a pod template:
//
//	contractHash = HMAC_SHA256(
//	    key = lengthFramed("pgshard.pod-contract.v1" ‖ class ‖ clusterUID ‖ shard ‖ member ‖ securityGeneration),
//	    msg = canonicalCBOR(templateNormalForm))
//
// The template normal form strips identity and controller/server residue and
// canonicalizes the projected-token and priority tuples, so a stamped template
// and any pod the controllers create from it reduce to the same normal form and
// therefore the same hash.
func ComputeContractStamp(class PodClass, clusterUID string, shard, member int32, securityGeneration int64, template *corev1.PodTemplateSpec) (string, error) {
	if template == nil {
		return "", fmt.Errorf("pod template is required to compute a contract stamp")
	}
	tree, err := templateNormalTree(NormContext{Class: class, Shard: shard, Member: member}, template)
	if err != nil {
		return "", err
	}
	message, err := canonicalEncMode.Marshal(tree)
	if err != nil {
		return "", fmt.Errorf("canonical-encode template normal form: %w", err)
	}
	mac := hmac.New(sha256.New, contractDomainKey(class, clusterUID, shard, member, securityGeneration))
	if _, err := mac.Write(message); err != nil {
		return "", fmt.Errorf("hash template normal form: %w", err)
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

func templateNormalTree(nc NormContext, template *corev1.PodTemplateSpec) (any, error) {
	normalized := template.DeepCopy()
	if err := normalizePodContract(nc, &normalized.ObjectMeta, &normalized.Spec, modeTemplate); err != nil {
		return nil, err
	}
	return jsonTree(normalized)
}

// contractDomainKey builds an unambiguous, length-framed HMAC key from the
// domain-separation tuple. Every component is prefixed with its 8-byte
// big-endian length so no concatenation collision is possible.
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

// ---------------------------------------------------------------------------
// Full-spec normalization (design v7 §A2/§3)
// ---------------------------------------------------------------------------

// CreateNormalForm normalizes a pod as it appears at CREATE (before scheduling
// binds it). It returns the normalized metadata + spec; the entire remaining
// structure is compared by the caller — there is no capability projection.
func CreateNormalForm(nc NormContext, meta metav1.ObjectMeta, spec corev1.PodSpec) (metav1.ObjectMeta, corev1.PodSpec, error) {
	m := *meta.DeepCopy()
	s := *spec.DeepCopy()
	err := normalizePodContract(nc, &m, &s, modeCreate)
	return m, s, err
}

// LiveNormalForm normalizes a running (bound) pod: it additionally permits,
// validates the shape of, and strips the scheduler/binding residue — nodeName,
// the node-UID/boot-ID annotations, and the zone/region topology labels — so a
// bound pod reduces to the same normal form as its template.
func LiveNormalForm(nc NormContext, meta metav1.ObjectMeta, spec corev1.PodSpec) (metav1.ObjectMeta, corev1.PodSpec, error) {
	m := *meta.DeepCopy()
	s := *spec.DeepCopy()
	err := normalizePodContract(nc, &m, &s, modeLive)
	return m, s, err
}

func normalizePodContract(nc NormContext, meta *metav1.ObjectMeta, spec *corev1.PodSpec, mode normMode) error {
	normalizeContractMetadata(meta, mode)
	return normalizeContractSpec(nc, spec, mode)
}

func normalizeContractMetadata(meta *metav1.ObjectMeta, mode normMode) {
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

	for _, key := range controllerAddedPodLabels {
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
}

func normalizeContractSpec(nc NormContext, spec *corev1.PodSpec, mode normMode) error {
	// The API server mirrors serviceAccountName into the deprecated field.
	spec.DeprecatedServiceAccount = ""

	if err := normalizeOrdinalIdentity(nc, spec, mode); err != nil {
		return err
	}
	if err := normalizeNodeName(spec, mode); err != nil {
		return err
	}
	normalizePriorityTuple(spec)
	if err := normalizeInjectedServiceAccountToken(nc.Class, spec, mode); err != nil {
		return err
	}
	return nil
}

func isMemberClass(class PodClass) bool {
	return class == ClassSource || class == ClassStandby || class == ClassSingleMember
}

// normalizeOrdinalIdentity derives (member) and validates (CREATE/LIVE) the
// StatefulSet-controller-assigned hostname/subdomain, then strips them so a
// template (which lacks them) and a pod (which carries the derived values)
// reduce identically. Supporting classes must not carry them.
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

func normalizeNodeName(spec *corev1.PodSpec, mode normMode) error {
	if (mode == modeTemplate || mode == modeCreate) && spec.NodeName != "" {
		return fmt.Errorf("managed pod must be created unassigned; spec.nodeName %q is preset", spec.NodeName)
	}
	spec.NodeName = ""
	return nil
}

// normalizePriorityTuple resolves the {priorityClassName, priority,
// preemptionPolicy} tuple for a pgshard-owned PriorityClass. The template
// carries only the class name; the admitted pod carries the plugin-resolved
// integer/policy. Filling nil→pinned makes the template match a correctly
// resolved pod while leaving a pod that carries a wrong non-nil value to fail
// comparison. A non-pgshard class name is left unresolved and simply mismatches
// the template's class name.
func normalizePriorityTuple(spec *corev1.PodSpec) {
	value, ok := priorityValueForClassName(spec.PriorityClassName)
	if !ok {
		return
	}
	if spec.Priority == nil {
		resolved := value
		spec.Priority = &resolved
	}
	if spec.PreemptionPolicy == nil {
		policy := pgShardPreemptionPolicy
		spec.PreemptionPolicy = &policy
	}
}

func classExpectsInjectedToken(class PodClass) bool {
	// Only the orchestrator runs with automountServiceAccountToken=true, so it
	// alone receives the ServiceAccount admission plugin's projected token.
	return class == ClassOrchestrator
}

// normalizeInjectedServiceAccountToken canonicalizes the ServiceAccount
// plugin's randomly-named projected token as a relational tuple: for the
// automounting class the template gets the exact expected tuple (with a fixed
// placeholder name), and a pod's injected `kube-api-access-*` volume is
// validated against that exact shape and renamed to the placeholder. The volume
// name is the only free variable; any extra source, volume, or mount, or a
// missing/mismatched tuple, fails comparison. Non-automounting classes expect
// no such tuple (a stray one then fails comparison against the template).
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
// when isolation is off. Ephemeral containers are always forbidden by the
// comparator, so they are not inspected here.
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
// mismatch returns the first differing field path. `stage` selects
// CreateNormalForm (StageCreate) or LiveNormalForm (StageLive) for the pod
// side; the template is always normalized in template mode.
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
