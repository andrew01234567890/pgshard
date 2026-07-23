package resources

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
)

// ComputeSecurityContractDigest hashes ONLY the security-relevant surface of a
// pod template: container images, command/args, security contexts and
// capabilities, volume mounts and devices, the pod security context, the service
// account and automount setting, the mounted volume set (Secret/ConfigMap
// projections included — a rotated Secret NAME is security-relevant), the
// host-namespace flags, and whether ephemeral/debug containers are present.
//
// It DELIBERATELY EXCLUDES benign fields — container env (e.g. an OpenTelemetry
// endpoint), resource requests/limits, labels and annotations — so a benign
// observability or resource change produces the SAME security digest and does not
// force a security-generation bump / revocation. Any change to the included
// surface changes the digest; the classifier is conservative (when a field could
// tighten the security contract it is included, so an ambiguous change is treated
// as strengthening).
func ComputeSecurityContractDigest(template *corev1.PodTemplateSpec) string {
	if template == nil {
		return ""
	}
	spec := template.Spec
	projection := securityRelevantTemplate{
		ServiceAccountName:           spec.ServiceAccountName,
		AutomountServiceAccountToken: spec.AutomountServiceAccountToken,
		HostNetwork:                  spec.HostNetwork,
		HostPID:                      spec.HostPID,
		HostIPC:                      spec.HostIPC,
		HostUsers:                    spec.HostUsers,
		PodSecurityContext:           spec.SecurityContext,
		Volumes:                      spec.Volumes,
		InitContainers:               securityRelevantContainers(spec.InitContainers),
		Containers:                   securityRelevantContainers(spec.Containers),
		HasEphemeralContainers:       len(spec.EphemeralContainers) > 0,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		// json.Marshal of these API structs does not error in practice; fall back to
		// a non-empty sentinel so a marshalling anomaly is treated as a change.
		return "unmarshalable-security-surface"
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

type securityRelevantContainer struct {
	Name            string
	Image           string
	Command         []string
	Args            []string
	SecurityContext *corev1.SecurityContext
	VolumeMounts    []corev1.VolumeMount
	VolumeDevices   []corev1.VolumeDevice
}

type securityRelevantTemplate struct {
	ServiceAccountName           string
	AutomountServiceAccountToken *bool
	HostNetwork                  bool
	HostPID                      bool
	HostIPC                      bool
	HostUsers                    *bool
	PodSecurityContext           *corev1.PodSecurityContext
	Volumes                      []corev1.Volume
	InitContainers               []securityRelevantContainer
	Containers                   []securityRelevantContainer
	HasEphemeralContainers       bool
}

func securityRelevantContainers(containers []corev1.Container) []securityRelevantContainer {
	out := make([]securityRelevantContainer, 0, len(containers))
	for i := range containers {
		container := &containers[i]
		out = append(out, securityRelevantContainer{
			Name:            container.Name,
			Image:           container.Image,
			Command:         container.Command,
			Args:            container.Args,
			SecurityContext: container.SecurityContext,
			VolumeMounts:    container.VolumeMounts,
			VolumeDevices:   container.VolumeDevices,
		})
	}
	return out
}
