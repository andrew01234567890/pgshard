package controller

import (
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	identityProbeNamePrefix = "pgshard-idprobe-"
	identityProbeLabel      = "pgshard.io/identity-probe-token"
	identityProbeImage      = "registry.k8s.io/pause:3.9"
)

// identityProbeObjects builds the disposable workloads whose admission observes
// the four controller identities. They carry the probe token so the webhooks
// record request.UserInfo and admit them, and a restricted PodSecurityContext so
// they are inert. They are never scheduled to run for the observation to occur —
// the record happens at admission (CREATE / scale), not at pod start. Order:
// StatefulSet and Deployment first, then the HPA that forces the Deployment up.
func identityProbeObjects(namespace, token string) []client.Object {
	stsName := identityProbeNamePrefix + "sts-" + token
	deployName := identityProbeNamePrefix + "deploy-" + token
	hpaName := identityProbeNamePrefix + "hpa-" + token

	stsSelector := map[string]string{identityProbeLabel: "sts-" + token}
	deploySelector := map[string]string{identityProbeLabel: "deploy-" + token}

	return []client.Object{
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: namespace, Annotations: probeAnnotations(token)},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    ptr.To(int32(1)),
				ServiceName: stsName,
				Selector:    &metav1.LabelSelector{MatchLabels: stsSelector},
				Template:    identityProbePodTemplate(token, stsSelector),
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: namespace, Annotations: probeAnnotations(token)},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: deploySelector},
				Template: identityProbePodTemplate(token, deploySelector),
			},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: hpaName, Namespace: namespace, Annotations: probeAnnotations(token)},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: deployName},
				MinReplicas:    ptr.To(int32(2)),
				MaxReplicas:    2,
			},
		},
	}
}

func probeAnnotations(token string) map[string]string {
	return map[string]string{podfence.IdentityProbeAnnotation: token}
}

func identityProbePodTemplate(token string, selector map[string]string) corev1.PodTemplateSpec {
	labels := map[string]string{}
	for k, v := range selector {
		labels[k] = v
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: probeAnnotations(token)},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   ptr.To(true),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:  "probe",
				Image: identityProbeImage,
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					RunAsNonRoot:             ptr.To(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}
}
