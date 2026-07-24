package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/operator/internal/podfence"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// controllerIdentitySet is the four authenticated controller identities the
// activation preflight must confirm: the StatefulSet, ReplicaSet, and Deployment
// controllers plus the HorizontalPodAutoscaler controller.
type controllerIdentitySet struct {
	statefulSet string
	replicaSet  string
	deployment  string
	hpa         string
}

func (s controllerIdentitySet) count() int {
	n := 0
	for _, v := range []string{s.statefulSet, s.replicaSet, s.deployment, s.hpa} {
		if v != "" {
			n++
		}
	}
	return n
}

func configuredControllerIdentities(identities podfence.ControllerIdentities) controllerIdentitySet {
	return controllerIdentitySet{
		statefulSet: identities.StatefulSetController,
		replicaSet:  identities.ReplicaSetController,
		deployment:  identities.DeploymentController,
		hpa:         identities.HorizontalPodAutoscalerController,
	}
}

// identitiesMatch reports whether every observed controller identity equals the
// configured allowlist entry and is a well-formed node or service-account
// principal. A blank observed identity means the controller was never seen and
// the probe fails closed.
func identitiesMatch(observed, configured controllerIdentitySet) (bool, string) {
	type check struct {
		name               string
		observed, expected string
	}
	var mismatches []string
	for _, c := range []check{
		{"statefulset-controller", observed.statefulSet, configured.statefulSet},
		{"replicaset-controller", observed.replicaSet, configured.replicaSet},
		{"deployment-controller", observed.deployment, configured.deployment},
		{"horizontalpodautoscaler-controller", observed.hpa, configured.hpa},
	} {
		if c.expected == "" || !isControllerPrincipal(c.expected) {
			mismatches = append(mismatches, fmt.Sprintf("%s configured identity %q is not a valid controller principal", c.name, c.expected))
			continue
		}
		if c.observed == "" {
			mismatches = append(mismatches, fmt.Sprintf("%s was never observed acting", c.name))
			continue
		}
		if c.observed != c.expected {
			mismatches = append(mismatches, fmt.Sprintf("%s observed %q, configured %q", c.name, c.observed, c.expected))
		}
	}
	if len(mismatches) == 0 {
		return true, ""
	}
	sort.Strings(mismatches)
	return false, strings.Join(mismatches, "; ")
}

// isControllerPrincipal reports whether an authenticated username is a node or
// service-account principal (never a plain user), the only shapes a trusted
// controller identity may take.
func isControllerPrincipal(username string) bool {
	return strings.HasPrefix(username, "system:serviceaccount:") || strings.HasPrefix(username, "system:node:")
}

// serverControllerIdentityProber confirms the configured controller-identity
// allowlist against reality by creating disposable probe workloads in the
// activating cluster's fenced namespace and reading back the ACTUAL authenticated
// controller usernames the admission webhooks observed for them:
//   - a probe StatefulSet (statefulset controller creates its pod),
//   - a probe Deployment (deployment controller creates its ReplicaSet, whose
//     replicaset controller creates the probe pod),
//   - a probe HorizontalPodAutoscaler forcing the probe Deployment above its
//     replica count (hpa controller issues the /scale).
//
// The webhooks record request.UserInfo (which the API server signs) into a
// process-shared IdentityObservationStore keyed by the probe token. The probe
// then compares the four observed usernames to the configured allowlist. It fails
// CLOSED: a mismatch, an unobserved controller, or a deadline all withhold
// activation. Probe workloads are benign (a paused container under a restricted
// PodSecurityContext) and are always cleaned up.
//
// INTEGRATION-ONLY SEAM: the create→observe→compare loop requires live
// StatefulSet/ReplicaSet/Deployment/HPA controllers, so it is exercised by
// integration tests, not the fake-client unit tests. identitiesMatch, the
// observation store, and the webhook recording are unit-tested directly.
type serverControllerIdentityProber struct {
	client         client.Client
	identities     podfence.ControllerIdentities
	store          *podfence.IdentityObservationStore
	timeout        time.Duration
	cleanupTimeout time.Duration
}

func NewServerControllerIdentityProber(c client.Client, identities podfence.ControllerIdentities, store *podfence.IdentityObservationStore) *serverControllerIdentityProber {
	return &serverControllerIdentityProber{client: c, identities: identities, store: store, timeout: 30 * time.Second, cleanupTimeout: 30 * time.Second}
}

func (p *serverControllerIdentityProber) Probe(ctx context.Context, cluster *pgshardv1alpha1.PgShardCluster) (bool, string, error) {
	configured := configuredControllerIdentities(p.identities)
	// The configured allowlist must be well-formed controller principals before we
	// spend a live probe.
	for name, expected := range map[string]string{
		"statefulset-controller":             configured.statefulSet,
		"replicaset-controller":              configured.replicaSet,
		"deployment-controller":              configured.deployment,
		"horizontalpodautoscaler-controller": configured.hpa,
	} {
		if expected == "" || !isControllerPrincipal(expected) {
			return false, fmt.Sprintf("%s configured identity %q is not a valid controller principal", name, expected), nil
		}
	}
	if p.store == nil {
		return false, "controller-identity observation store is not wired", nil
	}

	token, err := newProbeToken()
	if err != nil {
		return false, "", fmt.Errorf("generate identity-probe token: %w", err)
	}
	defer p.store.Forget(token)
	objects := identityProbeObjects(cluster.Namespace, token)
	// Registration precedes creation: the owner keys are the prober-chosen names,
	// so the webhooks accept observations only for the authoritative probe
	// objects — never for a forged owner reference — with no create/registration
	// race window.
	ownerKeys := []string{}
	for _, object := range objects {
		switch object.(type) {
		case *appsv1.StatefulSet:
			ownerKeys = append(ownerKeys, podfence.IdentityOwnerKey("StatefulSet", object.GetName()))
		case *appsv1.Deployment:
			ownerKeys = append(ownerKeys, podfence.IdentityOwnerKey("Deployment", object.GetName()))
		}
	}
	p.store.Register(token, ownerKeys...)
	created := []client.Object{}
	for _, object := range objects {
		if err := p.client.Create(ctx, object); err != nil {
			cleanupErr := p.cleanupProbe(ctx, cluster.Namespace, token, created)
			return false, "", errors.Join(fmt.Errorf("create identity-probe object %T: %w", object, err), cleanupErr)
		}
		created = append(created, object)
	}

	observed, conflicted, complete := p.awaitObservations(ctx, token)
	if cleanupErr := p.cleanupProbe(ctx, cluster.Namespace, token, created); cleanupErr != nil {
		// Cleanup completion is VERIFIED (foreground delete + poll to absence): a
		// probe workload left behind is a foreign object in the fenced namespace and
		// must fail the probe, not be ignored.
		return false, "", cleanupErr
	}
	if conflicted {
		return false, "controller-identity probe observed conflicting verified identities for a role; refusing to activate", nil
	}
	if !complete {
		return false, fmt.Sprintf("controller-identity probe did not observe every controller within %s (observed %d of 4)", p.timeout, observed.count()), nil
	}
	if matched, detail := identitiesMatch(observed, configured); !matched {
		return false, detail, nil
	}
	return true, "", nil
}

// awaitObservations polls the store until all four controller roles are recorded
// for the token, a conflict is detected, or the probe deadline elapses.
func (p *serverControllerIdentityProber) awaitObservations(ctx context.Context, token string) (controllerIdentitySet, bool, bool) {
	deadline := time.Now().Add(p.timeout)
	for {
		roles, conflicted := p.store.Observed(token)
		observed := controllerIdentitySet{
			statefulSet: roles[podfence.IdentityRoleStatefulSet],
			replicaSet:  roles[podfence.IdentityRoleReplicaSet],
			deployment:  roles[podfence.IdentityRoleDeployment],
			hpa:         roles[podfence.IdentityRoleHPA],
		}
		if conflicted {
			return observed, true, false
		}
		if observed.statefulSet != "" && observed.replicaSet != "" && observed.deployment != "" && observed.hpa != "" {
			return observed, false, true
		}
		if time.Now().After(deadline) {
			return observed, false, false
		}
		select {
		case <-ctx.Done():
			return observed, false, false
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// cleanupProbe FOREGROUND-deletes every probe object and then POLLS the
// authoritative API until every probe object AND its dependent ReplicaSets/Pods
// are absent. Foreground deletion removes the dependents before the owner, and
// the absence poll is verified against the live API — so a stale probe object
// can never linger into the namespace inventory or the next probe. Delete and
// confirmation errors are returned, never ignored. Cleanup runs on a cancellation
// -detached context so a cancelled reconcile still tears the probe down, bounded
// by cleanupTimeout.
func (p *serverControllerIdentityProber) cleanupProbe(ctx context.Context, namespace, token string, objects []client.Object) error {
	ctx = context.WithoutCancel(ctx)
	foreground := metav1.DeletePropagationForeground
	var errs []error
	for _, object := range objects {
		if err := p.client.Delete(ctx, object, &client.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete identity-probe object %T %s: %w", object, object.GetName(), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	deadline := time.Now().Add(p.cleanupTimeout)
	for {
		absent, err := p.probeArtifactsAbsent(ctx, namespace, token, objects)
		if err != nil {
			return err
		}
		if absent {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("identity-probe artifacts for token %s were not confirmed absent within %s", token, p.cleanupTimeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// probeArtifactsAbsent reports whether every named probe object and every
// dependent ReplicaSet/Pod carrying the probe token label is gone from the live
// API.
func (p *serverControllerIdentityProber) probeArtifactsAbsent(ctx context.Context, namespace, token string, objects []client.Object) (bool, error) {
	for _, object := range objects {
		probe := object.DeepCopyObject().(client.Object)
		err := p.client.Get(ctx, client.ObjectKeyFromObject(object), probe)
		if err == nil {
			return false, nil
		}
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("confirm identity-probe object %T %s absence: %w", object, object.GetName(), err)
		}
	}
	pods := &corev1.PodList{}
	if err := p.client.List(ctx, pods, client.InNamespace(namespace), client.HasLabels{identityProbeLabel}); err != nil {
		return false, fmt.Errorf("list identity-probe pods for cleanup confirmation: %w", err)
	}
	for i := range pods.Items {
		if strings.HasSuffix(pods.Items[i].Labels[identityProbeLabel], token) {
			return false, nil
		}
	}
	replicaSets := &appsv1.ReplicaSetList{}
	if err := p.client.List(ctx, replicaSets, client.InNamespace(namespace), client.HasLabels{identityProbeLabel}); err != nil {
		return false, fmt.Errorf("list identity-probe ReplicaSets for cleanup confirmation: %w", err)
	}
	for i := range replicaSets.Items {
		if strings.HasSuffix(replicaSets.Items[i].Labels[identityProbeLabel], token) {
			return false, nil
		}
	}
	return true, nil
}

func newProbeToken() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
