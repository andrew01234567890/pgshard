package operator

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// CASecretName holds the cluster's own certificate authority. Its key
// never leaves the operator: workloads are given the certificates it signs
// and the ca.crt to verify them, never the means to sign more.
func CASecretName(cluster string) string { return cluster + "-internal-ca" }

// RoleTLSSecretName holds one workload role's certificate.
func RoleTLSSecretName(cluster, role string) string { return cluster + "-tls-" + role }

// issuedRoles are the workloads the operator mints certificates for, with
// what each one does. The agent and the pooler are both server and client:
// each answers RPCs and makes them. The router serves its peers and the
// VStream API as well as dialling poolers.
var issuedRoles = map[string]pki.Request{
	pki.RoleAgent:  {Server: true, Client: true},
	pki.RolePooler: {Server: true, Client: true},
	pki.RoleRouter: {Server: true, Client: true},
	// Serves as well as dials: the router and the operator both call it.
	pki.RoleController: {Server: true, Client: true},
	pki.RoleOperator:   {Client: true},
	pki.RoleAdmin:      {Client: true},
}

// issuedRoleNames is the roles in a stable order, so anything digesting
// them all gets the same answer every pass.
func issuedRoleNames() []string {
	out := make([]string, 0, len(issuedRoles))
	for role := range issuedRoles {
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

// reconcilePKI keeps the cluster's CA and one certificate per workload
// role current. It does nothing unless the spec asks the operator to issue
// them; a cluster given a secretRef keeps using it untouched.
func (r *ClusterReconciler) reconcilePKI(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	if !c.Spec.InternalTLS.Issue {
		return nil
	}
	ca, err := r.ensureCA(ctx, c)
	if err != nil {
		return err
	}
	for _, role := range issuedRoleNames() {
		if err := r.ensureRoleCert(ctx, c, ca, role, issuedRoles[role]); err != nil {
			return fmt.Errorf("certificate for %s: %w", role, err)
		}
	}
	return nil
}

// ensureCA reads the cluster's authority, minting one the first time and
// replacing it when it is close enough to expiry that certificates signed
// now would outlive it.
func (r *ClusterReconciler) ensureCA(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (*pki.CA, error) {
	key := client.ObjectKey{Namespace: c.Namespace, Name: CASecretName(c.Name)}
	var sec corev1.Secret
	err := r.Get(ctx, key, &sec)
	switch {
	case err == nil:
		ca, lerr := pki.LoadCA(pki.Material{CertPEM: sec.Data["ca.crt"], KeyPEM: sec.Data["ca.key"]})
		if lerr == nil && !pki.NeedsRenewal(sec.Data["ca.crt"], r.now()) {
			return ca, nil
		}
		// An unreadable authority is replaced rather than refused: it can
		// only have been corrupted or hand-edited, and a cluster with no
		// usable CA cannot issue anything at all.
	case !apierrors.IsNotFound(err):
		return nil, err
	}
	ca, err := pki.NewCA(c.Name, r.now())
	if err != nil {
		return nil, err
	}
	sec = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
			Labels: map[string]string{LabelCluster: c.Name}},
		Data: map[string][]byte{"ca.crt": ca.CertPEM, "ca.key": ca.KeyPEM},
	}
	if err := r.ensureOwned(ctx, c, &sec, func() error {
		sec.Data = map[string][]byte{"ca.crt": ca.CertPEM, "ca.key": ca.KeyPEM}
		return nil
	}); err != nil {
		return nil, err
	}
	return ca, nil
}

// ensureRoleCert keeps one role's certificate current: issued the first
// time, reissued when it nears expiry or when the CA behind it changed.
func (r *ClusterReconciler) ensureRoleCert(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, ca *pki.CA, role string, req pki.Request) error {
	req.Identity = pki.Identity{Namespace: c.Namespace, Cluster: c.Name, Role: role}
	req.DNSNames = roleDNSNames(c, role)
	key := client.ObjectKey{Namespace: c.Namespace, Name: RoleTLSSecretName(c.Name, role)}
	var sec corev1.Secret
	err := r.Get(ctx, key, &sec)
	if err == nil && currentFor(sec, ca, r.now()) {
		return nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	m, err := ca.Issue(req, r.now())
	if err != nil {
		return err
	}
	desired := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
			Labels: map[string]string{LabelCluster: c.Name, LabelRole: role}},
		Type: corev1.SecretTypeTLS,
	}
	return r.ensureOwned(ctx, c, &desired, func() error {
		desired.Type = corev1.SecretTypeTLS
		desired.Data = map[string][]byte{"tls.crt": m.CertPEM, "tls.key": m.KeyPEM, "ca.crt": ca.CertPEM}
		return nil
	})
}

// currentFor reports whether a stored certificate still does its job: it
// has to be signed by the CA in hand, and far enough from expiry. The CA
// check is what makes a CA rotation propagate -- without it a certificate
// signed by the previous authority would sit there until its own expiry,
// trusted by nobody.
func currentFor(sec corev1.Secret, ca *pki.CA, now time.Time) bool {
	if len(sec.Data["tls.crt"]) == 0 || len(sec.Data["tls.key"]) == 0 {
		return false
	}
	if string(sec.Data["ca.crt"]) != string(ca.CertPEM) {
		return false
	}
	return !pki.NeedsRenewal(sec.Data["tls.crt"], now)
}

// roleDNSNames are the names a role's listener is reached by, and none for
// a role that only dials. A client certificate valid to serve some name is
// a certificate that can impersonate that name.
func roleDNSNames(c *pgshardv1alpha1.PgShardCluster, role string) []string {
	svc := func(name string) []string {
		return []string{name, name + "." + c.Namespace, name + "." + c.Namespace + ".svc",
			"*." + name + "." + c.Namespace + ".svc"}
	}
	switch role {
	case pki.RoleRouter:
		return svc(RouterName(c.Name))
	case pki.RoleController:
		return svc(ControllerName(c.Name))
	case pki.RolePooler, pki.RoleAgent:
		// Reached pod by pod rather than through a Service: a caller that
		// dials a member by name must find that name on the certificate.
		return append(svc(c.Name), "*."+c.Namespace+".svc", "*."+c.Namespace+".pod")
	default:
		return nil
	}
}
