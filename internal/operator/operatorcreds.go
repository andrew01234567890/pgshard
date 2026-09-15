package operator

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/grpccreds"
	"github.com/andrew01234567890/pgshard/internal/pki"
)

// IssuedCredentials builds what the operator dials a cluster's agents and
// controller with when the operator issues that cluster's certificates.
//
// They come from the cluster's own <cluster>-tls-operator Secret. The
// operator's process-wide --agent-tls-* and --controller-tls-* flags cannot
// serve: every issuing cluster has its own CA, so one certificate chains to
// at most one of them, and a deployment that set none dialled every agent
// of an issuing cluster in plaintext, which those agents refuse (PGS-860).
type IssuedCredentials struct {
	Client client.Reader

	mu    sync.Mutex
	cache map[types.NamespacedName]issuedCreds
}

type issuedCreds struct {
	version           string
	agent, controller credentials.TransportCredentials
}

// For returns the cluster's agent and controller credentials, or nil for
// both when the operator does not issue its certificates. The same values
// are returned until the Secret changes, so a connection dialled with them
// is kept rather than redialled every pass.
func (o *IssuedCredentials) For(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) (agent, controller credentials.TransportCredentials, err error) {
	if o == nil || !c.Spec.InternalTLS.Issue {
		return nil, nil, nil
	}
	key := types.NamespacedName{Namespace: c.Namespace, Name: RoleTLSSecretName(c.Name, pki.RoleOperator)}
	var sec corev1.Secret
	if err := o.Client.Get(ctx, key, &sec); err != nil {
		return nil, nil, fmt.Errorf("operator credentials for %s/%s: %w", c.Namespace, c.Name, err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if got, ok := o.cache[key]; ok && got.version == sec.ResourceVersion {
		return got.agent, got.controller, nil
	}
	// Agents are dialled at a pod IP or member host no certificate names, so
	// they are verified by the name every agent certificate carries -- and,
	// that name being shared by the cluster's poolers, by role as well. The
	// controller is dialled at its Service host, which its certificate names.
	agent, err = grpccreds.DialerPEM(sec.Data["tls.crt"], sec.Data["tls.key"], sec.Data["ca.crt"], IssuedMemberServerName(c),
		grpccreds.Authorize(pki.Serves(pki.RoleAgent)))
	if err != nil {
		return nil, nil, fmt.Errorf("operator credentials for %s/%s: %w", c.Namespace, c.Name, err)
	}
	// The controller's Service host, whatever endpoint a backup policy names
	// to reach it: its certificate carries that name.
	controller, err = grpccreds.DialerPEM(sec.Data["tls.crt"], sec.Data["tls.key"], sec.Data["ca.crt"], ControllerName(c.Name)+"."+c.Namespace+".svc",
		grpccreds.Authorize(pki.Serves(pki.RoleController)))
	if err != nil {
		return nil, nil, fmt.Errorf("operator credentials for %s/%s: %w", c.Namespace, c.Name, err)
	}
	if o.cache == nil {
		o.cache = map[types.NamespacedName]issuedCreds{}
	}
	o.cache[key] = issuedCreds{version: sec.ResourceVersion, agent: agent, controller: controller}
	return agent, controller, nil
}
