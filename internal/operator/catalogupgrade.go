package operator

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

// Catalog upgrade stages recorded in status.catalogUpgrade.stage.
const (
	CatalogUpgradeProvisioning = "provisioning"
	CatalogUpgradeCopying      = "copying"
	CatalogUpgradeCatchingUp   = "catching_up"
	CatalogUpgradeCutover      = "cutover"
	CatalogUpgradeRetiring     = "retiring"
)

// CatalogUpgradeRequested reports whether the catalog group still runs a
// lower major than the spec asks for while every shard set already reached
// it: the catalog goes last in the upgrade's group iteration.
func CatalogUpgradeRequested(c *pgshardv1alpha1.PgShardCluster) bool {
	if c.Spec.Upgrade.Strategy == UpgradeStrategyOffline {
		return false
	}
	if c.Status.CatalogPGMajor == 0 || c.Status.CatalogPGMajor >= c.Spec.PostgreSQL.Major {
		return false
	}
	return c.Status.Reshard == nil && c.Status.ServingPGMajor == c.Spec.PostgreSQL.Major
}

// CatalogUpgradeBlockers lists why a requested catalog upgrade cannot
// start yet.
func CatalogUpgradeBlockers(c *pgshardv1alpha1.PgShardCluster) []string {
	var blockers []string
	if c.Status.Reshard != nil {
		blockers = append(blockers, "reshard "+c.Status.Reshard.Name+" is in flight")
	}
	if c.Status.ServingPGMajor != c.Spec.PostgreSQL.Major {
		blockers = append(blockers, fmt.Sprintf("serving shard set runs major %d; the shard upgrade to %d must complete first", c.Status.ServingPGMajor, c.Spec.PostgreSQL.Major))
	}
	for _, p := range c.Status.PlacementWorkflows {
		if p.State == "pending" || p.State == "running" || p.State == "paused" {
			blockers = append(blockers, fmt.Sprintf("table placement workflow %s is %s", p.WorkflowID, p.State))
		}
	}
	return blockers
}

// reconcileCatalogUpgrade drives the catalog group's blue/green major
// upgrade: provision a new-major catalog group, logically copy the pgshard
// catalog, fence, carry sequences, repoint the stable catalog Service and
// retire the old group. Routers keep dialing the stable Service name, so
// the flip re-points them without a redeploy.
func (r *ClusterReconciler) reconcileCatalogUpgrade(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, dsn, password string, pol *pgshardv1alpha1.PgShardBackupPolicy, repoReady bool) ([]groupObservation, error) {
	log := logf.FromContext(ctx)
	base := c.DeepCopy()
	patch := func() error {
		if equality.Semantic.DeepEqual(base.Status, c.Status) {
			return nil
		}
		return r.Status().Patch(ctx, c, client.MergeFrom(base))
	}
	if c.Status.CatalogPGMajor == 0 {
		major, err := r.Prober.ServerMajor(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("probe catalog major: %w", err)
		}
		c.Status.CatalogPGMajor = major
		if c.Status.CatalogGeneration == 0 {
			c.Status.CatalogGeneration = 1
		}
	}
	// Captured while the spec still describes this catalog group, so it
	// survives a spec that moves on to the next major.
	if c.Status.CatalogPGImage == "" || c.Status.CatalogPGMajor == c.Spec.PostgreSQL.Major {
		c.Status.CatalogPGImage = Image(c)
	}
	up := c.Status.CatalogUpgrade
	if up == nil {
		if !CatalogUpgradeRequested(c) {
			return nil, patch()
		}
		if blockers := CatalogUpgradeBlockers(c); len(blockers) > 0 {
			log.Info("catalog upgrade blocked", "blockers", blockers)
			return nil, patch()
		}
		c.Status.CatalogUpgrade = &pgshardv1alpha1.ClusterCatalogUpgradeStatus{
			FromMajor:  c.Status.CatalogPGMajor,
			ToMajor:    c.Spec.PostgreSQL.Major,
			Generation: CatalogGeneration(c) + 1,
			Stage:      CatalogUpgradeProvisioning,
		}
		log.Info("catalog upgrade started", "from", c.Status.CatalogPGMajor, "to", c.Spec.PostgreSQL.Major)
		return nil, patch()
	}
	up.RollbackRequested = c.Annotations[pgshardv1alpha1.AnnotationCatalogUpgrade] == pgshardv1alpha1.UpgradeActionRollback

	var obs []groupObservation
	target := CatalogTargetGroup(c)
	if target != nil {
		o, err := r.reconcileGroup(ctx, c, *target, password, pol, repoReady)
		if err != nil {
			return obs, fmt.Errorf("catalog target group %s: %w", target.Name(), err)
		}
		obs = append(obs, o)
	}
	retiredGroup := RetiredCatalogGroup(c)
	if retiredGroup != nil {
		o, err := r.reconcileGroup(ctx, c, *retiredGroup, password, pol, repoReady)
		if err != nil {
			return obs, fmt.Errorf("retired catalog group %s: %w", retiredGroup.Name(), err)
		}
		obs = append(obs, o)
	}

	if up.RollbackRequested && up.Stage != CatalogUpgradeRetiring {
		// Before the flip a rollback is a plain cancellation.
		log.Info("catalog upgrade rollback requested before the flip; cancelling")
		if target != nil {
			if err := r.deleteCatalogGroup(ctx, c, *target); err != nil {
				return obs, err
			}
		}
		c.Status.CatalogUpgrade = nil
		return nil, patch()
	}

	switch up.Stage {
	case CatalogUpgradeProvisioning:
		if target == nil || len(obs) == 0 || !obs[0].ready() {
			up.Message = "waiting for the new-major catalog group"
			break
		}
		targetDSN := r.catalogTargetDSN(c, *target, password)
		if err := r.Prober.MigrateCatalog(ctx, targetDSN); err != nil {
			up.Message = "migrate new catalog: " + err.Error()
			break
		}
		// Roles are not logically replicated, so the copy will not carry
		// the router's password across: the new catalog has the role from
		// the migration and no password for it, and would refuse every
		// router the moment the endpoint moved.
		routerPW, err := r.ensureRouterSecret(ctx, c)
		if err != nil {
			up.Message = "router credential: " + err.Error()
			break
		}
		if err := r.Prober.SetRouterPassword(ctx, targetDSN, routerPW); err != nil {
			up.Message = "router credential on the new catalog: " + err.Error()
			break
		}
		up.Stage = CatalogUpgradeCopying
		up.Message = ""
	case CatalogUpgradeCopying:
		if err := r.Prober.EnsureCatalogCopy(ctx, CatalogSide{DSN: dsn}, CatalogSide{DSN: r.catalogTargetDSN(c, *target, password)}); err != nil {
			up.Message = "catalog copy: " + err.Error()
			break
		}
		up.Stage = CatalogUpgradeCatchingUp
		up.Message = ""
	case CatalogUpgradeCatchingUp:
		ok, lag, err := r.Prober.CatalogCopyCaughtUp(ctx, dsn)
		if err != nil {
			up.Message = "catalog catch-up: " + err.Error()
			break
		}
		if !ok {
			up.Message = lag
			break
		}
		up.Stage = CatalogUpgradeCutover
		up.Message = ""
	case CatalogUpgradeCutover:
		// The stable endpoint is about to start selecting the new group,
		// and for generation 1 that is the same Service object the old
		// group is reached through. Give the old generation its own
		// address first, or the rollback would talk to the new catalog on
		// both connections.
		if err := r.ensureCatalogGenerationEndpoint(ctx, c, Groups(c)[0]); err != nil {
			return obs, err
		}
		if err := r.Prober.CutoverCatalog(ctx, CatalogSide{DSN: dsn}, CatalogSide{DSN: r.catalogTargetDSN(c, *target, password)}); err != nil {
			up.Message = "catalog cutover: " + err.Error()
			break
		}
		up.RetiredGeneration = CatalogGeneration(c)
		up.RetiredMajor = c.Status.CatalogPGMajor
		now := metav1.Now()
		up.SwitchedAt = &now
		c.Status.CatalogGeneration = up.Generation
		c.Status.CatalogPGMajor = up.ToMajor
		up.Stage = CatalogUpgradeRetiring
		up.Message = ""
		if err := r.ensureCatalogEndpoint(ctx, c); err != nil {
			return obs, err
		}
		log.Info("catalog upgrade switched", "generation", up.Generation, "major", up.ToMajor)
	case CatalogUpgradeRetiring:
		// Once the replay has run and status names the old catalog again,
		// the rollback is past the point where it can be called off: the
		// old group is the one holding the writes and the one the Service
		// is being moved to. Withdrawing the request then would release
		// both catalogs and, at retirement, delete the group that is
		// serving. So the request is what starts a rollback, not what keeps
		// it going.
		if up.RollbackRequested || rollbackCommitted(c) {
			// Replay first: the endpoint must not move back to the old
			// group until everything the new catalog accepted since the
			// cutover has been applied to it.
			// Address both groups explicitly: Groups(c)[0] follows the
			// status generation, which still names the new group until the
			// endpoint moves below.
			oldDSN := DSN(CatalogGenerationServiceRW(c.Name, up.RetiredGeneration), c.Namespace, password)
			newDSN := r.catalogTargetDSN(c, catalogGroupAt(c, up.Generation, up.ToMajor), password)
			// Record the intent before acting on it: a pass that dies part
			// way through has to be recognisable as a rollback in progress,
			// and the whole sequence below is idempotent when it is.
			if !up.RollbackStarted {
				up.RollbackStarted = true
				if err := patch(); err != nil {
					return obs, err
				}
				// A patch replaces the object with the server's answer, so
				// up now points at a detached copy; take it again or every
				// later write to it is lost.
				up = c.Status.CatalogUpgrade
			}
			if err := r.Prober.RollbackCatalog(ctx, oldDSN, newDSN); err != nil {
				up.Message = "catalog rollback: " + err.Error()
				break
			}
			// The replay left the old catalog serving and the new one
			// fenced, so status names the old one before anything moves or
			// is deleted. Patched here rather than at the end of the pass:
			// status that still named the new group after the endpoint had
			// moved sent schema reconciliation at a fenced catalog, and
			// once the delete below had run it made the operator rebuild an
			// empty group of that generation and point the stable Service
			// at it.
			c.Status.CatalogGeneration = up.RetiredGeneration
			c.Status.CatalogPGMajor = up.RetiredMajor
			if err := patch(); err != nil {
				return obs, err
			}
			up = c.Status.CatalogUpgrade
			log.Info("catalog upgrade rollback: repointing the catalog endpoint at the old group")
			if err := r.ensureCatalogEndpoint(ctx, c); err != nil {
				return obs, err
			}
			if err := r.Prober.ReleaseCatalog(ctx, oldDSN); err != nil {
				up.Message = "release old catalog: " + err.Error()
				break
			}
			if err := r.deleteCatalogGroup(ctx, c, catalogGroupAt(c, up.Generation, up.ToMajor)); err != nil {
				return obs, err
			}
			c.Status.CatalogUpgrade = nil
			break
		}
		if up.RollbackStarted {
			// The rollback was abandoned after it had already fenced the
			// catalog that is serving. Carrying on to retirement would
			// leave the cluster read-only for good, so put it back before
			// anything else.
			newDSN := r.catalogTargetDSN(c, catalogGroupAt(c, up.Generation, up.ToMajor), password)
			if err := r.Prober.ReleaseCatalog(ctx, newDSN); err != nil {
				up.Message = "release the catalog after an abandoned rollback: " + err.Error()
				break
			}
			if err := r.Prober.DisableCatalogRollback(ctx, DSN(CatalogGenerationServiceRW(c.Name, up.RetiredGeneration), c.Namespace, password)); err != nil {
				up.Message = "quiesce the rollback stream: " + err.Error()
				break
			}
			up.RollbackStarted = false
			log.Info("catalog upgrade: abandoned rollback undone, catalog serving again")
		}
		var retireAfter time.Duration
		if d := c.Spec.Resharding.RetireOldGroupsAfter; d != nil {
			retireAfter = d.Duration
		}
		if up.SwitchedAt != nil && time.Since(up.SwitchedAt.Time) < retireAfter {
			up.Message = fmt.Sprintf("old catalog group retained for rollback until %s", up.SwitchedAt.Add(retireAfter).Format(time.RFC3339))
			break
		}
		// The reverse slot lives on the catalog that is now serving and
		// pins its WAL; nothing will read it once the old group is gone.
		if err := r.Prober.DropCatalogRollback(ctx, r.catalogTargetDSN(c, catalogGroupAt(c, up.Generation, up.ToMajor), password)); err != nil {
			up.Message = "drop catalog rollback stream: " + err.Error()
			break
		}
		if retiredGroup != nil {
			if err := r.deleteCatalogGroup(ctx, c, *retiredGroup); err != nil {
				return obs, err
			}
		}
		log.Info("catalog upgrade complete", "major", c.Status.CatalogPGMajor)
		c.Status.CatalogUpgrade = nil
	}
	return obs, patch()
}

// catalogTargetDSN is the DSN of one catalog group's own -rw Service (not
// the stable endpoint, which follows the flip).
// rollbackCommitted reports a catalog rollback that has passed the point
// where it can be called off: the replay has run and status names the old
// catalog, so that group holds the writes and the stable Service is on its
// way to it. Withdrawing the request after that would take the abandoned
// path, which releases both catalogs and, at retirement, deletes the group
// that is serving.
func rollbackCommitted(c *pgshardv1alpha1.PgShardCluster) bool {
	up := c.Status.CatalogUpgrade
	return up != nil && up.RollbackStarted && up.RetiredGeneration != 0 &&
		c.Status.CatalogGeneration == up.RetiredGeneration
}

func (r *ClusterReconciler) catalogTargetDSN(c *pgshardv1alpha1.PgShardCluster, g Group, password string) string {
	return DSN(g.ServiceRW(), c.Namespace, password)
}

// ensureCatalogEndpoint keeps the stable catalog Service pointing at the
// active catalog group's primary. Generation 1 needs nothing: the stable
// name is that group's own -rw Service.
// ensureCatalogGenerationEndpoint gives one catalog generation an address
// that does not move when the stable endpoint is repointed.
func (r *ClusterReconciler) ensureCatalogGenerationEndpoint(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) error {
	desired := r.Renderer.CatalogGenerationService(c, g)
	svc := &corev1.Service{ObjectMeta: desired.ObjectMeta}
	return r.ensureOwned(ctx, c, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return nil
	})
}

func (r *ClusterReconciler) ensureCatalogEndpoint(ctx context.Context, c *pgshardv1alpha1.PgShardCluster) error {
	desired := r.Renderer.CatalogEndpointService(c, Groups(c)[0])
	svc := &corev1.Service{ObjectMeta: desired.ObjectMeta}
	return r.ensureOwned(ctx, c, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return nil
	})
}

// deleteCatalogGroup removes every object of one catalog group except the
// stable catalog endpoint Service.
func (r *ClusterReconciler) deleteCatalogGroup(ctx context.Context, c *pgshardv1alpha1.PgShardCluster, g Group) error {
	sel := client.MatchingLabels{LabelCluster: c.Name, LabelGroup: g.Name()}
	ns := client.InNamespace(c.Namespace)
	stable := CatalogServiceRW(c.Name)
	var services corev1.ServiceList
	if err := r.List(ctx, &services, ns, sel); err != nil {
		return err
	}
	for i := range services.Items {
		if services.Items[i].Name == stable {
			continue
		}
		if err := r.Delete(ctx, &services.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for _, obj := range []client.Object{&corev1.Pod{}, &corev1.PersistentVolumeClaim{}, &corev1.ConfigMap{},
		&policyv1.PodDisruptionBudget{}, &pgshardv1alpha1.PgShardGroup{}} {
		if err := r.DeleteAllOf(ctx, obj, ns, sel); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete catalog group %s: %w", g.Name(), err)
		}
	}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: g.LeaseName(), Namespace: c.Namespace}}
	if err := r.Delete(ctx, lease); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
