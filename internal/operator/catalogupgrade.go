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
		if err := r.Prober.MigrateCatalog(ctx, r.catalogTargetDSN(c, *target, password)); err != nil {
			up.Message = "migrate new catalog: " + err.Error()
			break
		}
		up.Stage = CatalogUpgradeCopying
		up.Message = ""
	case CatalogUpgradeCopying:
		if err := r.Prober.EnsureCatalogCopy(ctx, dsn, r.catalogTargetDSN(c, *target, password)); err != nil {
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
		if err := r.Prober.CutoverCatalog(ctx, dsn, r.catalogTargetDSN(c, *target, password)); err != nil {
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
		if up.RollbackRequested {
			log.Info("catalog upgrade rollback: repointing the catalog endpoint at the old group")
			c.Status.CatalogGeneration = up.RetiredGeneration
			c.Status.CatalogPGMajor = up.RetiredMajor
			if err := r.ensureCatalogEndpoint(ctx, c); err != nil {
				return obs, err
			}
			if err := r.Prober.ReleaseCatalog(ctx, r.catalogTargetDSN(c, Groups(c)[0], password)); err != nil {
				up.Message = "release old catalog: " + err.Error()
				break
			}
			if err := r.deleteCatalogGroup(ctx, c, catalogGroupAt(c, up.Generation, up.ToMajor)); err != nil {
				return obs, err
			}
			c.Status.CatalogUpgrade = nil
			break
		}
		var retireAfter time.Duration
		if d := c.Spec.Resharding.RetireOldGroupsAfter; d != nil {
			retireAfter = d.Duration
		}
		if up.SwitchedAt != nil && time.Since(up.SwitchedAt.Time) < retireAfter {
			up.Message = fmt.Sprintf("old catalog group retained for rollback until %s", up.SwitchedAt.Add(retireAfter).Format(time.RFC3339))
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
func (r *ClusterReconciler) catalogTargetDSN(c *pgshardv1alpha1.PgShardCluster, g Group, password string) string {
	return DSN(g.ServiceRW(), c.Namespace, password)
}

// ensureCatalogEndpoint keeps the stable catalog Service pointing at the
// active catalog group's primary. Generation 1 needs nothing: the stable
// name is that group's own -rw Service.
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
