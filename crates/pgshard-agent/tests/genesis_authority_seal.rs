//! The genesis authority seal, exercised from a separate crate.
//!
//! This file links `pgshard-agent` the way any other crate would, so what it
//! can reach is exactly what an outside caller can reach. Everything that would
//! let such a caller obtain a `GenesisPermit` is a compile error rather than a
//! test, and those are pinned by the `compile_fail` doc tests on
//! `pgshard_agent::genesis_authority`; this file pins the other half, that the
//! one entry point which remains open mints nothing but an opaque observation
//! and still refuses an owner it should refuse.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::DynamicObject;
use kube::core::{ApiResource, GroupVersionKind};
use pgshard_agent::genesis_authority::{
    GENESIS_AUTHORITY_RECORD_VERSION, GENESIS_TAKEN_RECORD_VERSION, OWNING_CLUSTER_API_VERSION,
    OWNING_CLUSTER_KIND, observe_owning_cluster,
};

const CLUSTER_UID: &str = "11111111-2222-3333-4444-555555555555";

fn owned(references: Vec<OwnerReference>) -> DynamicObject {
    let mut object = DynamicObject::new(
        "demo-shard-0000-catalog-activation",
        &ApiResource::from_gvk(&GroupVersionKind::gvk(
            "pgshard.io",
            "v1alpha1",
            "PgshardCatalogActivation",
        )),
    );
    object.metadata.owner_references = Some(references);
    object
}

fn controlling_owner() -> OwnerReference {
    OwnerReference {
        api_version: OWNING_CLUSTER_API_VERSION.to_owned(),
        block_owner_deletion: Some(true),
        controller: Some(true),
        kind: OWNING_CLUSTER_KIND.to_owned(),
        name: "demo".to_owned(),
        uid: CLUSTER_UID.to_owned(),
    }
}

/// The module is genuinely public and its constants are readable, so a
/// `compile_fail` elsewhere cannot be passing because a path is misspelled.
#[test]
fn the_module_is_reachable_from_another_crate() {
    assert!(GENESIS_AUTHORITY_RECORD_VERSION.starts_with("pgshard."));
    assert!(GENESIS_TAKEN_RECORD_VERSION.starts_with("pgshard."));
    assert_eq!(OWNING_CLUSTER_KIND, "PgshardCluster");
}

/// The one mint still reachable from outside. It yields an opaque observation
/// with no readable UID and no way to reach the other three right-hand sides,
/// and it still applies its own rules.
#[test]
fn the_only_reachable_mint_still_refuses_an_owner_it_should_refuse() {
    let observed = observe_owning_cluster(&owned(vec![controlling_owner()]), "demo")
        .expect("a single controlling owner is observed");
    let repeated = observe_owning_cluster(&owned(vec![controlling_owner()]), "demo")
        .expect("observation is stable");
    assert_eq!(observed, repeated);

    // Nothing here exposes the UID, so the observation cannot be unpacked and
    // re-created from a value of the caller's choosing.
    assert!(format!("{observed:?}").contains(CLUSTER_UID));

    assert_eq!(
        observe_owning_cluster(&owned(vec![controlling_owner()]), "another-cluster"),
        None
    );
    assert_eq!(observe_owning_cluster(&owned(Vec::new()), "demo"), None);
    assert_eq!(
        observe_owning_cluster(
            &owned(vec![controlling_owner(), controlling_owner()]),
            "demo"
        ),
        None
    );
}
