//! Live etcd coordination recovery and exclusivity coverage.

use std::time::Duration;

use pgshard_orch::coordination::{CoordinationConfig, CoordinationError, supervise};
use pgshard_orch::domain::{OrchReadinessReason, OrchState, OrchestratorIdentity};
use tokio::sync::watch;
use url::Url;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires PGSHARD_TEST_ETCD_ENDPOINTS pointing at disposable etcd 3.6"]
async fn incarnation_is_exclusive_and_recovers_through_persistent_marker() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("pgshard_orch=debug")),
        )
        .with_test_writer()
        .try_init();
    let endpoints = std::env::var("PGSHARD_TEST_ETCD_ENDPOINTS")
        .expect("PGSHARD_TEST_ETCD_ENDPOINTS")
        .split(',')
        .map(|value| Url::parse(value).expect("valid test endpoint"))
        .collect::<Vec<_>>();
    let identity = OrchestratorIdentity {
        cluster_id: format!("coord-test-{}", Uuid::new_v4()),
        orchestrator_id: "orch-fixed".to_owned(),
    };
    let cluster_uid = Uuid::new_v4().to_string();
    let config = CoordinationConfig::new(
        endpoints.clone(),
        identity.clone(),
        cluster_uid,
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .expect("valid test coordination");
    let replacement_config = CoordinationConfig::new(
        endpoints,
        identity.clone(),
        Uuid::new_v4().to_string(),
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .expect("valid replacement coordination");

    let first_state = OrchState::with_identity(identity.clone(), 15_000).expect("first state");
    let (first_shutdown, first_shutdown_rx) = watch::channel(false);
    let first_task = tokio::spawn(supervise(
        config.clone(),
        first_state.clone(),
        first_shutdown_rx,
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !first_task.is_finished(),
        "first supervisor exited early: {:?}",
        first_task.await
    );
    wait_for_readiness(&first_state, true).await;
    let first_snapshot = first_state.snapshot();
    assert!(first_snapshot.coordination_ready);
    assert!(first_snapshot.coordination_cluster_id.is_some());
    assert_ne!(first_snapshot.coordination_revision, "0");
    assert!(!first_snapshot.persistence_enabled);

    assert_replacement_incarnation_is_rejected(replacement_config, identity.clone()).await;

    let contender_state = OrchState::with_identity(identity, 15_000).expect("contender state");
    let (contender_shutdown, contender_shutdown_rx) = watch::channel(false);
    let contender_task = tokio::spawn(supervise(
        config,
        contender_state.clone(),
        contender_shutdown_rx,
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !contender_task.is_finished(),
        "contender supervisor exited early: {:?}",
        contender_task.await
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        contender_state.readiness().reason,
        OrchReadinessReason::CoordinationUnavailable
    );

    first_shutdown.send(true).expect("stop first supervisor");
    tokio::time::timeout(Duration::from_secs(5), first_task)
        .await
        .expect("first supervisor shutdown timeout")
        .expect("first supervisor task")
        .expect("first supervisor result");
    assert!(!first_state.readiness().ready);

    wait_for_readiness(&contender_state, true).await;
    assert_eq!(
        contender_state.readiness().reason,
        OrchReadinessReason::Ready
    );
    contender_shutdown
        .send(true)
        .expect("stop contender supervisor");
    tokio::time::timeout(Duration::from_secs(5), contender_task)
        .await
        .expect("contender supervisor shutdown timeout")
        .expect("contender supervisor task")
        .expect("contender supervisor result");
    assert!(!contender_state.readiness().ready);
}

async fn assert_replacement_incarnation_is_rejected(
    config: CoordinationConfig,
    identity: OrchestratorIdentity,
) {
    let state = OrchState::with_identity(identity, 15_000).expect("replacement state");
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        supervise(config, state.clone(), shutdown_rx),
    )
    .await
    .expect("replacement supervisor timeout");
    assert_eq!(result, Err(CoordinationError::ClusterMarkerConflict));
    assert!(!state.readiness().ready);
}

async fn wait_for_readiness(state: &OrchState, wanted: bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if state.readiness().ready == wanted {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("readiness did not become {wanted}: {:?}", state.snapshot()));
}
