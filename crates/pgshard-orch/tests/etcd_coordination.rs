//! Live etcd coordination recovery and exclusivity coverage.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::header::CONTENT_TYPE;
use hyper::{Request, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use pgshard_orch::coordination::{CoordinationConfig, CoordinationError, supervise};
use pgshard_orch::domain::{OrchReadinessReason, OrchState, OrchestratorIdentity};
use serde_json::{Value, json};
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
    let mutation_endpoint = endpoints[0].clone();
    let identity = OrchestratorIdentity {
        cluster_id: format!("coord-test-{}", Uuid::new_v4()),
        orchestrator_id: "orch-fixed".to_owned(),
    };
    let cluster_uid = Uuid::new_v4().to_string();
    let config = CoordinationConfig::new(
        endpoints.clone(),
        identity.clone(),
        cluster_uid.clone(),
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

    let contender_state =
        OrchState::with_identity(identity.clone(), 15_000).expect("contender state");
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

    exercise_session_and_marker_rebinding(
        &mutation_endpoint,
        &identity,
        &cluster_uid,
        (first_state, first_shutdown, first_task),
        (contender_state, contender_shutdown, contender_task),
    )
    .await;
}

type Supervisor = (
    OrchState,
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), CoordinationError>>,
);

async fn exercise_session_and_marker_rebinding(
    endpoint: &Url,
    identity: &OrchestratorIdentity,
    cluster_uid: &str,
    first: Supervisor,
    contender: Supervisor,
) {
    let (first_state, first_shutdown, first_task) = first;
    let (contender_state, contender_shutdown, contender_task) = contender;
    let session_key = format!(
        "/pgshard/v1/clusters/{}/incarnations/{cluster_uid}/orchestrators/{}",
        identity.cluster_id, identity.orchestrator_id
    );
    let original_token = etcd_get(endpoint, session_key.as_bytes()).await;
    let replacement_lease = etcd_grant(endpoint, 15).await;
    etcd_put_with_lease(
        endpoint,
        session_key.as_bytes(),
        &original_token,
        replacement_lease,
    )
    .await;
    wait_for_readiness(&first_state, false).await;
    etcd_put(endpoint, session_key.as_bytes(), b"foreign-session-token").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !first_state.readiness().ready,
        "the displaced owner regained readiness with rebound session evidence"
    );

    etcd_delete(endpoint, session_key.as_bytes()).await;
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

    let marker_key = format!("/pgshard/v1/clusters/{}/identity", identity.cluster_id);
    etcd_put(endpoint, marker_key.as_bytes(), b"foreign-cluster-marker").await;
    let contender_result = tokio::time::timeout(Duration::from_secs(15), contender_task)
        .await
        .expect("contender marker-conflict timeout")
        .expect("contender supervisor task");
    assert_eq!(
        contender_result,
        Err(CoordinationError::ClusterMarkerConflict)
    );
    assert!(!contender_state.readiness().ready);
    drop(contender_shutdown);
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

async fn etcd_delete(endpoint: &Url, key: &[u8]) {
    let _ = etcd_post(
        endpoint,
        "v3/kv/deleterange",
        json!({"key": BASE64.encode(key)}),
    )
    .await;
}

async fn etcd_get(endpoint: &Url, key: &[u8]) -> Vec<u8> {
    let response = etcd_post(endpoint, "v3/kv/range", json!({"key": BASE64.encode(key)})).await;
    let kvs = response
        .get("kvs")
        .and_then(Value::as_array)
        .expect("test etcd range values");
    assert_eq!(kvs.len(), 1, "test etcd range = {response}");
    BASE64
        .decode(
            kvs[0]
                .get("value")
                .and_then(Value::as_str)
                .expect("test etcd range value"),
        )
        .expect("decode test etcd range value")
}

async fn etcd_grant(endpoint: &Url, ttl: i64) -> i64 {
    let response = etcd_post(endpoint, "v3/lease/grant", json!({"TTL": ttl.to_string()})).await;
    response
        .get("ID")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .expect("test etcd lease ID")
}

async fn etcd_put(endpoint: &Url, key: &[u8], value: &[u8]) {
    let _ = etcd_post(
        endpoint,
        "v3/kv/put",
        json!({"key": BASE64.encode(key), "value": BASE64.encode(value)}),
    )
    .await;
}

async fn etcd_put_with_lease(endpoint: &Url, key: &[u8], value: &[u8], lease: i64) {
    let _ = etcd_post(
        endpoint,
        "v3/kv/put",
        json!({
            "key": BASE64.encode(key),
            "value": BASE64.encode(value),
            "lease": lease.to_string()
        }),
    )
    .await;
}

async fn etcd_post(endpoint: &Url, path: &str, body: Value) -> Value {
    let mut connector = HttpConnector::new();
    connector.enforce_http(true);
    connector.set_connect_timeout(Some(Duration::from_secs(2)));
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(connector);
    let uri: Uri = endpoint
        .join(path)
        .expect("test etcd path")
        .as_str()
        .parse()
        .expect("test etcd URI");
    let request = Request::post(uri)
        .header(CONTENT_TYPE, "application/json")
        .header("grpc-metadata-hasleader", "true")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&body).expect("encode test etcd request"),
        )))
        .expect("test etcd request");
    let response = tokio::time::timeout(Duration::from_secs(5), client.request(request))
        .await
        .expect("test etcd request timeout")
        .expect("test etcd request transport");
    assert!(
        response.status().is_success(),
        "test etcd response {response:?}"
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("read test etcd response")
        .to_bytes();
    serde_json::from_slice(&body).expect("decode test etcd response")
}
