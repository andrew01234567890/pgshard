//! Renewable etcd-backed orchestrator incarnation and readiness supervision.

use std::future::Future;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use pgshard_version::{GIT_SHA, VERSION};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::watch;
use url::Url;
use uuid::Uuid;

use crate::domain::{OrchState, OrchestratorIdentity};

const RESPONSE_LIMIT_BYTES: usize = 16 * 1024;
const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(5);
const REVOKE_TIMEOUT: Duration = Duration::from_secs(1);
const CONTENT_TYPE_JSON: HeaderValue = HeaderValue::from_static("application/json");
const REQUIRE_LEADER: HeaderValue = HeaderValue::from_static("true");
const REQUIRE_LEADER_HEADER: &str = "grpc-metadata-hasleader";

/// Fully validated settings for one orchestrator coordination supervisor.
#[derive(Clone, Debug)]
pub struct CoordinationConfig {
    endpoints: Vec<Url>,
    identity: OrchestratorIdentity,
    cluster_uid: String,
    session_ttl: Duration,
    request_timeout: Duration,
}

impl CoordinationConfig {
    /// Creates settings after checking the standalone caller supplied the same
    /// bounds enforced by [`crate::config::OrchConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error for an empty endpoint set, an unrepresentable TTL, or
    /// timing that could spend more than one third of a lease cycling endpoints.
    pub fn new(
        endpoints: Vec<Url>,
        identity: OrchestratorIdentity,
        cluster_uid: String,
        session_ttl: Duration,
        request_timeout: Duration,
    ) -> Result<Self, CoordinationError> {
        let ttl_seconds = session_ttl.as_secs();
        let request_millis = u64::try_from(request_timeout.as_millis()).unwrap_or(u64::MAX);
        let endpoint_count = u64::try_from(endpoints.len()).unwrap_or(u64::MAX);
        let endpoints_are_safe = (1..=9).contains(&endpoints.len())
            && endpoints.iter().enumerate().all(|(index, endpoint)| {
                endpoint.scheme() == "http"
                    && endpoint.host_str().is_some()
                    && endpoint.port().is_some()
                    && endpoint.username().is_empty()
                    && endpoint.password().is_none()
                    && endpoint.path() == "/"
                    && endpoint.query().is_none()
                    && endpoint.fragment().is_none()
                    && !endpoints[..index].contains(endpoint)
            });
        let identity_is_safe = [
            &identity.cluster_id,
            &identity.orchestrator_id,
            &cluster_uid,
        ]
        .into_iter()
        .all(|value| {
            !value.is_empty()
                && value.len() <= 63
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        });
        if !endpoints_are_safe
            || !identity_is_safe
            || session_ttl.subsec_nanos() != 0
            || !request_timeout.subsec_nanos().is_multiple_of(1_000_000)
            || !(6..=300).contains(&ttl_seconds)
            || !(100..=5_000).contains(&request_millis)
            || request_millis.saturating_mul(endpoint_count) > ttl_seconds.saturating_mul(1_000) / 3
        {
            return Err(CoordinationError::InvalidSettings);
        }
        Ok(Self {
            endpoints,
            identity,
            cluster_uid,
            session_ttl,
            request_timeout,
        })
    }
}

/// Maintains an exclusive, lease-backed key for one orchestrator incarnation.
///
/// The persistent cluster marker prevents an endpoint set from silently
/// changing logical pgshard clusters. The ephemeral incarnation key is attached
/// to an etcd lease and refreshed through the v3 HTTP gateway. Readiness is
/// removed immediately after any failed refresh; it never depends on the HTTP
/// probe changing an in-memory boolean itself.
///
/// # Errors
///
/// Returns only for a permanent protocol/evidence violation or after shutdown.
pub async fn supervise(
    config: CoordinationConfig,
    state: OrchState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CoordinationError> {
    state.record_coordination_unavailable();
    let token = session_value(&config.identity, &config.cluster_uid, Uuid::new_v4());
    let mut gateway = EtcdGateway::new(&config.endpoints, config.request_timeout);
    let mut retry = INITIAL_RETRY;

    while !stopping(&shutdown) {
        let mut pending_lease = None;
        let Some(result) = run_or_stop(
            &mut shutdown,
            establish_session(&mut gateway, &config, &token, &mut pending_lease),
        )
        .await
        else {
            state.record_coordination_unavailable();
            retire_lease(&mut gateway, pending_lease).await;
            return Ok(());
        };
        match result {
            Ok(session) => {
                if stopping(&shutdown) {
                    state.record_coordination_unavailable();
                    retire_lease(&mut gateway, Some(session.lease_id)).await;
                    return Ok(());
                }
                if !state.record_coordination_ready(
                    session.cluster_id,
                    session.revision,
                    session.deadline,
                ) {
                    state.record_coordination_unavailable();
                    retire_lease(&mut gateway, Some(session.lease_id)).await;
                    return Err(CoordinationError::StateEvidenceRejected);
                }
                tracing::info!(
                    etcd_cluster_id = session.cluster_id,
                    etcd_revision = session.revision,
                    "orchestrator coordination incarnation established"
                );
                retry = INITIAL_RETRY;
                if maintain_session(
                    &mut gateway,
                    &config,
                    &token,
                    &state,
                    &mut shutdown,
                    session.lease_id,
                )
                .await?
                    == SessionOutcome::Shutdown
                {
                    return Ok(());
                }
            }
            Err(error) if !error.is_permanent() => {
                state.record_coordination_unavailable();
                retire_lease(&mut gateway, pending_lease.take()).await;
                tracing::warn!(reason = %error, "orchestrator coordination unavailable");
            }
            Err(error) => {
                state.record_coordination_unavailable();
                retire_lease(&mut gateway, pending_lease.take()).await;
                return Err(error);
            }
        }

        if wait_or_stop(&mut shutdown, retry).await {
            state.record_coordination_unavailable();
            return Ok(());
        }
        retry = retry.saturating_mul(2).min(MAX_RETRY);
    }

    state.record_coordination_unavailable();
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionOutcome {
    Retry,
    Shutdown,
}

async fn maintain_session(
    gateway: &mut EtcdGateway,
    config: &CoordinationConfig,
    token: &[u8],
    state: &OrchState,
    shutdown: &mut watch::Receiver<bool>,
    lease_id: i64,
) -> Result<SessionOutcome, CoordinationError> {
    loop {
        if wait_or_stop(shutdown, config.session_ttl / 3).await {
            state.record_coordination_unavailable();
            retire_lease(gateway, Some(lease_id)).await;
            return Ok(SessionOutcome::Shutdown);
        }
        let renewal = async {
            let renewal = gateway.keep_alive(lease_id, config.session_ttl).await?;
            let observation = gateway
                .verify_session(&config.identity, &config.cluster_uid, token, lease_id)
                .await?;
            Ok::<_, CoordinationError>((renewal, observation))
        };
        let Some(result) = run_or_stop(shutdown, renewal).await else {
            state.record_coordination_unavailable();
            retire_lease(gateway, Some(lease_id)).await;
            return Ok(SessionOutcome::Shutdown);
        };
        match result {
            Ok((renewal, observation)) => {
                if !state.record_coordination_ready(
                    observation.cluster_id,
                    observation.revision,
                    renewal.deadline,
                ) {
                    state.record_coordination_unavailable();
                    retire_lease(gateway, Some(lease_id)).await;
                    return Err(CoordinationError::StateEvidenceRejected);
                }
            }
            Err(error) if !error.is_permanent() => {
                state.record_coordination_unavailable();
                tracing::warn!(reason = %error, "orchestrator coordination lost");
                retire_lease(gateway, Some(lease_id)).await;
                return Ok(SessionOutcome::Retry);
            }
            Err(error) => {
                state.record_coordination_unavailable();
                retire_lease(gateway, Some(lease_id)).await;
                return Err(error);
            }
        }
    }
}

async fn establish_session(
    gateway: &mut EtcdGateway,
    config: &CoordinationConfig,
    token: &[u8],
    pending_lease: &mut Option<i64>,
) -> Result<Session, CoordinationError> {
    gateway
        .ensure_cluster_marker(&config.identity.cluster_id, &config.cluster_uid)
        .await?;
    let lease_id = gateway.grant(config.session_ttl).await?;
    *pending_lease = Some(lease_id);
    let key = session_key(&config.identity, &config.cluster_uid);
    match gateway
        .put_if_absent_or_get(&key, token, Some(lease_id))
        .await?
    {
        (true, _, _) => {}
        (false, Some(existing), _) if existing == token => {
            if gateway
                .replace_exact(&key, token, lease_id)
                .await?
                .is_none()
            {
                return Err(CoordinationError::SessionOwned);
            }
        }
        (false, _, _) => return Err(CoordinationError::SessionOwned),
    }
    let renewal = gateway.keep_alive(lease_id, config.session_ttl).await?;
    let observation = gateway
        .verify_session(&config.identity, &config.cluster_uid, token, lease_id)
        .await?;
    Ok(Session {
        lease_id,
        cluster_id: observation.cluster_id,
        revision: observation.revision,
        deadline: renewal.deadline,
    })
}

#[derive(Clone, Copy, Debug)]
struct Observation {
    cluster_id: u64,
    revision: u64,
}

#[derive(Clone, Copy, Debug)]
struct Renewal {
    deadline: Instant,
}

#[derive(Clone, Copy, Debug)]
struct Session {
    lease_id: i64,
    cluster_id: u64,
    revision: u64,
    deadline: Instant,
}

type HttpClient = Client<HttpConnector, Full<Bytes>>;

struct EtcdGateway {
    client: HttpClient,
    endpoints: Vec<Url>,
    request_timeout: Duration,
    next_endpoint: usize,
    cluster_id: Option<u64>,
    highest_revision: u64,
}

impl EtcdGateway {
    fn new(endpoints: &[Url], request_timeout: Duration) -> Self {
        let mut connector = HttpConnector::new();
        connector.enforce_http(true);
        connector.set_connect_timeout(Some(request_timeout));
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build(connector);
        Self {
            client,
            endpoints: endpoints.to_vec(),
            request_timeout,
            next_endpoint: 0,
            cluster_id: None,
            highest_revision: 0,
        }
    }

    async fn ensure_cluster_marker(
        &mut self,
        logical_cluster: &str,
        cluster_incarnation: &str,
    ) -> Result<Observation, CoordinationError> {
        let key = cluster_marker_key(logical_cluster);
        let value = cluster_marker_value(logical_cluster, cluster_incarnation);
        let (created, existing, observation) =
            self.put_if_absent_or_get(&key, &value, None).await?;
        if !created && existing.as_deref() != Some(value.as_slice()) {
            return Err(CoordinationError::ClusterMarkerConflict);
        }
        Ok(observation)
    }

    async fn grant(&mut self, ttl: Duration) -> Result<i64, CoordinationError> {
        let ttl = i64::try_from(ttl.as_secs()).map_err(|_| CoordinationError::InvalidSettings)?;
        let value = self
            .post("v3/lease/grant", &json!({"TTL": ttl.to_string()}))
            .await?;
        self.observe_cluster_header(&value)?;
        let lease_id = parse_i64_string(&value, "ID")?;
        let granted_ttl = parse_i64_string(&value, "TTL")?;
        if lease_id <= 0 || granted_ttl < ttl {
            return Err(CoordinationError::InvalidResponse("unsafe lease grant"));
        }
        Ok(lease_id)
    }

    async fn keep_alive(
        &mut self,
        lease_id: i64,
        minimum_ttl: Duration,
    ) -> Result<Renewal, CoordinationError> {
        let requested_at = Instant::now();
        let value = self
            .post("v3/lease/keepalive", &json!({"ID": lease_id.to_string()}))
            .await?;
        let result = value.get("result").ok_or(CoordinationError::LeaseExpired)?;
        let returned_id =
            parse_i64_string(result, "ID").map_err(|_| CoordinationError::LeaseExpired)?;
        let ttl = parse_i64_string(result, "TTL").map_err(|_| CoordinationError::LeaseExpired)?;
        if returned_id != lease_id || ttl < i64::try_from(minimum_ttl.as_secs()).unwrap_or(i64::MAX)
        {
            return Err(CoordinationError::LeaseExpired);
        }
        let deadline = requested_at
            .checked_add(Duration::from_secs(u64::try_from(ttl).unwrap_or(u64::MAX)))
            .ok_or(CoordinationError::LeaseExpired)?;
        if Instant::now() >= deadline {
            return Err(CoordinationError::LeaseExpired);
        }
        self.observe_cluster_header(result)?;
        Ok(Renewal { deadline })
    }

    async fn revoke(&mut self, lease_id: i64) -> Result<(), CoordinationError> {
        let value = self
            .post("v3/lease/revoke", &json!({"ID": lease_id.to_string()}))
            .await?;
        self.observe_cluster_header(&value)?;
        Ok(())
    }

    async fn verify_session(
        &mut self,
        identity: &OrchestratorIdentity,
        cluster_uid: &str,
        token: &[u8],
        lease_id: i64,
    ) -> Result<Observation, CoordinationError> {
        let marker_key = BASE64.encode(cluster_marker_key(&identity.cluster_id));
        let marker_value = BASE64.encode(cluster_marker_value(&identity.cluster_id, cluster_uid));
        let session_key = BASE64.encode(session_key(identity, cluster_uid));
        let session_value = BASE64.encode(token);
        let response = self
            .post(
                "v3/kv/txn",
                &json!({
                    "compare": [
                        {
                            "result": "EQUAL",
                            "target": "VALUE",
                            "key": marker_key,
                            "value": marker_value
                        },
                        {
                            "result": "EQUAL",
                            "target": "VALUE",
                            "key": session_key,
                            "value": session_value
                        },
                        {
                            "result": "EQUAL",
                            "target": "LEASE",
                            "key": session_key,
                            "lease": lease_id.to_string()
                        }
                    ],
                    // A non-serializable range makes this read-only transaction
                    // pass through etcd's linearizable read barrier.
                    "success": [{"request_range": {"key": session_key}}],
                    "failure": [
                        {"request_range": {"key": marker_key}},
                        {"request_range": {"key": session_key}}
                    ]
                }),
            )
            .await?;
        let observation = self.observe_header(&response)?;
        let succeeded = response
            .get("succeeded")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !succeeded {
            validate_range_responses(&response, 2)?;
            return Err(CoordinationError::SessionEvidenceLost);
        }
        validate_session_range(&response, &session_key, &session_value, lease_id)?;
        Ok(observation)
    }

    async fn put_if_absent_or_get(
        &mut self,
        key: &[u8],
        value: &[u8],
        lease_id: Option<i64>,
    ) -> Result<(bool, Option<Vec<u8>>, Observation), CoordinationError> {
        let key = BASE64.encode(key);
        let value = BASE64.encode(value);
        let mut request_put = json!({"key": key, "value": value});
        if let Some(lease_id) = lease_id {
            request_put["lease"] = Value::String(lease_id.to_string());
        }
        let response = self
            .post(
                "v3/kv/txn",
                &json!({
                    "compare": [{
                        "result": "EQUAL",
                        "target": "CREATE",
                        "key": key,
                        "create_revision": "0"
                    }],
                    "success": [{"request_put": request_put}],
                    "failure": [{"request_range": {"key": key}}]
                }),
            )
            .await?;
        let observation = self.observe_header(&response)?;
        // ProtoJSON omits the default `false` value, so absence is the
        // compare-failed branch rather than malformed evidence.
        let succeeded = response
            .get("succeeded")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if succeeded {
            validate_single_response(&response, "response_put")?;
            return Ok((true, None, observation));
        }
        let existing = transaction_range_value(&response)?;
        Ok((false, Some(existing), observation))
    }

    async fn replace_exact(
        &mut self,
        key: &[u8],
        value: &[u8],
        lease_id: i64,
    ) -> Result<Option<Observation>, CoordinationError> {
        let key = BASE64.encode(key);
        let value = BASE64.encode(value);
        let response = self
            .post(
                "v3/kv/txn",
                &json!({
                    "compare": [{
                        "result": "EQUAL",
                        "target": "VALUE",
                        "key": key,
                        "value": value
                    }],
                    "success": [{"request_put": {
                        "key": key,
                        "value": value,
                        "lease": lease_id.to_string()
                    }}],
                    "failure": [{"request_range": {"key": key}}]
                }),
            )
            .await?;
        let observation = self.observe_header(&response)?;
        let succeeded = response
            .get("succeeded")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if succeeded {
            validate_single_response(&response, "response_put")?;
            Ok(Some(observation))
        } else {
            let _ = transaction_optional_range_value(&response)?;
            Ok(None)
        }
    }

    async fn post(&mut self, path: &str, value: &Value) -> Result<Value, CoordinationError> {
        let body = serde_json::to_vec(value)
            .map_err(|_| CoordinationError::InvalidResponse("request encoding failed"))?;
        for offset in 0..self.endpoints.len() {
            let index = (self.next_endpoint + offset) % self.endpoints.len();
            match self.post_one(index, path, body.clone()).await {
                Ok(value) => {
                    self.next_endpoint = (index + 1) % self.endpoints.len();
                    return Ok(value);
                }
                Err(error) => tracing::debug!(
                    endpoint = %self.endpoints[index],
                    reason = %error,
                    "etcd gateway endpoint attempt failed"
                ),
            }
        }
        Err(CoordinationError::GatewayUnavailable)
    }

    async fn post_one(
        &self,
        index: usize,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Value, EndpointError> {
        let endpoint = self.endpoints[index]
            .join(path)
            .map_err(|_| EndpointError::InvalidUri)?;
        let uri: Uri = endpoint
            .as_str()
            .parse()
            .map_err(|_| EndpointError::InvalidUri)?;
        let request = Request::post(uri)
            .header(CONTENT_TYPE, CONTENT_TYPE_JSON)
            // grpc-gateway only forwards non-IANA request metadata through
            // this prefix. etcd's server interceptor consumes `hasleader`.
            .header(REQUIRE_LEADER_HEADER, REQUIRE_LEADER)
            .body(Full::new(Bytes::from(body)))
            .map_err(|_| EndpointError::InvalidRequest)?;
        let (status, content_type_is_json, bytes) =
            tokio::time::timeout(self.request_timeout, async {
                let response = self
                    .client
                    .request(request)
                    .await
                    .map_err(|_| EndpointError::Transport)?;
                let status = response.status();
                let content_type_is_json = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.starts_with("application/json"));
                let bytes = Limited::new(response.into_body(), RESPONSE_LIMIT_BYTES)
                    .collect()
                    .await
                    .map_err(|_| EndpointError::Body)?
                    .to_bytes();
                Ok::<_, EndpointError>((status, content_type_is_json, bytes))
            })
            .await
            .map_err(|_| EndpointError::Timeout)??;
        if !status.is_success() {
            return Err(EndpointError::Status(status));
        }
        if !content_type_is_json {
            return Err(EndpointError::ContentType);
        }
        serde_json::from_slice(&bytes).map_err(|_| EndpointError::Json)
    }

    fn observe_header(&mut self, value: &Value) -> Result<Observation, CoordinationError> {
        let header = value
            .get("header")
            .ok_or(CoordinationError::InvalidResponse(
                "missing response header",
            ))?;
        let cluster_id = parse_u64_string(header, "cluster_id")?;
        let revision = parse_u64_string(header, "revision")?;
        if cluster_id == 0 || revision == 0 {
            return Err(CoordinationError::InvalidResponse(
                "zero cluster identity or revision",
            ));
        }
        if self.cluster_id.is_some_and(|current| current != cluster_id) {
            return Err(CoordinationError::ClusterIdentityChanged);
        }
        if revision < self.highest_revision {
            return Err(CoordinationError::RevisionRegressed);
        }
        self.cluster_id = Some(cluster_id);
        self.highest_revision = revision;
        Ok(Observation {
            cluster_id,
            revision,
        })
    }

    fn observe_cluster_header(&mut self, value: &Value) -> Result<u64, CoordinationError> {
        let header = value
            .get("header")
            .ok_or(CoordinationError::InvalidResponse(
                "missing response header",
            ))?;
        let cluster_id = parse_u64_string(header, "cluster_id")?;
        if cluster_id == 0 {
            return Err(CoordinationError::InvalidResponse("zero cluster identity"));
        }
        if self.cluster_id.is_some_and(|current| current != cluster_id) {
            return Err(CoordinationError::ClusterIdentityChanged);
        }
        self.cluster_id = Some(cluster_id);
        Ok(cluster_id)
    }
}

fn parse_u64_string(value: &Value, field: &'static str) -> Result<u64, CoordinationError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .ok_or(CoordinationError::InvalidResponse(field))
}

fn parse_i64_string(value: &Value, field: &'static str) -> Result<i64, CoordinationError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .ok_or(CoordinationError::InvalidResponse(field))
}

fn validate_single_response(
    response: &Value,
    expected: &'static str,
) -> Result<(), CoordinationError> {
    let responses = response.get("responses").and_then(Value::as_array).ok_or(
        CoordinationError::InvalidResponse("missing transaction responses"),
    )?;
    if responses.len() != 1 || responses[0].get(expected).is_none() {
        return Err(CoordinationError::InvalidResponse(
            "unexpected transaction response",
        ));
    }
    Ok(())
}

fn validate_range_responses(response: &Value, expected: usize) -> Result<(), CoordinationError> {
    let responses = response.get("responses").and_then(Value::as_array).ok_or(
        CoordinationError::InvalidResponse("missing transaction responses"),
    )?;
    if responses.len() != expected
        || responses
            .iter()
            .any(|response| response.get("response_range").is_none())
    {
        return Err(CoordinationError::InvalidResponse(
            "unexpected transaction range responses",
        ));
    }
    Ok(())
}

fn validate_session_range(
    response: &Value,
    expected_key: &str,
    expected_value: &str,
    expected_lease: i64,
) -> Result<(), CoordinationError> {
    validate_single_response(response, "response_range")?;
    let kvs = response["responses"][0]["response_range"]
        .get("kvs")
        .and_then(Value::as_array)
        .ok_or(CoordinationError::InvalidResponse(
            "missing verified session range",
        ))?;
    if kvs.len() != 1 {
        return Err(CoordinationError::InvalidResponse(
            "verified session range was not singular",
        ));
    }
    let kv = &kvs[0];
    if kv.get("key").and_then(Value::as_str) != Some(expected_key)
        || kv.get("value").and_then(Value::as_str) != Some(expected_value)
        || parse_i64_string(kv, "lease")? != expected_lease
    {
        return Err(CoordinationError::InvalidResponse(
            "verified session evidence changed",
        ));
    }
    Ok(())
}

fn transaction_range_value(response: &Value) -> Result<Vec<u8>, CoordinationError> {
    transaction_optional_range_value(response)?.ok_or(CoordinationError::InvalidResponse(
        "transaction range was empty",
    ))
}

fn transaction_optional_range_value(
    response: &Value,
) -> Result<Option<Vec<u8>>, CoordinationError> {
    validate_single_response(response, "response_range")?;
    let kvs = response["responses"][0]["response_range"]
        .get("kvs")
        .and_then(Value::as_array)
        .ok_or(CoordinationError::InvalidResponse(
            "missing transaction range",
        ))?;
    if kvs.len() > 1 {
        return Err(CoordinationError::InvalidResponse(
            "transaction range was not singular",
        ));
    }
    let Some(kv) = kvs.first() else {
        return Ok(None);
    };
    let encoded =
        kv.get("value")
            .and_then(Value::as_str)
            .ok_or(CoordinationError::InvalidResponse(
                "missing transaction value",
            ))?;
    BASE64
        .decode(encoded)
        .map(Some)
        .map_err(|_| CoordinationError::InvalidResponse("invalid transaction value"))
}

fn cluster_marker_key(logical_cluster: &str) -> Vec<u8> {
    format!("/pgshard/v1/clusters/{logical_cluster}/identity").into_bytes()
}

fn cluster_marker_value(logical_cluster: &str, cluster_uid: &str) -> Vec<u8> {
    format!("pgshard-orchestrator-coordination-v1\0{logical_cluster}\0{cluster_uid}").into_bytes()
}

fn session_key(identity: &OrchestratorIdentity, cluster_uid: &str) -> Vec<u8> {
    format!(
        "/pgshard/v1/clusters/{}/incarnations/{cluster_uid}/orchestrators/{}",
        identity.cluster_id, identity.orchestrator_id,
    )
    .into_bytes()
}

fn session_value(identity: &OrchestratorIdentity, cluster_uid: &str, incarnation: Uuid) -> Vec<u8> {
    format!(
        "pgshard-orchestrator-incarnation-v1\0{}\0{}\0{cluster_uid}\0{}\0{}\0{}",
        identity.cluster_id, identity.orchestrator_id, incarnation, VERSION, GIT_SHA,
    )
    .into_bytes()
}

fn stopping(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn run_or_stop<T>(
    shutdown: &mut watch::Receiver<bool>,
    future: impl Future<Output = T>,
) -> Option<T> {
    if stopping(shutdown) {
        return None;
    }
    tokio::select! {
        biased;
        () = wait_for_stop(shutdown) => None,
        value = future => Some(value),
    }
}

async fn wait_for_stop(shutdown: &mut watch::Receiver<bool>) {
    while !stopping(shutdown) {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn retire_lease(gateway: &mut EtcdGateway, lease_id: Option<i64>) {
    let Some(lease_id) = lease_id else {
        return;
    };
    match tokio::time::timeout(REVOKE_TIMEOUT, gateway.revoke(lease_id)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!(%error, lease_id, "best-effort etcd lease revoke failed");
        }
        Err(_) => {
            tracing::debug!(lease_id, "best-effort etcd lease revoke timed out");
        }
    }
}

async fn wait_or_stop(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    if stopping(shutdown) {
        return true;
    }
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        changed = shutdown.changed() => changed.is_err() || stopping(shutdown),
    }
}

#[derive(Debug, Error)]
enum EndpointError {
    #[error("invalid endpoint URI")]
    InvalidUri,
    #[error("invalid HTTP request")]
    InvalidRequest,
    #[error("request timed out")]
    Timeout,
    #[error("transport failed")]
    Transport,
    #[error("response body failed or exceeded its bound")]
    Body,
    #[error("unexpected HTTP status {0}")]
    Status(StatusCode),
    #[error("response is not JSON")]
    ContentType,
    #[error("response JSON is invalid")]
    Json,
}

/// Coordination setup, transport, or evidence failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CoordinationError {
    /// Standalone settings do not satisfy the bounded lease timing contract.
    #[error("invalid orchestrator coordination settings")]
    InvalidSettings,
    /// No configured endpoint completed a bounded v3 gateway request.
    #[error("all etcd coordination endpoints are unavailable")]
    GatewayUnavailable,
    /// Another process still owns the same operator-assigned identity.
    #[error("the orchestrator identity already has a live etcd incarnation")]
    SessionOwned,
    /// The lease expired or did not renew to its configured safe TTL.
    #[error("the etcd coordination lease is expired or under-length")]
    LeaseExpired,
    /// The linearizable ownership proof no longer binds the expected marker,
    /// process token, and lease.
    #[error("the orchestrator incarnation lost its exact etcd session evidence")]
    SessionEvidenceLost,
    /// A persistent marker belongs to another logical cluster contract.
    #[error("the persistent etcd cluster marker conflicts with this pgshard cluster")]
    ClusterMarkerConflict,
    /// Configured endpoints changed etcd cluster identity within one process.
    #[error("the configured etcd endpoints disagree on cluster identity")]
    ClusterIdentityChanged,
    /// A sequential response moved backward in the pinned etcd revision.
    #[error("the pinned etcd revision regressed")]
    RevisionRegressed,
    /// The response omitted or malformed required bounded evidence.
    #[error("invalid etcd gateway response: {0}")]
    InvalidResponse(&'static str),
    /// The in-process readiness guard rejected cluster or revision evidence.
    #[error("orchestrator state rejected coordination evidence")]
    StateEvidenceRejected,
}

impl CoordinationError {
    const fn is_permanent(&self) -> bool {
        !matches!(
            self,
            Self::GatewayUnavailable
                | Self::SessionOwned
                | Self::LeaseExpired
                | Self::SessionEvidenceLost
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt as _;

    fn identity() -> OrchestratorIdentity {
        OrchestratorIdentity {
            cluster_id: "cluster-1".to_owned(),
            orchestrator_id: "orch-1".to_owned(),
        }
    }

    #[test]
    fn session_paths_and_values_are_domain_separated() {
        assert_eq!(
            cluster_marker_key("cluster-1"),
            b"/pgshard/v1/clusters/cluster-1/identity"
        );
        assert_eq!(
            cluster_marker_value("cluster-1", "cluster-uid"),
            b"pgshard-orchestrator-coordination-v1\0cluster-1\0cluster-uid"
        );
        assert_eq!(
            session_key(&identity(), "cluster-uid"),
            b"/pgshard/v1/clusters/cluster-1/incarnations/cluster-uid/orchestrators/orch-1"
        );
        let value = session_value(&identity(), "cluster-uid", Uuid::nil());
        assert!(
            value.starts_with(
                b"pgshard-orchestrator-incarnation-v1\0cluster-1\0orch-1\0cluster-uid\0"
            )
        );
        assert!(!value.windows(2).any(|window| window == b"//"));
    }

    #[test]
    fn transaction_range_requires_one_bounded_base64_value() {
        let response = json!({
            "responses": [{"response_range": {"kvs": [{"value": "dmFsdWU="}]}}]
        });
        assert_eq!(transaction_range_value(&response).expect("value"), b"value");
        assert!(transaction_range_value(&json!({"responses": []})).is_err());
        assert!(
            transaction_range_value(&json!({
                "responses": [{"response_range": {"kvs": []}}]
            }))
            .is_err()
        );
    }

    #[test]
    fn standalone_timing_preserves_two_thirds_of_the_lease() {
        let endpoints = vec![
            Url::parse("http://127.0.0.1:2379").expect("url"),
            Url::parse("http://127.0.0.2:2379").expect("url"),
            Url::parse("http://127.0.0.3:2379").expect("url"),
        ];
        assert!(
            CoordinationConfig::new(
                endpoints.clone(),
                identity(),
                "cluster-uid".to_owned(),
                Duration::from_secs(15),
                Duration::from_secs(1),
            )
            .is_ok()
        );
        assert!(matches!(
            CoordinationConfig::new(
                endpoints,
                identity(),
                "cluster-uid".to_owned(),
                Duration::from_secs(6),
                Duration::from_secs(1),
            ),
            Err(CoordinationError::InvalidSettings)
        ));

        let ten_endpoints = (1..=10)
            .map(|last_octet| {
                Url::parse(&format!("http://127.0.0.{last_octet}:2379")).expect("url")
            })
            .collect();
        assert!(matches!(
            CoordinationConfig::new(
                ten_endpoints,
                identity(),
                "cluster-uid".to_owned(),
                Duration::from_mins(5),
                Duration::from_millis(100),
            ),
            Err(CoordinationError::InvalidSettings)
        ));
        for (ttl, timeout) in [
            (Duration::new(15, 1), Duration::from_millis(100)),
            (Duration::from_secs(15), Duration::from_micros(100_001)),
        ] {
            assert!(matches!(
                CoordinationConfig::new(
                    vec![Url::parse("http://127.0.0.1:2379").expect("url")],
                    identity(),
                    "cluster-uid".to_owned(),
                    ttl,
                    timeout,
                ),
                Err(CoordinationError::InvalidSettings)
            ));
        }
    }

    #[test]
    fn lease_headers_pin_only_cluster_identity() {
        let endpoint = Url::parse("http://127.0.0.1:2379").expect("url");
        let mut gateway = EtcdGateway::new(&[endpoint], Duration::from_millis(100));
        gateway.cluster_id = Some(7);
        gateway.highest_revision = 10;

        assert_eq!(
            gateway
                .observe_cluster_header(&json!({
                    "header": {"cluster_id": "7", "revision": "1"}
                }))
                .expect("same etcd cluster"),
            7
        );
        assert_eq!(gateway.highest_revision, 10);
        assert!(matches!(
            gateway.observe_cluster_header(&json!({
                "header": {"cluster_id": "8", "revision": "11"}
            })),
            Err(CoordinationError::ClusterIdentityChanged)
        ));
    }

    #[test]
    fn verified_session_range_binds_key_value_and_lease() {
        let key = BASE64.encode(b"session-key");
        let value = BASE64.encode(b"session-token");
        let response = json!({
            "responses": [{"response_range": {"kvs": [{
                "key": key,
                "value": value,
                "lease": "42"
            }]}}]
        });
        validate_session_range(&response, &key, &value, 42).expect("exact evidence");
        assert!(validate_session_range(&response, &key, &value, 43).is_err());
    }

    #[tokio::test]
    async fn request_timeout_includes_a_stalled_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n",
                )
                .await
                .expect("write response headers");
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let endpoint = Url::parse(&format!("http://{address}")).expect("valid local test endpoint");
        let gateway = EtcdGateway::new(&[endpoint], Duration::from_millis(50));

        assert!(matches!(
            gateway.post_one(0, "v3/kv/range", Vec::new()).await,
            Err(EndpointError::Timeout)
        ));
        server.abort();
    }
}
