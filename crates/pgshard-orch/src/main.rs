//! `pgshard-orch` Linux container entry point.

use pgshard_orch::catalog_activation_supervisor;
use pgshard_orch::catalog_candidate;
use pgshard_orch::config::{ConfigError, OrchConfig};
use pgshard_orch::coordination::{CoordinationConfig, supervise};
use pgshard_orch::domain::OrchState;
use pgshard_orch::identity_binding;
use pgshard_orch::topology::{
    CatalogCandidateObservationPlan, ExpectedTopologyIdentity, TopologyDiagnostics, TopologyError,
    TopologyV1,
};
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (config, topology) = initialize()?;
    let identity_binding_enabled = config.identity_binding_mode.enabled();
    let (topology_diagnostics, observation_targets, candidate_plan) =
        configured_topology(&topology, identity_binding_enabled);
    let coordination = CoordinationConfig::new(
        config.lease_namespace,
        config.lease_name,
        config.identity.clone(),
        config.cluster_uid,
        config.pod_uid.clone(),
        config.kubernetes_lease_duration,
        config.kubernetes_lease_retry_period,
        config.kubernetes_request_timeout,
    )?;
    let state = OrchState::with_identity_topology_and_dispatcher(
        config.identity,
        config.pod_uid,
        config.lease_ttl_ms,
        topology_diagnostics.clone(),
    )?;
    log_start(config.http_bind, config.lease_ttl_ms, &topology_diagnostics);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let coordination_future = supervise(coordination, state.clone(), shutdown_rx.clone());
    let identity_binding_future = supervise_identity_binding(
        identity_binding_enabled,
        observation_targets,
        state.clone(),
        shutdown_rx.clone(),
        config.kubernetes_request_timeout,
        config.kubernetes_lease_retry_period,
        config.identity_binding_freshness,
    );
    let catalog_activation_future = supervise_catalog_activation(
        identity_binding_enabled,
        candidate_plan.clone(),
        state.clone(),
        shutdown_rx.clone(),
        config.kubernetes_request_timeout,
        config.kubernetes_lease_retry_period,
        config.identity_binding_freshness,
    );
    let catalog_candidate_future = supervise_catalog_candidates(
        identity_binding_enabled,
        candidate_plan,
        state.clone(),
        shutdown_rx.clone(),
        config.kubernetes_request_timeout,
        config.kubernetes_lease_retry_period,
        config.identity_binding_freshness,
    );
    let server_future = pgshard_orch::http::serve(
        config.http_bind,
        state.clone(),
        wait_for_shutdown(shutdown_rx.clone()),
    );
    let diagnostic_future =
        supervise_diagnostics(identity_binding_future, catalog_candidate_future);
    Box::pin(supervise_services(
        state,
        shutdown_tx,
        coordination_future,
        diagnostic_future,
        catalog_activation_future,
        server_future,
        shutdown_signal(),
    ))
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiagnosticService {
    AgentIdentityBinding,
    CatalogCandidate,
}

impl DiagnosticService {
    const fn stopped_message(self) -> &'static str {
        match self {
            Self::AgentIdentityBinding => {
                "orchestrator identity binding stopped before process shutdown"
            }
            Self::CatalogCandidate => {
                "orchestrator catalog candidate observation stopped before process shutdown"
            }
        }
    }
}

async fn supervise_diagnostics<I, D>(identity_binding: I, catalog_candidate: D) -> DiagnosticService
where
    I: Future<Output = ()>,
    D: Future<Output = ()>,
{
    tokio::pin!(identity_binding);
    tokio::pin!(catalog_candidate);
    tokio::select! {
        () = &mut identity_binding => DiagnosticService::AgentIdentityBinding,
        () = &mut catalog_candidate => DiagnosticService::CatalogCandidate,
    }
}

async fn supervise_services<C, D, A, S, H, CE, SE>(
    state: OrchState,
    shutdown_tx: watch::Sender<bool>,
    coordination_future: C,
    diagnostic_future: D,
    catalog_activation_future: A,
    server_future: S,
    shutdown_future: H,
) -> Result<(), Box<dyn std::error::Error>>
where
    C: Future<Output = Result<(), CE>>,
    D: Future<Output = DiagnosticService>,
    A: Future<Output = ()>,
    S: Future<Output = Result<(), SE>>,
    H: Future<Output = ()>,
    CE: std::error::Error + 'static,
    SE: std::error::Error + 'static,
{
    tokio::pin!(coordination_future);
    tokio::pin!(diagnostic_future);
    tokio::pin!(catalog_activation_future);
    tokio::pin!(server_future);
    tokio::pin!(shutdown_future);

    tokio::select! {
        () = &mut shutdown_future => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            match tokio::time::timeout(SHUTDOWN_GRACE, async {
                tokio::join!(
                    &mut coordination_future,
                    &mut diagnostic_future,
                    &mut catalog_activation_future,
                    &mut server_future
                )
            }).await {
                Ok((coordination_result, _, (), server_result)) => {
                    coordination_result?;
                    server_result?;
                }
                Err(_) => {
                    tracing::warn!(
                        grace_seconds = SHUTDOWN_GRACE.as_secs(),
                        "orchestrator shutdown grace expired; dropping remaining work"
                    );
                }
            }
        }
        coordination_result = &mut coordination_future => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            if let Some((_, (), server_result)) =
                drain_three(&mut diagnostic_future, &mut catalog_activation_future, &mut server_future).await
            {
                server_result?;
            }
            coordination_result?;
            return Err("orchestrator coordination stopped before process shutdown".into());
        }
        diagnostic_service = &mut diagnostic_future => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            if let Some((coordination_result, (), server_result)) =
                drain_three(&mut coordination_future, &mut catalog_activation_future, &mut server_future).await
            {
                coordination_result?;
                server_result?;
            }
            return Err(diagnostic_service.stopped_message().into());
        }
        () = &mut catalog_activation_future => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            if let Some((coordination_result, _, server_result)) =
                drain_three(&mut coordination_future, &mut diagnostic_future, &mut server_future).await
            {
                coordination_result?;
                server_result?;
            }
            return Err("orchestrator catalog-activation publisher stopped before process shutdown".into());
        }
        server_result = &mut server_future => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            if let Some((coordination_result, _, ())) =
                drain_three(&mut coordination_future, &mut diagnostic_future, &mut catalog_activation_future).await
            {
                coordination_result?;
            }
            server_result?;
        }
    }
    tracing::info!("orchestrator shutdown complete");
    Ok(())
}

async fn drain_three<A, B, C>(
    first: &mut A,
    second: &mut B,
    third: &mut C,
) -> Option<(A::Output, B::Output, C::Output)>
where
    A: Future + Unpin,
    B: Future + Unpin,
    C: Future + Unpin,
{
    if let Ok(results) =
        tokio::time::timeout(SHUTDOWN_GRACE, async { tokio::join!(first, second, third) }).await
    {
        Some(results)
    } else {
        tracing::warn!(
            grace_seconds = SHUTDOWN_GRACE.as_secs(),
            "orchestrator service drain grace expired"
        );
        None
    }
}

fn configured_topology(
    topology: &TopologyV1,
    identity_binding_enabled: bool,
) -> (
    TopologyDiagnostics,
    Vec<pgshard_orch::topology::UnboundAgentObservationTarget>,
    Option<CatalogCandidateObservationPlan>,
) {
    let diagnostics = topology.diagnostics(identity_binding_enabled);
    let targets = if identity_binding_enabled {
        topology.agent_observation_targets()
    } else {
        Vec::new()
    };
    let candidate_plan = identity_binding_enabled
        .then(|| topology.catalog_candidate_observation_plan())
        .flatten();
    (diagnostics, targets, candidate_plan)
}

async fn supervise_identity_binding(
    enabled: bool,
    targets: Vec<pgshard_orch::topology::UnboundAgentObservationTarget>,
    state: OrchState,
    shutdown: watch::Receiver<bool>,
    request_timeout: Duration,
    retry_period: Duration,
    freshness: Duration,
) {
    if enabled {
        identity_binding::supervise(
            targets,
            state,
            shutdown,
            request_timeout,
            retry_period,
            freshness,
        )
        .await;
    } else {
        wait_for_shutdown(shutdown).await;
    }
}

async fn supervise_catalog_candidates(
    enabled: bool,
    plan: Option<CatalogCandidateObservationPlan>,
    state: OrchState,
    shutdown: watch::Receiver<bool>,
    request_timeout: Duration,
    retry_period: Duration,
    freshness: Duration,
) {
    if enabled && let Some(plan) = plan {
        catalog_candidate::supervise(
            plan,
            state,
            shutdown,
            request_timeout,
            retry_period,
            freshness,
        )
        .await;
    } else {
        wait_for_shutdown(shutdown).await;
    }
}

async fn supervise_catalog_activation(
    enabled: bool,
    plan: Option<CatalogCandidateObservationPlan>,
    state: OrchState,
    shutdown: watch::Receiver<bool>,
    request_timeout: Duration,
    retry_period: Duration,
    evidence_timeout: Duration,
) {
    if enabled && let Some(plan) = plan {
        catalog_activation_supervisor::supervise(
            plan,
            state,
            shutdown,
            request_timeout,
            retry_period,
            evidence_timeout,
        )
        .await;
    } else {
        wait_for_shutdown(shutdown).await;
    }
}

fn initialize() -> Result<(OrchConfig, TopologyV1), Box<dyn std::error::Error>> {
    let config = match OrchConfig::from_env() {
        Ok(config) => config,
        Err(ConfigError::Arguments(error)) => error.exit(),
        Err(error) => return Err(error.into()),
    };
    let topology = load_topology(&config)?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let telemetry = config.telemetry.status();
    if telemetry.endpoint_configured {
        tracing::warn!(reason = telemetry.reason, "OpenTelemetry export disabled");
    } else {
        tracing::info!(reason = telemetry.reason, "OpenTelemetry export disabled");
    }
    Ok((config, topology))
}

fn load_topology(config: &OrchConfig) -> Result<TopologyV1, TopologyError> {
    TopologyV1::load(
        &config.topology_file,
        ExpectedTopologyIdentity {
            cluster_id: &config.identity.cluster_id,
            cluster_uid: &config.cluster_uid,
            namespace: &config.lease_namespace,
        },
    )
}

fn log_start(bind: std::net::SocketAddr, lease_ttl_ms: u64, topology: &TopologyDiagnostics) {
    tracing::info!(
        bind = %bind,
        lease_ttl_ms,
        version = pgshard_version::VERSION,
        git_sha = pgshard_version::GIT_SHA,
        topology_schema = %topology.schema_version,
        topology_shards = topology.shard_count,
        topology_members = topology.member_count,
        "starting orchestrator HTTP server; durable operation persistence and automated failover remain disabled"
    );
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(%error, "could not install SIGTERM handler");
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "SIGINT handler failed");
                }
                return;
            }
        };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "SIGINT handler failed");
                }
            }
            _ = terminate.recv() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use tokio::sync::oneshot;

    async fn stop_successfully(shutdown: watch::Receiver<bool>) -> io::Result<()> {
        wait_for_shutdown(shutdown).await;
        Ok(())
    }

    #[tokio::test]
    async fn service_supervisor_polls_all_background_services_during_normal_operation() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (identity_started_tx, identity_started_rx) = oneshot::channel();
        let (catalog_started_tx, catalog_started_rx) = oneshot::channel();
        let (activation_started_tx, activation_started_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let identity_shutdown = shutdown_rx.clone();
        let catalog_shutdown = shutdown_rx.clone();
        let activation_shutdown = shutdown_rx.clone();
        let diagnostics = supervise_diagnostics(
            async move {
                let _ = identity_started_tx.send(());
                wait_for_shutdown(identity_shutdown).await;
            },
            async move {
                let _ = catalog_started_tx.send(());
                wait_for_shutdown(catalog_shutdown).await;
            },
        );
        let activation = async move {
            let _ = activation_started_tx.send(());
            wait_for_shutdown(activation_shutdown).await;
        };
        let supervisor = supervise_services(
            OrchState::default(),
            shutdown_tx,
            stop_successfully(shutdown_rx.clone()),
            diagnostics,
            activation,
            stop_successfully(shutdown_rx),
            async move {
                let _ = stop_rx.await;
            },
        );
        tokio::pin!(supervisor);
        let service_start = tokio::time::timeout(Duration::from_secs(1), async {
            identity_started_rx
                .await
                .expect("identity binding start signal");
            catalog_started_rx
                .await
                .expect("catalog candidate start signal");
            activation_started_rx
                .await
                .expect("catalog activation start signal");
        });
        tokio::pin!(service_start);

        tokio::select! {
            result = &mut supervisor => panic!("service supervisor stopped before identity binding started: {result:?}"),
            result = &mut service_start => result.expect("background services were not all polled"),
        }
        stop_tx.send(()).expect("request graceful shutdown");
        supervisor.await.expect("graceful service shutdown");
    }

    #[tokio::test]
    async fn identity_binding_completion_stops_peer_services_and_fails_process() {
        assert_diagnostic_completion_fails_process(
            DiagnosticService::AgentIdentityBinding,
            "orchestrator identity binding stopped before process shutdown",
        )
        .await;
    }

    #[tokio::test]
    async fn catalog_candidate_completion_stops_peer_services_and_fails_process() {
        assert_diagnostic_completion_fails_process(
            DiagnosticService::CatalogCandidate,
            "orchestrator catalog candidate observation stopped before process shutdown",
        )
        .await;
    }

    #[tokio::test]
    async fn catalog_activation_completion_stops_peer_services_and_fails_process() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (coordination_stopped_tx, coordination_stopped_rx) = oneshot::channel();
        let (server_stopped_tx, server_stopped_rx) = oneshot::channel();
        let coordination_shutdown = shutdown_rx.clone();
        let coordination = async move {
            wait_for_shutdown(coordination_shutdown).await;
            let _ = coordination_stopped_tx.send(());
            Ok::<(), io::Error>(())
        };
        let diagnostics = supervise_diagnostics(
            wait_for_shutdown(shutdown_rx.clone()),
            wait_for_shutdown(shutdown_rx.clone()),
        );
        let server = async move {
            wait_for_shutdown(shutdown_rx).await;
            let _ = server_stopped_tx.send(());
            Ok::<(), io::Error>(())
        };

        let error = supervise_services(
            OrchState::default(),
            shutdown_tx,
            coordination,
            diagnostics,
            async {},
            server,
            std::future::pending(),
        )
        .await
        .expect_err("unexpected publisher completion must fail the process");

        assert_eq!(
            error.to_string(),
            "orchestrator catalog-activation publisher stopped before process shutdown"
        );
        coordination_stopped_rx
            .await
            .expect("coordination observed shutdown");
        server_stopped_rx
            .await
            .expect("HTTP server observed shutdown");
    }

    async fn assert_diagnostic_completion_fails_process(
        diagnostic_service: DiagnosticService,
        expected_error: &str,
    ) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (coordination_stopped_tx, coordination_stopped_rx) = oneshot::channel();
        let (activation_stopped_tx, activation_stopped_rx) = oneshot::channel();
        let (server_stopped_tx, server_stopped_rx) = oneshot::channel();
        let coordination_shutdown = shutdown_rx.clone();
        let activation_shutdown = shutdown_rx.clone();
        let coordination = async move {
            wait_for_shutdown(coordination_shutdown).await;
            let _ = coordination_stopped_tx.send(());
            Ok::<(), io::Error>(())
        };
        let server = async move {
            wait_for_shutdown(shutdown_rx).await;
            let _ = server_stopped_tx.send(());
            Ok::<(), io::Error>(())
        };
        let activation = async move {
            wait_for_shutdown(activation_shutdown).await;
            let _ = activation_stopped_tx.send(());
        };
        let complete_identity = diagnostic_service == DiagnosticService::AgentIdentityBinding;
        let diagnostics = supervise_diagnostics(
            async move {
                if complete_identity {
                    return;
                }
                std::future::pending::<()>().await;
            },
            async move {
                if !complete_identity {
                    return;
                }
                std::future::pending::<()>().await;
            },
        );

        let error = supervise_services(
            OrchState::default(),
            shutdown_tx,
            coordination,
            diagnostics,
            activation,
            server,
            std::future::pending(),
        )
        .await
        .expect_err("unexpected diagnostic completion must fail the process");

        assert_eq!(error.to_string(), expected_error);
        coordination_stopped_rx
            .await
            .expect("coordination observed shutdown");
        activation_stopped_rx
            .await
            .expect("catalog activation observed shutdown");
        server_stopped_rx
            .await
            .expect("HTTP server observed shutdown");
    }
}
