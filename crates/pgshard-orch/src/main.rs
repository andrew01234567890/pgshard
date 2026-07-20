//! `pgshard-orch` Linux container entry point.

use pgshard_orch::config::{ConfigError, OrchConfig};
use pgshard_orch::coordination::{CoordinationConfig, supervise};
use pgshard_orch::domain::OrchState;
use pgshard_orch::identity_binding;
use pgshard_orch::topology::{
    ExpectedTopologyIdentity, TopologyDiagnostics, TopologyError, TopologyV1,
};
use std::time::Duration;
use tokio::sync::watch;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (config, topology) = initialize()?;
    let identity_binding_enabled = config.identity_binding_mode.enabled();
    let (topology_diagnostics, observation_targets) =
        configured_topology(&topology, identity_binding_enabled);
    let coordination = CoordinationConfig::new(
        config.lease_namespace,
        config.lease_name,
        config.identity.clone(),
        config.cluster_uid,
        config.pod_uid,
        config.kubernetes_lease_duration,
        config.kubernetes_lease_retry_period,
        config.kubernetes_request_timeout,
    )?;
    let state = OrchState::with_identity_and_topology(
        config.identity,
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
    let server_future = pgshard_orch::http::serve(
        config.http_bind,
        state.clone(),
        wait_for_shutdown(shutdown_rx.clone()),
    );
    tokio::pin!(coordination_future);
    tokio::pin!(identity_binding_future);
    tokio::pin!(server_future);

    tokio::select! {
        () = shutdown_signal() => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            match tokio::time::timeout(SHUTDOWN_GRACE, async {
                tokio::join!(
                    &mut coordination_future,
                    &mut identity_binding_future,
                    &mut server_future
                )
            }).await {
                Ok((coordination_result, (), server_result)) => {
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
            if let Ok(server_result) =
                tokio::time::timeout(SHUTDOWN_GRACE, async {
                    tokio::join!(&mut identity_binding_future, &mut server_future)
                }).await
            {
                server_result.1?;
            } else {
                tracing::warn!(
                    grace_seconds = SHUTDOWN_GRACE.as_secs(),
                    "orchestrator HTTP drain grace expired"
                );
            }
            coordination_result?;
            return Err("orchestrator coordination stopped before process shutdown".into());
        }
        server_result = &mut server_future => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            if let Ok(coordination_result) =
                tokio::time::timeout(SHUTDOWN_GRACE, async {
                    tokio::join!(&mut coordination_future, &mut identity_binding_future)
                }).await
            {
                coordination_result.0?;
            } else {
                tracing::warn!(
                    grace_seconds = SHUTDOWN_GRACE.as_secs(),
                    "orchestrator coordination shutdown grace expired"
                );
            }
            server_result?;
        }
    }
    tracing::info!("orchestrator shutdown complete");
    Ok(())
}

fn configured_topology(
    topology: &TopologyV1,
    identity_binding_enabled: bool,
) -> (
    TopologyDiagnostics,
    Vec<pgshard_orch::topology::UnboundAgentObservationTarget>,
) {
    let diagnostics = topology.diagnostics(identity_binding_enabled);
    let targets = if identity_binding_enabled {
        topology.agent_observation_targets()
    } else {
        Vec::new()
    };
    (diagnostics, targets)
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
