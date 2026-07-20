//! `pgshard-orch` Linux container entry point.

use pgshard_orch::config::{ConfigError, OrchConfig};
use pgshard_orch::coordination::{CoordinationConfig, supervise};
use pgshard_orch::domain::OrchState;
use pgshard_orch::topology::{
    ExpectedTopologyIdentity, TopologyDiagnostics, TopologyError, TopologyV1,
};
use std::time::Duration;
use tokio::sync::watch;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match OrchConfig::from_env() {
        Ok(config) => config,
        Err(ConfigError::Arguments(error)) => error.exit(),
        Err(error) => return Err(error.into()),
    };
    let topology_diagnostics = load_topology(&config)?;
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
    let server_future = pgshard_orch::http::serve(
        config.http_bind,
        state.clone(),
        wait_for_shutdown(shutdown_rx),
    );
    tokio::pin!(coordination_future);
    tokio::pin!(server_future);

    tokio::select! {
        () = shutdown_signal() => {
            state.begin_shutdown();
            let _ = shutdown_tx.send(true);
            match tokio::time::timeout(SHUTDOWN_GRACE, async {
                tokio::join!(&mut coordination_future, &mut server_future)
            }).await {
                Ok((coordination_result, server_result)) => {
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
                tokio::time::timeout(SHUTDOWN_GRACE, &mut server_future).await
            {
                server_result?;
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
                tokio::time::timeout(SHUTDOWN_GRACE, &mut coordination_future).await
            {
                coordination_result?;
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

fn load_topology(config: &OrchConfig) -> Result<TopologyDiagnostics, TopologyError> {
    TopologyV1::load(
        &config.topology_file,
        ExpectedTopologyIdentity {
            cluster_id: &config.identity.cluster_id,
            cluster_uid: &config.cluster_uid,
            namespace: &config.lease_namespace,
        },
    )
    .map(|topology| topology.diagnostics())
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
