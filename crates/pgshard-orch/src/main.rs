//! `pgshard-orch` Linux container entry point.

use pgshard_orch::config::{ConfigError, OrchConfig};
use pgshard_orch::coordination::{CoordinationConfig, supervise};
use pgshard_orch::domain::OrchState;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match OrchConfig::from_env() {
        Ok(config) => config,
        Err(ConfigError::Arguments(error)) => error.exit(),
        Err(error) => return Err(error.into()),
    };
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
        config.etcd_endpoints,
        config.identity.clone(),
        config.cluster_uid,
        config.etcd_session_ttl,
        config.etcd_request_timeout,
    )?;
    let state = OrchState::with_identity(config.identity, config.lease_ttl_ms)?;
    tracing::info!(
        bind = %config.http_bind,
        lease_ttl_ms = config.lease_ttl_ms,
        version = pgshard_version::VERSION,
        git_sha = pgshard_version::GIT_SHA,
        "starting orchestrator HTTP server; durable operation persistence and automated failover remain disabled"
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let coordination_future = supervise(coordination, state.clone(), shutdown_rx.clone());
    let server_future =
        pgshard_orch::http::serve(config.http_bind, state, wait_for_shutdown(shutdown_rx));
    tokio::pin!(coordination_future);
    tokio::pin!(server_future);

    tokio::select! {
        () = shutdown_signal() => {
            let _ = shutdown_tx.send(true);
            let (coordination_result, server_result) =
                tokio::join!(&mut coordination_future, &mut server_future);
            coordination_result?;
            server_result?;
        }
        coordination_result = &mut coordination_future => {
            let _ = shutdown_tx.send(true);
            server_future.await?;
            coordination_result?;
            return Err("orchestrator coordination stopped before process shutdown".into());
        }
        server_result = &mut server_future => {
            let _ = shutdown_tx.send(true);
            coordination_future.await?;
            server_result?;
        }
    }
    tracing::info!("orchestrator shutdown complete");
    Ok(())
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
