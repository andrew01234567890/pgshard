//! `pgshard-orch` Linux container entry point.

use pgshard_orch::config::OrchConfig;
use pgshard_orch::domain::OrchState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = OrchConfig::from_env()?;
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

    let state = OrchState::with_identity(config.identity);
    tracing::info!(
        bind = %config.http_bind,
        lease_ttl_ms = config.lease_ttl_ms,
        version = pgshard_version::VERSION,
        git_sha = pgshard_version::GIT_SHA,
        "starting orchestrator HTTP server; persistence and automated failover remain disabled"
    );
    pgshard_orch::http::serve(config.http_bind, state, shutdown_signal()).await?;
    tracing::info!("orchestrator shutdown complete");
    Ok(())
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
