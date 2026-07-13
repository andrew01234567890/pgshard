//! `pgshard-pooler` Linux control-runtime entry point.

use pgshard_pooler::config::{PoolerConfig, PoolerConfigError};
use pgshard_pooler::runtime::PoolerRuntime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match PoolerConfig::from_env() {
        Ok(config) => config,
        Err(PoolerConfigError::Arguments(error)) => error.exit(),
        Err(error) => return Err(error.into()),
    };
    let http_bind = config.http_bind();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let listener = tokio::net::TcpListener::bind(http_bind).await?;
    let runtime = PoolerRuntime::new(config);
    tracing::warn!(
        "PostgreSQL client listeners, remote catalog transport, and OpenTelemetry export remain disabled"
    );
    tracing::info!(
        bind = %http_bind,
        version = pgshard_version::VERSION,
        git_sha = pgshard_version::GIT_SHA,
        "starting pooler control runtime"
    );
    runtime.run(listener, shutdown_signal()).await?;
    tracing::info!("pooler control runtime stopped");
    Ok(())
}

async fn shutdown_signal() {
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
