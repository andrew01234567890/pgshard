//! Composition of catalog supervision and the HTTP control surface.

use std::future::Future;
use std::sync::Arc;

use pgshard_catalog::{CatalogCache, CatalogSupervisor};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinError;
use tokio_postgres::NoTls;

use crate::config::PoolerConfig;
use crate::state::PoolerState;

/// One fail-closed pooler control runtime.
pub struct PoolerRuntime {
    catalog: CatalogSupervisor,
    catalog_config: tokio_postgres::Config,
    state: PoolerState,
}

impl PoolerRuntime {
    /// Builds a runtime whose readiness is driven by one catalog supervisor.
    #[must_use]
    pub fn new(config: PoolerConfig) -> Self {
        let (catalog_config, supervisor_config) = config.into_runtime_parts();
        let catalog = CatalogSupervisor::new(Arc::new(CatalogCache::new()), supervisor_config);
        let state = PoolerState::new(catalog.status());
        Self {
            catalog,
            catalog_config,
            state,
        }
    }

    /// Returns a cloneable handle for health, readiness, status, and metrics.
    #[must_use]
    pub fn state(&self) -> PoolerState {
        self.state.clone()
    }

    /// Runs catalog supervision and the HTTP server until shutdown.
    ///
    /// The current connector is intentionally restricted by [`PoolerConfig`]
    /// to loopback IP literals or Unix sockets with `sslmode=disable`. It is a
    /// development bridge, not the future authenticated cluster transport.
    /// Dropping this future broadcasts shutdown to both child tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if either child task panics or the HTTP server fails.
    pub async fn run<F>(self, listener: TcpListener, shutdown: F) -> Result<(), PoolerRuntimeError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            catalog,
            catalog_config,
            state,
        } = self;
        let (stop_sender, stop_receiver) = watch::channel(false);
        let stop_guard = StopOnDrop(stop_sender);

        let catalog_shutdown = wait_for_stop(stop_receiver.clone());
        let mut catalog_task = tokio::spawn(catalog.run(
            move || {
                let config = catalog_config.clone();
                async move { config.connect(NoTls).await }
            },
            catalog_shutdown,
        ));
        let mut http_task = tokio::spawn(crate::http::serve_listener(
            listener,
            state,
            wait_for_stop(stop_receiver),
        ));
        tokio::pin!(shutdown);

        let first = tokio::select! {
            biased;
            () = shutdown.as_mut() => RuntimeExit::Shutdown,
            result = &mut catalog_task => RuntimeExit::Catalog(result),
            result = &mut http_task => RuntimeExit::Http(result),
        };
        stop_guard.stop();

        let (catalog_result, http_result) = match first {
            RuntimeExit::Shutdown => (catalog_task.await, http_task.await),
            RuntimeExit::Catalog(result) => (result, http_task.await),
            RuntimeExit::Http(result) => (catalog_task.await, result),
        };
        catalog_result.map_err(PoolerRuntimeError::CatalogTask)?;
        let http_result = http_result.map_err(PoolerRuntimeError::HttpTask)?;
        http_result.map_err(PoolerRuntimeError::Http)
    }
}

enum RuntimeExit {
    Shutdown,
    Catalog(Result<(), JoinError>),
    Http(Result<std::io::Result<()>, JoinError>),
}

struct StopOnDrop(watch::Sender<bool>);

impl StopOnDrop {
    fn stop(&self) {
        self.0.send_replace(true);
    }
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn wait_for_stop(mut receiver: watch::Receiver<bool>) {
    if *receiver.borrow_and_update() {
        return;
    }
    while receiver.changed().await.is_ok() {
        if *receiver.borrow_and_update() {
            return;
        }
    }
}

/// Pooler child-task or HTTP-serving failure.
#[derive(Debug, Error)]
pub enum PoolerRuntimeError {
    /// The catalog supervisor task panicked.
    #[error("catalog supervisor task failed: {0}")]
    CatalogTask(#[source] JoinError),
    /// The HTTP server task panicked.
    #[error("pooler HTTP task failed: {0}")]
    HttpTask(#[source] JoinError),
    /// The HTTP server returned an I/O failure.
    #[error("pooler HTTP server failed: {0}")]
    Http(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pgshard_catalog::{
        CatalogConnectionPhase, CatalogOperationTimeout, CatalogPollInterval,
        CatalogSupervisorConfig,
    };
    use tokio::net::TcpStream;

    use super::*;

    fn config() -> PoolerConfig {
        let mut catalog_config: tokio_postgres::Config = "postgresql://postgres@127.0.0.1:1/shardschema?sslmode=disable&target_session_attrs=read-write"
            .parse()
            .expect("test catalog config");
        catalog_config.application_name("pgshard-pooler-runtime-test");
        let supervisor_config = CatalogSupervisorConfig::new(
            CatalogPollInterval::new(Duration::from_secs(1)).expect("poll interval"),
            Duration::from_secs(3),
            Duration::from_millis(10),
            Duration::from_millis(20),
        )
        .expect("supervisor config")
        .with_timeouts(
            Duration::from_millis(100),
            CatalogOperationTimeout::new(Duration::from_millis(100)).expect("operation timeout"),
        )
        .expect("supervisor timeouts");
        PoolerConfig::from_runtime_parts(
            "127.0.0.1:0".parse().expect("HTTP bind"),
            catalog_config,
            supervisor_config,
        )
    }

    #[tokio::test]
    async fn composes_http_catalog_retry_and_graceful_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let runtime = PoolerRuntime::new(config());
        let state = runtime.state();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(runtime.run(listener, async move {
            let _ = shutdown_receiver.await;
        }));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.snapshot().catalog.total_failures > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("catalog connector entered retry within one second");
        assert!(!state.snapshot().ready);
        let connection = TcpStream::connect(address)
            .await
            .expect("HTTP listener accepts a connection");
        drop(connection);

        shutdown_sender
            .send(())
            .expect("runtime retains shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("runtime stops within one second")
            .expect("runtime task")
            .expect("clean runtime shutdown");
        assert_eq!(
            state.snapshot().catalog.phase,
            CatalogConnectionPhase::Stopped.as_str()
        );
    }
}
