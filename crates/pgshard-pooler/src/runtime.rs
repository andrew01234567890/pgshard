//! Composition of catalog supervision, HTTP control, and client handshakes.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pgshard_catalog::{CatalogCache, CatalogSupervisor};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio_postgres::NoTls;

use crate::config::{PoolerConfig, SupervisedCatalogConfig};
use crate::state::PoolerState;

const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// One fail-closed pooler runtime.
pub struct PoolerRuntime {
    catalog: Option<SupervisedCatalog>,
    state: PoolerState,
}

struct SupervisedCatalog {
    supervisor: CatalogSupervisor,
    config: tokio_postgres::Config,
}

impl PoolerRuntime {
    /// Builds a fail-closed runtime that remains application-unready.
    #[must_use]
    pub fn new(config: PoolerConfig) -> Self {
        match config.into_runtime_parts() {
            Some(SupervisedCatalogConfig {
                catalog,
                supervisor,
            }) => {
                let supervisor = CatalogSupervisor::new(Arc::new(CatalogCache::new()), supervisor);
                let state = PoolerState::control_only(supervisor.status());
                Self {
                    catalog: Some(SupervisedCatalog {
                        supervisor,
                        config: catalog,
                    }),
                    state,
                }
            }
            None => Self {
                catalog: None,
                state: PoolerState::bootstrap_unavailable(),
            },
        }
    }

    /// Returns a cloneable handle for health, readiness, status, and metrics.
    #[must_use]
    pub fn state(&self) -> PoolerState {
        self.state.clone()
    }

    /// Runs optional catalog supervision, HTTP control, and the `PostgreSQL`
    /// handshake boundary until shutdown.
    ///
    /// In local mode the connector is intentionally restricted by
    /// [`PoolerConfig`] to loopback IP literals or Unix sockets with
    /// `sslmode=disable`. Bootstrap-unavailable mode starts no connector.
    /// Dropping this future broadcasts shutdown to all child tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if a child task panics, a listener fails, or child
    /// shutdown exceeds the hard runtime deadline.
    pub async fn run<F>(
        self,
        http_listener: TcpListener,
        frontend_listener: TcpListener,
        shutdown: F,
    ) -> Result<(), PoolerRuntimeError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self { catalog, state } = self;
        let (stop_sender, stop_receiver) = watch::channel(false);
        let stop_guard = StopOnDrop(stop_sender);

        let catalog_shutdown = wait_for_stop(stop_receiver.clone());
        let mut catalog_task = match catalog {
            Some(SupervisedCatalog { supervisor, config }) => tokio::spawn(supervisor.run(
                move || {
                    let config = config.clone();
                    async move { config.connect(NoTls).await }
                },
                catalog_shutdown,
            )),
            None => tokio::spawn(catalog_shutdown),
        };
        let mut http_task = tokio::spawn(crate::http::serve_listener(
            http_listener,
            state,
            wait_for_stop(stop_receiver.clone()),
        ));
        let mut frontend_task = tokio::spawn(crate::frontend::serve_listener(
            frontend_listener,
            wait_for_stop(stop_receiver),
        ));
        tokio::pin!(shutdown);

        let mut catalog_result = None;
        let mut http_result = None;
        let mut frontend_result = None;
        tokio::select! {
            biased;
            () = shutdown.as_mut() => {}
            result = &mut catalog_task => catalog_result = Some(result),
            result = &mut http_task => http_result = Some(result),
            result = &mut frontend_task => frontend_result = Some(result),
        }
        stop_guard.stop();

        let joined = tokio::time::timeout(RUNTIME_SHUTDOWN_TIMEOUT, async {
            tokio::join!(
                async {
                    if catalog_result.is_none() {
                        catalog_result = Some((&mut catalog_task).await);
                    }
                },
                async {
                    if http_result.is_none() {
                        http_result = Some((&mut http_task).await);
                    }
                },
                async {
                    if frontend_result.is_none() {
                        frontend_result = Some((&mut frontend_task).await);
                    }
                }
            );
        })
        .await;
        if joined.is_err() {
            return abort_timed_out_children(
                catalog_task,
                http_task,
                frontend_task,
                catalog_result,
                http_result,
                frontend_result,
            )
            .await;
        }

        let catalog_result = catalog_result.ok_or_else(shutdown_timeout_error)?;
        let http_result = http_result.ok_or_else(shutdown_timeout_error)?;
        let frontend_result = frontend_result.ok_or_else(shutdown_timeout_error)?;
        catalog_result.map_err(PoolerRuntimeError::CatalogTask)?;
        let http_result = http_result.map_err(PoolerRuntimeError::HttpTask)?;
        http_result.map_err(PoolerRuntimeError::Http)?;
        let frontend_result = frontend_result.map_err(PoolerRuntimeError::FrontendTask)?;
        frontend_result.map_err(PoolerRuntimeError::Frontend)
    }
}

async fn abort_timed_out_children(
    catalog_task: JoinHandle<()>,
    http_task: JoinHandle<std::io::Result<()>>,
    frontend_task: JoinHandle<std::io::Result<()>>,
    mut catalog_result: Option<Result<(), JoinError>>,
    mut http_result: Option<Result<std::io::Result<()>, JoinError>>,
    mut frontend_result: Option<Result<std::io::Result<()>, JoinError>>,
) -> Result<(), PoolerRuntimeError> {
    if catalog_result.is_none() {
        catalog_task.abort();
    }
    if http_result.is_none() {
        http_task.abort();
    }
    if frontend_result.is_none() {
        frontend_task.abort();
    }
    if catalog_result.is_none() {
        catalog_result = Some(catalog_task.await);
    }
    if http_result.is_none() {
        http_result = Some(http_task.await);
    }
    if frontend_result.is_none() {
        frontend_result = Some(frontend_task.await);
    }
    if let Some(result) = catalog_result {
        match result {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(PoolerRuntimeError::CatalogTask(error)),
        }
    }
    if let Some(result) = http_result {
        match result {
            Ok(result) => result.map_err(PoolerRuntimeError::Http)?,
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(PoolerRuntimeError::HttpTask(error)),
        }
    }
    if let Some(result) = frontend_result {
        match result {
            Ok(result) => result.map_err(PoolerRuntimeError::Frontend)?,
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(PoolerRuntimeError::FrontendTask(error)),
        }
    }
    Err(shutdown_timeout_error())
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
    /// The `PostgreSQL` handshake task panicked.
    #[error("pooler PostgreSQL handshake task failed: {0}")]
    FrontendTask(#[source] JoinError),
    /// The `PostgreSQL` handshake server returned an I/O failure.
    #[error("pooler PostgreSQL handshake server failed: {0}")]
    Frontend(#[source] std::io::Error),
    /// Child tasks did not stop inside the hard runtime drain deadline.
    #[error("pooler child tasks exceeded shutdown timeout {timeout:?}")]
    ShutdownTimeout {
        /// Hard drain deadline before remaining tasks are aborted.
        timeout: Duration,
    },
}

fn shutdown_timeout_error() -> PoolerRuntimeError {
    PoolerRuntimeError::ShutdownTimeout {
        timeout: RUNTIME_SHUTDOWN_TIMEOUT,
    }
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
            "127.0.0.1:0".parse().expect("read-write bind"),
            catalog_config,
            supervisor_config,
        )
    }

    #[tokio::test]
    async fn composes_http_catalog_retry_and_graceful_shutdown() {
        let http_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HTTP test listener");
        let http_address = http_listener.local_addr().expect("HTTP test address");
        let frontend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind PostgreSQL test listener");
        let frontend_address = frontend_listener
            .local_addr()
            .expect("PostgreSQL test address");
        let runtime = PoolerRuntime::new(config());
        let state = runtime.state();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(runtime.run(http_listener, frontend_listener, async move {
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
        let connection = TcpStream::connect(http_address)
            .await
            .expect("HTTP listener accepts a connection");
        drop(connection);
        let connection = TcpStream::connect(frontend_address)
            .await
            .expect("PostgreSQL listener accepts a connection");
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

    #[tokio::test]
    async fn bootstrap_without_catalog_serves_control_and_stops_cleanly() {
        let http_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bootstrap HTTP listener");
        let http_address = http_listener.local_addr().expect("bootstrap HTTP address");
        let frontend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind bootstrap PostgreSQL listener");
        let frontend_address = frontend_listener
            .local_addr()
            .expect("bootstrap PostgreSQL address");
        let runtime = PoolerRuntime::new(PoolerConfig::bootstrap_unavailable(
            http_address,
            frontend_address,
        ));
        let state = runtime.state();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(runtime.run(http_listener, frontend_listener, async move {
            let _ = shutdown_receiver.await;
        }));

        assert_eq!(state.readiness().reason, "catalog_not_configured");
        TcpStream::connect(http_address)
            .await
            .expect("bootstrap HTTP listener accepts a connection");
        TcpStream::connect(frontend_address)
            .await
            .expect("bootstrap PostgreSQL listener accepts a connection");

        shutdown_sender
            .send(())
            .expect("runtime retains bootstrap shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("bootstrap runtime stops within one second")
            .expect("bootstrap runtime task")
            .expect("clean bootstrap runtime shutdown");
        assert_eq!(state.snapshot().catalog.phase, "not_configured");
    }
}
