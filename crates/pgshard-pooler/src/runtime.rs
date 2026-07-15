//! Composition of catalog supervision, HTTP control, and client handshakes.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pgshard_catalog::{CatalogCache, CatalogFailureKind, CatalogSupervisor};
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
        let catalog_task = match catalog {
            Some(SupervisedCatalog { supervisor, config }) => {
                tokio::spawn(supervisor.run_classified(
                    move || {
                        let config = config.clone();
                        async move { config.connect(NoTls).await }
                    },
                    CatalogFailureKind::from,
                    catalog_shutdown,
                ))
            }
            None => tokio::spawn(catalog_shutdown),
        };
        let http_task = tokio::spawn(crate::http::serve_listener(
            http_listener,
            state,
            wait_for_stop(stop_receiver.clone()),
        ));
        let frontend_task = tokio::spawn(crate::frontend::serve_listener(
            frontend_listener,
            wait_for_stop(stop_receiver),
        ));

        supervise_tasks(stop_guard, catalog_task, http_task, frontend_task, shutdown).await
    }
}

async fn supervise_tasks<F>(
    stop_guard: StopOnDrop,
    mut catalog_task: JoinHandle<()>,
    mut http_task: JoinHandle<std::io::Result<()>>,
    mut frontend_task: JoinHandle<std::io::Result<()>>,
    shutdown: F,
) -> Result<(), PoolerRuntimeError>
where
    F: Future<Output = ()> + Send,
{
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

    let (Some(catalog_result), Some(http_result), Some(frontend_result)) =
        (catalog_result, http_result, frontend_result)
    else {
        return Err(shutdown_timeout_error());
    };
    combine_component_results(catalog_result, http_result, frontend_result, false)
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
    let mut failures = match (catalog_result, http_result, frontend_result) {
        (Some(catalog), Some(http), Some(frontend)) => {
            component_failures(catalog, http, frontend, true)
        }
        _ => Vec::new(),
    };
    failures.push(shutdown_timeout_error());
    combine_runtime_failures(failures)
}

fn combine_component_results(
    catalog: Result<(), JoinError>,
    http: Result<std::io::Result<()>, JoinError>,
    frontend: Result<std::io::Result<()>, JoinError>,
    ignore_cancelled: bool,
) -> Result<(), PoolerRuntimeError> {
    combine_runtime_failures(component_failures(
        catalog,
        http,
        frontend,
        ignore_cancelled,
    ))
}

fn component_failures(
    catalog: Result<(), JoinError>,
    http: Result<std::io::Result<()>, JoinError>,
    frontend: Result<std::io::Result<()>, JoinError>,
    ignore_cancelled: bool,
) -> Vec<PoolerRuntimeError> {
    let mut failures = Vec::with_capacity(3);
    match catalog {
        Ok(()) => {}
        Err(error) if ignore_cancelled && error.is_cancelled() => {}
        Err(error) => failures.push(PoolerRuntimeError::CatalogTask(error)),
    }
    match http {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(PoolerRuntimeError::Http(error)),
        Err(error) if ignore_cancelled && error.is_cancelled() => {}
        Err(error) => failures.push(PoolerRuntimeError::HttpTask(error)),
    }
    match frontend {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(PoolerRuntimeError::Frontend(error)),
        Err(error) if ignore_cancelled && error.is_cancelled() => {}
        Err(error) => failures.push(PoolerRuntimeError::FrontendTask(error)),
    }
    failures
}

fn combine_runtime_failures(
    failures: impl IntoIterator<Item = PoolerRuntimeError>,
) -> Result<(), PoolerRuntimeError> {
    let mut failures = failures.into_iter();
    let Some(primary) = failures.next() else {
        return Ok(());
    };
    Err(
        failures.fold(primary, |primary, secondary| PoolerRuntimeError::Combined {
            primary: Box::new(primary),
            secondary: Box::new(secondary),
        }),
    )
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
    /// More than one supervised component failed during the same shutdown.
    #[error("{primary}; secondary pooler failure: {secondary}")]
    Combined {
        /// Deterministically ordered primary failure.
        #[source]
        primary: Box<PoolerRuntimeError>,
        /// Additional failure that occurred before shutdown completed.
        secondary: Box<PoolerRuntimeError>,
    },
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

    #[test]
    fn preserves_simultaneous_http_and_frontend_failures() {
        let result = combine_component_results(
            Ok(()),
            Ok(Err(std::io::Error::other("HTTP failed"))),
            Ok(Err(std::io::Error::other("frontend failed"))),
            false,
        );
        let PoolerRuntimeError::Combined { primary, secondary } =
            result.expect_err("both listener failures survive")
        else {
            panic!("simultaneous listener failures were not combined");
        };
        assert!(matches!(*primary, PoolerRuntimeError::Http(_)));
        assert!(matches!(*secondary, PoolerRuntimeError::Frontend(_)));
    }

    #[tokio::test]
    async fn terminal_frontend_failure_stops_http_listener() {
        let http_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind supervised HTTP listener");
        let http_address = http_listener.local_addr().expect("supervised HTTP address");
        let (stop_sender, stop_receiver) = watch::channel(false);
        let catalog_task = tokio::spawn(wait_for_stop(stop_receiver.clone()));
        let http_task = tokio::spawn(crate::http::serve_listener(
            http_listener,
            PoolerState::bootstrap_unavailable(),
            wait_for_stop(stop_receiver),
        ));
        let (fail_sender, fail_receiver) = tokio::sync::oneshot::channel();
        let frontend_task = tokio::spawn(async move {
            let _ = fail_receiver.await;
            Err(std::io::Error::other("injected frontend failure"))
        });
        let supervisor = tokio::spawn(supervise_tasks(
            StopOnDrop(stop_sender),
            catalog_task,
            http_task,
            frontend_task,
            std::future::pending(),
        ));

        let connection =
            tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(http_address))
                .await
                .expect("HTTP listener becomes reachable")
                .expect("connect before frontend failure");
        drop(connection);
        fail_sender
            .send(())
            .expect("frontend failure task retains trigger");
        let error = tokio::time::timeout(Duration::from_secs(1), supervisor)
            .await
            .expect("joint supervisor stops promptly")
            .expect("joint supervisor task")
            .expect_err("frontend failure is terminal");
        assert!(matches!(error, PoolerRuntimeError::Frontend(_)));

        assert!(TcpStream::connect(http_address).await.is_err());
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
