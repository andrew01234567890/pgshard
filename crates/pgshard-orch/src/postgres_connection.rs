//! Bounded ownership of a dedicated `tokio-postgres` connection driver.

use std::time::Duration;

use thiserror::Error;
use tokio::{
    task::{JoinError, JoinHandle},
    time::timeout,
};

pub(crate) const CONNECTION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) struct ConnectionTask {
    handle: Option<JoinHandle<Result<(), tokio_postgres::Error>>>,
}

impl ConnectionTask {
    pub(crate) fn new(handle: JoinHandle<Result<(), tokio_postgres::Error>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn handle_mut(&mut self) -> &mut JoinHandle<Result<(), tokio_postgres::Error>> {
        self.handle
            .as_mut()
            .expect("dedicated PostgreSQL connection task is consumed exactly once")
    }

    pub(crate) async fn abort_and_wait(mut self) {
        self.abort();
        let _ = timeout(CONNECTION_CLEANUP_TIMEOUT, self.handle_mut()).await;
        self.handle.take();
    }

    pub(crate) async fn finish<V>(mut self, value: V) -> Result<V, ConnectionTaskError> {
        let result = match timeout(CONNECTION_CLEANUP_TIMEOUT, self.handle_mut()).await {
            Ok(Ok(Ok(()))) => Ok(value),
            Ok(Ok(Err(source))) => Err(ConnectionTaskError::Connection(source)),
            Ok(Err(source)) => Err(ConnectionTaskError::Task(source)),
            Err(_) => {
                self.abort();
                let _ = timeout(CONNECTION_CLEANUP_TIMEOUT, self.handle_mut()).await;
                Err(ConnectionTaskError::CleanupTimeout {
                    duration: CONNECTION_CLEANUP_TIMEOUT,
                })
            }
        };
        self.handle.take();
        result
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConnectionTaskError {
    #[error("dedicated PostgreSQL connection failed: {0}")]
    Connection(#[source] tokio_postgres::Error),
    #[error("dedicated PostgreSQL connection task failed: {0}")]
    Task(#[source] JoinError),
    #[error("dedicated PostgreSQL connection cleanup exceeded {duration:?}")]
    CleanupTimeout { duration: Duration },
}
