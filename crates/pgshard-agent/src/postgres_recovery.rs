//! Continuous proof that a supervised standby remains in recovery.

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::oneshot;
#[cfg(test)]
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, Config, NoTls};

const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(25);
const OBSERVATION_INTERVAL: Duration = Duration::from_millis(250);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECTION_OPTIONS: &str = "-c search_path=pg_catalog \
    -c session_preload_libraries= -c local_preload_libraries= \
    -c event_triggers=off -c jit=off -c default_tablespace= -c temp_tablespaces= \
    -c default_transaction_read_only=on -c row_security=off \
    -c log_statement=none -c log_min_error_statement=panic \
    -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0 \
    -c statement_timeout=2s -c lock_timeout=2s";
const RECOVERY_QUERY: &str = "SELECT pg_catalog.pg_is_in_recovery(), \
    COALESCE((SELECT pg_catalog.bool_or(status = 'streaming') \
              FROM pg_catalog.pg_stat_wal_receiver), false)";

/// Continuously verifies recovery over the private peer-authenticated socket.
///
/// Before the first recovery-plus-streaming observation, startup connection
/// failures and an absent WAL receiver leave the sealed process in its starting
/// state. Once recovery is confirmed, every false recovery value, query
/// failure, timeout, or local connection loss is terminal; an upstream outage
/// alone does not kill a `PostgreSQL` process that remains safely in recovery.
pub(crate) async fn monitor_standby_recovery(
    socket_dir: PathBuf,
    confirmed: oneshot::Sender<()>,
) -> Result<(), PostgresRecoveryError> {
    #[cfg(test)]
    if let Some(observations) = take_test_recovery_observations() {
        return monitor_test_recovery(observations, confirmed).await;
    }
    let mut confirmed = Some(confirmed);
    loop {
        let connection = timeout(OPERATION_TIMEOUT, connect(&socket_dir)).await;
        let connection = match connection {
            Ok(Ok(connection)) => connection,
            Ok(Err(source)) if confirmed.is_some() => {
                tracing::debug!(reason = %source, "waiting for standby PostgreSQL socket");
                sleep(CONNECT_RETRY_DELAY).await;
                continue;
            }
            Ok(Err(source)) => return Err(PostgresRecoveryError::Database(source)),
            Err(_) if confirmed.is_some() => {
                sleep(CONNECT_RETRY_DELAY).await;
                continue;
            }
            Err(_) => return Err(PostgresRecoveryError::OperationTimeout(OPERATION_TIMEOUT)),
        };

        loop {
            let observation =
                timeout(OPERATION_TIMEOUT, observe_recovery(&connection.client)).await;
            match observation {
                Ok(Ok((true, true))) => {
                    if let Some(sender) = confirmed.take() {
                        sender
                            .send(())
                            .map_err(|()| PostgresRecoveryError::ConfirmationReceiverDropped)?;
                    }
                }
                Ok(Ok((true, false))) => {}
                Ok(Ok((false, _))) => return Err(PostgresRecoveryError::RecoveryEnded),
                Ok(Err(source)) if confirmed.is_some() => {
                    tracing::debug!(reason = %source, "waiting for standby recovery query");
                    break;
                }
                Ok(Err(source)) => return Err(PostgresRecoveryError::Database(source)),
                Err(_) if confirmed.is_some() => break,
                Err(_) => return Err(PostgresRecoveryError::OperationTimeout(OPERATION_TIMEOUT)),
            }
            sleep(OBSERVATION_INTERVAL).await;
        }
        drop(connection);
        sleep(CONNECT_RETRY_DELAY).await;
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestRecoveryObservation {
    Pending,
    InRecovery,
    RecoveryEnded,
    Unknown,
}

#[cfg(test)]
static TEST_RECOVERY_OBSERVATIONS: std::sync::Mutex<
    Option<watch::Receiver<TestRecoveryObservation>>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn set_test_recovery_observations(
    observations: watch::Receiver<TestRecoveryObservation>,
) {
    let mut slot = TEST_RECOVERY_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        slot.replace(observations).is_none(),
        "test recovery monitor already installed"
    );
}

#[cfg(test)]
fn take_test_recovery_observations() -> Option<watch::Receiver<TestRecoveryObservation>> {
    TEST_RECOVERY_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(test)]
async fn monitor_test_recovery(
    mut observations: watch::Receiver<TestRecoveryObservation>,
    confirmed: oneshot::Sender<()>,
) -> Result<(), PostgresRecoveryError> {
    let mut confirmed = Some(confirmed);
    loop {
        match *observations.borrow_and_update() {
            TestRecoveryObservation::Pending => {}
            TestRecoveryObservation::InRecovery => {
                if let Some(sender) = confirmed.take() {
                    sender
                        .send(())
                        .map_err(|()| PostgresRecoveryError::ConfirmationReceiverDropped)?;
                }
            }
            TestRecoveryObservation::RecoveryEnded => {
                return Err(PostgresRecoveryError::RecoveryEnded);
            }
            TestRecoveryObservation::Unknown => {
                return Err(PostgresRecoveryError::ObservationUnknown);
            }
        }
        observations
            .changed()
            .await
            .map_err(|_| PostgresRecoveryError::ObservationUnknown)?;
    }
}

async fn observe_recovery(client: &Client) -> Result<(bool, bool), tokio_postgres::Error> {
    let row = client.query_one(RECOVERY_QUERY, &[]).await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

async fn connect(socket_dir: &Path) -> Result<ConnectedPostgres, tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host_path(socket_dir)
        .port(5432)
        .user("postgres")
        .dbname("postgres")
        .application_name("pgshard-recovery-monitor")
        .options(CONNECTION_OPTIONS);
    let (client, connection) = config.connect(NoTls).await?;
    let driver = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(reason = %error, "PostgreSQL recovery-monitor connection ended");
        }
    });
    Ok(ConnectedPostgres { client, driver })
}

struct ConnectedPostgres {
    client: Client,
    driver: JoinHandle<()>,
}

impl Drop for ConnectedPostgres {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// Loss of continuous standby recovery proof.
#[derive(Debug, Error)]
pub(crate) enum PostgresRecoveryError {
    /// `PostgreSQL` reported that recovery ended without an authorized role restart.
    #[error("PostgreSQL standby left recovery without an authorized role restart")]
    RecoveryEnded,
    /// A previously verified recovery connection or query failed.
    #[error("PostgreSQL standby recovery observation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    /// A previously verified recovery operation exceeded its bounded deadline.
    #[error("PostgreSQL standby recovery observation exceeded {0:?}")]
    OperationTimeout(Duration),
    /// The owning supervisor disappeared before accepting initial recovery proof.
    #[error("PostgreSQL standby recovery confirmation receiver disappeared")]
    ConfirmationReceiverDropped,
    /// A test or transport path could no longer establish a recovery value.
    #[cfg(test)]
    #[error("PostgreSQL standby recovery observation became unknown")]
    ObservationUnknown,
}
