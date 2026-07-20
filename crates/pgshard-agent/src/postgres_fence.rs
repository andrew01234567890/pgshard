//! Peer-authenticated installation of exact writable authority into `PostgreSQL`.
//!
//! The shared-preload target starts disarmed after every postmaster start. The
//! agent connects only through its owner-only Unix socket and immutable
//! `peer` HBA rule, creates the fixed extension objects, and accepts an
//! installation only when `PostgreSQL` returns the exact canonical generation
//! and absolute `CLOCK_BOOTTIME` deadline unchanged.

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls};

use crate::writable::{
    DurableWritableGeneration, WritableAuthorityObserver, WritableAuthoritySnapshot,
};

const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(25);
const CONNECTION_OPTIONS: &str = "-c search_path=pg_catalog \
    -c session_preload_libraries= -c local_preload_libraries= \
    -c event_triggers=off -c jit=off -c default_tablespace= -c temp_tablespaces= \
    -c default_transaction_read_only=off -c row_security=off \
    -c synchronous_commit=on -c log_statement=none \
    -c log_min_error_statement=panic -c log_parameter_max_length=0 \
    -c log_parameter_max_length_on_error=0";
const CREATE_EXTENSION: &str =
    "CREATE EXTENSION IF NOT EXISTS pgshard_fence WITH SCHEMA pg_catalog";
const INSTALL_AUTHORITY: &str = "\
    SELECT installed_identity, installed_deadline_boottime_ns \
    FROM pg_catalog.pgshard_fence_install($1::bytea, $2::bytea)";

/// One retained target-control session that must survive every Lease renewal.
pub(crate) struct TargetFenceSession {
    socket_dir: PathBuf,
    connection: ConnectedPostgres,
    installed: Option<WritableAuthoritySnapshot>,
}

impl TargetFenceSession {
    /// Connects through the private peer-authenticated socket and installs a
    /// snapshot that is still exact when the target ACK returns.
    pub(crate) async fn connect_and_install(
        socket_dir: &Path,
        observer: &WritableAuthorityObserver,
        expected_generation: &DurableWritableGeneration,
        required_margin: Duration,
    ) -> Result<Self, PostgresFenceError> {
        let connection = loop {
            validate_expected_authority(observer, expected_generation, required_margin)?;
            match connect(socket_dir).await {
                Ok(connection) => break connection,
                Err(error) => {
                    tracing::debug!(
                        reason = %error,
                        "waiting for peer-authenticated PostgreSQL fence socket"
                    );
                    sleep(CONNECT_RETRY_DELAY).await;
                }
            }
        };
        connection.client.batch_execute(CREATE_EXTENSION).await?;
        let mut session = Self {
            socket_dir: socket_dir.to_owned(),
            connection,
            installed: None,
        };
        session
            .install_until_stable(observer, expected_generation, required_margin)
            .await?;
        Ok(session)
    }

    /// Keeps the exact target deadline synchronized. Any lost session,
    /// malformed ACK, regressive target transition, or authority change ends
    /// the future so the owner can fence the postmaster process tree.
    pub(crate) async fn supervise(
        mut self,
        mut observer: WritableAuthorityObserver,
        expected_generation: DurableWritableGeneration,
        required_margin: Duration,
    ) -> Result<(), PostgresFenceError> {
        loop {
            tokio::select! {
                biased;
                result = &mut self.connection.driver => {
                    return match result {
                        Ok(Ok(())) => Err(PostgresFenceError::ConnectionEnded),
                        Ok(Err(error)) => Err(PostgresFenceError::Postgres(error)),
                        Err(error) => Err(PostgresFenceError::ConnectionDriver(error)),
                    };
                }
                changed = observer.changed() => {
                    changed.map_err(|_| PostgresFenceError::AuthorityChannelClosed)?;
                }
            }
            self.install_until_stable(&observer, &expected_generation, required_margin)
                .await?;
        }
    }

    async fn install_until_stable(
        &mut self,
        observer: &WritableAuthorityObserver,
        expected_generation: &DurableWritableGeneration,
        required_margin: Duration,
    ) -> Result<(), PostgresFenceError> {
        loop {
            let requested =
                validate_expected_authority(observer, expected_generation, required_margin)?;
            if self.installed.as_ref() != Some(&requested) {
                self.install_exact(&requested).await?;
                self.installed = Some(requested.clone());
            }
            if observer.snapshot_is_current(&requested, required_margin) {
                return Ok(());
            }
        }
    }

    async fn install_exact(
        &mut self,
        requested: &WritableAuthoritySnapshot,
    ) -> Result<(), PostgresFenceError> {
        let identity = requested.generation.canonical_bytes();
        let deadline = requested.deadline.as_nanos().to_be_bytes().to_vec();
        let row = self
            .connection
            .client
            .query_one(INSTALL_AUTHORITY, &[&identity, &deadline])
            .await?;
        let acknowledged_identity = row.try_get::<_, Vec<u8>>(0)?;
        let acknowledged_deadline = row.try_get::<_, Vec<u8>>(1)?;
        validate_ack(
            &identity,
            &deadline,
            &acknowledged_identity,
            &acknowledged_deadline,
        )
        .map_err(|()| PostgresFenceError::AcknowledgementMismatch {
            socket_dir: self.socket_dir.clone(),
        })
    }
}

pub(crate) fn validate_expected_authority(
    observer: &WritableAuthorityObserver,
    expected_generation: &DurableWritableGeneration,
    required_margin: Duration,
) -> Result<WritableAuthoritySnapshot, PostgresFenceError> {
    let snapshot = observer
        .snapshot_valid_for(required_margin)
        .ok_or(PostgresFenceError::AuthorityChanged)?;
    if snapshot.generation != *expected_generation {
        return Err(PostgresFenceError::AuthorityChanged);
    }
    Ok(snapshot)
}

fn validate_ack(
    expected_identity: &[u8],
    expected_deadline: &[u8],
    acknowledged_identity: &[u8],
    acknowledged_deadline: &[u8],
) -> Result<(), ()> {
    if acknowledged_identity == expected_identity && acknowledged_deadline == expected_deadline {
        Ok(())
    } else {
        Err(())
    }
}

async fn connect(socket_dir: &Path) -> Result<ConnectedPostgres, tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host_path(socket_dir)
        .port(5432)
        .user("postgres")
        .dbname("postgres")
        .application_name("pgshard-fence-installer")
        .options(CONNECTION_OPTIONS);
    let (client, connection) = config.connect(NoTls).await?;
    let driver = tokio::spawn(connection);
    Ok(ConnectedPostgres { client, driver })
}

struct ConnectedPostgres {
    client: Client,
    driver: JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl Drop for ConnectedPostgres {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// Failure to install or continuously renew local target authority.
#[derive(Debug, Error)]
pub enum PostgresFenceError {
    /// The continuous installer returned without a target or authority error.
    #[error("PostgreSQL target-fence supervisor stopped unexpectedly")]
    UnexpectedStop,
    /// The attempt-private authority changed or lost its safety margin.
    #[error("attempt-private writable authority changed during target installation")]
    AuthorityChanged,
    /// The private authority sender disappeared.
    #[error("attempt-private writable authority channel closed")]
    AuthorityChannelClosed,
    /// The peer-authenticated target connection or command failed.
    #[error("PostgreSQL target-fence control failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    /// The retained `PostgreSQL` protocol driver closed without an error.
    #[error("PostgreSQL target-fence control connection ended")]
    ConnectionEnded,
    /// The retained `PostgreSQL` protocol driver task could not be joined.
    #[error("PostgreSQL target-fence control task failed: {0}")]
    ConnectionDriver(#[source] tokio::task::JoinError),
    /// `PostgreSQL` did not echo the exact installed immutable record.
    #[error("PostgreSQL target fence at {socket_dir:?} returned a non-exact authority ACK")]
    AcknowledgementMismatch {
        /// Private socket directory used for the control request.
        socket_dir: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::boottime::BoottimeInstant;
    use crate::writable::{durable_generation_for_test, writable_attempt_pair_for_test};

    #[test]
    fn exact_ack_is_required() {
        let identity = b"generation";
        let deadline = 42_u64.to_be_bytes();
        assert_eq!(
            validate_ack(identity, &deadline, identity, &deadline),
            Ok(())
        );

        let other_deadline = 43_u64.to_be_bytes();
        assert_eq!(
            validate_ack(identity, &deadline, identity, &other_deadline),
            Err(())
        );
        assert_eq!(
            validate_ack(identity, &deadline, b"other", &deadline),
            Err(())
        );
    }

    #[test]
    fn deadline_wire_value_is_unsigned_big_endian() {
        assert_eq!(u64::MAX.to_be_bytes(), [0xff; 8]);
        assert_eq!(1_u64.to_be_bytes(), [0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[tokio::test]
    #[ignore = "requires a peer-authenticated PostgreSQL 18 target-fence Unix socket"]
    async fn live_postgres18_installs_renews_and_detects_control_session_loss() {
        let socket_dir = PathBuf::from(
            std::env::var_os("PGSHARD_TARGET_FENCE_TEST_SOCKET")
                .expect("PGSHARD_TARGET_FENCE_TEST_SOCKET is required"),
        );
        let generation = durable_generation_for_test(1);
        let (lease_attempt, postgres_attempt) = writable_attempt_pair_for_test();
        let initial_deadline = BoottimeInstant::now()
            .expect("read initial boot clock")
            .checked_add(Duration::from_secs(30))
            .expect("bounded initial deadline");
        lease_attempt.install_authority(initial_deadline, generation.clone());
        let observer = postgres_attempt.authority_observer();
        let mut session = TargetFenceSession::connect_and_install(
            &socket_dir,
            &observer,
            &generation,
            Duration::from_secs(1),
        )
        .await
        .expect("install exact initial authority");
        assert_eq!(
            session.installed,
            Some(WritableAuthoritySnapshot {
                deadline: initial_deadline,
                generation: generation.clone(),
            })
        );

        let renewed_deadline = initial_deadline
            .checked_add(Duration::from_secs(30))
            .expect("bounded renewed deadline");
        lease_attempt.install_authority(renewed_deadline, generation.clone());
        session
            .install_until_stable(&observer, &generation, Duration::from_secs(1))
            .await
            .expect("install exact renewed authority");
        assert_eq!(
            session.installed,
            Some(WritableAuthoritySnapshot {
                deadline: renewed_deadline,
                generation: generation.clone(),
            })
        );

        let backend_pid = session
            .connection
            .client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
            .expect("read retained control backend PID")
            .get::<_, i32>(0);
        let killer = connect(&socket_dir)
            .await
            .expect("connect peer-authenticated termination session");
        let terminated = killer
            .client
            .query_one(
                "SELECT pg_catalog.pg_terminate_backend($1)",
                &[&backend_pid],
            )
            .await
            .expect("terminate retained control backend")
            .get::<_, bool>(0);
        assert!(terminated, "retained control backend was not terminated");

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            session.supervise(observer, generation, Duration::from_secs(1)),
        )
        .await
        .expect("retained-session loss was detected")
        .expect_err("retained-session loss must be terminal");
        assert!(
            matches!(
                error,
                PostgresFenceError::ConnectionEnded
                    | PostgresFenceError::ConnectionDriver(_)
                    | PostgresFenceError::Postgres(_)
            ),
            "unexpected retained-session failure: {error}"
        );
    }
}
