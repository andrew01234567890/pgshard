//! Peer-authenticated installation of an exact statement-admission fence.
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

use crate::domain::{AgentState, TargetFenceAcknowledgement, unix_time_ms};
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
// Extension bootstrap is local target setup, not the WAL-backed writable
// generation publication.  A replication source can require synchronous
// standby replay before any standby has been admitted through this fence, so
// waiting for synchronous replication here would deadlock target installation
// with the first standby's IDENTIFY_SYSTEM request.  SET LOCAL leaves the
// retained control session's fail-closed `synchronous_commit=on` default intact.
const CREATE_EXTENSION: &str = "\
    BEGIN;\
    SET LOCAL synchronous_commit = local;\
    CREATE EXTENSION IF NOT EXISTS pgshard_fence WITH SCHEMA pg_catalog;\
    COMMIT";
const EXTENSION_IDENTITY_CHECK_COUNT: usize = 6;
const VALIDATE_EXTENSION_IDENTITY: &str = r#"
    WITH extension_identity AS (
        SELECT e.oid, e.extowner, e.extnamespace, e.extrelocatable,
               e.extversion, e.extconfig, e.extcondition,
               owner.rolname::text AS owner_name,
               namespace.nspname::text AS namespace_name
        FROM pg_catalog.pg_extension AS e
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = e.extowner
        JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = e.extnamespace
        WHERE e.extname = 'pgshard_fence'
    ), extension_members AS (
        SELECT dependency.classid, dependency.objid, dependency.objsubid
        FROM extension_identity AS extension
        JOIN pg_catalog.pg_depend AS dependency
          ON dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
         AND dependency.refobjid = extension.oid
         AND dependency.refobjsubid = 0
         AND dependency.deptype = 'e'
    ), member_function AS (
        SELECT function.*, namespace.nspname::text AS namespace_name,
               owner.rolname::text AS owner_name, language.lanname::text AS language_name
        FROM extension_members AS member
        JOIN pg_catalog.pg_proc AS function
          ON member.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
         AND member.objid = function.oid
         AND member.objsubid = 0
        JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = function.proowner
        JOIN pg_catalog.pg_language AS language ON language.oid = function.prolang
    ), function_acl AS (
        SELECT acl.grantor, acl.grantee, acl.privilege_type, acl.is_grantable,
               function.proowner
        FROM member_function AS function
        CROSS JOIN LATERAL pg_catalog.aclexplode(function.proacl) AS acl
    )
    SELECT ARRAY[
        (SELECT count(*) = 1 FROM extension_identity),
        (SELECT count(*) = 1 AND COALESCE(bool_and(
             owner_name = 'postgres' AND namespace_name = 'pg_catalog'
             AND extversion = '1.0' AND NOT extrelocatable
             AND extconfig IS NULL AND extcondition IS NULL), false)
         FROM extension_identity),
        (SELECT count(*) = 1 FROM extension_members),
        (SELECT count(*) = 1 FROM member_function),
        (SELECT count(*) = 1 AND COALESCE(bool_and(
             proname = 'pgshard_fence_install' AND namespace_name = 'pg_catalog'
             AND owner_name = 'postgres' AND language_name = 'c'
             AND probin = '$libdir/pgshard_fence' AND prosrc = 'pgshard_fence_install'
             AND prokind = 'f' AND NOT prosecdef AND NOT proleakproof
             AND proisstrict AND NOT proretset AND provolatile = 'v' AND proparallel = 'u'
             AND proconfig IS NULL AND provariadic = 0 AND prosupport = 0
             AND procost = 1 AND prorows = 0
             AND pronargs = 2 AND pronargdefaults = 0 AND proargdefaults IS NULL
             AND protrftypes IS NULL AND prosqlbody IS NULL
             AND prorettype = 'pg_catalog.record'::pg_catalog.regtype
             AND proargtypes = ARRAY[
                 'pg_catalog.bytea'::pg_catalog.regtype,
                 'pg_catalog.bytea'::pg_catalog.regtype]::pg_catalog.oidvector
             AND proallargtypes = ARRAY[
                 'pg_catalog.bytea'::pg_catalog.regtype,
                 'pg_catalog.bytea'::pg_catalog.regtype,
                 'pg_catalog.bytea'::pg_catalog.regtype,
                 'pg_catalog.bytea'::pg_catalog.regtype]::oid[]
             AND proargmodes = ARRAY['i', 'i', 'o', 'o']::pg_catalog."char"[]
             AND proargnames = ARRAY[
                 'identity', 'deadline_boottime_ns',
                 'installed_identity', 'installed_deadline_boottime_ns']::text[]), false)
         FROM member_function),
        (SELECT count(*) = 1 AND COALESCE(bool_and(
             grantor = proowner AND grantee = proowner
             AND privilege_type = 'EXECUTE' AND NOT is_grantable), false)
         FROM function_acl)
    ]::boolean[]
"#;
const INSTALL_AUTHORITY: &str = "\
    SELECT installed_identity, installed_deadline_boottime_ns, \
           pg_catalog.pg_backend_pid() \
    FROM pg_catalog.pgshard_fence_install($1::bytea, $2::bytea)";

/// One retained target-control session that must survive every Lease renewal.
pub(crate) struct TargetFenceSession {
    socket_dir: PathBuf,
    connection: ConnectedPostgres,
    installed: Option<WritableAuthoritySnapshot>,
    state: AgentState,
    postmaster_pid: u32,
    boot_id: Option<String>,
}

impl TargetFenceSession {
    /// Connects through the private peer-authenticated socket and installs a
    /// snapshot that is still exact when the target ACK returns.
    pub(crate) async fn connect_and_install(
        socket_dir: &Path,
        observer: &WritableAuthorityObserver,
        expected_generation: &DurableWritableGeneration,
        required_margin: Duration,
        state: AgentState,
        postmaster_pid: u32,
        boot_id: Option<String>,
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
        validate_extension_identity(&connection.client, socket_dir).await?;
        let mut session = Self {
            socket_dir: socket_dir.to_owned(),
            connection,
            installed: None,
            state,
            postmaster_pid,
            boot_id,
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
                self.install_exact(&requested, observer).await?;
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
        observer: &WritableAuthorityObserver,
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
        let control_backend_pid = row.try_get::<_, i32>(2)?;
        validate_ack(
            &identity,
            &deadline,
            &acknowledged_identity,
            &acknowledged_deadline,
        )
        .map_err(|()| PostgresFenceError::AcknowledgementMismatch {
            socket_dir: self.socket_dir.clone(),
        })?;
        let control_backend_pid = u32::try_from(control_backend_pid)
            .ok()
            .filter(|pid| *pid != 0 && *pid != self.postmaster_pid)
            .ok_or_else(|| PostgresFenceError::AcknowledgementMismatch {
                socket_dir: self.socket_dir.clone(),
            })?;
        let generation_identity = String::from_utf8(identity).map_err(|_| {
            PostgresFenceError::AcknowledgementMismatch {
                socket_dir: self.socket_dir.clone(),
            }
        })?;
        let observed_at_unix_ms = unix_time_ms();
        let remaining_validity_at_ack_ms = observer
            .remaining_validity(requested)
            .and_then(|remaining| u64::try_from(remaining.as_millis()).ok())
            .filter(|remaining| *remaining != 0)
            .ok_or(PostgresFenceError::AuthorityChanged)?;
        if let Some(boot_id) = self.boot_id.as_ref() {
            self.state
                .set_target_fence_acknowledgement(TargetFenceAcknowledgement {
                    observed_at_unix_ms,
                    generation_identity,
                    deadline_boottime_ns: requested.deadline.as_nanos(),
                    remaining_validity_at_ack_ms,
                    boot_id: boot_id.clone(),
                    postmaster_pid: self.postmaster_pid,
                    control_backend_pid,
                });
        } else {
            self.state.clear_target_fence_acknowledgement();
        }
        Ok(())
    }
}

impl Drop for TargetFenceSession {
    fn drop(&mut self) {
        self.state.clear_target_fence_acknowledgement();
    }
}

async fn validate_extension_identity(
    client: &Client,
    socket_dir: &Path,
) -> Result<(), PostgresFenceError> {
    let row = client.query_one(VALIDATE_EXTENSION_IDENTITY, &[]).await?;
    let checks = row.try_get::<_, Vec<bool>>(0)?;
    if extension_identity_is_exact(&checks) {
        Ok(())
    } else {
        Err(PostgresFenceError::CatalogIdentityMismatch {
            socket_dir: socket_dir.to_owned(),
        })
    }
}

fn extension_identity_is_exact(checks: &[bool]) -> bool {
    checks.len() == EXTENSION_IDENTITY_CHECK_COUNT && checks.iter().all(|check| *check)
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
    /// The installed extension catalog shape differs from the released target.
    #[error("PostgreSQL target fence at {socket_dir:?} has an incompatible catalog identity")]
    CatalogIdentityMismatch {
        /// Private socket directory whose target failed catalog attestation.
        socket_dir: PathBuf,
    },
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
    use crate::domain::{
        ActivationConfigEvidence, ActivationPostgresConfig, AgentIdentity,
        GenerationDurabilityEvidence, PostgresProcessState,
    };
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

    #[test]
    fn extension_bootstrap_is_local_without_weakening_the_retained_session() {
        assert!(CREATE_EXTENSION.starts_with("BEGIN;"));
        assert!(CREATE_EXTENSION.contains("SET LOCAL synchronous_commit = local;"));
        assert!(CREATE_EXTENSION.ends_with("COMMIT"));
        assert!(CONNECTION_OPTIONS.contains("synchronous_commit=on"));
        assert!(!CONNECTION_OPTIONS.contains("synchronous_commit=local"));
    }

    #[test]
    fn extension_catalog_identity_requires_every_exact_check() {
        assert!(extension_identity_is_exact(
            &[true; EXTENSION_IDENTITY_CHECK_COUNT]
        ));
        assert!(!extension_identity_is_exact(
            &[true; EXTENSION_IDENTITY_CHECK_COUNT - 1]
        ));
        assert!(!extension_identity_is_exact(
            &[true; EXTENSION_IDENTITY_CHECK_COUNT + 1]
        ));
        for rejected in 0..EXTENSION_IDENTITY_CHECK_COUNT {
            let mut checks = [true; EXTENSION_IDENTITY_CHECK_COUNT];
            checks[rejected] = false;
            assert!(!extension_identity_is_exact(&checks));
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    #[ignore = "requires a peer-authenticated PostgreSQL 18 target-fence Unix socket"]
    async fn live_postgres18_installs_renews_and_detects_control_session_loss() {
        let socket_dir = PathBuf::from(
            std::env::var_os("PGSHARD_TARGET_FENCE_TEST_SOCKET")
                .expect("PGSHARD_TARGET_FENCE_TEST_SOCKET is required"),
        );
        let generation = durable_generation_for_test(1);
        let identity = AgentIdentity {
            cluster_id: "cluster-1".to_owned(),
            shard_id: pgshard_types::ShardId(0),
            instance_id: "cluster-1-shard-0-0".to_owned(),
        };
        let state =
            AgentState::with_identity(identity.clone(), 60_000).expect("valid activation state");
        state.set_activation_config(ActivationConfigEvidence {
            identity,
            cluster_uid: "11111111-2222-3333-4444-555555555555".to_owned(),
            pod_uid: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_owned(),
            postgres: ActivationPostgresConfig::Source {
                lease_namespace: "database".to_owned(),
                lease_name: "cluster-1-cell-0000-writable".to_owned(),
                lease_uid: "99999999-8888-7777-6666-555555555555".to_owned(),
                durability: GenerationDurabilityEvidence::Local,
                target_fence_required_margin_ms: 1_000,
            },
        });
        state.set_postgres_process(PostgresProcessState::StartingReplicationBootstrap);
        let boot_id = "11111111-2222-3333-8444-555555555555".to_owned();
        state.set_postgres_process_identity(999, boot_id.clone());
        let (lease_attempt, postgres_attempt) = writable_attempt_pair_for_test();
        let initial_deadline = BoottimeInstant::now()
            .expect("read initial boot clock")
            .checked_add(Duration::from_secs(30))
            .expect("bounded initial deadline");
        lease_attempt.install_authority(initial_deadline, generation.clone());
        let observer = postgres_attempt.authority_observer();
        let mut session = tokio::time::timeout(
            Duration::from_secs(3),
            TargetFenceSession::connect_and_install(
                &socket_dir,
                &observer,
                &generation,
                Duration::from_secs(1),
                state.clone(),
                999,
                Some(boot_id.clone()),
            ),
        )
        .await
        .expect("target installation completed without a synchronous standby")
        .expect("install exact initial authority");
        assert_eq!(
            session.installed,
            Some(WritableAuthoritySnapshot {
                deadline: initial_deadline,
                generation: generation.clone(),
            })
        );
        let retained_commit_mode = session
            .connection
            .client
            .query_one("SHOW synchronous_commit", &[])
            .await
            .expect("read retained control-session commit mode")
            .get::<_, String>(0);
        assert_eq!(retained_commit_mode, "on");
        let initial_status = state
            .snapshot()
            .target_fence_acknowledgement
            .expect("initial target ACK status");
        assert!(initial_status.remaining_validity_at_ack_ms > 1_000);
        assert_eq!(initial_status.boot_id, boot_id);

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
        let renewed_status = state
            .snapshot()
            .target_fence_acknowledgement
            .expect("renewed target ACK status");
        assert!(
            renewed_status.remaining_validity_at_ack_ms
                > initial_status.remaining_validity_at_ack_ms
        );
        assert_eq!(renewed_status.boot_id, initial_status.boot_id);

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
        assert!(state.snapshot().target_fence_acknowledgement.is_none());
    }

    #[tokio::test]
    #[ignore = "requires a peer-authenticated PostgreSQL 18 target with an altered extension"]
    async fn live_postgres18_rejects_incompatible_extension_before_installation() {
        let socket_dir = PathBuf::from(
            std::env::var_os("PGSHARD_TARGET_FENCE_TEST_SOCKET")
                .expect("PGSHARD_TARGET_FENCE_TEST_SOCKET is required"),
        );
        let generation = durable_generation_for_test(1);
        let (lease_attempt, postgres_attempt) = writable_attempt_pair_for_test();
        let deadline = BoottimeInstant::now()
            .expect("read target deadline clock")
            .checked_add(Duration::from_secs(30))
            .expect("bounded target deadline");
        lease_attempt.install_authority(deadline, generation.clone());
        let observer = postgres_attempt.authority_observer();
        let state = AgentState::with_identity(
            AgentIdentity {
                cluster_id: "cluster-1".to_owned(),
                shard_id: pgshard_types::ShardId(0),
                instance_id: "cluster-1-shard-0-0".to_owned(),
            },
            60_000,
        )
        .expect("valid test state");
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            TargetFenceSession::connect_and_install(
                &socket_dir,
                &observer,
                &generation,
                Duration::from_secs(1),
                state,
                999,
                None,
            ),
        )
        .await
        .expect("incompatible target validation remained bounded");
        let Err(error) = result else {
            panic!("incompatible target installed authority");
        };
        assert!(matches!(
            error,
            PostgresFenceError::CatalogIdentityMismatch { .. }
        ));
    }
}
