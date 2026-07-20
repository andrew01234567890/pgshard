//! Fail-closed `PostgreSQL` 18 data-directory and process supervision.

use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::{self, DirBuilder, File, Metadata};
use std::future::Future;
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use pgshard_types::writable_generation::{
    WritableGenerationTransition, WritableGenerationTransitionError,
    classify_writable_generation_transition,
};
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, CWD, FlockOperation, Mode, OFlags, StatxFlags, flock, open, statx};
use rustix::process::{
    Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, WaitIdStatus, WaitOptions, geteuid, getpid,
    kill_process_group, pidfd_open, pidfd_send_signal, wait, waitid,
};
#[cfg(not(test))]
use rustix::process::{child_subreaper, set_child_subreaper};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, sleep, timeout};

use crate::domain::{AgentState, PostgresProcessState};
#[cfg(not(test))]
use crate::postgres_generation;
use crate::postgres_generation::{
    GenerationDurability, PostgresGenerationError, is_canonical_managed_member_name,
    validate_generation_durability,
};
use crate::postgres_recovery::{self, PostgresRecoveryError};
use crate::postgres_replication::{self, ReplicationEvidenceError};
#[cfg(test)]
use crate::writable::durable_generation_for_test;
use crate::writable::{DurableWritableGeneration, WritablePostgresAttempt};

const POSTGRES_MAJOR: &str = "18";
type AuthorityLossFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

struct AuthorityLossFutures {
    publication: Option<AuthorityLossFuture>,
    running: Option<AuthorityLossFuture>,
}
const PG_CONTROL_FILE_SIZE: u64 = 8_192;
const MAX_POSTGRES_LOCK_FILE_BYTES: u64 = 8_192;
const MAX_EXTERNAL_PID_FILE_BYTES: u64 = 64;
const MAX_POSTGRES_PATH_BYTES: usize = 1_023;
const SOCKET_LOCK_FILE: &str = ".s.PGSQL.5432.lock";
const EXTERNAL_PID_FILE: &str = "postmaster.external.pid";
const BOOTSTRAP_IDENTITY_FILE: &str = ".pgshard-bootstrap-complete";
const DURABLE_WRITABLE_GENERATION_FILE: &str = ".pgshard-writable-generation";
const DURABLE_WRITABLE_GENERATION_STAGING_FILE: &str = ".pgshard-writable-generation.next";
const MAX_BOOTSTRAP_IDENTITY_BYTES: u64 = 512;
const MAX_DURABLE_WRITABLE_GENERATION_BYTES: u64 = 1_024;
// Linux sockaddr_un.sun_path is 108 bytes. PostgreSQL requires the forced
// 14-byte `/.s.PGSQL.5432` suffix plus the directory to fit below that size.
const MAX_SOCKET_DIRECTORY_BYTES: usize = 93;
const MIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10);
const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(55);
const MAX_SHUTDOWN_BUDGET: Duration = Duration::from_secs(55);
const KILL_REAP_TIMEOUT: Duration = Duration::from_secs(1);
const TARGET_FENCE_CLEANUP_STAGES: u32 = 3;
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(30);
const WRITABLE_GENERATION_PUBLICATION_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_STANDBY_PASSFILE_BYTES: u64 = 4_096;
const QUARANTINE_HBA_CONTENT: &[u8] =
    b"local postgres postgres peer\nlocal all all reject\nlocal replication all reject\n";
const REPLICATION_BOOTSTRAP_PRIMARY_HBA_CONTENT: &[u8] = b"local postgres postgres peer\n\
local all all reject\n\
local replication all reject\n\
host replication pgshard_replication 0.0.0.0/0 scram-sha-256\n\
host replication pgshard_replication ::0/0 scram-sha-256\n\
host all all 0.0.0.0/0 reject\n\
host all all ::0/0 reject\n";

// A process forked while a test owns a writable fixture descriptor inherits
// that descriptor until exec, even with O_CLOEXEC. Serialize test fixture
// writes with every child-process creation in this unit-test binary.
#[cfg(test)]
static TEST_EXEC_HANDOFF: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
std::thread_local! {
    static TEST_EXEC_HANDOFF_OBSERVER:
        std::cell::RefCell<Option<std::sync::mpsc::SyncSender<()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
std::thread_local! {
    static TEST_POSTGRES_GENERATION_PUBLICATION_GATE:
        std::cell::RefCell<Option<watch::Receiver<bool>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn gate_next_postgres_generation_publication(receiver: watch::Receiver<bool>) {
    TEST_POSTGRES_GENERATION_PUBLICATION_GATE.with(|slot| {
        assert!(
            slot.borrow_mut().replace(receiver).is_none(),
            "test thread already has a PostgreSQL generation-publication gate"
        );
    });
}

#[cfg(test)]
fn test_exec_handoff_guard() -> std::sync::MutexGuard<'static, ()> {
    let observer = TEST_EXEC_HANDOFF_OBSERVER.with(|observer| observer.borrow_mut().take());
    if let Some(observer) = observer {
        let _ = observer.send(());
    }
    TEST_EXEC_HANDOFF
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
fn observe_next_test_exec_handoff(observer: std::sync::mpsc::SyncSender<()>) {
    TEST_EXEC_HANDOFF_OBSERVER.with(|slot| {
        assert!(
            slot.borrow_mut().replace(observer).is_none(),
            "test thread already has an exec-handoff observer"
        );
    });
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GenerationPublicationCheckpoint {
    StagingFileSynced,
    GenerationRenamed,
    DirectorySyncPending,
}

#[cfg(test)]
std::thread_local! {
    static TEST_GENERATION_PUBLICATION_FAULT:
        std::cell::Cell<Option<GenerationPublicationCheckpoint>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
struct GenerationPublicationFaultGuard;

#[cfg(test)]
impl Drop for GenerationPublicationFaultGuard {
    fn drop(&mut self) {
        TEST_GENERATION_PUBLICATION_FAULT.with(|fault| fault.set(None));
    }
}

#[cfg(test)]
fn inject_generation_publication_fault(
    checkpoint: GenerationPublicationCheckpoint,
) -> GenerationPublicationFaultGuard {
    TEST_GENERATION_PUBLICATION_FAULT.with(|fault| {
        assert!(
            fault.replace(Some(checkpoint)).is_none(),
            "test thread already has a generation-publication fault"
        );
    });
    GenerationPublicationFaultGuard
}

#[cfg(test)]
fn generation_publication_checkpoint(
    checkpoint: GenerationPublicationCheckpoint,
) -> Result<(), PostgresError> {
    let injected = TEST_GENERATION_PUBLICATION_FAULT.with(|fault| {
        if fault.get() == Some(checkpoint) {
            fault.set(None);
            true
        } else {
            false
        }
    });
    if injected {
        Err(PostgresError::InjectedGenerationPublicationFault)
    } else {
        Ok(())
    }
}

/// The only network/runtime roles the agent can supervise.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresRuntimeRole {
    /// No TCP listener and no replication ingress.
    Quarantine,
    /// Writable-Lease-fenced bootstrap source for physical clones.
    ReplicationBootstrapPrimary,
    /// TCP-closed physical standby that must remain in recovery.
    ReplicationStandby,
}

/// Typed upstream identity for a physical standby.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresStandbyConfig {
    primary_host: String,
    primary_port: u16,
    slot_name: String,
    passfile: PathBuf,
}

impl PostgresStandbyConfig {
    /// Creates an exact password-free primary connection description.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe host, non-canonical managed slot, zero
    /// port, or a passfile path that could not be embedded unambiguously.
    pub fn new(
        primary_host: String,
        primary_port: u16,
        slot_name: String,
        passfile: PathBuf,
    ) -> Result<Self, PostgresConfigError> {
        validate_primary_host(&primary_host)?;
        if primary_port == 0 {
            return Err(PostgresConfigError::InvalidPrimaryPort);
        }
        validate_managed_member_name(&slot_name)?;
        validate_absolute_normal_path("PostgreSQL replication passfile", &passfile, false)?;
        if !passfile
            .as_os_str()
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
        {
            return Err(PostgresConfigError::UnsafePassfilePath(passfile));
        }
        Ok(Self {
            primary_host,
            primary_port,
            slot_name,
            passfile,
        })
    }

    fn primary_conninfo(&self) -> String {
        format!(
            "host={} port={} user=pgshard_replication application_name={} passfile={} sslmode=disable",
            self.primary_host,
            self.primary_port,
            self.slot_name,
            self.passfile.display()
        )
    }
}

/// Configuration for an opt-in postmaster with a fail-closed runtime role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresConfig {
    role: PostgresRuntimeRole,
    standby: Option<PostgresStandbyConfig>,
    generation_durability: GenerationDurability,
    data_dir: PathBuf,
    executable: PathBuf,
    controldata_executable: PathBuf,
    socket_dir: PathBuf,
    hba_file: PathBuf,
    smart_shutdown_timeout: Duration,
    fast_shutdown_timeout: Duration,
    immediate_shutdown_timeout: Duration,
}

impl PostgresConfig {
    /// Creates a validated quarantine supervision configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe paths or a shutdown sequence that can exceed
    /// the bounded supervisor shutdown budget.
    pub fn new(
        data_dir: PathBuf,
        executable: PathBuf,
        socket_dir: PathBuf,
        hba_file: PathBuf,
        smart_shutdown_timeout: Duration,
        fast_shutdown_timeout: Duration,
        immediate_shutdown_timeout: Duration,
    ) -> Result<Self, PostgresConfigError> {
        Self::new_for_role(
            PostgresRuntimeRole::Quarantine,
            None,
            GenerationDurability::Local,
            data_dir,
            executable,
            socket_dir,
            hba_file,
            smart_shutdown_timeout,
            fast_shutdown_timeout,
            immediate_shutdown_timeout,
        )
    }

    /// Creates a validated replication-bootstrap-primary configuration.
    ///
    /// This role still requires exact writable-Lease authority at runtime. It
    /// opens `PostgreSQL` TCP only for the fixed `pgshard_replication` role and
    /// rejects every ordinary database connection in its immutable HBA file.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe paths or a shutdown sequence that can exceed
    /// the bounded supervisor shutdown budget.
    pub fn new_replication_bootstrap_primary(
        data_dir: PathBuf,
        executable: PathBuf,
        socket_dir: PathBuf,
        hba_file: PathBuf,
        smart_shutdown_timeout: Duration,
        fast_shutdown_timeout: Duration,
        immediate_shutdown_timeout: Duration,
    ) -> Result<Self, PostgresConfigError> {
        Self::new_for_role(
            PostgresRuntimeRole::ReplicationBootstrapPrimary,
            None,
            GenerationDurability::Local,
            data_dir,
            executable,
            socket_dir,
            hba_file,
            smart_shutdown_timeout,
            fast_shutdown_timeout,
            immediate_shutdown_timeout,
        )
    }

    /// Creates a validated TCP-closed replication-standby configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe paths, upstream identity, or shutdown bounds.
    #[allow(clippy::too_many_arguments)]
    pub fn new_replication_standby(
        standby: PostgresStandbyConfig,
        data_dir: PathBuf,
        executable: PathBuf,
        socket_dir: PathBuf,
        hba_file: PathBuf,
        smart_shutdown_timeout: Duration,
        fast_shutdown_timeout: Duration,
        immediate_shutdown_timeout: Duration,
    ) -> Result<Self, PostgresConfigError> {
        Self::new_for_role(
            PostgresRuntimeRole::ReplicationStandby,
            Some(standby),
            GenerationDurability::Local,
            data_dir,
            executable,
            socket_dir,
            hba_file,
            smart_shutdown_timeout,
            fast_shutdown_timeout,
            immediate_shutdown_timeout,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_role(
        role: PostgresRuntimeRole,
        standby: Option<PostgresStandbyConfig>,
        generation_durability: GenerationDurability,
        data_dir: PathBuf,
        executable: PathBuf,
        socket_dir: PathBuf,
        hba_file: PathBuf,
        smart_shutdown_timeout: Duration,
        fast_shutdown_timeout: Duration,
        immediate_shutdown_timeout: Duration,
    ) -> Result<Self, PostgresConfigError> {
        if (role == PostgresRuntimeRole::ReplicationStandby) != standby.is_some() {
            return Err(PostgresConfigError::InvalidStandbyComposition);
        }
        if role != PostgresRuntimeRole::ReplicationBootstrapPrimary
            && generation_durability != GenerationDurability::Local
        {
            return Err(PostgresConfigError::InvalidGenerationDurabilityComposition);
        }
        validate_generation_durability(&generation_durability)
            .map_err(|_| PostgresConfigError::InvalidGenerationDurabilityComposition)?;
        validate_absolute_normal_path("PGDATA", &data_dir, false)?;
        validate_absolute_normal_path("PostgreSQL executable", &executable, false)?;
        let controldata_executable = executable
            .parent()
            .map(|parent| parent.join("pg_controldata"))
            .ok_or_else(|| PostgresConfigError::UnsafePath {
                name: "PostgreSQL executable",
                path: executable.clone(),
            })?;
        validate_absolute_normal_path(
            "PostgreSQL control-data executable",
            &controldata_executable,
            false,
        )?;
        validate_absolute_normal_path("PostgreSQL socket directory", &socket_dir, true)?;
        validate_absolute_normal_path(hba_policy_name(role), &hba_file, false)?;
        if socket_dir.starts_with(&data_dir) || data_dir.starts_with(&socket_dir) {
            return Err(PostgresConfigError::OverlappingPaths {
                data_dir,
                socket_dir,
            });
        }
        if hba_file.starts_with(&data_dir) || hba_file.starts_with(&socket_dir) {
            return Err(PostgresConfigError::MutableHbaFile { hba_file });
        }
        if let Some(standby) = standby.as_ref()
            && (standby.passfile.starts_with(&data_dir)
                || standby.passfile.starts_with(&socket_dir))
        {
            return Err(PostgresConfigError::MutablePassfile {
                passfile: standby.passfile.clone(),
            });
        }
        for (name, value) in [
            ("smart", smart_shutdown_timeout),
            ("fast", fast_shutdown_timeout),
            ("immediate", immediate_shutdown_timeout),
        ] {
            if !(MIN_SHUTDOWN_TIMEOUT..=MAX_SHUTDOWN_TIMEOUT).contains(&value) {
                return Err(PostgresConfigError::InvalidShutdownTimeout { name, value });
            }
        }
        let total = smart_shutdown_timeout
            .checked_add(fast_shutdown_timeout)
            .and_then(|value| value.checked_add(immediate_shutdown_timeout))
            .and_then(|value| value.checked_add(KILL_REAP_TIMEOUT))
            .ok_or(PostgresConfigError::ShutdownBudgetOverflow)?;
        if total > MAX_SHUTDOWN_BUDGET {
            return Err(PostgresConfigError::ShutdownBudgetExceeded {
                requested: total,
                maximum: MAX_SHUTDOWN_BUDGET,
            });
        }
        Ok(Self {
            role,
            standby,
            generation_durability,
            data_dir,
            executable,
            controldata_executable,
            socket_dir,
            hba_file,
            smart_shutdown_timeout,
            fast_shutdown_timeout,
            immediate_shutdown_timeout,
        })
    }

    /// Returns the configured data directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn generation_durability(&self) -> &GenerationDurability {
        &self.generation_durability
    }

    fn synchronous_standby_names_argument(&self) -> String {
        format!(
            "synchronous_standby_names={}",
            self.generation_durability
                .synchronous_standby_names_setting()
        )
    }

    /// Returns whether starting this role requires writable-Lease authority.
    #[must_use]
    pub(crate) fn requires_writable_authority(&self) -> bool {
        self.role == PostgresRuntimeRole::ReplicationBootstrapPrimary
    }

    pub(crate) fn forbids_writable_authority(&self) -> bool {
        self.role == PostgresRuntimeRole::ReplicationStandby
    }

    fn is_replication_standby(&self) -> bool {
        self.role == PostgresRuntimeRole::ReplicationStandby
    }

    fn standby_member_slot_name(&self) -> Option<&str> {
        self.standby
            .as_ref()
            .map(|standby| standby.slot_name.as_str())
    }

    fn runtime_network_settings(
        &self,
    ) -> (
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        &'static str,
    ) {
        match self.role {
            PostgresRuntimeRole::Quarantine => (
                "listen_addresses=",
                Some("max_wal_senders=0"),
                None,
                "archive_mode=on",
            ),
            PostgresRuntimeRole::ReplicationBootstrapPrimary => (
                "listen_addresses=*",
                Some("max_wal_senders=5"),
                Some("max_replication_slots=5"),
                "archive_mode=off",
            ),
            PostgresRuntimeRole::ReplicationStandby => {
                ("listen_addresses=", None, None, "archive_mode=off")
            }
        }
    }

    fn starting_process_state(&self) -> PostgresProcessState {
        match self.role {
            PostgresRuntimeRole::Quarantine => PostgresProcessState::StartingQuarantined,
            PostgresRuntimeRole::ReplicationBootstrapPrimary => {
                PostgresProcessState::StartingReplicationBootstrap
            }
            PostgresRuntimeRole::ReplicationStandby => {
                PostgresProcessState::StartingReplicationStandby
            }
        }
    }

    fn running_process_state(&self) -> PostgresProcessState {
        match self.role {
            PostgresRuntimeRole::Quarantine => PostgresProcessState::RunningQuarantined,
            PostgresRuntimeRole::ReplicationBootstrapPrimary => {
                PostgresProcessState::RunningReplicationBootstrap
            }
            PostgresRuntimeRole::ReplicationStandby => {
                PostgresProcessState::RunningReplicationStandby
            }
        }
    }

    /// Returns the bounded signal and process-tree cleanup interval that must
    /// fit inside a writable Lease's post-renewal fencing margin.
    ///
    /// If the kernel cannot provide bounded process-absence proof, cleanup
    /// deliberately remains blocked beyond this interval and retains the
    /// PGDATA lock. The configured margin covers the normal immediate-stop,
    /// kill, and reap stages; it is not permission to release an inconclusive
    /// local storage fence.
    #[must_use]
    pub(crate) fn target_fence_budget(&self) -> Duration {
        self.immediate_shutdown_timeout
            .saturating_add(KILL_REAP_TIMEOUT.saturating_mul(TARGET_FENCE_CLEANUP_STAGES))
    }
}

/// Requested termination mode inside the supervised postmaster implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PostgresStopMode {
    /// Preserve `PostgreSQL`'s smart, fast, then immediate shutdown ordering.
    Graceful,
    /// Revoke local authority and immediately fence the complete process tree.
    Fence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PostgresStartDecision {
    Start,
    StartWritable(DurableWritableGeneration),
    Shutdown,
    AuthorityMissing,
}

#[derive(Debug, Eq, PartialEq)]
enum PostgresStartAuthorization {
    Direct,
    Writable(DurableWritableGeneration),
}

/// Offline-validated postmaster configuration ready to spawn.
#[derive(Debug)]
pub struct PreparedPostgres {
    config: PostgresConfig,
    validated: ValidatedPostgresState,
    supervisor_lock: SupervisorLock,
}

/// Linear proof that a writable `PostgreSQL` supervision attempt completed with
/// no remaining postmaster process tree.
///
/// The writable-term coordinator consumes this proof before it can clear an
/// exact Kubernetes Lease holder. The proof carries one half of the exact
/// single-use writable-attempt identity and is intentionally neither
/// constructible outside this crate nor cloneable.
#[derive(Debug)]
#[must_use]
pub(crate) struct WritablePostgresStopped {
    pub(crate) attempt: WritablePostgresAttempt,
}

#[derive(Debug)]
struct SupervisorLock {
    file: File,
    path: PathBuf,
    snapshot: FileSnapshot,
    expected_uid: u32,
    expected_mount_id: u64,
}

#[derive(Debug)]
struct ChildSubreaper {
    enabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirectChildProcess {
    pid: Pid,
    live: bool,
}

#[derive(Debug)]
struct PostgresProcessFence {
    // Owning the Tokio handle lets Drop synchronously reap through try_wait
    // before the supervisor lock field is released.
    child: Child,
    process_group: Option<Pid>,
    child_subreaper: ChildSubreaper,
    armed: bool,
    _supervisor_lock: SupervisorLock,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlDataState {
    StartingUp,
    ShutDown,
    ShutDownInRecovery,
    ShuttingDown,
    InCrashRecovery,
    InArchiveRecovery,
    InProduction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileSnapshot {
    device: u64,
    inode: u64,
    size: u64,
    mode: u32,
    owner: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ValidatedPostgresState {
    data: ValidatedDataDir,
    executable: FileSnapshot,
    controldata_executable: FileSnapshot,
    control_data_state: ControlDataState,
    socket_dir: FileSnapshot,
    socket_lock: Option<PostmasterLockSnapshot>,
    external_pid_file: Option<FileSnapshot>,
    hba_file: FileSnapshot,
    standby_passfile: Option<FileSnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ValidatedDataDir {
    data_dir: FileSnapshot,
    mount_id: u64,
    version_file: FileSnapshot,
    global_directory: FileSnapshot,
    control_file: FileSnapshot,
    wal_directory: FileSnapshot,
    tablespace_directory: FileSnapshot,
    postmaster_lock: Option<PostmasterLockSnapshot>,
    standby_signal: Option<FileSnapshot>,
}

#[derive(Debug)]
struct ManagedGenerationFile {
    file: File,
    snapshot: FileSnapshot,
    contents: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PostmasterLockSnapshot {
    file: FileSnapshot,
    pid: u32,
}

impl SupervisorLock {
    fn acquire(data_dir: &Path) -> Result<Self, PostgresError> {
        let expected_uid = geteuid().as_raw();
        let path_metadata = validate_owned_directory("PGDATA", data_dir, expected_uid)?;
        let expected_mount_id = mount_id("PGDATA", data_dir)?;
        let path = data_dir.to_owned();
        let fd = open(
            &path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| PostgresError::OpenSupervisorLock {
            path: path.clone(),
            source: source.into(),
        })?;
        let file = File::from(fd);
        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(source) if source == rustix::io::Errno::WOULDBLOCK => {
                return Err(PostgresError::SupervisorLockHeld { path });
            }
            Err(source) => {
                return Err(PostgresError::AcquireSupervisorLock {
                    path,
                    source: source.into(),
                });
            }
        }
        let metadata = file.metadata().map_err(|source| PostgresError::Metadata {
            name: "PostgreSQL supervisor lock",
            path: path.clone(),
            source,
        })?;
        let snapshot = file_snapshot(&path_metadata);
        if file_snapshot(&metadata) != snapshot {
            return Err(PostgresError::PreparedStateChanged);
        }
        let lock = Self {
            file,
            path,
            snapshot,
            expected_uid,
            expected_mount_id,
        };
        lock.validate()?;
        Ok(lock)
    }

    fn validate(&self) -> Result<(), PostgresError> {
        let fd_metadata = self
            .file
            .metadata()
            .map_err(|source| PostgresError::Metadata {
                name: "PostgreSQL supervisor lock",
                path: self.path.clone(),
                source,
            })?;
        let path_metadata = validate_owned_directory("PGDATA", &self.path, self.expected_uid)?;
        require_same_mount(
            self.expected_mount_id,
            "PostgreSQL supervisor lock",
            &self.path,
        )?;
        if file_snapshot(&fd_metadata) != self.snapshot
            || file_snapshot(&path_metadata) != self.snapshot
        {
            return Err(PostgresError::PreparedStateChanged);
        }
        Ok(())
    }

    fn validate_identity(&self) -> Result<(), PostgresError> {
        let fd_metadata = self
            .file
            .metadata()
            .map_err(|source| PostgresError::Metadata {
                name: "PostgreSQL supervisor lock",
                path: self.path.clone(),
                source,
            })?;
        let path_metadata = validate_owned_directory("PGDATA", &self.path, self.expected_uid)?;
        require_same_mount(
            self.expected_mount_id,
            "PostgreSQL supervisor lock",
            &self.path,
        )?;
        if !snapshot_has_file_identity(self.snapshot, &fd_metadata)
            || !snapshot_has_file_identity(self.snapshot, &path_metadata)
            || !same_file_identity(&fd_metadata, &path_metadata)
        {
            return Err(PostgresError::PreparedStateChanged);
        }
        Ok(())
    }
}

impl ChildSubreaper {
    #[cfg(not(test))]
    fn claim() -> Result<Self, PostgresError> {
        set_child_subreaper(Some(Pid::INIT))
            .map_err(|source| PostgresError::ConfigureChildSubreaper(source.into()))?;
        if child_subreaper()
            .map_err(|source| PostgresError::InspectChildSubreaper(source.into()))?
            .is_none()
        {
            return Err(PostgresError::ChildSubreaperNotEnabled);
        }
        if let Some(child) = direct_child_processes()?.first() {
            return Err(PostgresError::ExistingChildProcess {
                pid: child.pid.as_raw_pid(),
            });
        }
        Ok(Self { enabled: true })
    }

    // Unit tests share one process and can run child-spawning cases in
    // parallel. Process-level tests exercise the production subreaper path in
    // an isolated agent process.
    #[cfg(test)]
    #[allow(clippy::unnecessary_wraps)]
    fn claim() -> Result<Self, PostgresError> {
        Ok(Self { enabled: false })
    }
}

impl PostgresProcessFence {
    fn new(child: Child, child_subreaper: ChildSubreaper, supervisor_lock: SupervisorLock) -> Self {
        Self {
            child,
            process_group: None,
            child_subreaper,
            armed: true,
            _supervisor_lock: supervisor_lock,
        }
    }

    fn set_process_group(&mut self, process_group: Pid) {
        self.process_group = Some(process_group);
    }

    fn disarm_if_reaped(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            self.armed = false;
        }
    }
}

impl Drop for PostgresProcessFence {
    fn drop(&mut self) {
        if self.armed {
            fence_child_on_drop(&mut self.child, self.process_group, &self.child_subreaper);
        }
    }
}

impl PreparedPostgres {
    /// Structurally preflights the executable, reject-only HBA policy, and
    /// required `PostgreSQL` 18 disk state, rejects role-aware recovery modes,
    /// then creates or validates the private socket directory.
    ///
    /// # Errors
    ///
    /// Returns an error before process creation for missing, incompatible,
    /// symlinked, wrong-owner, runtime-writable, or malformed state. This is a
    /// point-in-time check of paths supplied by a trusted deployment; it is not
    /// a sandbox against an actor concurrently mutating those paths.
    pub fn prepare(config: PostgresConfig) -> Result<Self, PostgresError> {
        let supervisor_lock = SupervisorLock::acquire(&config.data_dir)?;
        let validated = validate_prepared_state(&config, true)?;
        supervisor_lock.validate()?;
        Ok(Self {
            config,
            validated,
            supervisor_lock,
        })
    }

    /// Runs the postmaster as a directly supervised child with client TCP and
    /// replication ingress disabled.
    ///
    /// An unexpected child exit is terminal. Requested shutdown first uses
    /// `PostgreSQL` smart shutdown, then fast shutdown, then immediate shutdown,
    /// and finally a kernel kill if all bounded waits expire. Linux exit status
    /// is observed without reaping the group leader; the PID remains reserved
    /// until descendant cleanup completes. The dedicated agent is a Linux
    /// child subreaper so `PostgreSQL` children that create their own sessions
    /// are adopted, pidfd-killed, and reaped after the postmaster exits.
    /// Dropping this future after spawn synchronously retains the PGDATA fence
    /// through the same complete process-tree proof.
    ///
    /// # Errors
    ///
    /// Returns an error if the child cannot start, cannot be tracked by a Linux
    /// pidfd, exits unexpectedly, or does not stop cleanly within the budget.
    pub async fn supervise(
        self,
        state: AgentState,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), PostgresError> {
        if self.config.requires_writable_authority() {
            state.clear_lease();
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(PostgresError::WritableAuthorityRequired);
        }
        state.clear_lease();
        self.supervise_with_stop_mode_and_start_guard(
            state,
            async {
                shutdown.await;
                PostgresStopMode::Graceful
            },
            || PostgresStartDecision::Start,
            None,
        )
        .await
    }

    /// Runs the postmaster only after attempt-private writable-term startup
    /// authority is proven.
    ///
    /// The guard is evaluated at the final user-space boundary before process
    /// creation. Every requested shutdown revokes local Lease evidence and
    /// immediately fences the complete process tree, skipping smart and fast
    /// waits that can outlive the Lease's fencing margin.
    ///
    /// # Errors
    ///
    /// Returns an error if startup authority is absent or if validation,
    /// process creation, supervision, or target fencing fails.
    pub(crate) async fn supervise_with_writable_authority(
        self,
        state: AgentState,
        shutdown: watch::Receiver<bool>,
        required_margin: Duration,
        attempt: WritablePostgresAttempt,
    ) -> Result<WritablePostgresStopped, PostgresError> {
        if self.config.forbids_writable_authority() {
            state.clear_lease();
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(PostgresError::WritableAuthorityForbidden);
        }
        let required_margin = required_margin.max(self.config.target_fence_budget());
        let authority = attempt.authority_observer();
        let publication_expiry = attempt.authority_observer();
        let running_expiry = attempt.authority_observer();
        self.supervise_with_writable_authority_guard_and_loss(
            state,
            shutdown,
            move || authority.generation_valid_for(required_margin),
            Some(AuthorityLossFutures {
                publication: Some(
                    publication_expiry.wait_until_current_generation_invalid(required_margin),
                ),
                running: Some(
                    running_expiry.wait_until_current_generation_invalid(required_margin),
                ),
            }),
            attempt,
        )
        .await
    }

    #[cfg(test)]
    async fn supervise_with_writable_authority_guard<G>(
        self,
        state: AgentState,
        shutdown: watch::Receiver<bool>,
        startup_guard: G,
        attempt: WritablePostgresAttempt,
    ) -> Result<WritablePostgresStopped, PostgresError>
    where
        G: Fn() -> Option<DurableWritableGeneration>,
    {
        self.supervise_with_writable_authority_guard_and_loss(
            state,
            shutdown,
            startup_guard,
            None,
            attempt,
        )
        .await
    }

    async fn supervise_with_writable_authority_guard_and_loss<G>(
        self,
        state: AgentState,
        mut shutdown: watch::Receiver<bool>,
        startup_guard: G,
        authority_loss: Option<AuthorityLossFutures>,
        attempt: WritablePostgresAttempt,
    ) -> Result<WritablePostgresStopped, PostgresError>
    where
        G: Fn() -> Option<DurableWritableGeneration>,
    {
        if self.config.forbids_writable_authority() {
            state.clear_lease();
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(PostgresError::WritableAuthorityForbidden);
        }
        let shutdown_state = state.clone();
        let final_shutdown = shutdown.clone();
        self.supervise_with_stop_mode_and_start_guard(
            state,
            async move {
                loop {
                    if watch_shutdown_requested(&mut shutdown) || shutdown.changed().await.is_err()
                    {
                        break;
                    }
                }
                shutdown_state.clear_lease();
                PostgresStopMode::Fence
            },
            move || {
                let generation = startup_guard();
                if watch_shutdown_observed(&final_shutdown) {
                    PostgresStartDecision::Shutdown
                } else if let Some(generation) = generation {
                    PostgresStartDecision::StartWritable(generation)
                } else {
                    PostgresStartDecision::AuthorityMissing
                }
            },
            authority_loss,
        )
        .await?;
        Ok(WritablePostgresStopped { attempt })
    }

    async fn supervise_with_stop_mode_and_start_guard<G>(
        self,
        state: AgentState,
        shutdown: impl Future<Output = PostgresStopMode>,
        startup_guard: G,
        mut authority_loss: Option<AuthorityLossFutures>,
    ) -> Result<(), PostgresError>
    where
        G: Fn() -> PostgresStartDecision,
    {
        state.clear_replication_evidence();
        tokio::pin!(shutdown);
        if shutdown_requested(shutdown.as_mut()).await {
            state.set_postgres_process(PostgresProcessState::Validated);
            return Ok(());
        }
        let config = self.config.clone();
        let mut validation =
            tokio::task::spawn_blocking(move || validate_prepared_state(&config, false));
        let current = match await_validation(&mut validation, shutdown.as_mut()).await {
            Ok(Some(current)) => current,
            Ok(None) => {
                state.set_postgres_process(PostgresProcessState::Validated);
                return Ok(());
            }
            Err(error) => return fail_postgres_start(&state, error),
        };
        if let Err(error) = self.supervisor_lock.validate() {
            return fail_postgres_start(&state, error);
        }
        if current != self.validated {
            return fail_postgres_start(&state, PostgresError::PreparedStateChanged);
        }
        tokio::task::yield_now().await;
        if shutdown_requested(shutdown.as_mut()).await {
            state.set_postgres_process(PostgresProcessState::Validated);
            return Ok(());
        }
        if let Err(error) = self.finalize_pre_spawn(&current) {
            return fail_postgres_start(&state, error);
        }
        tokio::task::yield_now().await;
        if shutdown_requested(shutdown.as_mut()).await {
            state.set_postgres_process(PostgresProcessState::Validated);
            return Ok(());
        }
        let shutdown_config = self.config.clone();
        let socket_dir = self.config.socket_dir.clone();
        let Some((mut process_group_fence, pidfd, process_group, authorization)) = self
            .spawn_tracked_postmaster(&state, &startup_guard)
            .await?
        else {
            return Ok(());
        };

        let source_generation =
            if let PostgresStartAuthorization::Writable(generation) = authorization {
                let publication_authority_loss = authority_loss
                    .as_mut()
                    .and_then(|authority_loss| authority_loss.publication.take());
                let publication = publish_generation_before_running(
                    &state,
                    &mut process_group_fence,
                    &pidfd,
                    process_group,
                    shutdown.as_mut(),
                    &shutdown_config,
                    &socket_dir,
                    &generation,
                    &startup_guard,
                    publication_authority_loss,
                )
                .await;
                match publication {
                    Ok(WritablePublicationOutcome::Published) => {}
                    Ok(WritablePublicationOutcome::Stopped) => {
                        process_group_fence.disarm_if_reaped();
                        return Ok(());
                    }
                    Err(error) => {
                        process_group_fence.disarm_if_reaped();
                        return Err(error);
                    }
                }
                Some(generation)
            } else {
                None
            };
        let running_authority_loss = authority_loss
            .as_mut()
            .and_then(|authority_loss| authority_loss.running.take());
        let result = supervise_running_postmaster(
            &state,
            &mut process_group_fence,
            &pidfd,
            process_group,
            &shutdown_config,
            shutdown.as_mut(),
            source_generation.as_ref(),
            &startup_guard,
            running_authority_loss,
        )
        .await;
        process_group_fence.disarm_if_reaped();
        result
    }

    fn finalize_pre_spawn(&self, current: &ValidatedPostgresState) -> Result<(), PostgresError> {
        for (path, lock) in [
            (
                self.config.data_dir.join("postmaster.pid"),
                current.data.postmaster_lock,
            ),
            (
                self.config.socket_dir.join(SOCKET_LOCK_FILE),
                current.socket_lock,
            ),
        ] {
            rewrite_agent_thread_lock(&path, lock)?;
        }
        revalidate_external_pid_file(
            &self.config.socket_dir.join(EXTERNAL_PID_FILE),
            current.external_pid_file,
        )
    }

    fn persist_durable_writable_generation(
        &self,
        generation: &DurableWritableGeneration,
    ) -> Result<(), PostgresError> {
        self.supervisor_lock.validate_identity()?;
        let generation_path = self.config.data_dir.join(DURABLE_WRITABLE_GENERATION_FILE);
        let contents = generation.canonical_bytes();
        if DurableWritableGeneration::parse_canonical(&contents).as_ref() != Some(generation) {
            return Err(PostgresError::InvalidRequestedWritableGeneration);
        }
        let (bootstrap_path, bootstrap) = self.validate_writable_bootstrap(generation)?;
        let staging_path = self
            .config
            .data_dir
            .join(DURABLE_WRITABLE_GENERATION_STAGING_FILE);
        self.remove_interrupted_generation_staging(&staging_path)?;

        let existing = self.read_generation_file(&generation_path)?;
        let existing_snapshot = existing.as_ref().map(|file| file.snapshot);
        if let Some(existing) = existing.as_ref()
            && self.existing_generation_is_current(generation, &generation_path, existing)?
        {
            revalidate_managed_generation_file(
                "PostgreSQL bootstrap identity",
                &bootstrap_path,
                &bootstrap,
            )?;
            return Ok(());
        }
        drop(existing);

        let staging = self.create_generation_staging(&staging_path, &contents)?;
        #[cfg(test)]
        generation_publication_checkpoint(GenerationPublicationCheckpoint::StagingFileSynced)?;
        self.publish_generation_staging(
            &staging_path,
            &generation_path,
            existing_snapshot,
            &contents,
            &staging,
        )?;
        revalidate_managed_generation_file(
            "PostgreSQL bootstrap identity",
            &bootstrap_path,
            &bootstrap,
        )
    }

    fn validate_writable_bootstrap(
        &self,
        generation: &DurableWritableGeneration,
    ) -> Result<(PathBuf, ManagedGenerationFile), PostgresError> {
        let expected_uid = self.supervisor_lock.expected_uid;
        let mount_id = self.validated.data.mount_id;
        let bootstrap_path = self.config.data_dir.join(BOOTSTRAP_IDENTITY_FILE);
        let bootstrap = read_managed_generation_file(
            "PostgreSQL bootstrap identity",
            &bootstrap_path,
            expected_uid,
            mount_id,
            MAX_BOOTSTRAP_IDENTITY_BYTES,
        )?
        .ok_or_else(|| PostgresError::BootstrapIdentityMissing {
            path: bootstrap_path.clone(),
        })?;
        if bootstrap.contents != generation.bootstrap_identity_bytes() {
            return Err(PostgresError::BootstrapIdentityMismatch {
                path: bootstrap_path,
            });
        }
        Ok((bootstrap_path, bootstrap))
    }

    fn remove_interrupted_generation_staging(
        &self,
        staging_path: &Path,
    ) -> Result<(), PostgresError> {
        if let Some(staging) = read_managed_generation_file(
            "durable writable-generation staging file",
            staging_path,
            self.supervisor_lock.expected_uid,
            self.validated.data.mount_id,
            MAX_DURABLE_WRITABLE_GENERATION_BYTES,
        )? {
            revalidate_managed_generation_file(
                "durable writable-generation staging file",
                staging_path,
                &staging,
            )?;
            drop(staging.file);
            fs::remove_file(staging_path).map_err(|source| {
                PostgresError::PersistWritableGeneration {
                    operation: "remove interrupted staging file",
                    path: staging_path.to_owned(),
                    source,
                }
            })?;
            self.sync_generation_directory("persist staging cleanup")?;
        }
        Ok(())
    }

    fn read_generation_file(
        &self,
        generation_path: &Path,
    ) -> Result<Option<ManagedGenerationFile>, PostgresError> {
        read_managed_generation_file(
            "durable writable-generation file",
            generation_path,
            self.supervisor_lock.expected_uid,
            self.validated.data.mount_id,
            MAX_DURABLE_WRITABLE_GENERATION_BYTES,
        )
    }

    fn existing_generation_is_current(
        &self,
        requested: &DurableWritableGeneration,
        path: &Path,
        existing: &ManagedGenerationFile,
    ) -> Result<bool, PostgresError> {
        let durable =
            DurableWritableGeneration::parse_canonical(&existing.contents).ok_or_else(|| {
                PostgresError::InvalidWritableGeneration {
                    path: path.to_owned(),
                }
            })?;
        match classify_writable_generation_transition(Some(&durable), requested) {
            Ok(WritableGenerationTransition::Advance) => return Ok(false),
            Ok(WritableGenerationTransition::Replay) => {}
            Ok(WritableGenerationTransition::Initialize) => {
                return Err(PostgresError::PreparedStateChanged);
            }
            Err(WritableGenerationTransitionError::ForeignUniverse) => {
                return Err(PostgresError::ForeignWritableGeneration {
                    path: path.to_owned(),
                });
            }
            Err(WritableGenerationTransitionError::Regression { durable, requested }) => {
                return Err(PostgresError::WritableGenerationRegression { durable, requested });
            }
            Err(WritableGenerationTransitionError::ConflictingHolder { term }) => {
                return Err(PostgresError::WritableGenerationConflict { term });
            }
        }
        existing
            .file
            .sync_all()
            .map_err(|source| PostgresError::PersistWritableGeneration {
                operation: "flush existing generation",
                path: path.to_owned(),
                source,
            })?;
        revalidate_managed_generation_file("durable writable-generation file", path, existing)?;
        self.sync_generation_directory("complete existing generation barrier")?;
        Ok(true)
    }

    fn create_generation_staging(
        &self,
        staging_path: &Path,
        contents: &[u8],
    ) -> Result<ManagedGenerationFile, PostgresError> {
        let fd = open(
            staging_path,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|source| PostgresError::PersistWritableGeneration {
            operation: "create staging file",
            path: staging_path.to_owned(),
            source: source.into(),
        })?;
        let mut staging_file = File::from(fd);
        staging_file
            .write_all(contents)
            .and_then(|()| staging_file.set_permissions(fs::Permissions::from_mode(0o600)))
            .and_then(|()| staging_file.sync_all())
            .map_err(|source| PostgresError::PersistWritableGeneration {
                operation: "write and flush staging file",
                path: staging_path.to_owned(),
                source,
            })?;
        let staging = read_managed_generation_file(
            "durable writable-generation staging file",
            staging_path,
            self.supervisor_lock.expected_uid,
            self.validated.data.mount_id,
            MAX_DURABLE_WRITABLE_GENERATION_BYTES,
        )?
        .ok_or(PostgresError::PreparedStateChanged)?;
        let staging_metadata =
            staging_file
                .metadata()
                .map_err(|source| PostgresError::Metadata {
                    name: "durable writable-generation staging file",
                    path: staging_path.to_owned(),
                    source,
                })?;
        let staging_path_metadata =
            strict_metadata("durable writable-generation staging file", staging_path)?;
        if staging.contents != contents
            || !same_file_identity(&staging_metadata, &staging_path_metadata)
            || staging.snapshot != file_snapshot(&staging_metadata)
        {
            return Err(PostgresError::PreparedStateChanged);
        }
        Ok(staging)
    }

    fn publish_generation_staging(
        &self,
        staging_path: &Path,
        generation_path: &Path,
        existing_snapshot: Option<FileSnapshot>,
        contents: &[u8],
        staging: &ManagedGenerationFile,
    ) -> Result<(), PostgresError> {
        let current = self.read_generation_file(generation_path)?;
        if current.as_ref().map(|file| file.snapshot) != existing_snapshot {
            return Err(PostgresError::PreparedStateChanged);
        }
        fs::rename(staging_path, generation_path).map_err(|source| {
            PostgresError::PersistWritableGeneration {
                operation: "publish generation",
                path: generation_path.to_owned(),
                source,
            }
        })?;
        #[cfg(test)]
        generation_publication_checkpoint(GenerationPublicationCheckpoint::GenerationRenamed)?;
        let installed = self
            .read_generation_file(generation_path)?
            .ok_or(PostgresError::PreparedStateChanged)?;
        let staging_metadata =
            staging
                .file
                .metadata()
                .map_err(|source| PostgresError::Metadata {
                    name: "durable writable-generation staging file",
                    path: staging_path.to_owned(),
                    source,
                })?;
        let installed_metadata =
            installed
                .file
                .metadata()
                .map_err(|source| PostgresError::Metadata {
                    name: "durable writable-generation file",
                    path: generation_path.to_owned(),
                    source,
                })?;
        if installed.contents != contents
            || !same_file_identity(&installed_metadata, &staging_metadata)
        {
            return Err(PostgresError::PreparedStateChanged);
        }
        #[cfg(test)]
        generation_publication_checkpoint(GenerationPublicationCheckpoint::DirectorySyncPending)?;
        self.sync_generation_directory("publish generation")?;
        revalidate_managed_generation_file(
            "durable writable-generation file",
            generation_path,
            &installed,
        )
    }

    fn sync_generation_directory(&self, operation: &'static str) -> Result<(), PostgresError> {
        self.supervisor_lock.validate_identity()?;
        self.supervisor_lock.file.sync_all().map_err(|source| {
            PostgresError::PersistWritableGeneration {
                operation,
                path: self.config.data_dir.clone(),
                source,
            }
        })?;
        self.supervisor_lock.validate_identity()
    }

    fn authorize_persisted_postmaster_start(
        &self,
        state: &AgentState,
        startup_guard: &impl Fn() -> PostgresStartDecision,
    ) -> Result<Option<PostgresStartAuthorization>, PostgresError> {
        let Some(authorization) = authorize_postmaster_start(state, startup_guard)? else {
            return Ok(None);
        };
        let PostgresStartAuthorization::Writable(generation) = &authorization else {
            return Ok(Some(authorization));
        };
        if let Err(error) = self.persist_durable_writable_generation(generation) {
            state.clear_lease();
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(error);
        }
        match authorize_postmaster_start(state, startup_guard)? {
            Some(PostgresStartAuthorization::Writable(current)) if &current == generation => {
                Ok(Some(PostgresStartAuthorization::Writable(current)))
            }
            None => Ok(None),
            Some(_) => {
                state.clear_lease();
                state.set_postgres_process(PostgresProcessState::Failed);
                Err(PostgresError::StartupAuthorityChanged)
            }
        }
    }

    async fn spawn_tracked_postmaster(
        self,
        state: &AgentState,
        startup_guard: &impl Fn() -> PostgresStartDecision,
    ) -> Result<
        Option<(
            PostgresProcessFence,
            AsyncFd<OwnedFd>,
            Pid,
            PostgresStartAuthorization,
        )>,
        PostgresError,
    > {
        let child_subreaper = match ChildSubreaper::claim() {
            Ok(child_subreaper) => child_subreaper,
            Err(error) => {
                state.set_postgres_process(PostgresProcessState::Failed);
                return Err(error);
            }
        };
        let (spawn_result, authorization) = {
            #[cfg(test)]
            let _exec_handoff = test_exec_handoff_guard();
            let Some(authorization) =
                self.authorize_persisted_postmaster_start(state, startup_guard)?
            else {
                return Ok(None);
            };
            (self.command().spawn(), authorization)
        };
        let child = match spawn_result {
            Ok(child) => child,
            Err(source) => {
                state.set_postgres_process(PostgresProcessState::Failed);
                return Err(PostgresError::Spawn {
                    executable: self.config.executable.clone(),
                    source,
                });
            }
        };
        state.set_postgres_process(self.config.starting_process_state());
        let mut process_group_fence =
            PostgresProcessFence::new(child, child_subreaper, self.supervisor_lock);
        let Some(child_id) = process_group_fence.child.id() else {
            return Err(cleanup_spawn_failure(
                state,
                &mut process_group_fence.child,
                None,
                &process_group_fence.child_subreaper,
                PostgresError::MissingChildPid,
            )
            .await);
        };
        let Ok(raw_pid) = i32::try_from(child_id) else {
            return Err(cleanup_spawn_failure(
                state,
                &mut process_group_fence.child,
                None,
                &process_group_fence.child_subreaper,
                PostgresError::InvalidChildPid,
            )
            .await);
        };
        let Some(pid) = Pid::from_raw(raw_pid) else {
            return Err(cleanup_spawn_failure(
                state,
                &mut process_group_fence.child,
                None,
                &process_group_fence.child_subreaper,
                PostgresError::InvalidChildPid,
            )
            .await);
        };
        process_group_fence.set_process_group(pid);
        let pidfd = match pidfd_open(pid, PidfdFlags::empty()) {
            Ok(pidfd) => pidfd,
            Err(source) => {
                let error = cleanup_spawn_failure(
                    state,
                    &mut process_group_fence.child,
                    Some(pid),
                    &process_group_fence.child_subreaper,
                    PostgresError::OpenPidfd {
                        pid: raw_pid,
                        source: source.into(),
                    },
                )
                .await;
                process_group_fence.disarm_if_reaped();
                return Err(error);
            }
        };
        let pidfd = match AsyncFd::with_interest(pidfd, Interest::READABLE) {
            Ok(pidfd) => pidfd,
            Err(source) => {
                let error = cleanup_spawn_failure(
                    state,
                    &mut process_group_fence.child,
                    Some(pid),
                    &process_group_fence.child_subreaper,
                    PostgresError::MonitorPidfd {
                        pid: raw_pid,
                        source,
                    },
                )
                .await;
                process_group_fence.disarm_if_reaped();
                return Err(error);
            }
        };
        Ok(Some((process_group_fence, pidfd, pid, authorization)))
    }

    fn command(&self) -> Command {
        let (listen_addresses, max_wal_senders, max_replication_slots, archive_mode) =
            self.config.runtime_network_settings();
        let data_directory = setting_with_path("data_directory=", &self.config.data_dir);
        let hba_file = setting_with_path("hba_file=", &self.config.hba_file);
        let mut external_pid_file = OsString::from("external_pid_file=");
        external_pid_file.push(self.config.socket_dir.join(EXTERNAL_PID_FILE));
        let mut command = Command::new(&self.config.executable);
        command
            .arg("-D")
            .arg(&self.config.data_dir)
            .arg("-c")
            .arg(data_directory)
            .arg("-c")
            .arg(hba_file)
            .arg("-c")
            .arg(external_pid_file)
            .arg("-c")
            .arg(listen_addresses)
            .arg("-c")
            .arg(format!(
                "unix_socket_directories={}",
                self.config.socket_dir.display()
            ))
            .arg("-c")
            .arg("unix_socket_permissions=0700")
            .arg("-c")
            .arg("unix_socket_group=")
            .arg("-c")
            .arg("port=5432")
            .arg("-c")
            .arg("ssl=off")
            .arg("-c")
            .arg("restart_after_crash=off");
        force_role_recovery_settings(&mut command, self.config.standby.as_ref());
        command
            .arg("-c")
            .arg("restore_command=")
            .arg("-c")
            .arg("archive_cleanup_command=")
            .arg("-c")
            .arg("recovery_end_command=")
            .arg("-c")
            // Quarantine preserves existing `.ready` archive state without
            // executing deployment callbacks. The bootstrap source disables
            // archiving until a verified pipeline exists, avoiding unbounded
            // WAL retention while physical clones are created.
            .arg(archive_mode)
            .arg("-c")
            .arg("archive_command=")
            .arg("-c")
            .arg("archive_library=");
        append_optional_postmaster_setting(&mut command, max_wal_senders);
        append_optional_postmaster_setting(&mut command, max_replication_slots);
        command
            .arg("-c")
            .arg("wal_level=logical")
            .arg("-c")
            .arg(self.config.synchronous_standby_names_argument())
            .arg("-c")
            .arg("synchronous_commit=local")
            .arg("-c")
            .arg("max_logical_replication_workers=0")
            .arg("-c")
            .arg("sync_replication_slots=off")
            .arg("-c")
            .arg("wal_receiver_create_temp_slot=off")
            .arg("-c")
            .arg("idle_replication_slot_timeout=0")
            .arg("-c")
            .arg("max_slot_wal_keep_size=-1")
            .arg("-c")
            .arg("shared_preload_libraries=");
        force_generation_publication_settings(&mut command);
        command
            .arg("-c")
            .arg("fsync=on")
            .arg("-c")
            .arg("full_page_writes=on")
            .arg("-c")
            .arg("ignore_invalid_pages=off")
            .arg("-c")
            .arg("data_sync_retry=off")
            .arg("-c")
            .arg("ignore_checksum_failure=off")
            .arg("-c")
            .arg("zero_damaged_pages=off")
            .arg("-c")
            .arg("logging_collector=off")
            .current_dir(&self.config.data_dir)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        command.as_std_mut().process_group(0);
        for name in ["LANG", "LC_ALL", "TZ"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command
    }
}

fn append_optional_postmaster_setting(command: &mut Command, setting: Option<&str>) {
    if let Some(setting) = setting {
        command.arg("-c").arg(setting);
    }
}

fn force_role_recovery_settings(command: &mut Command, standby: Option<&PostgresStandbyConfig>) {
    let (primary_conninfo, primary_slot_name, read_only, feedback) = match standby {
        Some(standby) => (
            format!("primary_conninfo={}", standby.primary_conninfo()),
            format!("primary_slot_name={}", standby.slot_name),
            "default_transaction_read_only=on",
            "hot_standby_feedback=on",
        ),
        None => (
            "primary_conninfo=".to_owned(),
            "primary_slot_name=".to_owned(),
            "default_transaction_read_only=off",
            "hot_standby_feedback=off",
        ),
    };
    for setting in [
        primary_conninfo,
        primary_slot_name,
        "recovery_target_action=shutdown".to_owned(),
        "recovery_target_timeline=latest".to_owned(),
        "hot_standby=on".to_owned(),
        feedback.to_owned(),
        read_only.to_owned(),
        "wal_receiver_status_interval=1s".to_owned(),
        "wal_receiver_timeout=5s".to_owned(),
        "wal_retrieve_retry_interval=100ms".to_owned(),
    ] {
        command.arg("-c").arg(setting);
    }
}

fn setting_with_path(prefix: &str, path: &Path) -> OsString {
    let mut setting = OsString::from(prefix);
    setting.push(path);
    setting
}

fn fail_postgres_start<T>(state: &AgentState, error: PostgresError) -> Result<T, PostgresError> {
    state.set_postgres_process(PostgresProcessState::Failed);
    Err(error)
}

fn force_generation_publication_settings(command: &mut Command) {
    for setting in [
        "session_preload_libraries=",
        "local_preload_libraries=",
        "event_triggers=off",
        "jit=off",
        "log_statement=none",
        "log_min_error_statement=panic",
        "log_parameter_max_length=0",
        "log_parameter_max_length_on_error=0",
    ] {
        command.arg("-c").arg(setting);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WritablePublicationOutcome {
    Published,
    Stopped,
}

#[allow(clippy::too_many_arguments)]
async fn publish_generation_before_running<F, G>(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    mut shutdown: Pin<&mut F>,
    shutdown_config: &PostgresConfig,
    socket_dir: &Path,
    generation: &DurableWritableGeneration,
    startup_guard: &G,
    authority_deadline: Option<AuthorityLossFuture>,
) -> Result<WritablePublicationOutcome, PostgresError>
where
    F: Future<Output = PostgresStopMode>,
    G: Fn() -> PostgresStartDecision,
{
    let authority_exact = || {
        matches!(
            startup_guard(),
            PostgresStartDecision::StartWritable(current) if current == *generation
        )
    };
    let generation_durability = shutdown_config.generation_durability();
    let publication = publish_postgres_generation(
        socket_dir,
        generation,
        generation_durability,
        &authority_exact,
    );
    let mut publication = Box::pin(async {
        if generation_durability.is_remote_apply() {
            publication.await
        } else {
            timeout(WRITABLE_GENERATION_PUBLICATION_TIMEOUT, publication)
                .await
                .map_err(|_| {
                    PostgresError::WritableGenerationPublicationTimeout(
                        WRITABLE_GENERATION_PUBLICATION_TIMEOUT,
                    )
                })?
        }
    });
    let authority_lost = wait_for_authority_loss(&authority_exact, authority_deadline);
    tokio::pin!(authority_lost);
    let result = tokio::select! {
        biased;
        status = wait_pidfd_exit(pidfd) => {
            let error = match status {
                Ok(status) => PostgresError::UnexpectedExit(status),
                Err(error) => error,
            };
            return Err(cleanup_tracked_startup_failure(
                state, process, pidfd, process_group, error,
            ).await);
        }
        stop_mode = shutdown.as_mut() => {
            stop_tracked_postmaster(
                state,
                process,
                pidfd,
                process_group,
                shutdown_config,
                stop_mode,
            ).await?;
            return Ok(WritablePublicationOutcome::Stopped);
        }
        () = &mut authority_lost => {
            return Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                PostgresError::StartupAuthorityChanged,
            ).await);
        }
        result = publication.as_mut() => result,
    };
    let publication_error = result.err();
    if let Some(error) = publication_error {
        return Err(
            cleanup_tracked_startup_failure(state, process, pidfd, process_group, error).await,
        );
    }
    if !authority_exact() {
        return Err(cleanup_tracked_startup_failure(
            state,
            process,
            pidfd,
            process_group,
            PostgresError::StartupAuthorityChanged,
        )
        .await);
    }
    Ok(WritablePublicationOutcome::Published)
}

async fn publish_postgres_generation<F>(
    socket_dir: &Path,
    generation: &DurableWritableGeneration,
    durability: &GenerationDurability,
    authority_exact: &F,
) -> Result<(), PostgresError>
where
    F: Fn() -> bool,
{
    #[cfg(test)]
    {
        let _ = socket_dir;
        let _ = generation;
        let _ = durability;
        let gate = TEST_POSTGRES_GENERATION_PUBLICATION_GATE.with(|slot| slot.borrow_mut().take());
        if let Some(mut gate) = gate {
            while !*gate.borrow_and_update() && gate.changed().await.is_ok() {}
        }
        tokio::task::yield_now().await;
        if authority_exact() {
            Ok(())
        } else {
            Err(PostgresError::StartupAuthorityChanged)
        }
    }
    #[cfg(not(test))]
    {
        Box::pin(
            postgres_generation::publish_writable_generation_with_durability(
                socket_dir,
                generation,
                durability,
                authority_exact,
            ),
        )
        .await
        .map_err(PostgresError::from)
    }
}

async fn stop_tracked_postmaster(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    config: &PostgresConfig,
    stop_mode: PostgresStopMode,
) -> Result<(), PostgresError> {
    state.clear_replication_evidence();
    state.clear_lease();
    state.set_postgres_process(PostgresProcessState::Stopping);
    let result = match stop_mode {
        PostgresStopMode::Graceful => {
            shutdown_child(
                &mut process.child,
                pidfd,
                process_group,
                &process.child_subreaper,
                config,
            )
            .await
        }
        PostgresStopMode::Fence => {
            target_fence_child(
                &mut process.child,
                pidfd,
                process_group,
                &process.child_subreaper,
                config,
            )
            .await
        }
    };
    state.set_postgres_process(if result.is_err() {
        PostgresProcessState::Failed
    } else if stop_mode == PostgresStopMode::Fence {
        PostgresProcessState::Fenced
    } else {
        PostgresProcessState::Validated
    });
    result
}

async fn wait_for_authority_loss<F>(
    authority_exact: &F,
    authority_deadline: Option<AuthorityLossFuture>,
) where
    F: Fn() -> bool,
{
    if let Some(deadline_wait) = authority_deadline {
        deadline_wait.await;
        return;
    }
    #[cfg(not(test))]
    {
        // Writable production paths always supply the absolute boot-time
        // watcher. Missing wiring is treated as immediate authority loss.
        let _ = authority_exact;
    }
    #[cfg(test)]
    {
        while authority_exact() {
            sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn cleanup_tracked_startup_failure(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    error: PostgresError,
) -> PostgresError {
    state.clear_replication_evidence();
    state.clear_lease();
    state.set_postgres_process(PostgresProcessState::Stopping);
    let error = cleanup_after_error(
        &mut process.child,
        Some(pidfd),
        Some(process_group),
        &process.child_subreaper,
        error,
    )
    .await;
    state.set_postgres_process(PostgresProcessState::Failed);
    error
}

#[allow(clippy::too_many_arguments)]
async fn supervise_running_postmaster<F, G>(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    config: &PostgresConfig,
    mut shutdown: Pin<&mut F>,
    source_generation: Option<&DurableWritableGeneration>,
    startup_guard: &G,
    authority_deadline: Option<AuthorityLossFuture>,
) -> Result<(), PostgresError>
where
    F: Future<Output = PostgresStopMode>,
    G: Fn() -> PostgresStartDecision,
{
    if config.is_replication_standby() {
        return supervise_replication_standby(
            state,
            process,
            pidfd,
            process_group,
            config,
            shutdown.as_mut(),
        )
        .await;
    }
    if config.role == PostgresRuntimeRole::ReplicationBootstrapPrimary {
        let generation = source_generation
            .expect("replication bootstrap source always has writable generation authority");
        return supervise_replication_source(
            state,
            process,
            pidfd,
            process_group,
            config,
            shutdown.as_mut(),
            generation,
            startup_guard,
            authority_deadline,
        )
        .await;
    }
    if let Some(generation) = source_generation {
        return supervise_writable_quarantine(
            state,
            process,
            pidfd,
            process_group,
            config,
            shutdown.as_mut(),
            generation,
            startup_guard,
            authority_deadline,
        )
        .await;
    }
    state.set_postgres_process(config.running_process_state());
    tokio::select! {
        status = wait_pidfd_exit(pidfd) => {
            state.clear_replication_evidence();
            state.set_postgres_process(PostgresProcessState::Stopping);
            let error = cleanup_after_error(
                &mut process.child,
                Some(pidfd),
                Some(process_group),
                &process.child_subreaper,
                match status {
                    Ok(status) => PostgresError::UnexpectedExit(status),
                    Err(error) => error,
                },
            ).await;
            state.set_postgres_process(PostgresProcessState::Failed);
            Err(error)
        }
        stop_mode = shutdown.as_mut() => {
            stop_tracked_postmaster(
                state, process, pidfd, process_group, config, stop_mode,
            ).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_writable_quarantine<F, G>(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    config: &PostgresConfig,
    mut shutdown: Pin<&mut F>,
    generation: &DurableWritableGeneration,
    startup_guard: &G,
    authority_deadline: Option<AuthorityLossFuture>,
) -> Result<(), PostgresError>
where
    F: Future<Output = PostgresStopMode>,
    G: Fn() -> PostgresStartDecision,
{
    debug_assert_eq!(config.role, PostgresRuntimeRole::Quarantine);
    let authority_exact = || {
        matches!(
            startup_guard(),
            PostgresStartDecision::StartWritable(current) if current == *generation
        )
    };
    let authority_lost = wait_for_authority_loss(&authority_exact, authority_deadline);
    tokio::pin!(authority_lost);
    state.set_postgres_process(config.running_process_state());
    tokio::select! {
        biased;
        status = wait_pidfd_exit(pidfd) => {
            state.clear_replication_evidence();
            state.set_postgres_process(PostgresProcessState::Stopping);
            let error = cleanup_after_error(
                &mut process.child,
                Some(pidfd),
                Some(process_group),
                &process.child_subreaper,
                match status {
                    Ok(status) => PostgresError::UnexpectedExit(status),
                    Err(error) => error,
                },
            ).await;
            state.set_postgres_process(PostgresProcessState::Failed);
            Err(error)
        }
        stop_mode = shutdown.as_mut() => {
            stop_tracked_postmaster(
                state, process, pidfd, process_group, config, stop_mode,
            ).await
        }
        () = &mut authority_lost => {
            Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                PostgresError::StartupAuthorityChanged,
            ).await)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_replication_source<F, G>(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    config: &PostgresConfig,
    mut shutdown: Pin<&mut F>,
    generation: &DurableWritableGeneration,
    startup_guard: &G,
    authority_deadline: Option<AuthorityLossFuture>,
) -> Result<(), PostgresError>
where
    F: Future<Output = PostgresStopMode>,
    G: Fn() -> PostgresStartDecision,
{
    let authority_exact = || {
        matches!(
            startup_guard(),
            PostgresStartDecision::StartWritable(current) if current == *generation
        )
    };
    let mut evidence_monitor = Box::pin(postgres_replication::monitor_source_replication_evidence(
        state.clone(),
        config.socket_dir.clone(),
        generation.clone(),
        config.generation_durability().clone(),
    ));
    let authority_lost = wait_for_authority_loss(&authority_exact, authority_deadline);
    tokio::pin!(authority_lost);
    state.set_postgres_process(config.running_process_state());
    tokio::select! {
        biased;
        status = wait_pidfd_exit(pidfd) => {
            state.clear_replication_evidence();
            state.set_postgres_process(PostgresProcessState::Stopping);
            let error = cleanup_after_error(
                &mut process.child,
                Some(pidfd),
                Some(process_group),
                &process.child_subreaper,
                match status {
                    Ok(status) => PostgresError::UnexpectedExit(status),
                    Err(error) => error,
                },
            ).await;
            state.set_postgres_process(PostgresProcessState::Failed);
            Err(error)
        }
        result = &mut evidence_monitor => {
            Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                replication_evidence_error(result),
            ).await)
        }
        stop_mode = shutdown.as_mut() => {
            drop(evidence_monitor);
            stop_tracked_postmaster(
                state, process, pidfd, process_group, config, stop_mode,
            ).await
        }
        () = &mut authority_lost => {
            Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                PostgresError::StartupAuthorityChanged,
            ).await)
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn supervise_replication_standby<F>(
    state: &AgentState,
    process: &mut PostgresProcessFence,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    config: &PostgresConfig,
    mut shutdown: Pin<&mut F>,
) -> Result<(), PostgresError>
where
    F: Future<Output = PostgresStopMode>,
{
    let member_slot_name = config
        .standby_member_slot_name()
        .expect("replication standby config always has a member slot")
        .to_owned();
    let (confirmed_tx, mut confirmed_rx) = oneshot::channel();
    let mut recovery_monitor = Box::pin(postgres_recovery::monitor_standby_recovery(
        config.socket_dir.clone(),
        confirmed_tx,
    ));
    let mut evidence_monitor =
        Box::pin(postgres_replication::monitor_standby_replication_evidence(
            state.clone(),
            config.socket_dir.clone(),
            member_slot_name,
        ));

    let confirmation = tokio::select! {
        status = wait_pidfd_exit(pidfd) => {
            let error = match status {
                Ok(status) => PostgresError::UnexpectedExit(status),
                Err(error) => error,
            };
            return Err(cleanup_tracked_startup_failure(
                state, process, pidfd, process_group, error,
            ).await);
        }
        stop_mode = shutdown.as_mut() => {
            drop(recovery_monitor);
            return stop_tracked_postmaster(
                state, process, pidfd, process_group, config, stop_mode,
            ).await;
        }
        result = &mut recovery_monitor => {
            return Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                standby_monitor_error(result),
            ).await);
        }
        result = &mut evidence_monitor => {
            return Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                replication_evidence_error(result),
            ).await);
        }
        result = &mut confirmed_rx => result,
    };
    match confirmation {
        Ok(()) => {}
        Err(_) => {
            return Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                PostgresError::StandbyRecoveryMonitorStopped,
            )
            .await);
        }
    }

    state.set_postgres_process(config.running_process_state());
    tokio::select! {
        status = wait_pidfd_exit(pidfd) => {
            state.clear_replication_evidence();
            state.set_postgres_process(PostgresProcessState::Stopping);
            let error = cleanup_after_error(
                &mut process.child,
                Some(pidfd),
                Some(process_group),
                &process.child_subreaper,
                match status {
                    Ok(status) => PostgresError::UnexpectedExit(status),
                    Err(error) => error,
                },
            ).await;
            state.set_postgres_process(PostgresProcessState::Failed);
            Err(error)
        }
        stop_mode = shutdown.as_mut() => {
            drop(recovery_monitor);
            drop(evidence_monitor);
            stop_tracked_postmaster(
                state, process, pidfd, process_group, config, stop_mode,
            ).await
        }
        result = &mut recovery_monitor => {
            Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                standby_monitor_error(result),
            ).await)
        }
        result = &mut evidence_monitor => {
            Err(cleanup_tracked_startup_failure(
                state,
                process,
                pidfd,
                process_group,
                replication_evidence_error(result),
            ).await)
        }
    }
}

fn replication_evidence_error(result: Result<(), ReplicationEvidenceError>) -> PostgresError {
    match result {
        Ok(()) => PostgresError::ReplicationEvidenceMonitorStopped,
        Err(error) => PostgresError::ReplicationEvidence {
            source: Box::new(error),
        },
    }
}

fn standby_monitor_error(result: Result<(), PostgresRecoveryError>) -> PostgresError {
    match result {
        Ok(()) => PostgresError::StandbyRecoveryMonitorStopped,
        Err(error) => PostgresError::StandbyRecovery {
            source: Box::new(error),
        },
    }
}

fn authorize_postmaster_start(
    state: &AgentState,
    startup_guard: &impl Fn() -> PostgresStartDecision,
) -> Result<Option<PostgresStartAuthorization>, PostgresError> {
    match startup_guard() {
        PostgresStartDecision::Start => Ok(Some(PostgresStartAuthorization::Direct)),
        PostgresStartDecision::StartWritable(generation) => {
            Ok(Some(PostgresStartAuthorization::Writable(generation)))
        }
        PostgresStartDecision::Shutdown => {
            state.clear_lease();
            state.set_postgres_process(PostgresProcessState::Validated);
            Ok(None)
        }
        PostgresStartDecision::AuthorityMissing => {
            state.clear_lease();
            state.set_postgres_process(PostgresProcessState::Failed);
            Err(PostgresError::StartupAuthorityMissing)
        }
    }
}

fn watch_shutdown_requested(shutdown: &mut watch::Receiver<bool>) -> bool {
    *shutdown.borrow_and_update()
}

fn watch_shutdown_observed(shutdown: &watch::Receiver<bool>) -> bool {
    shutdown.has_changed().is_err() || *shutdown.borrow()
}

async fn await_validation<F>(
    task: &mut tokio::task::JoinHandle<Result<ValidatedPostgresState, PostgresError>>,
    shutdown: Pin<&mut F>,
) -> Result<Option<ValidatedPostgresState>, PostgresError>
where
    F: Future,
{
    tokio::select! {
        biased;
        _ = shutdown => Ok(None),
        result = timeout(VALIDATION_TIMEOUT, task) => {
            result
                .map_err(|_| PostgresError::ValidationTimeout(VALIDATION_TIMEOUT))?
                .map_err(PostgresError::ValidationTask)?
                .map(Some)
        }
    }
}

async fn shutdown_requested<F>(shutdown: Pin<&mut F>) -> bool
where
    F: Future,
{
    tokio::select! {
        biased;
        _ = shutdown => true,
        () = std::future::ready(()) => false,
    }
}

async fn cleanup_spawn_failure(
    state: &AgentState,
    child: &mut Child,
    process_group: Option<Pid>,
    child_subreaper: &ChildSubreaper,
    error: PostgresError,
) -> PostgresError {
    state.set_postgres_process(PostgresProcessState::Stopping);
    let error = cleanup_after_error(child, None, process_group, child_subreaper, error).await;
    state.set_postgres_process(PostgresProcessState::Failed);
    error
}

fn rewrite_agent_thread_lock(
    path: &Path,
    lock: Option<PostmasterLockSnapshot>,
) -> Result<(), PostgresError> {
    let Some(lock) = lock else {
        return Ok(());
    };
    let process_pid = getpid().as_raw_pid();
    let Ok(lock_pid) = i32::try_from(lock.pid) else {
        return Ok(());
    };
    if lock_pid == process_pid || process_tgid(lock.pid) != Some(process_pid) {
        return Ok(());
    }

    let (original, contents) = read_validated_lock(path, lock.file)?;
    let newline = contents
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| PostgresError::InvalidPostmasterLock {
            path: path.to_owned(),
        })?;
    let mut replacement = process_pid.to_string().into_bytes();
    replacement.extend_from_slice(&contents[newline..]);
    replace_validated_lock(path, lock.file, &original, &replacement)
}

fn read_validated_lock(
    path: &Path,
    expected: FileSnapshot,
) -> Result<(File, Vec<u8>), PostgresError> {
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|source| PostgresError::Read {
        name: "PostgreSQL lock file",
        path: path.to_owned(),
        source: source.into(),
    })?;
    let mut original = File::from(fd);
    let metadata = original
        .metadata()
        .map_err(|source| PostgresError::Metadata {
            name: "PostgreSQL lock file",
            path: path.to_owned(),
            source,
        })?;
    if file_snapshot(&metadata) != expected {
        return Err(PostgresError::PreparedStateChanged);
    }
    let mut contents = Vec::new();
    Read::by_ref(&mut original)
        .take(MAX_POSTGRES_LOCK_FILE_BYTES + 1)
        .read_to_end(&mut contents)
        .map_err(|source| PostgresError::Read {
            name: "PostgreSQL lock file",
            path: path.to_owned(),
            source,
        })?;
    if contents.len() as u64 > MAX_POSTGRES_LOCK_FILE_BYTES
        || file_snapshot(
            &original
                .metadata()
                .map_err(|source| PostgresError::Metadata {
                    name: "PostgreSQL lock file",
                    path: path.to_owned(),
                    source,
                })?,
        ) != expected
    {
        return Err(PostgresError::PreparedStateChanged);
    }
    Ok((original, contents))
}

fn replace_validated_lock(
    path: &Path,
    expected: FileSnapshot,
    original: &File,
    replacement: &[u8],
) -> Result<(), PostgresError> {
    let parent = path.parent().ok_or_else(|| PostgresError::MissingParent {
        path: path.to_owned(),
    })?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(|source| {
        PostgresError::RewriteThreadCollisionLock {
            path: path.to_owned(),
            source,
        }
    })?;
    temporary
        .write_all(replacement)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|source| PostgresError::RewriteThreadCollisionLock {
            path: path.to_owned(),
            source,
        })?;

    let path_metadata = strict_metadata("PostgreSQL lock file", path)?;
    let original_metadata = original
        .metadata()
        .map_err(|source| PostgresError::Metadata {
            name: "PostgreSQL lock file",
            path: path.to_owned(),
            source,
        })?;
    if file_snapshot(&path_metadata) != expected
        || file_snapshot(&original_metadata) != expected
        || !same_file_identity(&path_metadata, &original_metadata)
    {
        return Err(PostgresError::PreparedStateChanged);
    }
    let file =
        temporary
            .persist(path)
            .map_err(|error| PostgresError::RewriteThreadCollisionLock {
                path: path.to_owned(),
                source: error.error,
            })?;
    let parent_fd = open(
        parent,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|source| PostgresError::RewriteThreadCollisionLock {
        path: path.to_owned(),
        source: source.into(),
    })?;
    File::from(parent_fd).sync_all().map_err(|source| {
        PostgresError::RewriteThreadCollisionLock {
            path: path.to_owned(),
            source,
        }
    })?;
    let path_metadata = strict_metadata("PostgreSQL lock file", path)?;
    let fd_metadata = file.metadata().map_err(|source| PostgresError::Metadata {
        name: "PostgreSQL lock file",
        path: path.to_owned(),
        source,
    })?;
    if !same_file_identity(&path_metadata, &fd_metadata)
        || fd_metadata.len() != replacement.len() as u64
    {
        return Err(PostgresError::PreparedStateChanged);
    }
    Ok(())
}

fn read_managed_generation_file(
    name: &'static str,
    path: &Path,
    expected_uid: u32,
    expected_mount_id: u64,
    maximum_bytes: u64,
) -> Result<Option<ManagedGenerationFile>, PostgresError> {
    let path_metadata = match fs::symlink_metadata(path) {
        Ok(_) => strict_metadata(name, path)?,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(PostgresError::Metadata {
                name,
                path: path.to_owned(),
                source,
            });
        }
    };
    validate_owned_regular_file(name, path, &path_metadata, expected_uid)?;
    require_same_mount(expected_mount_id, name, path)?;
    if path_metadata.len() > maximum_bytes {
        return Err(PostgresError::OversizedManagedGenerationFile {
            name,
            path: path.to_owned(),
            bytes: path_metadata.len(),
            maximum: maximum_bytes,
        });
    }
    let expected = file_snapshot(&path_metadata);
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|source| PostgresError::Read {
        name,
        path: path.to_owned(),
        source: source.into(),
    })?;
    let mut file = File::from(fd);
    let fd_metadata = file.metadata().map_err(|source| PostgresError::Metadata {
        name,
        path: path.to_owned(),
        source,
    })?;
    if file_snapshot(&fd_metadata) != expected || !same_file_identity(&path_metadata, &fd_metadata)
    {
        return Err(PostgresError::PreparedStateChanged);
    }
    let mut contents = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum_bytes + 1)
        .read_to_end(&mut contents)
        .map_err(|source| PostgresError::Read {
            name,
            path: path.to_owned(),
            source,
        })?;
    if contents.len() as u64 > maximum_bytes {
        return Err(PostgresError::OversizedManagedGenerationFile {
            name,
            path: path.to_owned(),
            bytes: contents.len() as u64,
            maximum: maximum_bytes,
        });
    }
    let result = ManagedGenerationFile {
        file,
        snapshot: expected,
        contents,
    };
    revalidate_managed_generation_file(name, path, &result)?;
    Ok(Some(result))
}

fn revalidate_managed_generation_file(
    name: &'static str,
    path: &Path,
    file: &ManagedGenerationFile,
) -> Result<(), PostgresError> {
    let path_metadata = strict_metadata(name, path)?;
    let fd_metadata = file
        .file
        .metadata()
        .map_err(|source| PostgresError::Metadata {
            name,
            path: path.to_owned(),
            source,
        })?;
    if file_snapshot(&path_metadata) != file.snapshot
        || file_snapshot(&fd_metadata) != file.snapshot
        || !same_file_identity(&path_metadata, &fd_metadata)
    {
        return Err(PostgresError::PreparedStateChanged);
    }
    Ok(())
}

fn revalidate_external_pid_file(
    path: &Path,
    snapshot: Option<FileSnapshot>,
) -> Result<(), PostgresError> {
    // PostgreSQL overwrites this file only after its data and socket locks are
    // established. Preserve a possible live owner's validated file until then.
    match snapshot {
        Some(snapshot) => {
            if file_snapshot(&strict_metadata("PostgreSQL external PID file", path)?) != snapshot {
                return Err(PostgresError::PreparedStateChanged);
            }
        }
        None => match fs::symlink_metadata(path) {
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return Err(PostgresError::PreparedStateChanged),
        },
    }
    Ok(())
}

fn process_tgid(pid: u32) -> Option<i32> {
    let status = fs::read(format!("/proc/{pid}/status")).ok()?;
    status_field(&status, b"Tgid:")?
        .split(u8::is_ascii_whitespace)
        .find(|value| !value.is_empty())
        .and_then(parse_ascii_i32)
}

async fn wait_pidfd_exit(pidfd: &AsyncFd<OwnedFd>) -> Result<ExitStatus, PostgresError> {
    pidfd
        .async_io(Interest::READABLE, |fd| {
            match waitid(
                WaitId::PidFd(fd.as_fd()),
                WaitIdOptions::EXITED | WaitIdOptions::NOWAIT | WaitIdOptions::NOHANG,
            )
            .map_err(std::io::Error::from)?
            {
                Some(status) => observed_exit_status(status),
                None => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            }
        })
        .await
        .map_err(PostgresError::Wait)
}

fn observed_exit_status(status: WaitIdStatus) -> std::io::Result<ExitStatus> {
    if let Some(code) = status.exit_status() {
        return Ok(ExitStatus::from_raw(code << 8));
    }
    if let Some(signal) = status.terminating_signal() {
        return Ok(ExitStatus::from_raw(
            signal | if status.dumped() { 0x80 } else { 0 },
        ));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "pidfd reported a non-terminal child state",
    ))
}

async fn shutdown_child(
    child: &mut Child,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    child_subreaper: &ChildSubreaper,
    config: &PostgresConfig,
) -> Result<(), PostgresError> {
    let result = shutdown_child_inner(pidfd, config).await;
    let cleanup = kill_and_reap(child, Some(pidfd), Some(process_group), child_subreaper).await;
    combine_shutdown_result(result, cleanup)
}

async fn target_fence_child(
    child: &mut Child,
    pidfd: &AsyncFd<OwnedFd>,
    process_group: Pid,
    child_subreaper: &ChildSubreaper,
    config: &PostgresConfig,
) -> Result<(), PostgresError> {
    let signal_result = signal_and_wait(pidfd, Signal::QUIT, config.immediate_shutdown_timeout)
        .await
        .map(|_| ());
    let cleanup = kill_and_reap(child, Some(pidfd), Some(process_group), child_subreaper).await;
    combine_target_fence_result(signal_result, cleanup)
}

fn combine_target_fence_result(
    signal: Result<(), PostgresError>,
    cleanup: Result<ProcessTreeCleanup, PostgresError>,
) -> Result<(), PostgresError> {
    match (signal, cleanup) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(PostgresError::CleanupFailed {
            error: Box::new(error),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn combine_shutdown_result(
    result: Result<(), PostgresError>,
    cleanup: Result<ProcessTreeCleanup, PostgresError>,
) -> Result<(), PostgresError> {
    match (result, cleanup) {
        (Ok(()), Ok(cleanup)) if !cleanup.observed_live_members => Ok(()),
        (Ok(()), Ok(_)) => Err(PostgresError::DescendantsSurvivedShutdown),
        (Err(error), Ok(_)) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(PostgresError::CleanupFailed {
            error: Box::new(error),
            cleanup: Box::new(cleanup),
        }),
    }
}

async fn shutdown_child_inner(
    pidfd: &AsyncFd<OwnedFd>,
    config: &PostgresConfig,
) -> Result<(), PostgresError> {
    if let Some(status) =
        signal_and_wait(pidfd, Signal::TERM, config.smart_shutdown_timeout).await?
    {
        return clean_shutdown(status);
    }
    if let Some(status) = signal_and_wait(pidfd, Signal::INT, config.fast_shutdown_timeout).await? {
        return clean_shutdown(status);
    }
    if let Some(status) =
        signal_and_wait(pidfd, Signal::QUIT, config.immediate_shutdown_timeout).await?
    {
        return Err(PostgresError::ImmediateShutdown(status));
    }

    pidfd_send_signal(pidfd.get_ref(), Signal::KILL).map_err(|source| PostgresError::Signal {
        signal: "SIGKILL",
        source: source.into(),
    })?;
    let status = timeout(KILL_REAP_TIMEOUT, wait_pidfd_exit(pidfd))
        .await
        .map_err(|_| PostgresError::KillWaitTimeout(KILL_REAP_TIMEOUT))??;
    Err(PostgresError::ForcedKill(status))
}

async fn signal_and_wait(
    pidfd: &AsyncFd<OwnedFd>,
    signal: Signal,
    wait: Duration,
) -> Result<Option<ExitStatus>, PostgresError> {
    if let Err(source) = pidfd_send_signal(pidfd.get_ref(), signal) {
        if source == rustix::io::Errno::SRCH {
            return match timeout(wait, wait_pidfd_exit(pidfd)).await {
                Ok(result) => result.map(Some),
                Err(_) => Ok(None),
            };
        }
        return Err(PostgresError::Signal {
            signal: signal_name(signal),
            source: source.into(),
        });
    }
    match timeout(wait, wait_pidfd_exit(pidfd)).await {
        Ok(result) => result.map(Some),
        Err(_) => Ok(None),
    }
}

async fn cleanup_after_error(
    child: &mut Child,
    pidfd: Option<&AsyncFd<OwnedFd>>,
    process_group: Option<Pid>,
    child_subreaper: &ChildSubreaper,
    error: PostgresError,
) -> PostgresError {
    match kill_and_reap(child, pidfd, process_group, child_subreaper).await {
        Ok(_) => error,
        Err(cleanup) => PostgresError::CleanupFailed {
            error: Box::new(error),
            cleanup: Box::new(cleanup),
        },
    }
}

async fn kill_and_reap(
    child: &mut Child,
    pidfd: Option<&AsyncFd<OwnedFd>>,
    process_group: Option<Pid>,
    child_subreaper: &ChildSubreaper,
) -> Result<ProcessTreeCleanup, PostgresError> {
    let process_group_result = if let Some(process_group) = process_group {
        kill_process_group_until_dead(process_group).await
    } else {
        if let Some(pidfd) = pidfd {
            let _ = pidfd_send_signal(pidfd.get_ref(), Signal::KILL);
        }
        let _ = child.start_kill();
        Ok(ProcessTreeCleanup::default())
    };
    let child_result = match timeout(KILL_REAP_TIMEOUT, child.wait()).await {
        Ok(result) => result.map(|_| ()).map_err(PostgresError::Wait),
        Err(_) => {
            // A direct child that remains uninterruptible keeps this future,
            // and therefore the PGDATA flock, alive until the kernel reaps it.
            child.wait().await.map(|_| ()).map_err(PostgresError::Wait)
        }
    };
    let direct_cleanup = combine_cleanup_results(
        process_group_result,
        child_result.map(|()| ProcessTreeCleanup::default()),
    );
    let adopted_cleanup = kill_adopted_children_until_dead(child_subreaper).await;
    combine_cleanup_results(direct_cleanup, adopted_cleanup)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ProcessTreeCleanup {
    observed_live_members: bool,
}

fn combine_cleanup_results(
    primary: Result<ProcessTreeCleanup, PostgresError>,
    cleanup: Result<ProcessTreeCleanup, PostgresError>,
) -> Result<ProcessTreeCleanup, PostgresError> {
    match (primary, cleanup) {
        (Ok(mut primary), Ok(cleanup)) => {
            primary.observed_live_members |= cleanup.observed_live_members;
            Ok(primary)
        }
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(PostgresError::CleanupFailed {
            error: Box::new(error),
            cleanup: Box::new(cleanup),
        }),
    }
}

async fn kill_adopted_children_until_dead(
    child_subreaper: &ChildSubreaper,
) -> Result<ProcessTreeCleanup, PostgresError> {
    if !child_subreaper.enabled {
        return Ok(ProcessTreeCleanup::default());
    }

    let deadline = Instant::now() + KILL_REAP_TIMEOUT;
    let mut exceeded_bound = false;
    let mut logged_inspection_error = false;
    let mut logged_reap_error = false;
    let mut first_error = None;
    let mut cleanup = ProcessTreeCleanup::default();
    let mut logged_pids = HashSet::new();

    loop {
        let reaping_complete = match reap_exited_adopted_children() {
            Ok(()) => true,
            Err(error) => {
                if !logged_reap_error {
                    tracing::warn!(%error, "cannot yet reap every adopted PostgreSQL descendant");
                    logged_reap_error = true;
                }
                if first_error.is_none() {
                    first_error = Some(error);
                }
                false
            }
        };
        let children = match direct_child_processes() {
            Ok(children) => children,
            Err(error) => {
                if !logged_inspection_error {
                    tracing::warn!(%error, "cannot yet prove every adopted PostgreSQL descendant is dead");
                    logged_inspection_error = true;
                }
                if Instant::now() >= deadline {
                    exceeded_bound = true;
                }
                sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        if Instant::now() >= deadline {
            exceeded_bound = true;
        }
        if reaping_complete && children.is_empty() {
            if let Some(error) = first_error {
                return Err(error);
            }
            if exceeded_bound {
                return Err(PostgresError::AdoptedChildCleanupTimeout(KILL_REAP_TIMEOUT));
            }
            return Ok(cleanup);
        }

        for child in children.into_iter().filter(|child| child.live) {
            cleanup.observed_live_members = true;
            if logged_pids.insert(child.pid.as_raw_pid()) {
                tracing::warn!(
                    pid = child.pid.as_raw_pid(),
                    "killing adopted PostgreSQL descendant"
                );
            }
            if let Err(error) = kill_adopted_child(child.pid)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if Instant::now() >= deadline {
            exceeded_bound = true;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn reap_exited_adopted_children() -> Result<(), PostgresError> {
    loop {
        match wait(WaitOptions::NOHANG) {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(()),
            Err(source) if source == rustix::io::Errno::CHILD => return Ok(()),
            Err(source) if source == rustix::io::Errno::INTR => {}
            Err(source) => return Err(PostgresError::ReapAdoptedChild(source.into())),
        }
    }
}

fn kill_adopted_child(pid: Pid) -> Result<(), PostgresError> {
    let pidfd = match pidfd_open(pid, PidfdFlags::empty()) {
        Ok(pidfd) => pidfd,
        Err(source) if source == rustix::io::Errno::SRCH => return Ok(()),
        Err(source) => {
            return Err(PostgresError::OpenAdoptedChildPidfd {
                pid: pid.as_raw_pid(),
                source: source.into(),
            });
        }
    };
    match pidfd_send_signal(&pidfd, Signal::KILL) {
        Ok(()) => Ok(()),
        Err(source) if source == rustix::io::Errno::SRCH => Ok(()),
        Err(source) => Err(PostgresError::SignalAdoptedChild {
            pid: pid.as_raw_pid(),
            source: source.into(),
        }),
    }
}

async fn kill_process_group_until_dead(
    process_group: Pid,
) -> Result<ProcessTreeCleanup, PostgresError> {
    kill_process_group_until_dead_with(
        process_group,
        process_group_has_live_members,
        kill_process_group,
        KILL_REAP_TIMEOUT,
    )
    .await
}

async fn kill_process_group_until_dead_with(
    process_group: Pid,
    mut inspect: impl FnMut(Pid) -> Result<bool, PostgresError>,
    mut signal: impl FnMut(Pid, Signal) -> Result<(), rustix::io::Errno>,
    cleanup_timeout: Duration,
) -> Result<ProcessTreeCleanup, PostgresError> {
    let deadline = Instant::now() + cleanup_timeout;
    let mut exceeded_bound = false;
    let mut logged_inspection_error = false;
    let mut signal_error = None;
    let mut cleanup = ProcessTreeCleanup::default();
    loop {
        match inspect(process_group) {
            Ok(false) => {
                if Instant::now() >= deadline {
                    exceeded_bound = true;
                }
                if let Some(source) = signal_error {
                    return Err(PostgresError::ProcessGroupSignal(source));
                }
                if exceeded_bound {
                    return Err(PostgresError::ProcessGroupCleanupTimeout(cleanup_timeout));
                }
                return Ok(cleanup);
            }
            Ok(true) => cleanup.observed_live_members = true,
            Err(error) => {
                if !logged_inspection_error {
                    tracing::warn!(%error, "cannot yet prove PostgreSQL process group is dead");
                    logged_inspection_error = true;
                }
            }
        }
        if let Err(source) = signal(process_group, Signal::KILL)
            && source != rustix::io::Errno::SRCH
            && signal_error.is_none()
        {
            signal_error = Some(std::io::Error::from(source));
        }
        if Instant::now() >= deadline {
            exceeded_bound = true;
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn fence_process_group_on_drop(process_group: Pid) {
    let mut logged_inspection_error = false;
    let mut logged_signal_error = false;
    loop {
        match process_group_has_live_members(process_group) {
            Ok(false) => return,
            Ok(true) => {}
            Err(error) => {
                if !logged_inspection_error {
                    tracing::error!(%error, "holding PGDATA while cancellation cleanup cannot prove the PostgreSQL process group is dead");
                    logged_inspection_error = true;
                }
            }
        }
        if let Err(error) = kill_process_group(process_group, Signal::KILL)
            && error != rustix::io::Errno::SRCH
            && !logged_signal_error
        {
            tracing::error!(%error, "holding PGDATA after cancellation cleanup could not kill the PostgreSQL process group");
            logged_signal_error = true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn fence_child_on_drop(
    child: &mut Child,
    process_group: Option<Pid>,
    child_subreaper: &ChildSubreaper,
) {
    if let Some(process_group) = process_group {
        fence_process_group_on_drop(process_group);
    }

    let mut logged_wait_error = false;
    let mut logged_group_signal_error = false;
    let mut logged_child_signal_error = false;
    loop {
        let child_may_be_running = match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => true,
            Err(error) => {
                if !logged_wait_error {
                    tracing::error!(%error, "holding PGDATA while cancellation cleanup cannot reap the direct PostgreSQL child");
                    logged_wait_error = true;
                }
                false
            }
        };

        if child_may_be_running
            && let Some(process_group) = process_group
            && let Err(error) = kill_process_group(process_group, Signal::KILL)
            && error != rustix::io::Errno::SRCH
            && !logged_group_signal_error
        {
            tracing::error!(%error, "holding PGDATA after cancellation cleanup could not kill the PostgreSQL process group");
            logged_group_signal_error = true;
        }
        if child_may_be_running
            && let Err(error) = child.start_kill()
            && !logged_child_signal_error
        {
            tracing::error!(%error, "holding PGDATA after cancellation cleanup could not kill the direct PostgreSQL child");
            logged_child_signal_error = true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    fence_adopted_children_on_drop(child_subreaper);
}

fn fence_adopted_children_on_drop(child_subreaper: &ChildSubreaper) {
    if !child_subreaper.enabled {
        return;
    }

    let mut logged_inspection_error = false;
    let mut logged_reap_error = false;
    let mut logged_signal_error = false;
    let mut logged_pids = HashSet::new();
    loop {
        let reaping_complete = match reap_exited_adopted_children() {
            Ok(()) => true,
            Err(error) => {
                if !logged_reap_error {
                    tracing::error!(%error, "holding PGDATA while cancellation cleanup cannot reap every adopted PostgreSQL descendant");
                    logged_reap_error = true;
                }
                false
            }
        };
        let children = match direct_child_processes() {
            Ok(children) => children,
            Err(error) => {
                if !logged_inspection_error {
                    tracing::error!(%error, "holding PGDATA while cancellation cleanup cannot prove every adopted PostgreSQL descendant is dead");
                    logged_inspection_error = true;
                }
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        if reaping_complete && children.is_empty() {
            return;
        }
        for child in children.into_iter().filter(|child| child.live) {
            if logged_pids.insert(child.pid.as_raw_pid()) {
                tracing::warn!(
                    pid = child.pid.as_raw_pid(),
                    "killing adopted PostgreSQL descendant during cancellation cleanup"
                );
            }
            if let Err(error) = kill_adopted_child(child.pid)
                && !logged_signal_error
            {
                tracing::error!(%error, "holding PGDATA after cancellation cleanup could not kill an adopted PostgreSQL descendant");
                logged_signal_error = true;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn direct_child_processes() -> Result<Vec<DirectChildProcess>, PostgresError> {
    let namespace_column = supervisor_pid_namespace_column()?;
    let supervisor_pid = getpid();
    let proc = Path::new("/proc");
    let entries = fs::read_dir(proc).map_err(|source| PostgresError::ReadDirectory {
        name: "Linux process table",
        path: proc.to_owned(),
        source,
    })?;
    let mut children = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| PostgresError::ReadDirectory {
            name: "Linux process table",
            path: proc.to_owned(),
            source,
        })?;
        if entry
            .file_name()
            .as_bytes()
            .iter()
            .any(|byte| !byte.is_ascii_digit())
        {
            continue;
        }
        let status_path = entry.path().join("status");
        let status = match fs::read(&status_path) {
            Ok(status) => status,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(PostgresError::Read {
                    name: "Linux process status",
                    path: status_path,
                    source,
                });
            }
        };
        if let Some(child) =
            direct_child_from_status(&status, &status_path, supervisor_pid, namespace_column)?
        {
            children.push(child);
        }
    }
    children.sort_unstable_by_key(|child| child.pid.as_raw_pid());
    Ok(children)
}

fn direct_child_from_status(
    status: &[u8],
    path: &Path,
    supervisor_pid: Pid,
    namespace_column: usize,
) -> Result<Option<DirectChildProcess>, PostgresError> {
    let parent = status_field(status, b"PPid:")
        .and_then(parse_ascii_i32)
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    if parent != supervisor_pid.as_raw_pid() {
        return Ok(None);
    }
    let state = status_field(status, b"State:")
        .and_then(|value| value.first().copied())
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    let namespace_pids = status_field(status, b"NSpid:")
        .and_then(parse_namespace_ids)
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    let raw_pid = namespace_pids
        .get(namespace_column)
        .copied()
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    let pid = Pid::from_raw(raw_pid).ok_or_else(|| PostgresError::InvalidProcessStatus {
        path: path.to_owned(),
    })?;
    Ok(Some(DirectChildProcess {
        pid,
        live: state != b'Z',
    }))
}

fn process_group_has_live_members(process_group: Pid) -> Result<bool, PostgresError> {
    let namespace_column = supervisor_pid_namespace_column()?;
    let proc = Path::new("/proc");
    let entries = fs::read_dir(proc).map_err(|source| PostgresError::ReadDirectory {
        name: "Linux process table",
        path: proc.to_owned(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| PostgresError::ReadDirectory {
            name: "Linux process table",
            path: proc.to_owned(),
            source,
        })?;
        if entry
            .file_name()
            .as_bytes()
            .iter()
            .any(|byte| !byte.is_ascii_digit())
        {
            continue;
        }
        let status_path = entry.path().join("status");
        let status = match fs::read(&status_path) {
            Ok(status) => status,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(PostgresError::Read {
                    name: "Linux process status",
                    path: status_path,
                    source,
                });
            }
        };
        if process_status_is_live_group_member(
            &status,
            &status_path,
            process_group,
            namespace_column,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn process_status_is_live_group_member(
    status: &[u8],
    path: &Path,
    process_group: Pid,
    namespace_column: usize,
) -> Result<bool, PostgresError> {
    let state = status_field(status, b"State:")
        .and_then(|value| value.first().copied())
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    let namespace_groups = status_field(status, b"NSpgid:")
        .and_then(parse_namespace_ids)
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    let Some(namespace_group) = namespace_groups.get(namespace_column) else {
        // A process reported only ancestor namespace IDs, so it is not visible
        // in the supervisor's PID namespace and cannot belong to this group.
        return Ok(false);
    };
    Ok(state != b'Z' && *namespace_group == process_group.as_raw_pid())
}

fn supervisor_pid_namespace_column() -> Result<usize, PostgresError> {
    let path = Path::new("/proc/self/status");
    let status = fs::read(path).map_err(|source| PostgresError::Read {
        name: "Linux supervisor process status",
        path: path.to_owned(),
        source,
    })?;
    current_pid_namespace_column(&status, path, getpid().as_raw_pid())
}

fn current_pid_namespace_column(
    status: &[u8],
    path: &Path,
    current_pid: i32,
) -> Result<usize, PostgresError> {
    let namespace_pids = status_field(status, b"NSpid:")
        .and_then(parse_namespace_ids)
        .ok_or_else(|| PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        })?;
    if namespace_pids.last().copied() != Some(current_pid) {
        return Err(PostgresError::InvalidProcessStatus {
            path: path.to_owned(),
        });
    }
    Ok(namespace_pids.len() - 1)
}

fn status_field<'a>(status: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    status
        .split(|byte| *byte == b'\n')
        .find_map(|line| line.strip_prefix(prefix).map(<[u8]>::trim_ascii))
}

fn parse_ascii_i32(value: &[u8]) -> Option<i32> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn parse_namespace_ids(value: &[u8]) -> Option<Vec<i32>> {
    let ids: Option<Vec<_>> = value
        .split(u8::is_ascii_whitespace)
        .filter(|value| !value.is_empty())
        .map(parse_ascii_i32)
        .collect();
    ids.filter(|ids| !ids.is_empty())
}

fn clean_shutdown(status: ExitStatus) -> Result<(), PostgresError> {
    if status.success() {
        Ok(())
    } else {
        Err(PostgresError::ShutdownExit(status))
    }
}

fn signal_name(signal: Signal) -> &'static str {
    if signal == Signal::TERM {
        "SIGTERM"
    } else if signal == Signal::INT {
        "SIGINT"
    } else if signal == Signal::QUIT {
        "SIGQUIT"
    } else {
        "unknown signal"
    }
}

fn validate_prepared_state(
    config: &PostgresConfig,
    create_socket_dir: bool,
) -> Result<ValidatedPostgresState, PostgresError> {
    let expected_uid = geteuid().as_raw();
    let data = validate_data_dir_for_role(&config.data_dir, expected_uid, config.role)?;
    let executable = validate_executable(&config.executable, expected_uid)?;
    let controldata_executable = validate_trusted_executable(
        "PostgreSQL control-data executable",
        &config.controldata_executable,
        expected_uid,
    )?;
    let control_data_state = validate_control_data(
        config,
        data.control_file,
        controldata_executable,
        expected_uid,
    )?;
    let hba_file = validate_hba_file(&config.hba_file, expected_uid, config.role)?;
    let socket_dir = if create_socket_dir {
        ensure_socket_dir(&config.socket_dir, expected_uid)?
    } else {
        validate_socket_dir(&config.socket_dir, expected_uid)?
    };
    let socket_lock =
        validate_postmaster_lock_at(&config.socket_dir.join(SOCKET_LOCK_FILE), expected_uid)?;
    let external_pid_file =
        validate_external_pid_file_at(&config.socket_dir.join(EXTERNAL_PID_FILE), expected_uid)?;
    let standby_passfile = config
        .standby
        .as_ref()
        .map(|standby| validate_standby_passfile(standby, expected_uid))
        .transpose()?;
    Ok(ValidatedPostgresState {
        data,
        executable,
        controldata_executable,
        control_data_state,
        socket_dir,
        socket_lock,
        external_pid_file,
        hba_file,
        standby_passfile,
    })
}

fn validate_data_dir_for_role(
    path: &Path,
    expected_uid: u32,
    role: PostgresRuntimeRole,
) -> Result<ValidatedDataDir, PostgresError> {
    validate_owned_directory("PGDATA", path, expected_uid)?;
    let data_mount_id = mount_id("PGDATA", path)?;

    let version_path = path.join("PG_VERSION");
    let version_metadata = strict_metadata("PG_VERSION", &version_path)?;
    validate_owned_regular_file("PG_VERSION", &version_path, &version_metadata, expected_uid)?;
    require_same_mount(data_mount_id, "PG_VERSION", &version_path)?;
    if version_metadata.len() > 64 {
        return Err(PostgresError::OversizedVersionFile {
            path: version_path,
            bytes: version_metadata.len(),
        });
    }
    let version = fs::read_to_string(&version_path).map_err(|source| PostgresError::Read {
        name: "PG_VERSION",
        path: version_path.clone(),
        source,
    })?;
    if version.trim_ascii() != POSTGRES_MAJOR {
        return Err(PostgresError::IncompatibleVersion {
            path: version_path,
            expected: POSTGRES_MAJOR,
        });
    }

    let global_path = path.join("global");
    validate_owned_directory("global", &global_path, expected_uid)?;
    require_same_mount(data_mount_id, "global", &global_path)?;
    let control_path = global_path.join("pg_control");
    let control_metadata = strict_metadata("pg_control", &control_path)?;
    validate_owned_regular_file("pg_control", &control_path, &control_metadata, expected_uid)?;
    require_same_mount(data_mount_id, "pg_control", &control_path)?;
    if control_metadata.len() != PG_CONTROL_FILE_SIZE {
        return Err(PostgresError::InvalidControlFileSize {
            path: control_path,
            bytes: control_metadata.len(),
            expected: PG_CONTROL_FILE_SIZE,
        });
    }
    let standby_signal = validate_recovery_state(path, expected_uid, data_mount_id, role)?;
    let wal_directory = validate_owned_data_subdirectory(path, "pg_wal", expected_uid)?;
    require_same_mount(data_mount_id, "pg_wal", &path.join("pg_wal"))?;
    let tablespace_directory = validate_no_tablespaces(path, expected_uid)?;
    require_same_mount(data_mount_id, "pg_tblspc", &path.join("pg_tblspc"))?;
    validate_storage_tree(path, expected_uid, data_mount_id)?;
    reject_nested_mounts(path, data_mount_id)?;
    let postmaster_lock = validate_postmaster_lock_at(&path.join("postmaster.pid"), expected_uid)?;
    Ok(ValidatedDataDir {
        data_dir: file_snapshot(&strict_metadata("PGDATA", path)?),
        mount_id: data_mount_id,
        version_file: file_snapshot(&strict_metadata("PG_VERSION", &version_path)?),
        global_directory: file_snapshot(&strict_metadata("global", &global_path)?),
        control_file: file_snapshot(&strict_metadata("pg_control", &control_path)?),
        wal_directory,
        tablespace_directory,
        postmaster_lock,
        standby_signal,
    })
}

#[cfg(test)]
fn validate_data_dir(path: &Path, expected_uid: u32) -> Result<ValidatedDataDir, PostgresError> {
    validate_data_dir_for_role(path, expected_uid, PostgresRuntimeRole::Quarantine)
}

fn validate_storage_tree(
    data_dir: &Path,
    expected_uid: u32,
    expected_mount_id: u64,
) -> Result<(), PostgresError> {
    let mut pending = vec![data_dir.to_owned()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).map_err(|source| PostgresError::ReadDirectory {
            name: "PostgreSQL storage tree",
            path: directory.clone(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| PostgresError::ReadDirectory {
                name: "PostgreSQL storage tree",
                path: directory.clone(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|source| PostgresError::Metadata {
                    name: "PostgreSQL storage entry",
                    path: path.clone(),
                    source,
                })?;
            if file_type.is_symlink() {
                return Err(PostgresError::Symlink {
                    name: "PostgreSQL storage entry",
                    path,
                });
            }
            if file_type.is_dir() {
                validate_owned_directory("PostgreSQL storage directory", &path, expected_uid)?;
                require_same_mount(expected_mount_id, "PostgreSQL storage directory", &path)?;
                pending.push(path);
            } else if !file_type.is_file() {
                return Err(PostgresError::WrongFileType {
                    name: "PostgreSQL storage entry",
                    path,
                    expected: "regular file or directory",
                });
            }
        }
    }
    Ok(())
}

fn reject_nested_mounts(data_dir: &Path, expected_mount_id: u64) -> Result<(), PostgresError> {
    let mountinfo_path = Path::new("/proc/self/mountinfo");
    let contents = fs::read(mountinfo_path).map_err(|source| PostgresError::Read {
        name: "Linux mount table",
        path: mountinfo_path.to_owned(),
        source,
    })?;
    for line in contents
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut fields = line.split(|byte| *byte == b' ');
        let mount_id = fields
            .next()
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(PostgresError::InvalidMountTable)?;
        for _ in 0..3 {
            fields.next().ok_or(PostgresError::InvalidMountTable)?;
        }
        let encoded_path = fields.next().ok_or(PostgresError::InvalidMountTable)?;
        let mount_path = PathBuf::from(OsString::from_vec(decode_mount_path(encoded_path)?));
        if mount_path != data_dir && mount_path.starts_with(data_dir) {
            return Err(PostgresError::ExternalMount {
                name: "PostgreSQL storage tree",
                path: mount_path,
                expected: expected_mount_id,
                actual: mount_id,
            });
        }
    }
    Ok(())
}

fn decode_mount_path(encoded: &[u8]) -> Result<Vec<u8>, PostgresError> {
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        if encoded[index] != b'\\' {
            decoded.push(encoded[index]);
            index += 1;
            continue;
        }
        let Some(octal) = encoded.get(index + 1..index + 4) else {
            return Err(PostgresError::InvalidMountTable);
        };
        if !octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
            return Err(PostgresError::InvalidMountTable);
        }
        let value = u16::from(octal[0] - b'0') * 64
            + u16::from(octal[1] - b'0') * 8
            + u16::from(octal[2] - b'0');
        decoded.push(u8::try_from(value).map_err(|_| PostgresError::InvalidMountTable)?);
        index += 4;
    }
    Ok(decoded)
}

fn validate_owned_data_subdirectory(
    data_dir: &Path,
    name: &'static str,
    expected_uid: u32,
) -> Result<FileSnapshot, PostgresError> {
    let path = data_dir.join(name);
    validate_owned_directory(name, &path, expected_uid)?;
    Ok(file_snapshot(&strict_metadata(name, &path)?))
}

fn validate_owned_directory(
    name: &'static str,
    path: &Path,
    expected_uid: u32,
) -> Result<Metadata, PostgresError> {
    let metadata = strict_metadata(name, path)?;
    if !metadata.is_dir() {
        return Err(PostgresError::WrongFileType {
            name,
            path: path.to_owned(),
            expected: "directory",
        });
    }
    if metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name,
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if !matches!(mode, 0o700 | 0o750) {
        return Err(PostgresError::UnsafePermissions {
            name,
            path: path.to_owned(),
            mode,
            expected: "0700 or 0750",
        });
    }
    Ok(metadata)
}

fn validate_owned_regular_file(
    name: &'static str,
    path: &Path,
    metadata: &Metadata,
    expected_uid: u32,
) -> Result<(), PostgresError> {
    require_regular(name, path, metadata)?;
    if metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name,
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if !matches!(mode, 0o600 | 0o640) {
        return Err(PostgresError::UnsafePermissions {
            name,
            path: path.to_owned(),
            mode,
            expected: "0600 or 0640",
        });
    }
    Ok(())
}

fn require_same_mount(expected: u64, name: &'static str, path: &Path) -> Result<(), PostgresError> {
    let actual = mount_id(name, path)?;
    if actual == expected {
        Ok(())
    } else {
        Err(PostgresError::ExternalMount {
            name,
            path: path.to_owned(),
            expected,
            actual,
        })
    }
}

fn mount_id(name: &'static str, path: &Path) -> Result<u64, PostgresError> {
    let value = statx(
        CWD,
        path,
        AtFlags::NO_AUTOMOUNT | AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::MNT_ID,
    )
    .map_err(|source| PostgresError::MountMetadata {
        name,
        path: path.to_owned(),
        source: source.into(),
    })?;
    if StatxFlags::from_bits_retain(value.stx_mask).contains(StatxFlags::MNT_ID) {
        Ok(value.stx_mnt_id)
    } else {
        Err(PostgresError::MissingMountIdentity {
            name,
            path: path.to_owned(),
        })
    }
}

fn validate_no_tablespaces(
    data_dir: &Path,
    expected_uid: u32,
) -> Result<FileSnapshot, PostgresError> {
    let path = data_dir.join("pg_tblspc");
    let snapshot = validate_owned_data_subdirectory(data_dir, "pg_tblspc", expected_uid)?;
    let mut entries = fs::read_dir(&path).map_err(|source| PostgresError::ReadDirectory {
        name: "pg_tblspc",
        path: path.clone(),
        source,
    })?;
    if let Some(entry) = entries.next() {
        let entry = entry.map_err(|source| PostgresError::ReadDirectory {
            name: "pg_tblspc",
            path: path.clone(),
            source,
        })?;
        return Err(PostgresError::TablespacePresent { path: entry.path() });
    }
    if file_snapshot(&strict_metadata("pg_tblspc", &path)?) != snapshot {
        return Err(PostgresError::PreparedStateChanged);
    }
    Ok(snapshot)
}

fn validate_recovery_state(
    data_dir: &Path,
    expected_uid: u32,
    expected_mount_id: u64,
    role: PostgresRuntimeRole,
) -> Result<Option<FileSnapshot>, PostgresError> {
    let standby_signal_path = data_dir.join("standby.signal");
    let standby_signal = match fs::symlink_metadata(&standby_signal_path) {
        Ok(_) if role == PostgresRuntimeRole::ReplicationStandby => {
            let metadata = strict_metadata("standby.signal", &standby_signal_path)?;
            validate_owned_regular_file(
                "standby.signal",
                &standby_signal_path,
                &metadata,
                expected_uid,
            )?;
            require_same_mount(expected_mount_id, "standby.signal", &standby_signal_path)?;
            if metadata.len() != 0 {
                return Err(PostgresError::InvalidStandbySignal {
                    path: standby_signal_path,
                });
            }
            Some(file_snapshot(&strict_metadata(
                "standby.signal",
                &standby_signal_path,
            )?))
        }
        Ok(_) => {
            return Err(PostgresError::RecoveryStatePresent {
                path: standby_signal_path,
            });
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            if role == PostgresRuntimeRole::ReplicationStandby {
                return Err(PostgresError::StandbySignalMissing {
                    path: standby_signal_path,
                });
            }
            None
        }
        Err(source) => {
            return Err(PostgresError::Metadata {
                name: "standby.signal",
                path: standby_signal_path,
                source,
            });
        }
    };
    for file_name in ["recovery.signal", "backup_label", "tablespace_map"] {
        let path = data_dir.join(file_name);
        match fs::symlink_metadata(&path) {
            Ok(_) => return Err(PostgresError::RecoveryStatePresent { path }),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(PostgresError::Metadata {
                    name: "PostgreSQL recovery state",
                    path,
                    source,
                });
            }
        }
    }
    Ok(standby_signal)
}

fn validate_standby_passfile(
    standby: &PostgresStandbyConfig,
    expected_uid: u32,
) -> Result<FileSnapshot, PostgresError> {
    let path = &standby.passfile;
    let metadata = strict_metadata("PostgreSQL standby passfile", path)?;
    require_regular("PostgreSQL standby passfile", path, &metadata)?;
    if metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name: "PostgreSQL standby passfile",
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if mode != 0o400 {
        return Err(PostgresError::UnsafePermissions {
            name: "PostgreSQL standby passfile",
            path: path.to_owned(),
            mode,
            expected: "runtime-UID-owned 0400",
        });
    }
    if metadata.len() == 0 || metadata.len() > MAX_STANDBY_PASSFILE_BYTES {
        return Err(PostgresError::InvalidStandbyPassfile {
            path: path.to_owned(),
        });
    }
    let file = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|source| PostgresError::Read {
        name: "PostgreSQL standby passfile",
        path: path.to_owned(),
        source: source.into(),
    })?;
    let fd_metadata = file.metadata().map_err(|source| PostgresError::Metadata {
        name: "PostgreSQL standby passfile",
        path: path.to_owned(),
        source,
    })?;
    if !same_file_identity(&metadata, &fd_metadata) {
        return Err(PostgresError::PreparedStateChanged);
    }
    let capacity =
        usize::try_from(metadata.len()).map_err(|_| PostgresError::InvalidStandbyPassfile {
            path: path.to_owned(),
        })?;
    let mut contents = Vec::with_capacity(capacity);
    file.take(MAX_STANDBY_PASSFILE_BYTES + 1)
        .read_to_end(&mut contents)
        .map_err(|source| PostgresError::Read {
            name: "PostgreSQL standby passfile",
            path: path.to_owned(),
            source,
        })?;
    if !valid_standby_passfile_contents(standby, &contents) {
        return Err(PostgresError::InvalidStandbyPassfile {
            path: path.to_owned(),
        });
    }
    let final_metadata = strict_metadata("PostgreSQL standby passfile", path)?;
    if file_snapshot(&metadata) != file_snapshot(&final_metadata) {
        return Err(PostgresError::PreparedStateChanged);
    }
    Ok(file_snapshot(&final_metadata))
}

fn valid_standby_passfile_contents(standby: &PostgresStandbyConfig, contents: &[u8]) -> bool {
    let prefix = format!(
        "{}:{}:*:pgshard_replication:",
        standby.primary_host, standby.primary_port
    );
    let Some(password) = contents
        .strip_prefix(prefix.as_bytes())
        .and_then(|contents| contents.strip_suffix(b"\n"))
    else {
        return false;
    };
    let mut offset = 0;
    while offset < password.len() {
        match password[offset] {
            b'\\' => {
                let Some(escaped) = password.get(offset + 1) else {
                    return false;
                };
                if !matches!(*escaped, b':' | b'\\') {
                    return false;
                }
                offset += 2;
            }
            b':' | 0..=31 | 127..=u8::MAX => return false,
            _ => offset += 1,
        }
    }
    offset != 0
}

fn validate_postmaster_lock_at(
    path: &Path,
    expected_uid: u32,
) -> Result<Option<PostmasterLockSnapshot>, PostgresError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(_) => strict_metadata("PostgreSQL lock file", path)?,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(PostgresError::Metadata {
                name: "PostgreSQL lock file",
                path: path.to_owned(),
                source,
            });
        }
    };
    validate_owned_regular_file("PostgreSQL lock file", path, &metadata, expected_uid)?;
    if metadata.len() > MAX_POSTGRES_LOCK_FILE_BYTES {
        return Err(PostgresError::InvalidPostmasterLock {
            path: path.to_owned(),
        });
    }
    let contents = fs::read(path).map_err(|source| PostgresError::Read {
        name: "PostgreSQL lock file",
        path: path.to_owned(),
        source,
    })?;
    let Some(newline) = contents.iter().position(|byte| *byte == b'\n') else {
        return Err(PostgresError::InvalidPostmasterLock {
            path: path.to_owned(),
        });
    };
    let pid_bytes = &contents[..newline];
    if pid_bytes.is_empty() || !pid_bytes.iter().all(u8::is_ascii_digit) {
        return Err(PostgresError::InvalidPostmasterLock {
            path: path.to_owned(),
        });
    }
    let pid = std::str::from_utf8(pid_bytes)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0 && i32::try_from(*value).is_ok())
        .ok_or_else(|| PostgresError::InvalidPostmasterLock {
            path: path.to_owned(),
        })?;
    Ok(Some(PostmasterLockSnapshot {
        file: file_snapshot(&strict_metadata("PostgreSQL lock file", path)?),
        pid,
    }))
}

fn validate_external_pid_file_at(
    path: &Path,
    expected_uid: u32,
) -> Result<Option<FileSnapshot>, PostgresError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(_) => strict_metadata("PostgreSQL external PID file", path)?,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(PostgresError::Metadata {
                name: "PostgreSQL external PID file",
                path: path.to_owned(),
                source,
            });
        }
    };
    require_regular("PostgreSQL external PID file", path, &metadata)?;
    if metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name: "PostgreSQL external PID file",
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if !matches!(mode, 0o600 | 0o640 | 0o644) {
        return Err(PostgresError::UnsafePermissions {
            name: "PostgreSQL external PID file",
            path: path.to_owned(),
            mode,
            expected: "0600 or 0640 while PostgreSQL is creating it, or 0644 after creation",
        });
    }
    if metadata.len() > MAX_EXTERNAL_PID_FILE_BYTES {
        return Err(PostgresError::InvalidExternalPidFile {
            path: path.to_owned(),
        });
    }
    let contents = fs::read(path).map_err(|source| PostgresError::Read {
        name: "PostgreSQL external PID file",
        path: path.to_owned(),
        source,
    })?;
    let canonical_pid = contents
        .strip_suffix(b"\n")
        .filter(|value| !value.is_empty() && value.iter().all(u8::is_ascii_digit))
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0 && i32::try_from(*value).is_ok());
    let transient_prefix = mode != 0o644
        && (contents.is_empty()
            || (contents.len() <= 10
                && contents[0] != b'0'
                && contents.iter().all(u8::is_ascii_digit)
                && std::str::from_utf8(&contents)
                    .ok()
                    .and_then(|value| value.parse::<i32>().ok())
                    .is_some_and(|value| value > 0)));
    if canonical_pid.is_none() && !transient_prefix {
        return Err(PostgresError::InvalidExternalPidFile {
            path: path.to_owned(),
        });
    }
    Ok(Some(file_snapshot(&strict_metadata(
        "PostgreSQL external PID file",
        path,
    )?)))
}

fn validate_executable(path: &Path, expected_uid: u32) -> Result<FileSnapshot, PostgresError> {
    validate_trusted_executable("PostgreSQL executable", path, expected_uid)
}

fn validate_trusted_executable(
    name: &'static str,
    path: &Path,
    expected_uid: u32,
) -> Result<FileSnapshot, PostgresError> {
    let metadata = strict_metadata(name, path)?;
    require_regular(name, path, &metadata)?;
    if metadata.uid() != 0 && metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name,
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if mode & 0o111 == 0
        || mode & 0o022 != 0
        || mode & 0o7_000 != 0
        || (metadata.uid() == expected_uid && mode & 0o200 != 0)
    {
        return Err(PostgresError::UnsafePermissions {
            name,
            path: path.to_owned(),
            mode,
            expected: "executable, without special mode bits, and not writable by the runtime identity, group, or world",
        });
    }
    Ok(file_snapshot(&strict_metadata(name, path)?))
}

fn validate_control_data(
    config: &PostgresConfig,
    expected_control_file: FileSnapshot,
    expected_executable: FileSnapshot,
    expected_uid: u32,
) -> Result<ControlDataState, PostgresError> {
    #[cfg(test)]
    let _exec_handoff = test_exec_handoff_guard();
    let output = std::process::Command::new(&config.controldata_executable)
        .arg(&config.data_dir)
        .env_clear()
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .env("PG_COLOR", "never")
        .env("TZ", "UTC")
        .stdin(Stdio::null())
        .output()
        .map_err(|source| PostgresError::InspectControlData {
            executable: config.controldata_executable.clone(),
            source,
        })?;
    let executable = validate_trusted_executable(
        "PostgreSQL control-data executable",
        &config.controldata_executable,
        expected_uid,
    )?;
    let control_file = file_snapshot(&strict_metadata(
        "pg_control",
        &config.data_dir.join("global/pg_control"),
    )?);
    if executable != expected_executable || control_file != expected_control_file {
        return Err(PostgresError::PreparedStateChanged);
    }
    if !output.status.success() {
        return Err(PostgresError::ControlDataExit {
            executable: config.controldata_executable.clone(),
            status: output.status,
        });
    }
    if !output.stderr.is_empty() || output.stdout.len() > 64 * 1024 {
        return Err(PostgresError::InvalidControlData {
            executable: config.controldata_executable.clone(),
        });
    }
    let state =
        parse_control_data(&output.stdout).ok_or_else(|| PostgresError::InvalidControlData {
            executable: config.controldata_executable.clone(),
        })?;
    match (config.role, state) {
        (_, ControlDataState::StartingUp) => Err(PostgresError::UnsafeControlState {
            state: control_data_state_name(state),
        }),
        (
            PostgresRuntimeRole::ReplicationStandby,
            ControlDataState::ShutDownInRecovery | ControlDataState::InArchiveRecovery,
        )
        | (
            PostgresRuntimeRole::Quarantine | PostgresRuntimeRole::ReplicationBootstrapPrimary,
            ControlDataState::ShutDown
            | ControlDataState::ShuttingDown
            | ControlDataState::InCrashRecovery
            | ControlDataState::InProduction,
        ) => Ok(state),
        (PostgresRuntimeRole::ReplicationStandby, _) => {
            Err(PostgresError::NonRecoveryControlState {
                state: control_data_state_name(state),
            })
        }
        (
            PostgresRuntimeRole::Quarantine | PostgresRuntimeRole::ReplicationBootstrapPrimary,
            ControlDataState::ShutDownInRecovery | ControlDataState::InArchiveRecovery,
        ) => Err(PostgresError::RecoveryControlState {
            state: control_data_state_name(state),
        }),
    }
}

fn parse_control_data(output: &[u8]) -> Option<ControlDataState> {
    let version = unique_control_data_value(output, b"pg_control version number:")?;
    if version != b"1800" {
        return None;
    }
    match unique_control_data_value(output, b"Database cluster state:")? {
        b"starting up" => Some(ControlDataState::StartingUp),
        b"shut down" => Some(ControlDataState::ShutDown),
        b"shut down in recovery" => Some(ControlDataState::ShutDownInRecovery),
        b"shutting down" => Some(ControlDataState::ShuttingDown),
        b"in crash recovery" => Some(ControlDataState::InCrashRecovery),
        b"in archive recovery" => Some(ControlDataState::InArchiveRecovery),
        b"in production" => Some(ControlDataState::InProduction),
        _ => None,
    }
}

fn unique_control_data_value<'a>(output: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let mut values = output
        .split(|byte| *byte == b'\n')
        .filter_map(|line| line.strip_prefix(prefix).map(<[u8]>::trim_ascii));
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

fn control_data_state_name(state: ControlDataState) -> &'static str {
    match state {
        ControlDataState::StartingUp => "starting up",
        ControlDataState::ShutDown => "shut down",
        ControlDataState::ShutDownInRecovery => "shut down in recovery",
        ControlDataState::ShuttingDown => "shutting down",
        ControlDataState::InCrashRecovery => "in crash recovery",
        ControlDataState::InArchiveRecovery => "in archive recovery",
        ControlDataState::InProduction => "in production",
    }
}

fn validate_hba_file(
    path: &Path,
    expected_uid: u32,
    role: PostgresRuntimeRole,
) -> Result<FileSnapshot, PostgresError> {
    let name = hba_policy_name(role);
    let expected_contents = match role {
        PostgresRuntimeRole::Quarantine | PostgresRuntimeRole::ReplicationStandby => {
            QUARANTINE_HBA_CONTENT
        }
        PostgresRuntimeRole::ReplicationBootstrapPrimary => {
            REPLICATION_BOOTSTRAP_PRIMARY_HBA_CONTENT
        }
    };
    let metadata = strict_metadata(name, path)?;
    require_regular(name, path, &metadata)?;
    if metadata.uid() != 0 && metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name,
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if mode & 0o022 != 0 || (metadata.uid() == expected_uid && mode & 0o200 != 0) {
        return Err(PostgresError::UnsafePermissions {
            name,
            path: path.to_owned(),
            mode,
            expected: "not writable by the runtime identity, group, or world",
        });
    }
    if metadata.len() != expected_contents.len() as u64 {
        return Err(invalid_hba(role, path));
    }
    let contents = fs::read(path).map_err(|source| PostgresError::Read {
        name,
        path: path.to_owned(),
        source,
    })?;
    if contents != expected_contents {
        return Err(invalid_hba(role, path));
    }
    Ok(file_snapshot(&strict_metadata(name, path)?))
}

fn hba_policy_name(role: PostgresRuntimeRole) -> &'static str {
    match role {
        PostgresRuntimeRole::Quarantine => "PostgreSQL quarantine HBA file",
        PostgresRuntimeRole::ReplicationBootstrapPrimary => {
            "PostgreSQL replication-bootstrap-primary HBA file"
        }
        PostgresRuntimeRole::ReplicationStandby => "PostgreSQL replication-standby HBA file",
    }
}

fn invalid_hba(role: PostgresRuntimeRole, path: &Path) -> PostgresError {
    match role {
        PostgresRuntimeRole::Quarantine => PostgresError::InvalidQuarantineHba {
            path: path.to_owned(),
        },
        PostgresRuntimeRole::ReplicationBootstrapPrimary => {
            PostgresError::InvalidReplicationBootstrapPrimaryHba {
                path: path.to_owned(),
            }
        }
        PostgresRuntimeRole::ReplicationStandby => PostgresError::InvalidReplicationStandbyHba {
            path: path.to_owned(),
        },
    }
}

fn ensure_socket_dir(path: &Path, expected_uid: u32) -> Result<FileSnapshot, PostgresError> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_socket_dir(path, expected_uid),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or_else(|| PostgresError::MissingParent {
                path: path.to_owned(),
            })?;
            let parent_metadata = strict_metadata("PostgreSQL socket parent", parent)?;
            if !parent_metadata.is_dir() {
                return Err(PostgresError::WrongFileType {
                    name: "PostgreSQL socket parent",
                    path: parent.to_owned(),
                    expected: "directory",
                });
            }
            if parent_metadata.uid() != 0 && parent_metadata.uid() != expected_uid {
                return Err(PostgresError::WrongOwner {
                    name: "PostgreSQL socket parent",
                    path: parent.to_owned(),
                    actual: parent_metadata.uid(),
                    expected: expected_uid,
                });
            }
            let parent_mode = parent_metadata.permissions().mode() & 0o7_777;
            if parent_mode & 0o022 != 0 && parent_mode & 0o1_000 == 0 {
                return Err(PostgresError::UnsafePermissions {
                    name: "PostgreSQL socket parent",
                    path: parent.to_owned(),
                    mode: parent_mode,
                    expected: "not group/world writable unless sticky",
                });
            }
            let mut builder = DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(path)
                .map_err(|source| PostgresError::CreateSocketDirectory {
                    path: path.to_owned(),
                    source,
                })?;
            validate_socket_dir(path, expected_uid)
        }
        Err(source) => Err(PostgresError::Metadata {
            name: "PostgreSQL socket directory",
            path: path.to_owned(),
            source,
        }),
    }
}

fn validate_socket_dir(path: &Path, expected_uid: u32) -> Result<FileSnapshot, PostgresError> {
    let metadata = strict_metadata("PostgreSQL socket directory", path)?;
    if !metadata.is_dir() {
        return Err(PostgresError::WrongFileType {
            name: "PostgreSQL socket directory",
            path: path.to_owned(),
            expected: "directory",
        });
    }
    if metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name: "PostgreSQL socket directory",
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    // Accept setgid only when provisioning has already retained runtime-UID
    // ownership and every group/world permission bit is clear. A Kubernetes
    // fsGroup alone normally produces group-writable storage and is not enough.
    if !matches!(mode, 0o700 | 0o2_700) {
        return Err(PostgresError::UnsafePermissions {
            name: "PostgreSQL socket directory",
            path: path.to_owned(),
            mode,
            expected: "runtime-UID-owned 0700 or owner-only setgid 2700",
        });
    }
    Ok(file_snapshot(&strict_metadata(
        "PostgreSQL socket directory",
        path,
    )?))
}

fn file_snapshot(metadata: &Metadata) -> FileSnapshot {
    FileSnapshot {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.size(),
        mode: metadata.mode(),
        owner: metadata.uid(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

fn same_file_identity(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.uid() == right.uid()
        && left.mode() == right.mode()
}

fn snapshot_has_file_identity(snapshot: FileSnapshot, metadata: &Metadata) -> bool {
    snapshot.device == metadata.dev()
        && snapshot.inode == metadata.ino()
        && snapshot.owner == metadata.uid()
        && snapshot.mode == metadata.mode()
}

fn strict_metadata(name: &'static str, path: &Path) -> Result<Metadata, PostgresError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| PostgresError::Metadata {
        name,
        path: path.to_owned(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(PostgresError::Symlink {
            name,
            path: path.to_owned(),
        });
    }
    let canonical = fs::canonicalize(path).map_err(|source| PostgresError::Canonicalize {
        name,
        path: path.to_owned(),
        source,
    })?;
    if canonical != path {
        return Err(PostgresError::SymlinkedAncestor {
            name,
            path: path.to_owned(),
            canonical,
        });
    }
    Ok(metadata)
}

fn require_regular(
    name: &'static str,
    path: &Path,
    metadata: &Metadata,
) -> Result<(), PostgresError> {
    if metadata.is_file() {
        Ok(())
    } else {
        Err(PostgresError::WrongFileType {
            name,
            path: path.to_owned(),
            expected: "regular file",
        })
    }
}

fn validate_absolute_normal_path(
    name: &'static str,
    path: &Path,
    socket_safe: bool,
) -> Result<(), PostgresConfigError> {
    let bytes = path.as_os_str().as_bytes();
    if !path.is_absolute()
        || path == Path::new("/")
        || bytes.is_empty()
        || bytes.len() > MAX_POSTGRES_PATH_BYTES
        || path
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err(PostgresConfigError::UnsafePath {
            name,
            path: path.to_owned(),
        });
    }
    if bytes.iter().any(u8::is_ascii_control) {
        return Err(PostgresConfigError::UnsafePath {
            name,
            path: path.to_owned(),
        });
    }
    if socket_safe
        && (bytes.len() > MAX_SOCKET_DIRECTORY_BYTES
            || !bytes.is_ascii()
            || bytes.last().is_some_and(u8::is_ascii_whitespace)
            || bytes
                .iter()
                .any(|byte| byte.is_ascii_control() || matches!(byte, b',' | b'\'' | b'"' | b'\\')))
    {
        return Err(PostgresConfigError::UnsafeSocketPath(path.to_owned()));
    }
    Ok(())
}

fn validate_primary_host(host: &str) -> Result<(), PostgresConfigError> {
    let valid = !host.is_empty()
        && host.len() <= 253
        && host.is_ascii()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        });
    if valid {
        Ok(())
    } else {
        Err(PostgresConfigError::InvalidPrimaryHost(host.to_owned()))
    }
}

fn validate_managed_member_name(name: &str) -> Result<(), PostgresConfigError> {
    if is_canonical_managed_member_name(name) {
        Ok(())
    } else {
        Err(PostgresConfigError::InvalidManagedMemberName(
            name.to_owned(),
        ))
    }
}

/// Invalid opt-in postmaster configuration.
#[derive(Debug, Error)]
pub enum PostgresConfigError {
    /// Standby fields and runtime role did not form one exact composition.
    #[error("PostgreSQL standby settings are incomplete or supplied for another runtime role")]
    InvalidStandbyComposition,
    /// Synchronous publication durability is valid only for the bootstrap source.
    #[error("PostgreSQL generation durability is incompatible with the selected runtime role")]
    InvalidGenerationDurabilityComposition,
    /// The primary endpoint was not a bounded DNS name.
    #[error("PostgreSQL primary host {0:?} must be a bounded ASCII DNS name")]
    InvalidPrimaryHost(String),
    /// Port zero cannot address a `PostgreSQL` primary.
    #[error("PostgreSQL primary port must be nonzero")]
    InvalidPrimaryPort,
    /// Physical slot and application identity must use the shared member name.
    #[error(
        "PostgreSQL member name {0:?} must be canonical pgshard_member_ plus at least four decimal digits"
    )]
    InvalidManagedMemberName(String),
    /// The passfile path could not be represented without conninfo escaping.
    #[error("PostgreSQL replication passfile path {0:?} contains unsafe conninfo bytes")]
    UnsafePassfilePath(PathBuf),
    /// A path is not absolute, normalized, non-root, and bounded.
    #[error(
        "{name} path {path:?} must be an absolute normalized non-root path of at most 1023 bytes"
    )]
    UnsafePath {
        /// Configuration field.
        name: &'static str,
        /// Rejected path.
        path: PathBuf,
    },
    /// A socket path cannot be represented as one `PostgreSQL` GUC list element.
    #[error(
        "PostgreSQL socket path {0:?} must be at most 93 bytes of safe ASCII without trailing whitespace, commas, quotes, control characters, or backslashes"
    )]
    UnsafeSocketPath(PathBuf),
    /// Runtime sockets must not be stored in or contain durable database state.
    #[error("PGDATA {data_dir:?} and PostgreSQL socket directory {socket_dir:?} must not overlap")]
    OverlappingPaths {
        /// Durable data directory.
        data_dir: PathBuf,
        /// Ephemeral socket directory.
        socket_dir: PathBuf,
    },
    /// The immutable deny-all HBA policy was placed in mutable runtime state.
    #[error(
        "PostgreSQL supervision HBA file {hba_file:?} must not be stored inside PGDATA or the socket directory"
    )]
    MutableHbaFile {
        /// Rejected HBA path.
        hba_file: PathBuf,
    },
    /// The replication credential was placed in mutable `PostgreSQL` state.
    #[error(
        "PostgreSQL replication passfile {passfile:?} must not be stored inside PGDATA or the socket directory"
    )]
    MutablePassfile {
        /// Rejected passfile path.
        passfile: PathBuf,
    },
    /// One shutdown phase is outside its bounded range.
    #[error("PostgreSQL {name} shutdown timeout {value:?} must be between 10ms and 55s")]
    InvalidShutdownTimeout {
        /// Shutdown phase.
        name: &'static str,
        /// Rejected timeout.
        value: Duration,
    },
    /// Duration addition overflowed.
    #[error("PostgreSQL shutdown timeout budget overflowed")]
    ShutdownBudgetOverflow,
    /// The whole shutdown sequence would outlive the container budget.
    #[error("PostgreSQL shutdown budget {requested:?} exceeds {maximum:?}")]
    ShutdownBudgetExceeded {
        /// Requested total timeout.
        requested: Duration,
        /// Maximum total timeout.
        maximum: Duration,
    },
}

/// Offline validation or direct-child supervision failure.
#[derive(Debug, Error)]
pub enum PostgresError {
    /// The per-PGDATA cross-namespace supervisor lock could not be opened.
    #[error("open PostgreSQL supervisor lock {path:?}: {source}")]
    OpenSupervisorLock {
        /// Lock path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Another agent already holds the per-PGDATA supervisor lock.
    #[error("PostgreSQL supervisor lock {path:?} is already held by another agent")]
    SupervisorLockHeld {
        /// Contended lock path.
        path: PathBuf,
    },
    /// The kernel could not acquire the per-PGDATA supervisor lock.
    #[error("acquire PostgreSQL supervisor lock {path:?}: {source}")]
    AcquireSupervisorLock {
        /// Lock path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Required filesystem metadata was unavailable.
    #[error("read {name} metadata at {path:?}: {source}")]
    Metadata {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// A final path component was a symbolic link.
    #[error("{name} at {path:?} must not be a symbolic link")]
    Symlink {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
    },
    /// An ancestor resolved through a symbolic link.
    #[error("{name} at {path:?} resolves to {canonical:?}; symlinked ancestors are not allowed")]
    SymlinkedAncestor {
        /// State being inspected.
        name: &'static str,
        /// Configured path.
        path: PathBuf,
        /// Resolved path.
        canonical: PathBuf,
    },
    /// Canonical path inspection failed.
    #[error("resolve {name} at {path:?}: {source}")]
    Canonicalize {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Linux mount identity inspection failed.
    #[error("read {name} mount identity at {path:?}: {source}")]
    MountMetadata {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// The kernel did not return the mount identity required for fail-closed validation.
    #[error("Linux did not report a mount identity for {name} at {path:?}")]
    MissingMountIdentity {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
    },
    /// Linux returned a malformed `/proc/self/mountinfo` record.
    #[error("Linux mount table contains a malformed record")]
    InvalidMountTable,
    /// Linux returned a process status without the namespace fields needed for fencing.
    #[error("Linux process status {path:?} does not contain valid namespace fencing fields")]
    InvalidProcessStatus {
        /// Malformed status path.
        path: PathBuf,
    },
    /// Durable state crossed a mount boundary inside PGDATA.
    #[error(
        "{name} at {path:?} is on mount {actual}, but PGDATA is on mount {expected}; supervision requires one PGDATA volume"
    )]
    ExternalMount {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// PGDATA mount ID.
        expected: u64,
        /// Nested mount ID.
        actual: u64,
    },
    /// A path did not contain the required filesystem object.
    #[error("{name} at {path:?} must be a {expected}")]
    WrongFileType {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Required type.
        expected: &'static str,
    },
    /// A protected path has the wrong owner.
    #[error(
        "{name} at {path:?} is owned by uid {actual}, which is not allowed; runtime uid is {expected}"
    )]
    WrongOwner {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Observed owner.
        actual: u32,
        /// Runtime effective owner.
        expected: u32,
    },
    /// A protected path is too permissive or not executable.
    #[error("{name} at {path:?} has mode {mode:04o}, expected {expected}")]
    UnsafePermissions {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Observed permission bits.
        mode: u32,
        /// Required permissions.
        expected: &'static str,
    },
    /// A version marker was unexpectedly large.
    #[error("PG_VERSION at {path:?} is {bytes} bytes; expected at most 64")]
    OversizedVersionFile {
        /// Version marker path.
        path: PathBuf,
        /// Observed size.
        bytes: u64,
    },
    /// A bounded operator-owned identity or generation file was oversized.
    #[error("{name} at {path:?} is {bytes} bytes; expected at most {maximum}")]
    OversizedManagedGenerationFile {
        /// Managed file being inspected.
        name: &'static str,
        /// Oversized file path.
        path: PathBuf,
        /// Observed size.
        bytes: u64,
        /// Accepted upper bound.
        maximum: u64,
    },
    /// A small required file could not be read.
    #[error("read {name} at {path:?}: {source}")]
    Read {
        /// State being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// A required directory could not be enumerated safely.
    #[error("read {name} directory at {path:?}: {source}")]
    ReadDirectory {
        /// Directory being inspected.
        name: &'static str,
        /// Inspected path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// The data directory is not `PostgreSQL` 18.
    #[error("PG_VERSION at {path:?} is not PostgreSQL {expected}")]
    IncompatibleVersion {
        /// Version marker path.
        path: PathBuf,
        /// Required major version.
        expected: &'static str,
    },
    /// The control file does not have `PostgreSQL` 18's fixed structural size.
    #[error("pg_control at {path:?} is {bytes} bytes, expected exactly {expected}")]
    InvalidControlFileSize {
        /// Control file path.
        path: PathBuf,
        /// Observed size.
        bytes: u64,
        /// Required `PostgreSQL` 18 size.
        expected: u64,
    },
    /// The trusted `PostgreSQL` 18 control-data tool could not be executed.
    #[error("run PostgreSQL control-data executable {executable:?}: {source}")]
    InspectControlData {
        /// Validated control-data executable.
        executable: PathBuf,
        /// Process creation or collection failure.
        source: std::io::Error,
    },
    /// The trusted `PostgreSQL` 18 control-data tool failed.
    #[error("PostgreSQL control-data executable {executable:?} returned {status}")]
    ControlDataExit {
        /// Validated control-data executable.
        executable: PathBuf,
        /// Non-success process status.
        status: ExitStatus,
    },
    /// `PostgreSQL` 18 could not authenticate a bounded control-file report.
    #[error("PostgreSQL control-data executable {executable:?} returned an untrusted report")]
    InvalidControlData {
        /// Validated control-data executable.
        executable: PathBuf,
    },
    /// The control file proves that this data directory was last a standby.
    #[error(
        "PostgreSQL control-file state {state:?} requires role-aware standby recovery before startup"
    )]
    RecoveryControlState {
        /// Rejected `PostgreSQL` control-file state.
        state: &'static str,
    },
    /// A standby data directory was not last shut down while in recovery.
    #[error("PostgreSQL control-file state {state:?} is not a standby recovery state")]
    NonRecoveryControlState {
        /// Rejected `PostgreSQL` control-file state.
        state: &'static str,
    },
    /// The control file is not in a complete primary state that supervision may recover.
    #[error("PostgreSQL control-file state {state:?} is not safe for supervised startup")]
    UnsafeControlState {
        /// Rejected `PostgreSQL` control-file state.
        state: &'static str,
    },
    /// Role-aware standby, archive, or base-backup recovery is outside this runtime role.
    #[error(
        "PostgreSQL recovery state {path:?} is not supported by this supervision role; a role-aware orchestrator must handle this state before startup"
    )]
    RecoveryStatePresent {
        /// Recovery marker that prevented process creation.
        path: PathBuf,
    },
    /// The role requires the exact physical-standby marker.
    #[error("PostgreSQL replication-standby marker is missing at {path:?}")]
    StandbySignalMissing {
        /// Required marker path.
        path: PathBuf,
    },
    /// The physical-standby marker was not an empty protected regular file.
    #[error(
        "PostgreSQL replication-standby marker at {path:?} must be an empty protected regular file"
    )]
    InvalidStandbySignal {
        /// Rejected marker path.
        path: PathBuf,
    },
    /// User tablespaces escape the single-volume supervision boundary.
    #[error(
        "PostgreSQL tablespace entry {path:?} is not supported by supervised modes; Milestone 1 requires all database state inside PGDATA"
    )]
    TablespacePresent {
        /// Entry that prevented process creation.
        path: PathBuf,
    },
    /// The HBA policy was not the exact private publisher-plus-reject policy.
    #[error(
        "PostgreSQL quarantine HBA file {path:?} must contain only the built-in generation-publisher and reject policy"
    )]
    InvalidQuarantineHba {
        /// Rejected HBA path.
        path: PathBuf,
    },
    /// The HBA policy was not the exact replication-role-plus-reject policy.
    #[error(
        "PostgreSQL replication-bootstrap-primary HBA file {path:?} must allow only the fixed SCRAM replication role and the built-in local publisher"
    )]
    InvalidReplicationBootstrapPrimaryHba {
        /// Rejected HBA path.
        path: PathBuf,
    },
    /// The standby HBA policy was not the exact private publisher-plus-reject policy.
    #[error(
        "PostgreSQL replication-standby HBA file {path:?} must contain only the built-in local publisher and reject policy"
    )]
    InvalidReplicationStandbyHba {
        /// Rejected HBA path.
        path: PathBuf,
    },
    /// The standby passfile was not one exact bounded upstream credential.
    #[error(
        "PostgreSQL standby passfile {path:?} must contain one bounded credential for the configured primary and fixed replication role"
    )]
    InvalidStandbyPassfile {
        /// Rejected credential file.
        path: PathBuf,
    },
    /// A crash lock was malformed, partial, or too large to handle safely.
    #[error("PostgreSQL lock file {path:?} does not contain a bounded positive postmaster PID")]
    InvalidPostmasterLock {
        /// Rejected lock file.
        path: PathBuf,
    },
    /// A stale external PID file was neither canonical nor a bounded creation transient.
    #[error("PostgreSQL external PID file {path:?} is not a bounded PostgreSQL creation state")]
    InvalidExternalPidFile {
        /// Rejected external PID file.
        path: PathBuf,
    },
    /// A lock colliding with a current agent thread could not be rewritten safely.
    #[error("rewrite PostgreSQL thread-PID collision lock file {path:?}: {source}")]
    RewriteThreadCollisionLock {
        /// Lock file path.
        path: PathBuf,
        /// Rewrite failure.
        source: std::io::Error,
    },
    /// A preflighted path changed before process creation.
    #[error("validated PostgreSQL state changed between prepare and process creation")]
    PreparedStateChanged,
    /// The final process-creation guard no longer proves writable authority.
    #[error("PostgreSQL startup authority is absent or inside its fencing margin")]
    StartupAuthorityMissing,
    /// A role that opens replication TCP was sent through direct supervision.
    #[error("PostgreSQL replication-bootstrap-primary mode requires writable-term Lease authority")]
    WritableAuthorityRequired,
    /// A physical standby was sent through writable-term supervision.
    #[error("PostgreSQL replication-standby mode forbids writable-term Lease authority")]
    WritableAuthorityForbidden,
    /// Attempt-private authority changed while its durable generation was flushed.
    #[error("PostgreSQL startup authority changed during durable generation publication")]
    StartupAuthorityChanged,
    /// Internal authority data was not representable in the canonical durable format.
    #[error("PostgreSQL startup authority contains an invalid writable generation")]
    InvalidRequestedWritableGeneration,
    /// Unit-test fault at one crash-consistency publication boundary.
    #[cfg(test)]
    #[error("injected durable writable-generation publication fault")]
    InjectedGenerationPublicationFault,
    /// Writable supervision requires the operator's exact durable PGDATA identity.
    #[error("PostgreSQL bootstrap identity is missing at {path:?}")]
    BootstrapIdentityMissing {
        /// Required bootstrap marker path.
        path: PathBuf,
    },
    /// PGDATA belongs to another cluster or physical cell.
    #[error("PostgreSQL bootstrap identity at {path:?} does not match writable authority")]
    BootstrapIdentityMismatch {
        /// Rejected bootstrap marker path.
        path: PathBuf,
    },
    /// The durable generation record was not in its one canonical format.
    #[error("durable PostgreSQL writable generation at {path:?} is invalid")]
    InvalidWritableGeneration {
        /// Rejected generation path.
        path: PathBuf,
    },
    /// The durable generation record belongs to another coordination universe.
    #[error("durable PostgreSQL writable generation at {path:?} belongs to another cell")]
    ForeignWritableGeneration {
        /// Rejected generation path.
        path: PathBuf,
    },
    /// Kubernetes attempted to authorize a term below the durable target floor.
    #[error(
        "writable-term regression rejected: requested term {requested}, durable term {durable}"
    )]
    WritableGenerationRegression {
        /// Term already durable on the target.
        durable: u64,
        /// Stale requested term.
        requested: u64,
    },
    /// One term was presented by two different holders.
    #[error("writable term {term} conflicts with the durable target holder")]
    WritableGenerationConflict {
        /// Conflicting fencing term.
        term: u64,
    },
    /// Crash-safe generation publication could not complete.
    #[error("{operation} for durable PostgreSQL writable generation at {path:?}: {source}")]
    PersistWritableGeneration {
        /// Failed publication stage.
        operation: &'static str,
        /// File or directory being persisted.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// WAL-backed generation publication failed while `PostgreSQL` was non-serving.
    #[error("publish WAL-backed PostgreSQL writable generation: {0}")]
    PublishWritableGeneration(#[from] PostgresGenerationError),
    /// WAL-backed generation publication did not finish within the startup bound.
    #[error("WAL-backed PostgreSQL writable-generation publication exceeded {0:?}")]
    WritableGenerationPublicationTimeout(Duration),
    /// Continuous recovery proof ended without returning an explicit failure.
    #[error("PostgreSQL standby recovery monitor stopped unexpectedly")]
    StandbyRecoveryMonitorStopped,
    /// Continuous recovery proof was lost.
    #[error("monitor PostgreSQL standby recovery: {source}")]
    StandbyRecovery {
        /// Exact connection, query, timeout, or recovery-state failure.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Continuous replication evidence ended without returning a failure.
    #[error("PostgreSQL replication evidence monitor stopped unexpectedly")]
    ReplicationEvidenceMonitorStopped,
    /// A previously coherent replication evidence stream was lost.
    #[error("monitor PostgreSQL replication evidence: {source}")]
    ReplicationEvidence {
        /// Exact bounded SQL observation failure without row or credential data.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The blocking revalidation worker did not complete normally.
    #[error("PostgreSQL pre-spawn validation task failed: {0}")]
    ValidationTask(#[source] tokio::task::JoinError),
    /// Offline validation exceeded the bounded pre-spawn interval.
    #[error("PostgreSQL pre-spawn validation exceeded {0:?}")]
    ValidationTimeout(Duration),
    /// The socket directory parent was unavailable.
    #[error("PostgreSQL socket directory {path:?} has no parent")]
    MissingParent {
        /// Socket directory path.
        path: PathBuf,
    },
    /// The private socket directory could not be created.
    #[error("create PostgreSQL socket directory {path:?}: {source}")]
    CreateSocketDirectory {
        /// Socket directory path.
        path: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// The postmaster could not be started.
    #[error("start PostgreSQL executable {executable:?}: {source}")]
    Spawn {
        /// Executable path.
        executable: PathBuf,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Linux could not make the dedicated agent a child subreaper.
    #[error("configure the PostgreSQL agent as a Linux child subreaper: {0}")]
    ConfigureChildSubreaper(#[source] std::io::Error),
    /// Linux could not report the dedicated agent's child-subreaper state.
    #[error("inspect the PostgreSQL agent Linux child-subreaper state: {0}")]
    InspectChildSubreaper(#[source] std::io::Error),
    /// Linux accepted the request but did not enable child-subreaper state.
    #[error("Linux did not enable PostgreSQL child-subreaper supervision")]
    ChildSubreaperNotEnabled,
    /// Another direct child would make adopted-process ownership ambiguous.
    #[error(
        "PostgreSQL supervision requires a dedicated process, but direct child {pid} already exists"
    )]
    ExistingChildProcess {
        /// Existing direct child PID in the agent namespace.
        pid: i32,
    },
    /// The child process did not expose a PID.
    #[error("spawned PostgreSQL process did not expose a PID")]
    MissingChildPid,
    /// The child PID cannot be represented safely.
    #[error("spawned PostgreSQL process returned an invalid PID")]
    InvalidChildPid,
    /// Linux could not create a race-free handle to the postmaster.
    #[error("open pidfd for PostgreSQL pid {pid}: {source}")]
    OpenPidfd {
        /// Child process ID.
        pid: i32,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Linux could not register the postmaster pidfd with the async reactor.
    #[error("monitor pidfd for PostgreSQL pid {pid}: {source}")]
    MonitorPidfd {
        /// Child process ID.
        pid: i32,
        /// Reactor registration failure.
        source: std::io::Error,
    },
    /// Waiting for the direct child failed.
    #[error("wait for PostgreSQL child: {0}")]
    Wait(std::io::Error),
    /// The postmaster exited without an authorized shutdown request.
    #[error("PostgreSQL child exited unexpectedly with {0}")]
    UnexpectedExit(ExitStatus),
    /// Sending a bounded shutdown signal failed.
    #[error("send {signal} to PostgreSQL child: {source}")]
    Signal {
        /// Signal name.
        signal: &'static str,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// A smart or fast shutdown returned a failure status.
    #[error("PostgreSQL child returned {0} during clean shutdown")]
    ShutdownExit(ExitStatus),
    /// Immediate shutdown was required and crash recovery will be needed.
    #[error("PostgreSQL required immediate shutdown and returned {0}")]
    ImmediateShutdown(ExitStatus),
    /// The kernel had to kill the postmaster after all bounded waits expired.
    #[error("PostgreSQL ignored all shutdown modes and was killed with {0}")]
    ForcedKill(ExitStatus),
    /// A kernel-killed process did not become waitable within the final bound.
    #[error("PostgreSQL did not become waitable within {0:?} after SIGKILL")]
    KillWaitTimeout(Duration),
    /// `PostgreSQL` exited but one or more descendants required forced cleanup.
    #[error("PostgreSQL descendants survived the postmaster shutdown and were killed")]
    DescendantsSurvivedShutdown,
    /// The kernel rejected a process-group kill while the storage fence remained held.
    #[error("kill PostgreSQL process group: {0}")]
    ProcessGroupSignal(#[source] std::io::Error),
    /// `PostgreSQL`'s process group outlived the bounded cleanup interval.
    #[error(
        "PostgreSQL process group remained live beyond {0:?}; PGDATA stayed fenced until it died"
    )]
    ProcessGroupCleanupTimeout(Duration),
    /// An adopted `PostgreSQL` child could not be reaped.
    #[error("reap an adopted PostgreSQL descendant: {0}")]
    ReapAdoptedChild(#[source] std::io::Error),
    /// Linux could not create an identity-stable handle for an adopted child.
    #[error("open pidfd for adopted PostgreSQL descendant {pid}: {source}")]
    OpenAdoptedChildPidfd {
        /// Adopted child PID in the agent namespace.
        pid: i32,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Linux rejected a pidfd-targeted adopted-child kill.
    #[error("kill adopted PostgreSQL descendant {pid}: {source}")]
    SignalAdoptedChild {
        /// Adopted child PID in the agent namespace.
        pid: i32,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Adopted `PostgreSQL` children outlived the bounded cleanup interval.
    #[error(
        "adopted PostgreSQL descendants remained live beyond {0:?}; PGDATA stayed fenced until they died"
    )]
    AdoptedChildCleanupTimeout(Duration),
    /// Cleanup after another failure could not reap the child within its bound.
    #[error("{error}; PostgreSQL cleanup also failed: {cleanup}")]
    CleanupFailed {
        /// Original supervision failure.
        error: Box<PostgresError>,
        /// Bounded cleanup failure.
        cleanup: Box<PostgresError>,
    },
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs::OpenOptions;
    use std::os::unix::fs::{OpenOptionsExt, symlink};
    use std::os::unix::net::UnixListener;
    use std::process::Child as StdChild;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    use std::task::{Context, Poll};
    use std::thread::JoinHandle;

    use tempfile::TempDir;

    use crate::boottime::{BoottimeClock, BoottimeInstant, FakeBoottimeClock};
    use crate::domain::{AgentIdentity, FencingLease, ReplicationEvidence};
    use pgshard_types::ShardId;

    use super::*;

    const TEST_WRITABLE_LEASE_UID: &str = "99999999-8888-7777-6666-555555555555";
    const TEST_WRITABLE_HOLDER: &str =
        "cluster-1-shard-0-0/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/0123456789abcdef01234567";

    struct ProcessGuard(StdChild);

    struct ReadyOnPoll {
        polls: usize,
        ready_on: usize,
    }

    impl Future for ReadyOnPoll {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls += 1;
            if self.polls >= self.ready_on {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl Drop for ProcessGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    struct ThreadGuard {
        stop: Arc<AtomicBool>,
        handle: Option<JoinHandle<()>>,
    }

    impl Drop for ThreadGuard {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn live_agent_thread() -> (ThreadGuard, u32) {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let (sender, receiver) = mpsc::sync_channel(1);
        let handle = std::thread::spawn(move || {
            sender
                .send(rustix::thread::gettid().as_raw_pid().cast_unsigned())
                .expect("publish agent thread PID");
            while !thread_stop.load(Ordering::Acquire) {
                std::thread::park_timeout(Duration::from_millis(5));
            }
        });
        let tid = receiver.recv().expect("receive agent thread PID");
        (
            ThreadGuard {
                stop,
                handle: Some(handle),
            },
            tid,
        )
    }

    #[test]
    fn structurally_preflights_owned_postgres_18_markers() {
        let fixture = pgdata_fixture();
        assert!(validate_data_dir(fixture.path(), geteuid().as_raw()).is_ok());
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw().saturating_add(1)),
            Err(PostgresError::WrongOwner { name: "PGDATA", .. })
        ));

        fs::set_permissions(fixture.path(), fs::Permissions::from_mode(0o755))
            .expect("make PGDATA unsafe");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::UnsafePermissions { name: "PGDATA", .. })
        ));
        fs::set_permissions(fixture.path(), fs::Permissions::from_mode(0o700))
            .expect("restore PGDATA permissions");

        fs::write(fixture.path().join("PG_VERSION"), "17\n").expect("replace version");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::IncompatibleVersion { .. })
        ));
        fs::write(fixture.path().join("PG_VERSION"), "18\n").expect("restore version");
        fs::set_permissions(
            fixture.path().join("PG_VERSION"),
            fs::Permissions::from_mode(0o644),
        )
        .expect("make PG_VERSION unsafe");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::UnsafePermissions {
                name: "PG_VERSION",
                ..
            })
        ));
        fs::set_permissions(
            fixture.path().join("PG_VERSION"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("restore PG_VERSION permissions");

        fs::set_permissions(
            fixture.path().join("global"),
            fs::Permissions::from_mode(0o755),
        )
        .expect("make global directory unsafe");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::UnsafePermissions { name: "global", .. })
        ));
        fs::set_permissions(
            fixture.path().join("global"),
            fs::Permissions::from_mode(0o700),
        )
        .expect("restore global directory permissions");

        fs::set_permissions(
            fixture.path().join("global/pg_control"),
            fs::Permissions::from_mode(0o660),
        )
        .expect("make control file unsafe");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::UnsafePermissions {
                name: "pg_control",
                ..
            })
        ));
        fs::set_permissions(
            fixture.path().join("global/pg_control"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("restore control file permissions");
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(fixture.path().join("global/pg_control"))
            .expect("truncate control file");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::InvalidControlFileSize { bytes: 0, .. })
        ));
    }

    #[test]
    fn rejects_symlinked_data_and_marker_state() {
        let parent = TempDir::new().expect("create fixture parent");
        let target = parent.path().join("target");
        let target_fixture = pgdata_fixture_at(&target);
        let linked = parent.path().join("linked");
        symlink(&target, &linked).expect("link PGDATA");
        assert!(matches!(
            validate_data_dir(&linked, geteuid().as_raw()),
            Err(PostgresError::Symlink { name: "PGDATA", .. })
        ));

        let version = target.join("PG_VERSION");
        fs::remove_file(&version).expect("remove version marker");
        fs::write(target.join("REAL_VERSION"), "18\n").expect("write real marker");
        symlink(target.join("REAL_VERSION"), &version).expect("link marker");
        assert!(matches!(
            validate_data_dir(&target, geteuid().as_raw()),
            Err(PostgresError::Symlink {
                name: "PG_VERSION",
                ..
            })
        ));
        drop(target_fixture);
    }

    #[test]
    fn rejects_symlinks_anywhere_in_the_postgres_storage_tree() {
        let fixture = pgdata_fixture();
        let base = fixture.path().join("base");
        fs::create_dir(&base).expect("create base directory");
        fs::set_permissions(&base, fs::Permissions::from_mode(0o700))
            .expect("protect base directory");
        let outside = fixture.path().join("outside");
        fs::write(&outside, b"outside").expect("write outside file");
        symlink(&outside, base.join("relation")).expect("link relation outside PGDATA");

        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::Symlink {
                name: "PostgreSQL storage entry",
                ..
            })
        ));
    }

    #[test]
    fn rejects_unsafe_directories_and_special_files_in_the_storage_tree() {
        let fixture = pgdata_fixture();
        let base = fixture.path().join("base");
        fs::create_dir(&base).expect("create base directory");
        fs::set_permissions(&base, fs::Permissions::from_mode(0o777))
            .expect("make base directory unsafe");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::UnsafePermissions {
                name: "PostgreSQL storage directory",
                ..
            })
        ));

        fs::set_permissions(&base, fs::Permissions::from_mode(0o700))
            .expect("protect base directory");
        let socket = UnixListener::bind(base.join("special.sock")).expect("create Unix socket");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::WrongFileType {
                name: "PostgreSQL storage entry",
                ..
            })
        ));
        drop(socket);
    }

    #[test]
    fn decodes_linux_mountinfo_paths_without_loss() {
        assert_eq!(
            decode_mount_path(br"/data/space\040tab\011slash\134name").expect("decode mount path"),
            b"/data/space tab\tslash\\name"
        );
        assert!(decode_mount_path(br"/data/bad\09x").is_err());
        assert!(decode_mount_path(br"/data/overflow\777").is_err());
    }

    #[test]
    fn process_status_fencing_fails_closed_on_missing_fields() {
        let process_group = Pid::from_raw(42).expect("positive process group");
        let path = Path::new("/proc/7/status");
        assert_eq!(
            current_pid_namespace_column(b"NSpid:\t700 7\n", path, 7)
                .expect("derive supervisor namespace column"),
            1
        );
        assert!(
            process_status_is_live_group_member(
                b"Name:\tworker\xff\nState:\tS (sleeping)\nNSpgid:\t700 42 9\n",
                path,
                process_group,
                1,
            )
            .expect("complete live status")
        );
        assert!(
            !process_status_is_live_group_member(
                b"State:\tZ (zombie)\nNSpgid:\t700 42 9\n",
                path,
                process_group,
                1,
            )
            .expect("complete zombie status")
        );
        assert!(
            process_status_is_live_group_member(
                b"State:\tS (sleeping)\nNSpgid:\t42 0\n",
                path,
                process_group,
                0,
            )
            .expect("nested namespace group")
        );
        assert!(
            !process_status_is_live_group_member(
                b"State:\tS (sleeping)\nNSpgid:\t700\n",
                path,
                process_group,
                1,
            )
            .expect("ancestor namespace process")
        );
        assert!(
            process_status_is_live_group_member(b"State:\tS (sleeping)\n", path, process_group, 0,)
                .is_err()
        );
        assert!(current_pid_namespace_column(b"NSpid:\t700 8\n", path, 7).is_err());
    }

    #[test]
    fn direct_child_status_uses_the_supervisor_namespace_and_zombie_state() {
        let supervisor = Pid::from_raw(7).expect("positive supervisor PID");
        let path = Path::new("/proc/42/status");
        assert_eq!(
            direct_child_from_status(
                b"Name:\tbackend\xff\nState:\tT (stopped)\nPPid:\t7\nNSpid:\t700 42 9\n",
                path,
                supervisor,
                1,
            )
            .expect("decode live direct child"),
            Some(DirectChildProcess {
                pid: Pid::from_raw(42).expect("positive child PID"),
                live: true,
            })
        );
        assert_eq!(
            direct_child_from_status(
                b"State:\tZ (zombie)\nPPid:\t7\nNSpid:\t700 42\n",
                path,
                supervisor,
                1,
            )
            .expect("decode zombie direct child"),
            Some(DirectChildProcess {
                pid: Pid::from_raw(42).expect("positive child PID"),
                live: false,
            })
        );
        assert_eq!(
            direct_child_from_status(
                b"State:\tS (sleeping)\nPPid:\t8\nNSpid:\t700 42\n",
                path,
                supervisor,
                1,
            )
            .expect("ignore another parent's child"),
            None
        );
        assert!(
            direct_child_from_status(
                b"State:\tS (sleeping)\nPPid:\t7\nNSpid:\t700\n",
                path,
                supervisor,
                1,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn transient_process_inspection_error_is_not_a_live_descendant() {
        let process_group = Pid::from_raw(42).expect("positive process group");
        let mut observations = [
            Err(PostgresError::ReadDirectory {
                name: "Linux process table",
                path: PathBuf::from("/proc"),
                source: std::io::Error::other("transient fixture failure"),
            }),
            Ok(false),
        ]
        .into_iter();
        let mut signals = 0;

        let cleanup = kill_process_group_until_dead_with(
            process_group,
            |_| observations.next().expect("bounded observation fixture"),
            |_, signal| {
                assert_eq!(signal, Signal::KILL);
                signals += 1;
                Ok(())
            },
            KILL_REAP_TIMEOUT,
        )
        .await
        .expect("later absence proof completes cleanup");

        assert_eq!(cleanup, ProcessTreeCleanup::default());
        assert_eq!(signals, 1);
        assert!(combine_shutdown_result(Ok(()), Ok(cleanup)).is_ok());
    }

    #[tokio::test]
    async fn observed_live_process_survives_in_cleanup_result() {
        let process_group = Pid::from_raw(42).expect("positive process group");
        let mut observations = [Ok(true), Ok(false)].into_iter();

        let cleanup = kill_process_group_until_dead_with(
            process_group,
            |_| observations.next().expect("bounded observation fixture"),
            |_, _| Ok(()),
            KILL_REAP_TIMEOUT,
        )
        .await
        .expect("live member is killed and absence is proved");

        assert!(cleanup.observed_live_members);
        assert!(matches!(
            combine_shutdown_result(Ok(()), Ok(cleanup)),
            Err(PostgresError::DescendantsSurvivedShutdown)
        ));
    }

    #[tokio::test]
    async fn final_absence_scan_crossing_cleanup_deadline_reports_timeout() {
        let process_group = Pid::from_raw(42).expect("positive process group");
        let cleanup_timeout = Duration::from_millis(10);

        let result = kill_process_group_until_dead_with(
            process_group,
            |_| {
                std::thread::sleep(Duration::from_millis(20));
                Ok(false)
            },
            |_, _| Ok(()),
            cleanup_timeout,
        )
        .await;

        assert!(matches!(
            result,
            Err(PostgresError::ProcessGroupCleanupTimeout(value))
                if value == cleanup_timeout
        ));
    }

    #[test]
    fn rejects_signal_and_base_backup_recovery_before_process_creation() {
        for marker in [
            "standby.signal",
            "recovery.signal",
            "backup_label",
            "tablespace_map",
        ] {
            let fixture = pgdata_fixture();
            fs::write(fixture.path().join(marker), []).expect("write recovery marker");
            assert!(matches!(
                validate_data_dir(fixture.path(), geteuid().as_raw()),
                Err(PostgresError::RecoveryStatePresent { path })
                    if path == fixture.path().join(marker)
            ));
        }
    }

    #[test]
    fn rejects_missing_signal_standby_control_states_before_process_creation() {
        for state in ["shut down in recovery", "in archive recovery"] {
            let root = TempDir::new().expect("create recovery-history fixture");
            let data_dir = root.path().join("data");
            pgdata_fixture_at(&data_dir);
            assert!(!data_dir.join("standby.signal").exists());
            assert!(!data_dir.join("recovery.signal").exists());
            let marker = root.path().join("postmaster-started");
            let executable = root.path().join("postgres");
            write_executable(
                &executable,
                &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
            );
            write_control_data_state(&executable, state, "");
            let config = test_config(data_dir, executable, root.path().join("socket"));

            assert!(matches!(
                prepare_fixture(config),
                Err(PostgresError::RecoveryControlState { state: actual }) if actual == state
            ));
            assert!(
                !marker.exists(),
                "standby history without a signal file started PostgreSQL"
            );
        }
    }

    #[test]
    fn executable_fixture_rewrites_replace_the_inode() {
        let root = TempDir::new().expect("create executable fixture");
        let executable = root.path().join("fixture");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let original = File::open(&executable).expect("open original executable fixture");

        write_executable(&executable, "#!/bin/sh\nexit 42\n");

        assert_ne!(
            original.metadata().expect("inspect original fixture").ino(),
            fs::metadata(&executable)
                .expect("inspect replacement fixture")
                .ino(),
            "fixture rewrites must not open the executable inode for writing"
        );
        let status = {
            let _exec_handoff = test_exec_handoff_guard();
            std::process::Command::new(&executable)
                .status()
                .expect("execute replacement fixture")
        };
        assert_eq!(status.code(), Some(42));
    }

    #[test]
    fn exec_handoff_blocks_control_data_while_its_fixture_is_writable() {
        let root = TempDir::new().expect("create exec-handoff fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let config = test_config(data_dir, executable.clone(), root.path().join("socket"));

        let handoff = test_exec_handoff_guard();
        let control_data = executable.with_file_name("pg_controldata");
        fs::set_permissions(&control_data, fs::Permissions::from_mode(0o700))
            .expect("make control-data fixture writable");
        let writer = OpenOptions::new()
            .write(true)
            .open(&control_data)
            .expect("hold control-data writer");
        fs::set_permissions(&control_data, fs::Permissions::from_mode(0o500))
            .expect("restore trusted control-data mode");

        let (attempt_tx, attempt_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let prepare = std::thread::spawn(move || {
            observe_next_test_exec_handoff(attempt_tx);
            result_tx
                .send(prepare_fixture(config).is_ok())
                .expect("publish preparation result");
        });
        attempt_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("observe control-data exec handoff attempt");
        assert_eq!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        drop(writer);
        drop(handoff);

        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("preparation completed after writer release"),
            "preparation failed after the executable writer closed"
        );
        prepare.join().expect("join fixture preparation");
    }

    #[test]
    fn rejects_untrusted_or_incomplete_control_data_reports() {
        let root = TempDir::new().expect("create control-data fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let config = test_config(
            data_dir.clone(),
            executable.clone(),
            root.path().join("socket"),
        );

        write_control_data_state(&executable, "shut down", "CRC mismatch");
        assert!(matches!(
            prepare_fixture(config.clone()),
            Err(PostgresError::InvalidControlData { .. })
        ));

        write_control_data_state(&executable, "starting up", "");
        assert!(matches!(
            prepare_fixture(config),
            Err(PostgresError::UnsafeControlState {
                state: "starting up"
            })
        ));
    }

    #[test]
    fn rejects_external_wal_and_user_tablespaces() {
        let fixture = pgdata_fixture();
        let external_wal = TempDir::new().expect("create external WAL directory");
        let wal = fixture.path().join("pg_wal");
        fs::remove_dir(&wal).expect("remove local WAL directory");
        symlink(external_wal.path(), &wal).expect("link external WAL directory");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::Symlink { name: "pg_wal", .. })
        ));

        fs::remove_file(&wal).expect("remove external WAL link");
        fs::create_dir(&wal).expect("restore local WAL directory");
        fs::set_permissions(&wal, fs::Permissions::from_mode(0o700))
            .expect("secure restored WAL directory");
        let tablespace = fixture.path().join("pg_tblspc/16384");
        symlink(external_wal.path(), &tablespace).expect("link user tablespace");
        assert!(matches!(
            validate_data_dir(fixture.path(), geteuid().as_raw()),
            Err(PostgresError::TablespacePresent { path }) if path == tablespace
        ));
    }

    #[test]
    fn supervisor_lock_excludes_a_second_agent_for_the_same_pgdata() {
        let fixture = pgdata_fixture();
        let first = SupervisorLock::acquire(fixture.path()).expect("acquire first supervisor lock");
        assert!(matches!(
            SupervisorLock::acquire(fixture.path()),
            Err(PostgresError::SupervisorLockHeld { path })
                if path == fixture.path()
        ));
        drop(first);
        // Parallel tests can fork while this CLOEXEC descriptor is open. The
        // forked process releases its inherited reference at exec, so allow
        // that bounded handoff without masking a lock that remains stuck.
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match SupervisorLock::acquire(fixture.path()) {
                Ok(lock) => {
                    drop(lock);
                    break;
                }
                Err(PostgresError::SupervisorLockHeld { .. }) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("lock released after first agent exits: {error}"),
            }
        }
    }

    #[test]
    fn config_rejects_overlap_unsafe_socket_and_excessive_budget() {
        let durations = || {
            (
                Duration::from_secs(5),
                Duration::from_secs(40),
                Duration::from_secs(5),
            )
        };
        let (smart, fast, immediate) = durations();
        assert!(matches!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from("/data/socket"),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                smart,
                fast,
                immediate,
            ),
            Err(PostgresConfigError::OverlappingPaths { .. })
        ));
        let (smart, fast, immediate) = durations();
        assert!(matches!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from("/run/bad,socket"),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                smart,
                fast,
                immediate,
            ),
            Err(PostgresConfigError::UnsafeSocketPath(_))
        ));
        let (smart, fast, immediate) = durations();
        assert!(matches!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from("/run/bad "),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                smart,
                fast,
                immediate,
            ),
            Err(PostgresConfigError::UnsafeSocketPath(_))
        ));
        let (smart, fast, immediate) = durations();
        assert!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from(format!("/{}", "s".repeat(92))),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                smart,
                fast,
                immediate,
            )
            .is_ok()
        );
        let (smart, fast, immediate) = durations();
        assert!(matches!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from(format!("/{}", "s".repeat(93))),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                smart,
                fast,
                immediate,
            ),
            Err(PostgresConfigError::UnsafeSocketPath(_))
        ));
        assert!(matches!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from("/run/postgres"),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                Duration::from_secs(10),
                Duration::from_secs(40),
                Duration::from_secs(10),
            ),
            Err(PostgresConfigError::ShutdownBudgetExceeded { .. })
        ));
        assert!(
            PostgresConfig::new(
                PathBuf::from("/data"),
                PathBuf::from("/bin/postgres"),
                PathBuf::from("/run/postgres"),
                PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
                Duration::from_secs(5),
                Duration::from_secs(44),
                Duration::from_secs(5),
            )
            .is_ok(),
            "phase timeouts plus final reap must fit the exact 55 second budget"
        );
    }

    #[test]
    fn rejects_mutable_or_non_denying_quarantine_hba() {
        let fixture = TempDir::new().expect("create HBA fixture");
        let hba = fixture.path().join("quarantine.pg_hba.conf");
        fs::write(&hba, "local all all trust\n").expect("write unsafe HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect unsafe HBA fixture");
        assert!(matches!(
            validate_hba_file(&hba, geteuid().as_raw(), PostgresRuntimeRole::Quarantine),
            Err(PostgresError::InvalidQuarantineHba { .. })
        ));

        fs::set_permissions(&hba, fs::Permissions::from_mode(0o600)).expect("make HBA replaceable");
        fs::write(&hba, QUARANTINE_HBA_CONTENT).expect("write deny-all HBA");
        assert!(matches!(
            validate_hba_file(&hba, geteuid().as_raw(), PostgresRuntimeRole::Quarantine),
            Err(PostgresError::UnsafePermissions {
                name: "PostgreSQL quarantine HBA file",
                ..
            })
        ));
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400)).expect("protect deny-all HBA");
        assert!(
            validate_hba_file(&hba, geteuid().as_raw(), PostgresRuntimeRole::Quarantine).is_ok()
        );

        fs::set_permissions(&hba, fs::Permissions::from_mode(0o600)).expect("resize HBA");
        OpenOptions::new()
            .write(true)
            .open(&hba)
            .expect("open sparse HBA")
            .set_len(1 << 40)
            .expect("create sparse oversized HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect oversized HBA");
        assert!(matches!(
            validate_hba_file(&hba, geteuid().as_raw(), PostgresRuntimeRole::Quarantine),
            Err(PostgresError::InvalidQuarantineHba { .. })
        ));
    }

    #[test]
    fn replication_bootstrap_primary_hba_allows_only_fixed_scram_replication_role() {
        let fixture = TempDir::new().expect("create HBA fixture");
        let hba = fixture
            .path()
            .join("replication-bootstrap-primary.pg_hba.conf");
        fs::write(&hba, REPLICATION_BOOTSTRAP_PRIMARY_HBA_CONTENT).expect("write replication HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect replication HBA");
        assert!(
            validate_hba_file(
                &hba,
                geteuid().as_raw(),
                PostgresRuntimeRole::ReplicationBootstrapPrimary,
            )
            .is_ok()
        );
        assert!(matches!(
            validate_hba_file(&hba, geteuid().as_raw(), PostgresRuntimeRole::Quarantine),
            Err(PostgresError::InvalidQuarantineHba { .. })
        ));

        fs::set_permissions(&hba, fs::Permissions::from_mode(0o600))
            .expect("make replication HBA replaceable");
        fs::write(&hba, b"host all all 0.0.0.0/0 scram-sha-256\n")
            .expect("write ordinary-client HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect ordinary-client HBA");
        assert!(matches!(
            validate_hba_file(
                &hba,
                geteuid().as_raw(),
                PostgresRuntimeRole::ReplicationBootstrapPrimary,
            ),
            Err(PostgresError::InvalidReplicationBootstrapPrimaryHba { .. })
        ));
    }

    #[test]
    fn replication_standby_requires_exact_recovery_state_and_private_credentials() {
        let root = TempDir::new().expect("create standby fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let socket = root.path().join("socket");
        let (config, passfile) =
            standby_test_config(&root, data_dir.clone(), executable.clone(), socket);

        assert!(prepare_fixture(config.clone()).is_ok());
        write_control_data_state(&executable, "in archive recovery", "");
        assert!(prepare_fixture(config.clone()).is_ok());
        for state in [
            "shut down",
            "shutting down",
            "in crash recovery",
            "in production",
        ] {
            write_control_data_state(&executable, state, "");
            assert!(matches!(
                prepare_fixture(config.clone()),
                Err(PostgresError::NonRecoveryControlState { state: actual }) if actual == state
            ));
        }
        write_control_data_state(&executable, "shut down in recovery", "");

        fs::remove_file(data_dir.join("standby.signal")).expect("remove standby signal");
        assert!(matches!(
            prepare_fixture(config.clone()),
            Err(PostgresError::StandbySignalMissing { .. })
        ));
        write_standby_signal(&data_dir);

        for forbidden in ["recovery.signal", "backup_label", "tablespace_map"] {
            let path = data_dir.join(forbidden);
            fs::write(&path, []).expect("write forbidden recovery marker");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("protect forbidden recovery marker");
            assert!(matches!(
                prepare_fixture(config.clone()),
                Err(PostgresError::RecoveryStatePresent { path: actual }) if actual == path
            ));
            fs::remove_file(path).expect("remove forbidden recovery marker");
        }

        let write_passfile = |contents: &[u8]| {
            fs::set_permissions(&passfile, fs::Permissions::from_mode(0o600))
                .expect("make passfile replaceable");
            fs::write(&passfile, contents).expect("replace passfile contents");
            fs::set_permissions(&passfile, fs::Permissions::from_mode(0o400))
                .expect("protect replaced passfile");
        };
        let invalid_passfiles = [
            Vec::new(),
            b"other.database.svc:5432:*:pgshard_replication:secret\n".to_vec(),
            b"primary.database.svc:5432:*:pgshard_replication:\n".to_vec(),
            b"primary.database.svc:5432:*:other:secret\n".to_vec(),
            b"primary.database.svc:5432:*:pgshard_replication:one\nprimary.database.svc:5432:*:pgshard_replication:two\n".to_vec(),
            b"primary.database.svc:5432:*:pgshard_replication:unescaped:colon\n".to_vec(),
            b"primary.database.svc:5432:*:pgshard_replication:dangling\\\n".to_vec(),
            vec![b'a'; usize::try_from(MAX_STANDBY_PASSFILE_BYTES).expect("bounded test size") + 1],
        ];
        for contents in invalid_passfiles {
            write_passfile(&contents);
            assert!(matches!(
                prepare_fixture(config.clone()),
                Err(PostgresError::InvalidStandbyPassfile { path }) if path == passfile
            ));
        }
        write_passfile(
            b"primary.database.svc:5432:*:pgshard_replication:escaped\\:colon\\\\slash\n",
        );
        assert!(prepare_fixture(config.clone()).is_ok());
        write_passfile(b"primary.database.svc:5432:*:pgshard_replication:secret\n");

        fs::set_permissions(&passfile, fs::Permissions::from_mode(0o600))
            .expect("make passfile writable");
        assert!(matches!(
            prepare_fixture(config),
            Err(PostgresError::UnsafePermissions {
                name: "PostgreSQL standby passfile",
                ..
            })
        ));
    }

    #[test]
    fn replication_standby_accepts_canonical_member_slot_identities() {
        for valid in [
            "pgshard_member_0000",
            "pgshard_member_9999",
            "pgshard_member_10000",
            "pgshard_member_65535",
        ] {
            assert!(validate_managed_member_name(valid).is_ok());
        }
        for invalid in [
            "pgshard_member_001",
            "pgshard_member_00000",
            "pgshard_member_00a0",
            "pgshard_member_65536",
            "pgshard-member-0000",
        ] {
            assert!(matches!(
                validate_managed_member_name(invalid),
                Err(PostgresConfigError::InvalidManagedMemberName(actual)) if actual == invalid
            ));
        }
    }

    #[test]
    fn replication_standby_command_is_tcp_closed_password_free_and_recovery_locked() {
        let root = TempDir::new().expect("create standby command fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let socket = root.path().join("socket");
        let (config, passfile) = standby_test_config(&root, data_dir, executable, socket);
        assert_eq!(
            config.starting_process_state(),
            PostgresProcessState::StartingReplicationStandby
        );
        assert_eq!(
            config.running_process_state(),
            PostgresProcessState::RunningReplicationStandby
        );
        let prepared = prepare_fixture(config).expect("prepare standby command fixture");
        let command = prepared.command();
        let arguments: Vec<_> = command.as_std().get_args().collect();
        let required_settings = vec![
            "listen_addresses=".to_owned(),
            "archive_mode=off".to_owned(),
            format!(
                "primary_conninfo=host=primary.database.svc port=5432 user=pgshard_replication application_name=pgshard_member_0001 passfile={} sslmode=disable",
                passfile.display()
            ),
            "primary_slot_name=pgshard_member_0001".to_owned(),
            "recovery_target_action=shutdown".to_owned(),
            "recovery_target_timeline=latest".to_owned(),
            "hot_standby=on".to_owned(),
            "hot_standby_feedback=on".to_owned(),
            "default_transaction_read_only=on".to_owned(),
            "wal_receiver_create_temp_slot=off".to_owned(),
        ];
        for required in required_settings {
            assert!(
                arguments.contains(&OsStr::new(&required)),
                "missing standby setting {required:?}"
            );
        }
        for preserved in ["max_wal_senders=", "max_replication_slots="] {
            assert!(
                !arguments
                    .iter()
                    .any(|argument| argument.as_bytes().starts_with(preserved.as_bytes())),
                "standby must not shrink recovery capacity with {preserved:?}"
            );
        }
        assert!(
            !arguments.iter().any(|argument| argument
                .as_bytes()
                .windows(b"password".len())
                .any(|window| window == b"password")),
            "standby command embedded a password"
        );
        assert!(
            !arguments.iter().any(|argument| argument
                .as_bytes()
                .windows(b"secret".len())
                .any(|window| window == b"secret")),
            "standby command embedded the fixture credential"
        );
        for (name, value) in command.as_std().get_envs() {
            assert_ne!(name, OsStr::new("PGPASSWORD"));
            assert!(
                value.is_none_or(|value| !value
                    .as_bytes()
                    .windows(b"secret".len())
                    .any(|window| window == b"secret")),
                "standby environment embedded the fixture credential"
            );
        }
    }

    #[tokio::test]
    async fn replication_standby_delays_running_and_fences_lost_recovery_proof() {
        for terminal in [
            crate::postgres_recovery::TestRecoveryObservation::RecoveryEnded,
            crate::postgres_recovery::TestRecoveryObservation::Unknown,
        ] {
            let root = TempDir::new().expect("create standby supervision fixture");
            let data_dir = root.path().join("data");
            pgdata_fixture_at(&data_dir);
            let marker = root.path().join("started");
            let executable = root.path().join("postgres");
            write_executable(
                &executable,
                &format!(
                    "#!/bin/sh\nprintf started > '{}'\ntrap '' TERM INT QUIT\nwhile :; do sleep 1; done\n",
                    marker.display()
                ),
            );
            let socket = root.path().join("socket");
            let (config, _) = standby_test_config(&root, data_dir, executable, socket.clone());
            let prepared = prepare_fixture(config).expect("prepare standby supervisor");
            let state = agent_state();
            let (observed_tx, observed_rx) =
                watch::channel(crate::postgres_recovery::TestRecoveryObservation::Pending);
            crate::postgres_recovery::set_test_recovery_observations(socket.clone(), observed_rx);
            let (evidence_tx, evidence_rx) = watch::channel(
                crate::postgres_replication::TestReplicationEvidenceObservation::Pending,
            );
            crate::postgres_replication::set_test_replication_evidence_observations(
                socket,
                evidence_rx,
            );
            let supervisor_state = state.clone();
            let supervisor = tokio::spawn(async move {
                prepared
                    .supervise(supervisor_state, std::future::pending())
                    .await
            });
            wait_for_marker(&marker).await;
            assert_eq!(
                state.snapshot().postgres_process,
                PostgresProcessState::StartingReplicationStandby
            );
            observed_tx
                .send(crate::postgres_recovery::TestRecoveryObservation::InRecovery)
                .expect("confirm recovery");
            evidence_tx
                .send(crate::postgres_replication::TestReplicationEvidenceObservation::Confirmed)
                .expect("confirm standby evidence before recovery loss");
            timeout(Duration::from_secs(1), async {
                loop {
                    let snapshot = state.snapshot();
                    if snapshot.postgres_process == PostgresProcessState::RunningReplicationStandby
                        && matches!(
                            snapshot.replication_evidence,
                            Some(ReplicationEvidence::Standby(_))
                        )
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("standby reached running only after recovery proof");
            observed_tx.send(terminal).expect("lose recovery proof");
            let result = timeout(Duration::from_secs(2), supervisor)
                .await
                .expect("standby fenced inside cleanup bound")
                .expect("join standby supervisor");
            assert!(matches!(result, Err(PostgresError::StandbyRecovery { .. })));
            assert_eq!(
                state.snapshot().postgres_process,
                PostgresProcessState::Failed
            );
            assert!(state.snapshot().replication_evidence.is_none());
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn replication_evidence_loss_fences_standby_and_wins_source_shutdown_race() {
        let root = TempDir::new().expect("create standby evidence-loss fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("standby-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\nprintf started > '{}'\ntrap '' TERM INT QUIT\nwhile :; do sleep 1; done\n",
                marker.display()
            ),
        );
        let socket_dir = root.path().join("socket");
        let (config, _) = standby_test_config(&root, data_dir, executable, socket_dir.clone());
        let prepared = prepare_fixture(config).expect("prepare standby evidence-loss supervisor");
        let state = agent_state();
        let (recovery_tx, recovery_rx) =
            watch::channel(crate::postgres_recovery::TestRecoveryObservation::Pending);
        crate::postgres_recovery::set_test_recovery_observations(socket_dir.clone(), recovery_rx);
        let (evidence_tx, evidence_rx) = watch::channel(
            crate::postgres_replication::TestReplicationEvidenceObservation::Pending,
        );
        crate::postgres_replication::set_test_replication_evidence_observations(
            socket_dir,
            evidence_rx,
        );
        let supervisor_state = state.clone();
        let supervisor = tokio::spawn(async move {
            prepared
                .supervise(supervisor_state, std::future::pending())
                .await
        });
        wait_for_marker(&marker).await;
        recovery_tx
            .send(crate::postgres_recovery::TestRecoveryObservation::InRecovery)
            .expect("confirm standby recovery");
        evidence_tx
            .send(crate::postgres_replication::TestReplicationEvidenceObservation::Confirmed)
            .expect("confirm standby evidence");
        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = state.snapshot();
                if snapshot.postgres_process == PostgresProcessState::RunningReplicationStandby
                    && matches!(
                        snapshot.replication_evidence,
                        Some(ReplicationEvidence::Standby(_))
                    )
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("standby reached running with evidence");
        evidence_tx
            .send(crate::postgres_replication::TestReplicationEvidenceObservation::Failed)
            .expect("lose standby evidence");
        let result = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("standby evidence loss fenced inside cleanup bound")
            .expect("join standby evidence-loss supervisor");
        assert!(matches!(
            result,
            Err(PostgresError::ReplicationEvidence { .. })
        ));
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
        assert!(state.snapshot().replication_evidence.is_none());
        assert!(state.snapshot().lease.is_none());

        let root = TempDir::new().expect("create source evidence-loss fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("source-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\nprintf started > '{}'\ntrap '' TERM INT QUIT\nwhile :; do sleep 1; done\n",
                marker.display()
            ),
        );
        let hba = root
            .path()
            .join("replication-bootstrap-primary.pg_hba.conf");
        fs::write(&hba, REPLICATION_BOOTSTRAP_PRIMARY_HBA_CONTENT)
            .expect("write source evidence-loss HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect source evidence-loss HBA");
        let socket_dir = root.path().join("socket");
        let config = PostgresConfig::new_for_role(
            PostgresRuntimeRole::ReplicationBootstrapPrimary,
            None,
            GenerationDurability::remote_apply_any_one(vec![
                "pgshard_member_0001".to_owned(),
                "pgshard_member_0002".to_owned(),
            ])
            .expect("valid source evidence durability"),
            data_dir,
            executable,
            socket_dir.clone(),
            hba,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid source evidence-loss config");
        let prepared = prepare_fixture(config).expect("prepare source evidence-loss supervisor");
        let state = state_with_writable_lease(1);
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state.lease_deadline().expect("monotonic source deadline"),
            durable_generation_for_test(1),
        );
        let (evidence_tx, evidence_rx) = watch::channel(
            crate::postgres_replication::TestReplicationEvidenceObservation::Pending,
        );
        crate::postgres_replication::set_test_replication_evidence_observations(
            socket_dir,
            evidence_rx,
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let supervisor_state = state.clone();
        let supervisor = tokio::spawn(async move {
            prepared
                .supervise_with_writable_authority(
                    supervisor_state,
                    shutdown_rx,
                    Duration::ZERO,
                    postgres_attempt,
                )
                .await
        });
        wait_for_marker(&marker).await;
        evidence_tx
            .send(crate::postgres_replication::TestReplicationEvidenceObservation::Confirmed)
            .expect("confirm source evidence");
        timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = state.snapshot();
                if snapshot.postgres_process == PostgresProcessState::RunningReplicationBootstrap
                    && matches!(
                        snapshot.replication_evidence,
                        Some(ReplicationEvidence::Source(_))
                    )
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source reached running with evidence");
        evidence_tx
            .send(crate::postgres_replication::TestReplicationEvidenceObservation::Failed)
            .expect("lose source evidence");
        shutdown_tx
            .send(true)
            .expect("race source evidence loss with composed shutdown");
        let result = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("source evidence loss fenced inside cleanup bound")
            .expect("join source evidence-loss supervisor");
        assert!(matches!(
            result,
            Err(PostgresError::ReplicationEvidence { .. })
        ));
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
        assert!(state.snapshot().replication_evidence.is_none());
        assert!(state.snapshot().lease.is_none());
    }

    #[tokio::test]
    async fn writable_quarantine_never_starts_replication_evidence_monitor() {
        let root = TempDir::new().expect("create writable-quarantine evidence fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("quarantine-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\nprintf started > '{}'\ntrap '' TERM INT QUIT\nwhile :; do sleep 1; done\n",
                marker.display()
            ),
        );
        let socket_dir = root.path().join("socket");
        let prepared = prepare_fixture(test_config(data_dir, executable, socket_dir.clone()))
            .expect("prepare writable quarantine evidence fixture");
        let state = state_with_writable_lease(1);
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state
                .lease_deadline()
                .expect("writable quarantine monotonic deadline"),
            durable_generation_for_test(1),
        );
        let (_evidence_tx, evidence_rx) =
            watch::channel(crate::postgres_replication::TestReplicationEvidenceObservation::Failed);
        crate::postgres_replication::set_test_replication_evidence_observations(
            socket_dir.clone(),
            evidence_rx,
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let supervisor_state = state.clone();
        let supervisor = tokio::spawn(async move {
            prepared
                .supervise_with_writable_authority(
                    supervisor_state,
                    shutdown_rx,
                    Duration::ZERO,
                    postgres_attempt,
                )
                .await
        });

        wait_for_marker(&marker).await;
        timeout(Duration::from_secs(1), async {
            while state.snapshot().postgres_process != PostgresProcessState::RunningQuarantined {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writable quarantine reached running state");
        sleep(Duration::from_millis(25)).await;
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::RunningQuarantined
        );
        assert!(state.snapshot().replication_evidence.is_none());
        assert!(
            crate::postgres_replication::remove_test_replication_evidence_observations(&socket_dir),
            "writable quarantine consumed a replication-evidence failure injection"
        );
        assert!(!supervisor.is_finished());

        lease_attempt.clear_authority();
        let result = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("writable quarantine fenced after authority loss")
            .expect("join writable-quarantine supervisor");
        assert!(matches!(
            result,
            Err(PostgresError::StartupAuthorityChanged)
        ));
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
        assert!(state.snapshot().replication_evidence.is_none());
        assert!(state.snapshot().lease.is_none());
    }

    #[tokio::test]
    async fn replication_standby_rejects_writable_supervision_independently() {
        let root = TempDir::new().expect("create standby authority fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let socket = root.path().join("socket");
        let (config, _) = standby_test_config(&root, data_dir, executable, socket);
        let prepared = prepare_fixture(config).expect("prepare standby authority fixture");
        let state = agent_state();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let result = prepared
            .supervise_with_writable_authority(
                state.clone(),
                shutdown_rx,
                Duration::from_secs(1),
                crate::writable::writable_attempt_pair_for_test().1,
            )
            .await;
        assert!(matches!(
            result,
            Err(PostgresError::WritableAuthorityForbidden)
        ));
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[test]
    fn accepts_only_owner_private_socket_directory_modes() {
        let fixture = TempDir::new().expect("create socket fixture");
        let socket = fixture.path().join("socket");
        fs::create_dir(&socket).expect("create socket directory");

        for mode in [0o700, 0o2_700] {
            fs::set_permissions(&socket, fs::Permissions::from_mode(mode))
                .expect("set private socket mode");
            assert!(validate_socket_dir(&socket, geteuid().as_raw()).is_ok());
        }
        for mode in [0o750, 0o2_750, 0o1_700] {
            fs::set_permissions(&socket, fs::Permissions::from_mode(mode))
                .expect("set unsafe socket mode");
            assert!(matches!(
                validate_socket_dir(&socket, geteuid().as_raw()),
                Err(PostgresError::UnsafePermissions {
                    name: "PostgreSQL socket directory",
                    mode: actual,
                    ..
                }) if actual == mode
            ));
        }
    }

    #[test]
    fn rewrites_only_agent_thread_pid_and_preserves_postgres_orphan_proof() {
        let fixture = pgdata_fixture();
        let lock = fixture.path().join("postmaster.pid");
        let (_thread, thread_pid) = live_agent_thread();
        let suffix = "\n/data\n123456\n5432\n/socket\n\n4242 4343\n";
        fs::write(&lock, format!("{thread_pid}{suffix}")).expect("write thread collision lock");
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).expect("protect crash lock");
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let runtime = TempDir::new().expect("create runtime fixture");
        let socket = runtime.path().join("socket");
        fs::create_dir(&socket).expect("create persistent socket directory");
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o700))
            .expect("protect persistent socket directory");
        let socket_lock = socket.join(SOCKET_LOCK_FILE);
        fs::write(&socket_lock, format!("{thread_pid}{suffix}")).expect("write socket crash lock");
        fs::set_permissions(&socket_lock, fs::Permissions::from_mode(0o600))
            .expect("protect socket crash lock");
        let data_lock_inode = fs::metadata(&lock).expect("inspect data lock").ino();
        let socket_lock_inode = fs::metadata(&socket_lock)
            .expect("inspect socket lock")
            .ino();
        let config = test_config(fixture.path().to_owned(), executable, socket);
        let prepared = prepare_fixture(config.clone()).expect("prepare crash recovery");
        rewrite_agent_thread_lock(&lock, prepared.validated.data.postmaster_lock)
            .expect("rewrite exact current-process thread collision");
        rewrite_agent_thread_lock(&socket_lock, prepared.validated.socket_lock)
            .expect("rewrite exact socket thread collision");
        let expected = format!("{}{suffix}", getpid().as_raw_pid());
        assert_eq!(fs::read_to_string(&lock).expect("read data lock"), expected);
        assert_eq!(
            fs::read_to_string(&socket_lock).expect("read socket lock"),
            expected
        );
        assert_ne!(
            fs::metadata(&lock)
                .expect("inspect replaced data lock")
                .ino(),
            data_lock_inode,
            "thread-PID rewrite must atomically replace rather than truncate the lock"
        );
        assert_ne!(
            fs::metadata(&socket_lock)
                .expect("inspect replaced socket lock")
                .ino(),
            socket_lock_inode,
            "socket-lock rewrite must atomically replace rather than truncate the lock"
        );
        drop(prepared);

        fs::write(&lock, format!("{}\n", std::process::id())).expect("write parent collision lock");
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600))
            .expect("protect parent collision lock");
        let parent = prepare_fixture(config.clone()).expect("prepare parent PID lock");
        rewrite_agent_thread_lock(&lock, parent.validated.data.postmaster_lock)
            .expect("leave direct-parent collision to PostgreSQL");
        assert!(
            lock.exists(),
            "PostgreSQL natively handles the direct parent PID exception"
        );
        drop(parent);

        let unrelated = {
            let _exec_handoff = test_exec_handoff_guard();
            ProcessGuard(
                std::process::Command::new("/bin/sleep")
                    .arg("30")
                    .spawn()
                    .expect("spawn unrelated live process"),
            )
        };
        fs::write(&lock, format!("{}\n", unrelated.0.id())).expect("replace crash lock PID");
        let unrelated_prepared = prepare_fixture(config).expect("prepare unrelated PID lock");
        rewrite_agent_thread_lock(&lock, unrelated_prepared.validated.data.postmaster_lock)
            .expect("leave unrelated live PID lock untouched");
        assert!(lock.exists(), "unrelated PID lock must remain fail-closed");

        for contents in ["", "-1\n", "not-a-pid\n", "4294967295\n"] {
            fs::write(&lock, contents).expect("replace malformed crash lock");
            assert!(matches!(
                validate_data_dir(fixture.path(), geteuid().as_raw()),
                Err(PostgresError::InvalidPostmasterLock { .. })
            ));
        }
    }

    #[test]
    fn rejects_external_pid_symlinks_without_touching_the_target() {
        let root = TempDir::new().expect("create external PID fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let socket = root.path().join("socket");
        fs::create_dir(&socket).expect("create socket directory");
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o700))
            .expect("protect socket directory");
        let sentinel = root.path().join("sentinel");
        fs::write(&sentinel, b"must survive\n").expect("write sentinel");
        symlink(&sentinel, socket.join(EXTERNAL_PID_FILE)).expect("link external PID file");

        let result = prepare_fixture(test_config(data_dir, executable, socket));

        assert!(matches!(
            result,
            Err(PostgresError::Symlink {
                name: "PostgreSQL external PID file",
                ..
            })
        ));
        assert_eq!(
            fs::read(&sentinel).expect("read sentinel"),
            b"must survive\n"
        );
    }

    #[test]
    fn preserves_only_an_exact_validated_external_pid_file() {
        let root = TempDir::new().expect("create external PID fixture");
        let path = root.path().join(EXTERNAL_PID_FILE);
        fs::write(&path, format!("{}\n", getpid().as_raw_pid())).expect("write external PID");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("set PostgreSQL external PID mode");
        let snapshot = validate_external_pid_file_at(&path, geteuid().as_raw())
            .expect("validate external PID")
            .expect("external PID exists");

        revalidate_external_pid_file(&path, Some(snapshot)).expect("preserve exact external PID");

        assert_eq!(
            fs::read_to_string(&path).expect("read preserved PID"),
            format!("{}\n", getpid().as_raw_pid())
        );

        let replacement = root.path().join("replacement.pid");
        fs::write(&replacement, "1\n").expect("write replacement PID");
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o644))
            .expect("set replacement PID mode");
        fs::rename(&replacement, &path).expect("replace validated PID path");

        assert!(matches!(
            revalidate_external_pid_file(&path, Some(snapshot)),
            Err(PostgresError::PreparedStateChanged)
        ));
        assert_eq!(fs::read_to_string(&path).expect("read replacement"), "1\n");

        let newly_created = root.path().join("new.pid");
        revalidate_external_pid_file(&newly_created, None)
            .expect("preserve absent external PID state");
        fs::write(&newly_created, "1\n").expect("create late external PID");
        assert!(matches!(
            revalidate_external_pid_file(&newly_created, None),
            Err(PostgresError::PreparedStateChanged)
        ));
    }

    #[test]
    fn preserves_exact_external_pid_creation_transients_for_postgresql() {
        let root = TempDir::new().expect("create external PID transient fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o750))
            .expect("make PGDATA group readable");
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let socket = root.path().join("socket");
        fs::create_dir(&socket).expect("create socket directory");
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o700))
            .expect("protect socket directory");
        let config = test_config(data_dir, executable, socket.clone());
        let path = socket.join(EXTERNAL_PID_FILE);

        for (mode, contents) in [
            (0o600, b"".as_slice()),
            (0o600, b"12".as_slice()),
            (0o640, b"".as_slice()),
            (0o640, b"12".as_slice()),
            (0o640, b"123\n".as_slice()),
        ] {
            fs::write(&path, contents).expect("write external PID creation transient");
            fs::set_permissions(&path, fs::Permissions::from_mode(mode))
                .expect("set external PID transient mode");
            let prepared =
                prepare_fixture(config.clone()).expect("accept PostgreSQL-created transient");
            prepared
                .finalize_pre_spawn(&prepared.validated)
                .expect("preserve exact PostgreSQL-created transient");
            assert_eq!(
                fs::read(&path).expect("read preserved external PID transient"),
                contents
            );
        }

        fs::write(&path, b"12").expect("write incomplete final external PID");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("set final external PID mode");
        assert!(matches!(
            prepare_fixture(config),
            Err(PostgresError::InvalidExternalPidFile { .. })
        ));
    }

    #[test]
    fn command_forces_network_recovery_and_crash_safety_settings() {
        let fixture = pgdata_fixture();
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let runtime = TempDir::new().expect("create runtime fixture");
        let socket = runtime.path().join("socket");
        let config = test_config(fixture.path().to_owned(), executable, socket);
        let prepared = prepare_fixture(config).expect("prepare command fixture");
        let command = prepared.command();
        let arguments: Vec<_> = command.as_std().get_args().collect();
        let mut data_directory = OsString::from("data_directory=");
        data_directory.push(fixture.path());
        assert!(
            arguments.contains(&data_directory.as_os_str()),
            "validated PGDATA must override configuration-file redirection"
        );
        let mut hba_file = OsString::from("hba_file=");
        hba_file.push(runtime.path().join("quarantine.pg_hba.conf"));
        assert!(
            arguments.contains(&hba_file.as_os_str()),
            "private generation-publisher HBA policy must override PGDATA configuration"
        );
        let mut external_pid_file = OsString::from("external_pid_file=");
        external_pid_file.push(runtime.path().join("socket/postmaster.external.pid"));
        assert!(
            arguments.contains(&external_pid_file.as_os_str()),
            "external PID writes must be confined to the private socket directory"
        );
        for required in [
            "listen_addresses=",
            "unix_socket_group=",
            "restart_after_crash=off",
            "primary_conninfo=",
            "primary_slot_name=",
            "restore_command=",
            "archive_cleanup_command=",
            "recovery_end_command=",
            "archive_mode=on",
            "archive_command=",
            "archive_library=",
            "max_wal_senders=0",
            "max_logical_replication_workers=0",
            "sync_replication_slots=off",
            "wal_receiver_create_temp_slot=off",
            "idle_replication_slot_timeout=0",
            "max_slot_wal_keep_size=-1",
            "shared_preload_libraries=",
            "session_preload_libraries=",
            "local_preload_libraries=",
            "event_triggers=off",
            "jit=off",
            "fsync=on",
            "full_page_writes=on",
            "ignore_invalid_pages=off",
            "data_sync_retry=off",
            "ignore_checksum_failure=off",
            "zero_damaged_pages=off",
            "log_statement=none",
            "log_min_error_statement=panic",
            "log_parameter_max_length=0",
            "log_parameter_max_length_on_error=0",
        ] {
            assert!(
                arguments.contains(&OsStr::new(required)),
                "missing {required:?}"
            );
        }
        assert!(
            !arguments
                .iter()
                .any(|argument| argument.as_bytes().starts_with(b"max_replication_slots=")),
            "quarantine must preserve persistent slots instead of shrinking their startup capacity"
        );
    }

    #[test]
    fn replication_bootstrap_primary_command_opens_only_physical_replication_paths() {
        let fixture = pgdata_fixture();
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let runtime = TempDir::new().expect("create runtime fixture");
        let socket = runtime.path().join("socket");
        let hba = runtime
            .path()
            .join("replication-bootstrap-primary.pg_hba.conf");
        fs::write(&hba, REPLICATION_BOOTSTRAP_PRIMARY_HBA_CONTENT).expect("write replication HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect replication HBA");
        let config = PostgresConfig::new_for_role(
            PostgresRuntimeRole::ReplicationBootstrapPrimary,
            None,
            GenerationDurability::remote_apply_any_one(vec![
                "pgshard_member_0001".to_owned(),
                "pgshard_member_0002".to_owned(),
            ])
            .expect("valid any-one topology"),
            fixture.path().to_owned(),
            executable,
            socket,
            hba,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid replication-bootstrap-primary config");
        assert_eq!(
            config.starting_process_state(),
            PostgresProcessState::StartingReplicationBootstrap
        );
        assert_eq!(
            config.running_process_state(),
            PostgresProcessState::RunningReplicationBootstrap
        );
        let prepared = prepare_fixture(config).expect("prepare replication bootstrap primary");
        let command = prepared.command();
        let arguments: Vec<_> = command.as_std().get_args().collect();
        for required in [
            "listen_addresses=*",
            "max_wal_senders=5",
            "max_replication_slots=5",
            "wal_level=logical",
            "synchronous_standby_names=ANY 1 (pgshard_member_0001, pgshard_member_0002)",
            "synchronous_commit=local",
            "archive_mode=off",
        ] {
            assert!(
                arguments.contains(&OsStr::new(required)),
                "missing {required:?}"
            );
        }
        for forbidden in ["listen_addresses=", "max_wal_senders=0"] {
            assert!(
                !arguments.contains(&OsStr::new(forbidden)),
                "replication bootstrap primary retained quarantine setting {forbidden:?}"
            );
        }
    }

    #[tokio::test]
    async fn direct_supervision_refuses_replication_bootstrap_primary_without_writable_authority() {
        let fixture = pgdata_fixture();
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let runtime = TempDir::new().expect("create runtime fixture");
        let socket = runtime.path().join("socket");
        let hba = runtime
            .path()
            .join("replication-bootstrap-primary.pg_hba.conf");
        fs::write(&hba, REPLICATION_BOOTSTRAP_PRIMARY_HBA_CONTENT).expect("write replication HBA");
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400))
            .expect("protect replication HBA");
        let config = PostgresConfig::new_replication_bootstrap_primary(
            fixture.path().to_owned(),
            executable,
            socket,
            hba,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid replication-bootstrap-primary config");
        let prepared = prepare_fixture(config).expect("prepare replication bootstrap primary");

        assert!(matches!(
            prepared
                .supervise(agent_state(), std::future::pending())
                .await,
            Err(PostgresError::WritableAuthorityRequired)
        ));
    }

    #[test]
    fn postgres_config_revalidates_remote_generation_candidates() {
        assert!(matches!(
            PostgresConfig::new_for_role(
                PostgresRuntimeRole::ReplicationBootstrapPrimary,
                None,
                GenerationDurability::RemoteApplyAnyOne {
                    application_names: vec!["pgshard_member_0001".to_owned()],
                },
                PathBuf::from("/var/lib/postgresql/data"),
                PathBuf::from("/usr/lib/postgresql/18/bin/postgres"),
                PathBuf::from("/run/pgshard/postgres"),
                PathBuf::from("/etc/pgshard/replication-primary.pg_hba.conf"),
                Duration::from_secs(5),
                Duration::from_secs(44),
                Duration::from_millis(500),
            ),
            Err(PostgresConfigError::InvalidGenerationDurabilityComposition)
        ));
    }

    #[test]
    fn rejects_postgres_executable_writable_by_runtime_group_or_world() {
        let fixture = TempDir::new().expect("create executable fixture");
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        for mode in [0o700, 0o520, 0o502] {
            fs::set_permissions(&executable, fs::Permissions::from_mode(mode))
                .expect("make executable writable");
            assert!(matches!(
                validate_executable(&executable, geteuid().as_raw()),
                Err(PostgresError::UnsafePermissions {
                    name: "PostgreSQL executable",
                    mode: actual,
                    ..
                }) if actual == mode
            ));
        }
    }

    #[test]
    fn rejects_special_mode_bits_on_postgres_executable() {
        let fixture = TempDir::new().expect("create executable fixture");
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");

        for mode in [0o4_755, 0o2_755] {
            fs::set_permissions(&executable, fs::Permissions::from_mode(mode))
                .expect("set special executable mode");
            assert!(matches!(
                validate_executable(&executable, geteuid().as_raw()),
                Err(PostgresError::UnsafePermissions {
                    name: "PostgreSQL executable",
                    mode: actual,
                    ..
                }) if actual == mode
            ));
        }
    }

    #[tokio::test]
    async fn already_requested_shutdown_never_starts_postgres() {
        let root = TempDir::new().expect("create cancellation fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let prepared = prepare_fixture(test_config(
            data_dir,
            executable,
            root.path().join("socket"),
        ))
        .expect("prepare cancellation fixture");
        let state = agent_state();

        let result = prepared
            .supervise(state.clone(), std::future::ready(()))
            .await;

        assert!(result.is_ok());
        assert!(!marker.exists(), "cancelled supervision started PostgreSQL");
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Validated
        );
    }

    #[tokio::test]
    async fn shutdown_requested_during_revalidation_never_starts_postgres() {
        let root = TempDir::new().expect("create cancellation fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let prepared = prepare_fixture(test_config(
            data_dir,
            executable,
            root.path().join("socket"),
        ))
        .expect("prepare cancellation fixture");
        let state = agent_state();

        let result = prepared
            .supervise(
                state.clone(),
                ReadyOnPoll {
                    polls: 0,
                    ready_on: 2,
                },
            )
            .await;

        assert!(result.is_ok());
        assert!(
            !marker.exists(),
            "shutdown observed after revalidation still started PostgreSQL"
        );
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Validated
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_a_genuinely_blocked_validation_wait() {
        let fixture = pgdata_fixture();
        let executable = fixture.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let runtime = TempDir::new().expect("create runtime fixture");
        let config = test_config(
            fixture.path().to_owned(),
            executable,
            runtime.path().join("socket"),
        );
        let validated = validate_prepared_state(&config, true).expect("validate fixture");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let mut task = tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("publish blocked validation");
            release_rx.recv().expect("release blocked validation");
            Ok(validated)
        });
        let shutdown = async {
            started_rx.await.expect("observe blocked validation");
        };
        tokio::pin!(shutdown);

        let result = await_validation(&mut task, shutdown.as_mut())
            .await
            .expect("shutdown is not a validation failure");

        assert!(result.is_none(), "blocked validation must be abandoned");
        release_tx.send(()).expect("release validation worker");
        task.await
            .expect("join released validation worker")
            .expect("released validation succeeds");
    }

    #[tokio::test]
    async fn revalidates_recovery_state_immediately_before_spawn() {
        let root = TempDir::new().expect("create revalidation fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let config = test_config(data_dir.clone(), executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare fixture");
        fs::write(data_dir.join("backup_label"), []).expect("add late recovery marker");
        let state = agent_state();

        let result = prepared
            .supervise(state.clone(), std::future::pending())
            .await;
        assert!(matches!(
            result,
            Err(PostgresError::RecoveryStatePresent { path })
                if path == data_dir.join("backup_label")
        ));
        assert!(
            !marker.exists(),
            "postmaster ran after recovery state changed"
        );
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[tokio::test]
    async fn replication_standby_revalidates_signal_and_passfile_before_spawn() {
        for mutate_signal in [true, false] {
            let root = TempDir::new().expect("create standby revalidation fixture");
            let data_dir = root.path().join("data");
            pgdata_fixture_at(&data_dir);
            let marker = root.path().join("postmaster-started");
            let executable = root.path().join("postgres");
            write_executable(
                &executable,
                &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
            );
            let socket = root.path().join("socket");
            let (config, passfile) =
                standby_test_config(&root, data_dir.clone(), executable, socket);
            let prepared = prepare_fixture(config).expect("prepare standby fixture");
            let path = if mutate_signal {
                data_dir.join("standby.signal")
            } else {
                passfile
            };
            fs::remove_file(&path).expect("remove prepared standby input");
            fs::write(
                &path,
                if mutate_signal {
                    &b""[..]
                } else {
                    &b"primary.database.svc:5432:*:pgshard_replication:replacement\n"[..]
                },
            )
            .expect("replace prepared standby input");
            fs::set_permissions(
                &path,
                fs::Permissions::from_mode(if mutate_signal { 0o600 } else { 0o400 }),
            )
            .expect("protect replaced standby input");
            let state = agent_state();
            let result = prepared
                .supervise(state.clone(), std::future::pending())
                .await;
            assert!(matches!(result, Err(PostgresError::PreparedStateChanged)));
            assert!(
                !marker.exists(),
                "postmaster ran after standby input changed"
            );
            assert_eq!(
                state.snapshot().postgres_process,
                PostgresProcessState::Failed
            );
        }
    }

    #[tokio::test]
    async fn revalidates_executable_identity_immediately_before_spawn() {
        let root = TempDir::new().expect("create revalidation fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let config = test_config(data_dir, executable.clone(), root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare fixture");
        fs::remove_file(&executable).expect("replace validated executable");
        write_executable(&executable, "#!/bin/sh\nexit 42\n");
        let state = agent_state();

        let result = prepared
            .supervise(state.clone(), std::future::pending())
            .await;
        assert!(matches!(result, Err(PostgresError::PreparedStateChanged)));
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[test]
    fn durable_writable_generation_advances_and_rejects_stale_or_foreign_state() {
        let root = TempDir::new().expect("create durable-generation fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 0\n");
        let config = test_config(data_dir.clone(), executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare durable-generation fixture");
        let generation_path = data_dir.join(DURABLE_WRITABLE_GENERATION_FILE);
        let staging_path = data_dir.join(DURABLE_WRITABLE_GENERATION_STAGING_FILE);
        let first = writable_generation(1, TEST_WRITABLE_HOLDER, TEST_WRITABLE_LEASE_UID);
        assert!(!generation_path.exists());

        prepared
            .persist_durable_writable_generation(&first)
            .expect("persist first generation");
        assert_eq!(
            fs::read(&generation_path).expect("read first generation"),
            first.canonical_bytes()
        );
        prepared
            .persist_durable_writable_generation(&first)
            .expect("replay exact generation and complete its barrier");

        fs::write(&staging_path, b"interrupted").expect("write interrupted staging file");
        fs::set_permissions(&staging_path, fs::Permissions::from_mode(0o600))
            .expect("protect interrupted staging file");
        let second = writable_generation(
            2,
            "cluster-1-shard-0-1/bbbbbbbb-cccc-dddd-eeee-ffffffffffff/89abcdef0123456789abcdef",
            TEST_WRITABLE_LEASE_UID,
        );
        prepared
            .persist_durable_writable_generation(&second)
            .expect("clean interrupted staging and advance generation");
        assert!(!staging_path.exists());
        assert_eq!(
            fs::read(&generation_path).expect("read second generation"),
            second.canonical_bytes()
        );

        assert!(matches!(
            prepared.persist_durable_writable_generation(&first),
            Err(PostgresError::WritableGenerationRegression {
                durable: 2,
                requested: 1,
            })
        ));
        let conflicting = writable_generation(
            2,
            "cluster-1-shard-0-2/cccccccc-dddd-eeee-ffff-000000000000/abcdef0123456789abcdef01",
            TEST_WRITABLE_LEASE_UID,
        );
        assert!(matches!(
            prepared.persist_durable_writable_generation(&conflicting),
            Err(PostgresError::WritableGenerationConflict { term: 2 })
        ));
        let foreign = writable_generation(
            3,
            TEST_WRITABLE_HOLDER,
            "77777777-6666-5555-4444-333333333333",
        );
        assert!(matches!(
            prepared.persist_durable_writable_generation(&foreign),
            Err(PostgresError::ForeignWritableGeneration { path }) if path == generation_path
        ));

        fs::write(&generation_path, b"not-canonical\n")
            .expect("corrupt durable generation fixture");
        assert!(matches!(
            prepared.persist_durable_writable_generation(&writable_generation(
                3,
                TEST_WRITABLE_HOLDER,
                TEST_WRITABLE_LEASE_UID,
            )),
            Err(PostgresError::InvalidWritableGeneration { path }) if path == generation_path
        ));
    }

    #[tokio::test]
    async fn publication_faults_block_spawn_and_recover_at_every_durable_boundary() {
        for checkpoint in [
            GenerationPublicationCheckpoint::StagingFileSynced,
            GenerationPublicationCheckpoint::GenerationRenamed,
            GenerationPublicationCheckpoint::DirectorySyncPending,
        ] {
            assert_publication_fault_blocks_and_recovers(checkpoint).await;
        }
    }

    async fn assert_publication_fault_blocks_and_recovers(
        checkpoint: GenerationPublicationCheckpoint,
    ) {
        let root = TempDir::new().expect("create publication-fault fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\n: > '{}'\ntrap '' TERM INT\nwhile :; do sleep 1; done\n",
                marker.display()
            ),
        );
        let config = test_config(data_dir.clone(), executable, root.path().join("socket"));
        let previous = writable_generation(1, TEST_WRITABLE_HOLDER, TEST_WRITABLE_LEASE_UID);
        let generation = writable_generation(
            2,
            "cluster-1-shard-0-1/bbbbbbbb-cccc-dddd-eeee-ffffffffffff/89abcdef0123456789abcdef",
            TEST_WRITABLE_LEASE_UID,
        );
        let state = state_with_writable_lease(2);
        let prepared = prepare_fixture(config.clone()).expect("prepare publication-fault fixture");
        prepared
            .persist_durable_writable_generation(&previous)
            .expect("persist prior durable generation");
        let fault = inject_generation_publication_fault(checkpoint);
        let failed = prepared
            .spawn_tracked_postmaster(&state, &|| {
                PostgresStartDecision::StartWritable(generation.clone())
            })
            .await;
        drop(fault);

        assert!(matches!(
            failed,
            Err(PostgresError::InjectedGenerationPublicationFault)
        ));
        assert!(!marker.exists(), "postmaster started before {checkpoint:?}");
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
        assert_interrupted_publication(&data_dir, checkpoint, &previous, &generation);

        let recovered_state = state_with_writable_lease(2);
        let recovered = prepare_fixture(config.clone()).expect("prepare publication recovery");
        let Some((mut process_fence, pidfd, process_group, _authorization)) = recovered
            .spawn_tracked_postmaster(&recovered_state, &|| {
                PostgresStartDecision::StartWritable(generation.clone())
            })
            .await
            .expect("complete publication barrier before retry spawn")
        else {
            panic!("recovered publication did not create a postmaster");
        };
        wait_for_marker(&marker).await;
        let generation_path = data_dir.join(DURABLE_WRITABLE_GENERATION_FILE);
        let staging_path = data_dir.join(DURABLE_WRITABLE_GENERATION_STAGING_FILE);
        assert_eq!(
            fs::read(generation_path).expect("read recovered generation"),
            generation.canonical_bytes()
        );
        assert!(!staging_path.exists());
        target_fence_child(
            &mut process_fence.child,
            &pidfd,
            process_group,
            &process_fence.child_subreaper,
            &config,
        )
        .await
        .expect("fence recovered publication fixture");
        process_fence.disarm_if_reaped();
    }

    fn assert_interrupted_publication(
        data_dir: &Path,
        checkpoint: GenerationPublicationCheckpoint,
        previous: &DurableWritableGeneration,
        requested: &DurableWritableGeneration,
    ) {
        let generation_path = data_dir.join(DURABLE_WRITABLE_GENERATION_FILE);
        let staging_path = data_dir.join(DURABLE_WRITABLE_GENERATION_STAGING_FILE);
        if checkpoint == GenerationPublicationCheckpoint::StagingFileSynced {
            assert_eq!(
                fs::read(generation_path).expect("read prior generation"),
                previous.canonical_bytes()
            );
            assert_eq!(
                fs::read(staging_path).expect("read staged generation"),
                requested.canonical_bytes()
            );
        } else {
            assert_eq!(
                fs::read(generation_path).expect("read requested generation"),
                requested.canonical_bytes()
            );
            assert!(!staging_path.exists());
        }
    }

    #[test]
    fn durable_writable_generation_requires_exact_bootstrap_identity() {
        for mismatch in [false, true] {
            let root = TempDir::new().expect("create bootstrap-identity fixture");
            let data_dir = root.path().join("data");
            pgdata_fixture_at(&data_dir);
            let bootstrap_path = data_dir.join(BOOTSTRAP_IDENTITY_FILE);
            if mismatch {
                fs::write(
                    &bootstrap_path,
                    b"cluster_uid=00000000-0000-0000-0000-000000000000\nshard=0\n",
                )
                .expect("write foreign bootstrap identity");
            } else {
                fs::remove_file(&bootstrap_path).expect("remove bootstrap identity");
            }
            let executable = root.path().join("postgres");
            write_executable(&executable, "#!/bin/sh\nexit 0\n");
            let prepared = prepare_fixture(test_config(
                data_dir,
                executable,
                root.path().join("socket"),
            ))
            .expect("prepare bootstrap-identity fixture");

            let result = prepared.persist_durable_writable_generation(&writable_generation(
                1,
                TEST_WRITABLE_HOLDER,
                TEST_WRITABLE_LEASE_UID,
            ));
            if mismatch {
                assert!(matches!(
                    result,
                    Err(PostgresError::BootstrapIdentityMismatch { path })
                        if path == bootstrap_path
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(PostgresError::BootstrapIdentityMissing { path })
                        if path == bootstrap_path
                ));
            }
        }
    }

    #[tokio::test]
    async fn changed_authority_after_generation_flush_never_starts_postgres() {
        let root = TempDir::new().expect("create generation-handoff fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let prepared = prepare_fixture(test_config(
            data_dir.clone(),
            executable,
            root.path().join("socket"),
        ))
        .expect("prepare generation-handoff fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install generation-handoff authority");
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = prepared
            .supervise_with_writable_authority_guard(
                state.clone(),
                shutdown_rx,
                || {
                    let call = calls.fetch_add(1, Ordering::AcqRel);
                    Some(writable_generation(
                        if call == 0 { 1 } else { 2 },
                        TEST_WRITABLE_HOLDER,
                        TEST_WRITABLE_LEASE_UID,
                    ))
                },
                crate::writable::writable_attempt_pair_for_test().1,
            )
            .await;

        assert!(matches!(
            result,
            Err(PostgresError::StartupAuthorityChanged)
        ));
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert!(!marker.exists(), "postmaster ran after its term changed");
        let durable = fs::read(data_dir.join(DURABLE_WRITABLE_GENERATION_FILE))
            .expect("read durable pre-spawn generation");
        assert_eq!(
            DurableWritableGeneration::parse_canonical(&durable)
                .expect("parse durable pre-spawn generation")
                .term(),
            1
        );
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[tokio::test]
    async fn startup_guard_blocks_process_creation_after_validation() {
        let root = TempDir::new().expect("create startup-authority fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare startup-authority fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install authority rejected by the final guard");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = prepared
            .supervise_with_writable_authority_guard(
                state.clone(),
                shutdown_rx,
                || None,
                crate::writable::writable_attempt_pair_for_test().1,
            )
            .await;

        assert!(matches!(
            result,
            Err(PostgresError::StartupAuthorityMissing)
        ));
        assert!(!marker.exists(), "postmaster ran without startup authority");
        assert!(
            state.snapshot().lease.is_none(),
            "failed startup guard retained local authority"
        );
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[tokio::test]
    async fn public_writable_supervisor_enforces_the_process_fence_budget() {
        let root = TempDir::new().expect("create fence-budget fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let generation_path = data_dir.join(DURABLE_WRITABLE_GENERATION_FILE);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare fence-budget fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 4_000,
                },
                1_000,
            )
            .expect("install authority inside the process fence budget");
        assert!(state.snapshot().lease.is_some());
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state.lease_deadline().expect("monotonic deadline"),
            durable_generation_for_test(1),
        );
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = prepared
            .supervise_with_writable_authority(
                state.clone(),
                shutdown_rx,
                Duration::ZERO,
                postgres_attempt,
            )
            .await;

        assert!(matches!(
            result,
            Err(PostgresError::StartupAuthorityMissing)
        ));
        assert!(!marker.exists(), "postmaster bypassed the fence budget");
        assert!(
            !generation_path.exists(),
            "authority inside the fence budget advanced durable generation"
        );
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[tokio::test]
    async fn wal_publication_gates_running_quarantined_state() {
        let root = TempDir::new().expect("create WAL-publication gate fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let ready = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap 'exit 0' QUIT\n: > '{}'\nwhile :; do sleep 1; done\n",
                ready.display()
            ),
        );
        let prepared = prepare_fixture(test_config(
            data_dir,
            executable,
            root.path().join("socket"),
        ))
        .expect("prepare WAL-publication gate fixture");
        let state = state_with_writable_lease(1);
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state.lease_deadline().expect("monotonic deadline"),
            durable_generation_for_test(1),
        );
        let (publication_tx, publication_rx) = watch::channel(false);
        gate_next_postgres_generation_publication(publication_rx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task_state = state.clone();
        let supervisor = tokio::spawn(async move {
            prepared
                .supervise_with_writable_authority(
                    task_state,
                    shutdown_rx,
                    Duration::ZERO,
                    postgres_attempt,
                )
                .await
        });

        wait_for_marker(&ready).await;
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::StartingQuarantined,
            "pidfd tracking must not imply a durable WAL generation"
        );
        publication_tx
            .send(true)
            .expect("release WAL-publication gate");
        timeout(Duration::from_secs(1), async {
            while state.snapshot().postgres_process != PostgresProcessState::RunningQuarantined {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("WAL publication advances the quarantine state");
        shutdown_tx.send(true).expect("request writable fence");
        let result = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("bounded writable shutdown")
            .expect("join writable supervisor");
        assert!(result.is_ok(), "writable shutdown failed: {result:?}");
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Fenced
        );
    }

    #[tokio::test]
    async fn shutdown_during_wal_publication_fences_without_running_state() {
        let root = TempDir::new().expect("create publication-shutdown fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let ready = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap 'exit 0' QUIT\n: > '{}'\nwhile :; do sleep 1; done\n",
                ready.display()
            ),
        );
        let prepared = prepare_fixture(test_config(
            data_dir,
            executable,
            root.path().join("socket"),
        ))
        .expect("prepare publication-shutdown fixture");
        let state = state_with_writable_lease(1);
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state.lease_deadline().expect("monotonic deadline"),
            durable_generation_for_test(1),
        );
        let (_publication_tx, publication_rx) = watch::channel(false);
        gate_next_postgres_generation_publication(publication_rx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task_state = state.clone();
        let supervisor = tokio::spawn(async move {
            prepared
                .supervise_with_writable_authority(
                    task_state,
                    shutdown_rx,
                    Duration::ZERO,
                    postgres_attempt,
                )
                .await
        });

        wait_for_marker(&ready).await;
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::StartingQuarantined
        );
        shutdown_tx
            .send(true)
            .expect("request fence during publication");
        let result = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("bounded publication shutdown")
            .expect("join publication shutdown");
        assert!(result.is_ok(), "publication shutdown failed: {result:?}");
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Fenced
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum AuthorityClockTestPhase {
        Publication,
        Running,
    }

    #[derive(Clone, Copy, Debug)]
    enum AuthorityClockTestFault {
        SuspendJump,
        ClockFailure,
    }

    #[tokio::test]
    async fn boottime_faults_during_publication_and_running_fence_the_complete_process_tree() {
        for phase in [
            AuthorityClockTestPhase::Publication,
            AuthorityClockTestPhase::Running,
        ] {
            for fault in [
                AuthorityClockTestFault::SuspendJump,
                AuthorityClockTestFault::ClockFailure,
            ] {
                assert_boottime_fault_fences_complete_process_tree(phase, fault).await;
            }
        }
    }

    #[tokio::test]
    async fn composed_exact_boottime_cutoff_deterministically_returns_retry() {
        let root = TempDir::new().expect("create composed cutoff fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let running = root.path().join("postmaster-running");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap 'exit 0' QUIT\n: > '{}'\nwhile :; do :; done\n",
                running.display()
            ),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let required_margin = config.target_fence_budget();
        let prepared = prepare_fixture(config).expect("prepare composed cutoff supervisor");
        let initial = BoottimeInstant::from_nanos_for_test(1_000_000_000);
        let clock = Arc::new(FakeBoottimeClock::new(initial));
        let state = AgentState::with_test_clock(
            AgentIdentity {
                cluster_id: "cluster-1".to_owned(),
                shard_id: ShardId(0),
                instance_id: "cluster-1-shard-0-0".to_owned(),
            },
            10_000,
            clock.clone(),
        )
        .expect("valid composed cutoff state");
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install composed cutoff authority");
        let deadline = state.lease_deadline().expect("composed cutoff deadline");
        let cutoff = deadline
            .checked_sub(required_margin)
            .expect("composed cutoff follows boot-clock origin");
        let (lease_attempt, postgres_attempt) =
            crate::writable::writable_attempt_pair_with_clock_for_test(clock.clone());
        lease_attempt.install_authority(deadline, durable_generation_for_test(1));
        let (attempt_shutdown_tx, attempt_shutdown_rx) = watch::channel(false);
        let (_external_shutdown_tx, external_shutdown_rx) = watch::channel(false);
        let postmaster_state = state.clone();
        let postmaster = async move {
            prepared
                .supervise_with_writable_authority(
                    postmaster_state,
                    attempt_shutdown_rx,
                    Duration::ZERO,
                    postgres_attempt,
                )
                .await
        };
        let coordination_clock = clock.clone();
        let coordination = async move {
            coordination_clock
                .wait_until(cutoff)
                .await
                .expect("fake coordination cutoff wait");
            Err::<crate::coordination::WritableLeaseShutdown, _>(
                crate::coordination::WritableLeaseError::RenewDeadlineExceeded,
            )
        };
        let supervisor = tokio::spawn(crate::writable::join_supervisors(
            external_shutdown_rx,
            attempt_shutdown_tx,
            postmaster,
            coordination,
        ));

        wait_for_marker(&running).await;
        timeout(Duration::from_secs(1), async {
            while state.snapshot().postgres_process != PostgresProcessState::RunningQuarantined {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("postmaster reached running before exact cutoff");
        clock
            .advance(cutoff.saturating_duration_since(initial))
            .expect("advance fake clock to exact cutoff");

        let outcome = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("composed exact cutoff completed inside fence bound")
            .expect("join composed exact cutoff supervisor")
            .expect("exact coordination cutoff is recoverable");
        assert!(matches!(
            outcome,
            crate::writable::WritableAttemptOutcome::Retry(
                crate::coordination::WritableLeaseError::RenewDeadlineExceeded
            )
        ));
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Fenced
        );
    }

    #[allow(clippy::too_many_lines)]
    async fn assert_boottime_fault_fences_complete_process_tree(
        phase: AuthorityClockTestPhase,
        fault: AuthorityClockTestFault,
    ) {
        let root = TempDir::new().expect("create boot-clock process-fence fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let descendant = root.path().join("descendant-pid");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap '' TERM INT QUIT\n(trap '' TERM INT QUIT; while :; do :; done) &\nprintf '%s\\n' \"$!\" > '{}'\nwhile :; do :; done\n",
                descendant.display()
            ),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config.clone()).expect("prepare boot-clock supervisor");
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = AgentState::with_test_clock(
            AgentIdentity {
                cluster_id: "cluster-1".to_owned(),
                shard_id: ShardId(0),
                instance_id: "cluster-1-shard-0-0".to_owned(),
            },
            10_000,
            clock.clone(),
        )
        .expect("valid boot-clock agent state");
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install boot-clock authority");
        let (lease_attempt, postgres_attempt) =
            crate::writable::writable_attempt_pair_with_clock_for_test(clock.clone());
        lease_attempt.install_authority(
            state.lease_deadline().expect("boot-clock deadline"),
            durable_generation_for_test(1),
        );
        let publication_gate = match phase {
            AuthorityClockTestPhase::Publication => {
                let (sender, receiver) = watch::channel(false);
                gate_next_postgres_generation_publication(receiver);
                Some(sender)
            }
            AuthorityClockTestPhase::Running => None,
        };
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let supervisor_state = state.clone();
        let supervisor = tokio::spawn(async move {
            prepared
                .supervise_with_writable_authority(
                    supervisor_state,
                    shutdown_rx,
                    Duration::ZERO,
                    postgres_attempt,
                )
                .await
        });

        wait_for_marker(&descendant).await;
        let expected_state = match phase {
            AuthorityClockTestPhase::Publication => PostgresProcessState::StartingQuarantined,
            AuthorityClockTestPhase::Running => PostgresProcessState::RunningQuarantined,
        };
        timeout(Duration::from_secs(1), async {
            while state.snapshot().postgres_process != expected_state {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("postmaster reached the injected boot-clock fault phase");

        match fault {
            AuthorityClockTestFault::SuspendJump => clock
                .advance(Duration::from_secs(6))
                .expect("advance boot clock beyond authority"),
            AuthorityClockTestFault::ClockFailure => clock.fail(),
        }
        let result = timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("boot-clock fault fenced inside the cleanup bound")
            .expect("join boot-clock fault supervisor");
        assert!(
            matches!(result, Err(PostgresError::StartupAuthorityChanged)),
            "unexpected {phase:?}/{fault:?} result: {result:?}"
        );
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
        let descendant_pid = fs::read_to_string(&descendant)
            .expect("read boot-clock descendant PID")
            .trim_ascii()
            .parse::<u32>()
            .expect("parse boot-clock descendant PID");
        if let Ok(status) = fs::read_to_string(format!("/proc/{descendant_pid}/status")) {
            assert!(
                status.lines().any(|line| line.starts_with("State:\tZ")),
                "live descendant survived {phase:?}/{fault:?} process fencing"
            );
        }
        drop(publication_gate);
        let reacquired = prepare_fixture(config).expect("reacquire boot-clock-fenced PGDATA");
        drop(reacquired);
    }

    #[tokio::test]
    async fn shutdown_at_final_exec_handoff_prevents_process_creation() {
        let root = TempDir::new().expect("create shutdown-handoff fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let marker = root.path().join("postmaster-started");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!("#!/bin/sh\n: > '{}'\nexit 0\n", marker.display()),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare shutdown-handoff fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install shutdown-handoff authority");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (guard_entered_tx, guard_entered_rx) = mpsc::sync_channel(1);
        let (guard_continue_tx, guard_continue_rx) = mpsc::sync_channel(1);
        let signal = std::thread::spawn(move || {
            guard_entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("observe final exec-handoff guard");
            shutdown_tx
                .send(true)
                .expect("publish shutdown at final exec handoff");
            guard_continue_tx
                .send(())
                .expect("release final exec-handoff guard");
        });

        let result = prepared
            .supervise_with_writable_authority_guard(
                state.clone(),
                shutdown_rx,
                || {
                    guard_entered_tx
                        .send(())
                        .expect("publish final exec-handoff guard");
                    guard_continue_rx
                        .recv_timeout(Duration::from_secs(1))
                        .expect("wait for shutdown publication");
                    Some(durable_generation_for_test(1))
                },
                crate::writable::writable_attempt_pair_for_test().1,
            )
            .await;
        signal.join().expect("join shutdown publisher");

        assert!(result.is_ok(), "shutdown handoff failed: {result:?}");
        assert!(
            !marker.exists(),
            "postmaster started after shutdown reached the final exec handoff"
        );
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Validated
        );
    }

    #[tokio::test]
    async fn pidfd_observation_keeps_group_leader_unreaped_until_cleanup() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("exit 42").kill_on_drop(true);
        command.as_std_mut().process_group(0);
        let mut child = {
            let _exec_handoff = test_exec_handoff_guard();
            command.spawn().expect("spawn exit fixture")
        };
        let child_id = child.id().expect("fixture exposes child PID");
        let process_group = Pid::from_raw(i32::try_from(child_id).expect("child PID fits i32"))
            .expect("positive child PID");
        let pidfd = pidfd_open(process_group, PidfdFlags::empty()).expect("open fixture pidfd");
        let pidfd =
            AsyncFd::with_interest(pidfd, Interest::READABLE).expect("monitor fixture pidfd");

        let status = wait_pidfd_exit(&pidfd)
            .await
            .expect("observe without reaping");

        assert_eq!(status.code(), Some(42));
        assert_eq!(
            child.id(),
            Some(child_id),
            "WNOWAIT observation must retain the leader PID as a reuse barrier"
        );
        let process_status = fs::read(format!("/proc/{child_id}/status"))
            .expect("unreaped group leader remains visible");
        assert_eq!(
            status_field(&process_status, b"State:").and_then(|value| value.first().copied()),
            Some(b'Z')
        );

        let child_subreaper = ChildSubreaper::claim().expect("create unit-test process fence");
        kill_and_reap(
            &mut child,
            Some(&pidfd),
            Some(process_group),
            &child_subreaper,
        )
        .await
        .expect("cleanup then reap fixture");
        assert!(child.id().is_none());
    }

    #[tokio::test]
    async fn unexpected_child_exit_is_terminal_and_reaped() {
        let root = TempDir::new().expect("create supervisor fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let executable = root.path().join("postgres");
        write_executable(&executable, "#!/bin/sh\nexit 42\n");
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 1_000,
                },
                100,
            )
            .expect("install lease before quarantine");

        let result = prepared
            .supervise(state.clone(), std::future::pending())
            .await;
        assert!(matches!(
            result,
            Err(PostgresError::UnexpectedExit(status)) if status.code() == Some(42)
        ));
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
        assert!(state.snapshot().lease.is_none());
    }

    #[tokio::test]
    async fn unexpected_leader_exit_cleans_descendants_before_releasing_pgdata() {
        let root = TempDir::new().expect("create unexpected-exit fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let descendant = root.path().join("descendant-pid");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\n(trap '' TERM INT QUIT; while :; do :; done) &\nprintf '%s\\n' \"$!\" > '{}'\nexit 42\n",
                descendant.display()
            ),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config.clone()).expect("prepare fixture");

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise(agent_state(), std::future::pending()),
        )
        .await
        .expect("bounded unexpected-exit cleanup");

        assert!(matches!(
            result,
            Err(PostgresError::UnexpectedExit(status)) if status.code() == Some(42)
        ));
        let descendant_pid = fs::read_to_string(&descendant)
            .expect("read descendant PID")
            .trim_ascii()
            .parse::<u32>()
            .expect("parse descendant PID");
        if let Ok(status) = fs::read(format!("/proc/{descendant_pid}/status")) {
            assert_eq!(
                status_field(&status, b"State:").and_then(|value| value.first().copied()),
                Some(b'Z'),
                "live descendant survived unexpected-leader cleanup"
            );
        }
        let reacquired = prepare_fixture(config).expect("reacquire safe PGDATA fence");
        drop(reacquired);
    }

    #[tokio::test]
    async fn requested_shutdown_signals_and_reaps_the_direct_child() {
        let root = TempDir::new().expect("create supervisor fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let ready = root.path().join("signal-handlers-ready");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap 'exit 0' TERM\n: > '{}'\nwhile :; do :; done\n",
                ready.display()
            ),
        );
        let hba_file = root.path().join("quarantine.pg_hba.conf");
        write_hba(&hba_file);
        let config = PostgresConfig::new(
            data_dir,
            executable,
            root.path().join("socket"),
            hba_file,
            Duration::from_millis(500),
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .expect("valid supervisor config");
        let prepared = prepare_fixture(config).expect("prepare fixture");
        let state = agent_state();

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise(state.clone(), wait_for_marker(&ready)),
        )
        .await
        .expect("bounded supervisor shutdown");
        assert!(result.is_ok(), "shutdown failed: {result:?}");
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Validated
        );
    }

    #[tokio::test]
    async fn target_fence_skips_smart_and_fast_shutdown() {
        let root = TempDir::new().expect("create target-fence fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let generation_path = data_dir.join(DURABLE_WRITABLE_GENERATION_FILE);
        let ready = root.path().join("signal-handlers-ready");
        let smart = root.path().join("smart-shutdown");
        let fast = root.path().join("fast-shutdown");
        let fenced = root.path().join("target-fenced");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap 'printf smart > \"{}\"; exit 0' TERM\ntrap 'printf fast > \"{}\"; exit 0' INT\ntrap 'printf fenced > \"{}\"; exit 0' QUIT\n: > '{}'\nwhile :; do :; done\n",
                smart.display(),
                fast.display(),
                fenced.display(),
                ready.display()
            ),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config).expect("prepare target-fence fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install target-fence authority");
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state.lease_deadline().expect("monotonic deadline"),
            durable_generation_for_test(1),
        );
        let shutdown_state = state.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shutdown_task = tokio::spawn(async move {
            assert!(
                shutdown_state.snapshot().lease.is_some(),
                "writable supervisor cleared authority before process creation"
            );
            wait_for_marker(&ready).await;
            assert!(
                shutdown_state.snapshot().lease.is_some(),
                "writable supervisor cleared authority while PostgreSQL was running"
            );
            shutdown_tx.send(true).expect("request target fence");
        });

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise_with_writable_authority(
                state.clone(),
                shutdown_rx,
                Duration::from_secs(1),
                postgres_attempt,
            ),
        )
        .await
        .expect("bounded target fence");
        shutdown_task.await.expect("join target-fence request");

        assert!(result.is_ok(), "target fence failed: {result:?}");
        assert!(!smart.exists(), "target fence attempted smart shutdown");
        assert!(!fast.exists(), "target fence attempted fast shutdown");
        assert_eq!(
            fs::read_to_string(fenced).expect("read target-fence marker"),
            "fenced"
        );
        assert_eq!(
            fs::read(generation_path).expect("read target-fence generation"),
            durable_generation_for_test(1).canonical_bytes()
        );
        assert!(state.snapshot().lease.is_none());
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Fenced
        );
    }

    #[tokio::test]
    async fn target_fence_kills_the_complete_unresponsive_process_tree() {
        let root = TempDir::new().expect("create target-fence process-tree fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let descendant = root.path().join("descendant-pid");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap '' TERM INT QUIT\n(trap '' TERM INT QUIT; while :; do :; done) &\nprintf '%s\\n' \"$!\" > '{}'\nwhile :; do :; done\n",
                descendant.display()
            ),
        );
        let config = test_config(data_dir, executable, root.path().join("socket"));
        let prepared = prepare_fixture(config.clone()).expect("prepare target-fence fixture");
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install process-tree fence authority");
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        lease_attempt.install_authority(
            state.lease_deadline().expect("monotonic deadline"),
            durable_generation_for_test(1),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shutdown_descendant = descendant.clone();
        let shutdown_task = tokio::spawn(async move {
            wait_for_marker(&shutdown_descendant).await;
            shutdown_tx.send(true).expect("request process-tree fence");
        });

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise_with_writable_authority(
                state,
                shutdown_rx,
                Duration::from_secs(1),
                postgres_attempt,
            ),
        )
        .await
        .expect("bounded target-fence process-tree cleanup");
        shutdown_task
            .await
            .expect("join process-tree fence request");

        assert!(result.is_ok(), "target-fence cleanup failed: {result:?}");
        let descendant_pid = fs::read_to_string(&descendant)
            .expect("read descendant PID")
            .trim_ascii()
            .parse::<u32>()
            .expect("parse descendant PID");
        if let Ok(status) = fs::read_to_string(format!("/proc/{descendant_pid}/status")) {
            assert!(
                status.lines().any(|line| line.starts_with("State:\tZ")),
                "live postmaster descendant survived the target fence"
            );
        }
        let reacquired = prepare_fixture(config).expect("reacquire target-fenced PGDATA");
        drop(reacquired);
    }

    #[tokio::test]
    async fn requested_shutdown_escalates_from_smart_to_fast() {
        let root = TempDir::new().expect("create supervisor fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let ready = root.path().join("signal-handlers-ready");
        let marker = root.path().join("fast-shutdown");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap ':' TERM\ntrap 'printf fast > \"{}\"; exit 0' INT\n: > '{}'\nwhile :; do :; done\n",
                marker.display(),
                ready.display()
            ),
        );
        let hba_file = root.path().join("quarantine.pg_hba.conf");
        write_hba(&hba_file);
        let config = PostgresConfig::new(
            data_dir,
            executable,
            root.path().join("socket"),
            hba_file,
            Duration::from_millis(30),
            Duration::from_millis(500),
            Duration::from_millis(500),
        )
        .expect("valid supervisor config");
        let prepared = prepare_fixture(config).expect("prepare fixture");

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise(agent_state(), wait_for_marker(&ready)),
        )
        .await
        .expect("bounded supervisor shutdown");
        assert!(result.is_ok(), "fast shutdown failed: {result:?}");
        assert_eq!(
            fs::read_to_string(marker).expect("read fast marker"),
            "fast"
        );
    }

    #[tokio::test]
    async fn requested_shutdown_forces_and_reaps_an_unresponsive_child() {
        let root = TempDir::new().expect("create supervisor fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let ready = root.path().join("signal-handlers-ready");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap '' TERM INT QUIT\n: > '{}'\nwhile :; do :; done\n",
                ready.display()
            ),
        );
        let hba_file = root.path().join("quarantine.pg_hba.conf");
        write_hba(&hba_file);
        let config = PostgresConfig::new(
            data_dir,
            executable,
            root.path().join("socket"),
            hba_file,
            Duration::from_millis(20),
            Duration::from_millis(20),
            Duration::from_millis(20),
        )
        .expect("valid supervisor config");
        let prepared = prepare_fixture(config).expect("prepare fixture");
        let state = agent_state();

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise(state.clone(), wait_for_marker(&ready)),
        )
        .await
        .expect("bounded forced shutdown");
        assert!(matches!(result, Err(PostgresError::ForcedKill(_))));
        assert_eq!(
            state.snapshot().postgres_process,
            PostgresProcessState::Failed
        );
    }

    #[tokio::test]
    async fn process_group_dies_before_pgdata_fence_is_released() {
        let root = TempDir::new().expect("create process-group fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let descendant = root.path().join("descendant-pid");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap '' TERM INT QUIT\n(trap '' TERM INT QUIT; while :; do :; done) &\nprintf '%s\\n' \"$!\" > '{}'\nwhile :; do :; done\n",
                descendant.display()
            ),
        );
        let socket = root.path().join("socket");
        let config = test_config(data_dir, executable, socket);
        let prepared = prepare_fixture(config.clone()).expect("prepare fixture");

        let result = timeout(
            Duration::from_secs(2),
            prepared.supervise(agent_state(), wait_for_marker(&descendant)),
        )
        .await
        .expect("bounded process-group shutdown");

        assert!(matches!(result, Err(PostgresError::ForcedKill(_))));
        let descendant_pid = fs::read_to_string(&descendant)
            .expect("read descendant PID")
            .trim_ascii()
            .parse::<u32>()
            .expect("parse descendant PID");
        if let Ok(status) = fs::read_to_string(format!("/proc/{descendant_pid}/status")) {
            assert!(
                status.lines().any(|line| line.starts_with("State:\tZ")),
                "live postmaster descendant survived PGDATA fence release"
            );
        }
        let reacquired = prepare_fixture(config).expect("reacquire safe PGDATA fence");
        drop(reacquired);
    }

    #[tokio::test]
    async fn cancellation_reaps_child_and_group_before_releasing_pgdata_fence() {
        let root = TempDir::new().expect("create cancellation fixture");
        let data_dir = root.path().join("data");
        pgdata_fixture_at(&data_dir);
        let leader = root.path().join("leader-pid");
        let descendant = root.path().join("descendant-pid");
        let executable = root.path().join("postgres");
        write_executable(
            &executable,
            &format!(
                "#!/bin/sh\ntrap '' TERM INT QUIT\nprintf '%s\\n' \"$$\" > '{}'\n(trap '' TERM INT QUIT; while :; do :; done) &\nprintf '%s\\n' \"$!\" > '{}'\nwhile :; do :; done\n",
                leader.display(),
                descendant.display()
            ),
        );
        let socket = root.path().join("socket");
        let config = test_config(data_dir.clone(), executable, socket);
        let prepared = prepare_fixture(config.clone()).expect("prepare fixture");
        let task = tokio::spawn(prepared.supervise(agent_state(), std::future::pending()));
        wait_for_marker(&descendant).await;
        assert!(matches!(
            SupervisorLock::acquire(&data_dir),
            Err(PostgresError::SupervisorLockHeld { .. })
        ));

        task.abort();
        let cancellation = timeout(Duration::from_secs(2), task)
            .await
            .expect("bounded cancellation cleanup")
            .expect_err("aborted supervisor must report cancellation");
        assert!(cancellation.is_cancelled());

        let leader_pid = fs::read_to_string(&leader)
            .expect("read leader PID")
            .trim_ascii()
            .parse::<u32>()
            .expect("parse leader PID");
        assert!(matches!(
            fs::read(format!("/proc/{leader_pid}/status")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound
        ));
        let descendant_pid = fs::read_to_string(&descendant)
            .expect("read descendant PID")
            .trim_ascii()
            .parse::<u32>()
            .expect("parse descendant PID");
        if let Ok(status) = fs::read_to_string(format!("/proc/{descendant_pid}/status")) {
            assert!(
                status.lines().any(|line| line.starts_with("State:\tZ")),
                "live postmaster descendant survived supervision cancellation"
            );
        }
        let reacquired = prepare_fixture(config).expect("reacquire safe PGDATA fence");
        drop(reacquired);
    }

    async fn wait_for_marker(path: &Path) {
        timeout(Duration::from_secs(1), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("child installed signal handlers before deadline");
    }

    fn pgdata_fixture() -> TempDir {
        let fixture = TempDir::new().expect("create PGDATA fixture");
        populate_pgdata(fixture.path());
        fixture
    }

    fn pgdata_fixture_at(path: &Path) -> PathBuf {
        fs::create_dir(path).expect("create PGDATA fixture");
        populate_pgdata(path);
        path.to_owned()
    }

    fn populate_pgdata(path: &Path) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("secure PGDATA");
        fs::create_dir(path.join("global")).expect("create global directory");
        fs::create_dir(path.join("pg_wal")).expect("create WAL directory");
        fs::create_dir(path.join("pg_tblspc")).expect("create tablespace directory");
        for directory in ["global", "pg_wal", "pg_tblspc"] {
            fs::set_permissions(path.join(directory), fs::Permissions::from_mode(0o700))
                .expect("secure data subdirectory");
        }
        fs::write(path.join("PG_VERSION"), "18\n").expect("write version");
        fs::set_permissions(path.join("PG_VERSION"), fs::Permissions::from_mode(0o600))
            .expect("protect version file");
        fs::write(
            path.join(BOOTSTRAP_IDENTITY_FILE),
            durable_generation_for_test(1).bootstrap_identity_bytes(),
        )
        .expect("write bootstrap identity");
        fs::set_permissions(
            path.join(BOOTSTRAP_IDENTITY_FILE),
            fs::Permissions::from_mode(0o600),
        )
        .expect("protect bootstrap identity");
        let control = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path.join("global/pg_control"))
            .expect("create control file");
        control
            .set_len(PG_CONTROL_FILE_SIZE)
            .expect("size control file");
    }

    fn test_config(data_dir: PathBuf, executable: PathBuf, socket_dir: PathBuf) -> PostgresConfig {
        let hba_file = socket_dir
            .parent()
            .expect("socket fixture parent")
            .join("quarantine.pg_hba.conf");
        write_hba(&hba_file);
        PostgresConfig::new(
            data_dir,
            executable,
            socket_dir,
            hba_file,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid test config")
    }

    fn standby_test_config(
        root: &TempDir,
        data_dir: PathBuf,
        executable: PathBuf,
        socket_dir: PathBuf,
    ) -> (PostgresConfig, PathBuf) {
        write_standby_signal(&data_dir);
        write_control_data_state(&executable, "shut down in recovery", "");
        let passfile = root.path().join("replication.pass");
        fs::write(
            &passfile,
            b"primary.database.svc:5432:*:pgshard_replication:secret\n",
        )
        .expect("write standby passfile");
        fs::set_permissions(&passfile, fs::Permissions::from_mode(0o400))
            .expect("protect standby passfile");
        let hba_file = root.path().join("standby.pg_hba.conf");
        write_hba(&hba_file);
        let standby = PostgresStandbyConfig::new(
            "primary.database.svc".to_owned(),
            5432,
            "pgshard_member_0001".to_owned(),
            passfile.clone(),
        )
        .expect("valid standby identity");
        let config = PostgresConfig::new_replication_standby(
            standby,
            data_dir,
            executable,
            socket_dir,
            hba_file,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid standby test config");
        (config, passfile)
    }

    fn write_standby_signal(data_dir: &Path) {
        let path = data_dir.join("standby.signal");
        fs::write(&path, []).expect("write standby signal");
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .expect("protect standby signal");
    }

    fn prepare_fixture(config: PostgresConfig) -> Result<PreparedPostgres, PostgresError> {
        // Parallel tests can fork while another fixture's CLOEXEC flock is
        // open. Retry only that bounded exec-handoff window; a persistent lock
        // still fails and production acquisition remains nonblocking.
        let deadline = Instant::now() + Duration::from_secs(1);
        let retry_config = config.clone();
        let mut result = PreparedPostgres::prepare(config);
        loop {
            match result {
                Err(PostgresError::SupervisorLockHeld { .. }) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                    result = PreparedPostgres::prepare(retry_config.clone());
                }
                result => return result,
            }
        }
    }

    fn agent_state() -> AgentState {
        AgentState::with_identity(
            AgentIdentity {
                cluster_id: "cluster-1".to_owned(),
                shard_id: ShardId(0),
                instance_id: "cluster-1-shard-0-0".to_owned(),
            },
            10_000,
        )
        .expect("valid agent state")
    }

    fn writable_generation(term: u64, holder: &str, lease_uid: &str) -> DurableWritableGeneration {
        DurableWritableGeneration::new(
            "cluster-1".to_owned(),
            "11111111-2222-3333-4444-555555555555".to_owned(),
            ShardId(0),
            "database".to_owned(),
            "cluster-1-cell-0000-writable".to_owned(),
            lease_uid.to_owned(),
            holder.to_owned(),
            term,
        )
        .expect("valid writable-generation fixture")
    }

    fn state_with_writable_lease(term: u64) -> AgentState {
        let state = agent_state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "cluster-1-shard-0-0".to_owned(),
                    epoch: term,
                    valid_until_unix_ms: 6_000,
                },
                1_000,
            )
            .expect("install writable fixture Lease");
        state
    }

    fn write_executable(path: &Path, contents: &str) {
        replace_executable_fixture(path, contents);
        if path.file_name() == Some(OsStr::new("postgres")) {
            let controldata = path.with_file_name("pg_controldata");
            if !controldata.exists() {
                write_control_data_state(path, "shut down", "");
            }
        }
    }

    fn write_control_data_state(postgres: &Path, state: &str, stderr: &str) {
        let controldata = postgres.with_file_name("pg_controldata");
        replace_executable_fixture(
            &controldata,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' 'pg_control version number:            1800' 'Database cluster state:               {state}'\nprintf '%s' '{stderr}' >&2\n"
            ),
        );
    }

    fn replace_executable_fixture(path: &Path, contents: &str) {
        let _exec_handoff = test_exec_handoff_guard();
        let parent = path.parent().expect("executable fixture has a parent");
        let mut temporary = NamedTempFile::new_in(parent).expect("create executable fixture");
        temporary
            .write_all(contents.as_bytes())
            .expect("write executable fixture");
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o500))
            .expect("make fixture executable");

        // Close the writable descriptor before the inode becomes executable by
        // name. In-place rewrites can otherwise race a parallel fork/exec and
        // make Linux return ETXTBSY from an unrelated test.
        temporary
            .into_temp_path()
            .persist(path)
            .expect("replace executable fixture");
    }

    fn write_hba(path: &Path) {
        fs::write(path, QUARANTINE_HBA_CONTENT).expect("write quarantine HBA fixture");
        fs::set_permissions(path, fs::Permissions::from_mode(0o400))
            .expect("protect quarantine HBA fixture");
    }
}
