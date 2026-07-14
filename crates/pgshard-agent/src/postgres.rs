//! Fail-closed `PostgreSQL` 18 data-directory and process supervision.

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

use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, CWD, FlockOperation, Mode, OFlags, StatxFlags, flock, open, statx};
use rustix::process::{
    Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, WaitIdStatus, geteuid, getpid,
    kill_process_group, pidfd_open, pidfd_send_signal, waitid,
};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep, timeout};

use crate::domain::{AgentState, PostgresProcessState};

const POSTGRES_MAJOR: &str = "18";
const PG_CONTROL_FILE_SIZE: u64 = 8_192;
const MAX_POSTGRES_LOCK_FILE_BYTES: u64 = 8_192;
const MAX_EXTERNAL_PID_FILE_BYTES: u64 = 64;
const MAX_POSTGRES_PATH_BYTES: usize = 1_023;
const SOCKET_LOCK_FILE: &str = ".s.PGSQL.5432.lock";
const EXTERNAL_PID_FILE: &str = "postmaster.external.pid";
// Linux sockaddr_un.sun_path is 108 bytes. PostgreSQL requires the forced
// 14-byte `/.s.PGSQL.5432` suffix plus the directory to fit below that size.
const MAX_SOCKET_DIRECTORY_BYTES: usize = 93;
const MIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(10);
const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(55);
const MAX_SHUTDOWN_BUDGET: Duration = Duration::from_secs(55);
const KILL_REAP_TIMEOUT: Duration = Duration::from_secs(1);
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(30);
const QUARANTINE_HBA_CONTENT: &[u8] = b"local all all reject\nlocal replication all reject\n";

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

/// Configuration for an opt-in postmaster that is isolated from network clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresConfig {
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
        validate_absolute_normal_path("PostgreSQL quarantine HBA file", &hba_file, false)?;
        if socket_dir.starts_with(&data_dir) || data_dir.starts_with(&socket_dir) {
            return Err(PostgresConfigError::OverlappingPaths {
                data_dir,
                socket_dir,
            });
        }
        if hba_file.starts_with(&data_dir) || hba_file.starts_with(&socket_dir) {
            return Err(PostgresConfigError::MutableHbaFile { hba_file });
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
}

/// Offline-validated postmaster configuration ready to spawn.
#[derive(Debug)]
pub struct PreparedPostgres {
    config: PostgresConfig,
    validated: ValidatedPostgresState,
    supervisor_lock: SupervisorLock,
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
struct PostgresProcessFence {
    // Owning the Tokio handle lets Drop synchronously reap through try_wait
    // before the supervisor lock field is released.
    child: Child,
    process_group: Option<Pid>,
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
}

impl PostgresProcessFence {
    fn new(child: Child, supervisor_lock: SupervisorLock) -> Self {
        Self {
            child,
            process_group: None,
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
            fence_child_on_drop(&mut self.child, self.process_group);
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
    /// until descendant cleanup completes. Dropping this future after spawn
    /// synchronously retains the PGDATA fence through the same process-group
    /// kill proof.
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
        state.clear_lease();
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
            Err(error) => {
                state.set_postgres_process(PostgresProcessState::Failed);
                return Err(error);
            }
        };
        if let Err(error) = self.supervisor_lock.validate() {
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(error);
        }
        if current != self.validated {
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(PostgresError::PreparedStateChanged);
        }
        tokio::task::yield_now().await;
        if shutdown_requested(shutdown.as_mut()).await {
            state.set_postgres_process(PostgresProcessState::Validated);
            return Ok(());
        }
        if let Err(error) = self.finalize_pre_spawn(&current) {
            state.set_postgres_process(PostgresProcessState::Failed);
            return Err(error);
        }
        tokio::task::yield_now().await;
        if shutdown_requested(shutdown.as_mut()).await {
            state.set_postgres_process(PostgresProcessState::Validated);
            return Ok(());
        }
        let shutdown_config = self.config.clone();
        let (mut process_group_fence, pidfd, process_group) =
            self.spawn_tracked_postmaster(&state).await?;

        let result = tokio::select! {
            status = wait_pidfd_exit(&pidfd) => {
                state.set_postgres_process(PostgresProcessState::Stopping);
                let error = cleanup_after_error(
                    &mut process_group_fence.child,
                    Some(&pidfd),
                    Some(process_group),
                    match status {
                        Ok(status) => PostgresError::UnexpectedExit(status),
                        Err(error) => error,
                    },
                ).await;
                state.set_postgres_process(PostgresProcessState::Failed);
                Err(error)
            }
            () = &mut shutdown => {
                state.set_postgres_process(PostgresProcessState::Stopping);
                let result = shutdown_child(
                    &mut process_group_fence.child,
                    &pidfd,
                    process_group,
                    &shutdown_config,
                ).await;
                state.set_postgres_process(if result.is_ok() {
                    PostgresProcessState::Validated
                } else {
                    PostgresProcessState::Failed
                });
                result
            }
        };
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

    async fn spawn_tracked_postmaster(
        self,
        state: &AgentState,
    ) -> Result<(PostgresProcessFence, AsyncFd<OwnedFd>, Pid), PostgresError> {
        let spawn_result = {
            #[cfg(test)]
            let _exec_handoff = test_exec_handoff_guard();
            self.command().spawn()
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
        state.set_postgres_process(PostgresProcessState::StartingQuarantined);
        let mut process_group_fence = PostgresProcessFence::new(child, self.supervisor_lock);
        let Some(child_id) = process_group_fence.child.id() else {
            return Err(cleanup_spawn_failure(
                state,
                &mut process_group_fence.child,
                None,
                PostgresError::MissingChildPid,
            )
            .await);
        };
        let Ok(raw_pid) = i32::try_from(child_id) else {
            return Err(cleanup_spawn_failure(
                state,
                &mut process_group_fence.child,
                None,
                PostgresError::InvalidChildPid,
            )
            .await);
        };
        let Some(pid) = Pid::from_raw(raw_pid) else {
            return Err(cleanup_spawn_failure(
                state,
                &mut process_group_fence.child,
                None,
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
        state.set_postgres_process(PostgresProcessState::RunningQuarantined);
        Ok((process_group_fence, pidfd, pid))
    }

    fn command(&self) -> Command {
        let mut data_directory = OsString::from("data_directory=");
        data_directory.push(&self.config.data_dir);
        let mut hba_file = OsString::from("hba_file=");
        hba_file.push(&self.config.hba_file);
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
            .arg("listen_addresses=")
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
            .arg("restart_after_crash=off")
            .arg("-c")
            .arg("primary_conninfo=")
            .arg("-c")
            .arg("primary_slot_name=")
            .arg("-c")
            .arg("restore_command=")
            .arg("-c")
            .arg("archive_cleanup_command=")
            .arg("-c")
            .arg("recovery_end_command=")
            .arg("-c")
            // An enabled archive mode with no callback retains every pending
            // WAL segment without executing deployment-supplied code. Turning
            // archiving off would let checkpoints recycle existing `.ready`
            // segments and could break backup/PITR continuity.
            .arg("archive_mode=on")
            .arg("-c")
            .arg("archive_command=")
            .arg("-c")
            .arg("archive_library=")
            .arg("-c")
            .arg("max_wal_senders=0")
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
            .arg("shared_preload_libraries=")
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

async fn await_validation<F>(
    task: &mut tokio::task::JoinHandle<Result<ValidatedPostgresState, PostgresError>>,
    shutdown: Pin<&mut F>,
) -> Result<Option<ValidatedPostgresState>, PostgresError>
where
    F: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown => Ok(None),
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
    F: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown => true,
        () = std::future::ready(()) => false,
    }
}

async fn cleanup_spawn_failure(
    state: &AgentState,
    child: &mut Child,
    process_group: Option<Pid>,
    error: PostgresError,
) -> PostgresError {
    state.set_postgres_process(PostgresProcessState::Stopping);
    let error = cleanup_after_error(child, None, process_group, error).await;
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
    config: &PostgresConfig,
) -> Result<(), PostgresError> {
    let result = shutdown_child_inner(pidfd, config).await;
    let had_live_descendants = process_group_has_live_members(process_group).unwrap_or(true);
    let cleanup = kill_and_reap(child, Some(pidfd), Some(process_group)).await;
    match (result, cleanup, had_live_descendants) {
        (Ok(()), Ok(()), false) => Ok(()),
        (Ok(()), Ok(()), true) => Err(PostgresError::DescendantsSurvivedShutdown),
        (Err(error), Ok(()), _) => Err(error),
        (Ok(()), Err(cleanup), _) => Err(cleanup),
        (Err(error), Err(cleanup), _) => Err(PostgresError::CleanupFailed {
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
    error: PostgresError,
) -> PostgresError {
    match kill_and_reap(child, pidfd, process_group).await {
        Ok(()) => error,
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
) -> Result<(), PostgresError> {
    let process_group_result = if let Some(process_group) = process_group {
        kill_process_group_until_dead(process_group).await
    } else {
        if let Some(pidfd) = pidfd {
            let _ = pidfd_send_signal(pidfd.get_ref(), Signal::KILL);
        }
        let _ = child.start_kill();
        Ok(())
    };
    let child_result = match timeout(KILL_REAP_TIMEOUT, child.wait()).await {
        Ok(result) => result.map(|_| ()).map_err(PostgresError::Wait),
        Err(_) => {
            // A direct child that remains uninterruptible keeps this future,
            // and therefore the PGDATA flock, alive until the kernel reaps it.
            child.wait().await.map(|_| ()).map_err(PostgresError::Wait)
        }
    };
    match (process_group_result, child_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(PostgresError::CleanupFailed {
            error: Box::new(error),
            cleanup: Box::new(cleanup),
        }),
    }
}

async fn kill_process_group_until_dead(process_group: Pid) -> Result<(), PostgresError> {
    let deadline = Instant::now() + KILL_REAP_TIMEOUT;
    let mut exceeded_bound = false;
    let mut logged_inspection_error = false;
    let mut signal_error = None;
    loop {
        match process_group_has_live_members(process_group) {
            Ok(false) => {
                if let Some(source) = signal_error {
                    return Err(PostgresError::ProcessGroupSignal(source));
                }
                if exceeded_bound {
                    return Err(PostgresError::ProcessGroupCleanupTimeout(KILL_REAP_TIMEOUT));
                }
                return Ok(());
            }
            Ok(true) => {}
            Err(error) => {
                if !logged_inspection_error {
                    tracing::warn!(%error, "cannot yet prove PostgreSQL process group is dead");
                    logged_inspection_error = true;
                }
            }
        }
        if let Err(source) = kill_process_group(process_group, Signal::KILL)
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

fn fence_child_on_drop(child: &mut Child, process_group: Option<Pid>) {
    if let Some(process_group) = process_group {
        fence_process_group_on_drop(process_group);
    }

    let mut logged_wait_error = false;
    let mut logged_group_signal_error = false;
    let mut logged_child_signal_error = false;
    loop {
        let child_may_be_running = match child.try_wait() {
            Ok(Some(_)) => return,
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
    let data = validate_data_dir(&config.data_dir, expected_uid)?;
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
    let hba_file = validate_hba_file(&config.hba_file, expected_uid)?;
    let socket_dir = if create_socket_dir {
        ensure_socket_dir(&config.socket_dir, expected_uid)?
    } else {
        validate_socket_dir(&config.socket_dir, expected_uid)?
    };
    let socket_lock =
        validate_postmaster_lock_at(&config.socket_dir.join(SOCKET_LOCK_FILE), expected_uid)?;
    let external_pid_file =
        validate_external_pid_file_at(&config.socket_dir.join(EXTERNAL_PID_FILE), expected_uid)?;
    Ok(ValidatedPostgresState {
        data,
        executable,
        controldata_executable,
        control_data_state,
        socket_dir,
        socket_lock,
        external_pid_file,
        hba_file,
    })
}

fn validate_data_dir(path: &Path, expected_uid: u32) -> Result<ValidatedDataDir, PostgresError> {
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
    reject_recovery_state(path)?;
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
    })
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

fn reject_recovery_state(data_dir: &Path) -> Result<(), PostgresError> {
    for file_name in [
        "standby.signal",
        "recovery.signal",
        "backup_label",
        "tablespace_map",
    ] {
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
    Ok(())
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
    match state {
        ControlDataState::ShutDownInRecovery | ControlDataState::InArchiveRecovery => {
            Err(PostgresError::RecoveryControlState {
                state: control_data_state_name(state),
            })
        }
        ControlDataState::StartingUp => Err(PostgresError::UnsafeControlState {
            state: control_data_state_name(state),
        }),
        ControlDataState::ShutDown
        | ControlDataState::ShuttingDown
        | ControlDataState::InCrashRecovery
        | ControlDataState::InProduction => Ok(state),
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

fn validate_hba_file(path: &Path, expected_uid: u32) -> Result<FileSnapshot, PostgresError> {
    let metadata = strict_metadata("PostgreSQL quarantine HBA file", path)?;
    require_regular("PostgreSQL quarantine HBA file", path, &metadata)?;
    if metadata.uid() != 0 && metadata.uid() != expected_uid {
        return Err(PostgresError::WrongOwner {
            name: "PostgreSQL quarantine HBA file",
            path: path.to_owned(),
            actual: metadata.uid(),
            expected: expected_uid,
        });
    }
    let mode = metadata.permissions().mode() & 0o7_777;
    if mode & 0o022 != 0 || (metadata.uid() == expected_uid && mode & 0o200 != 0) {
        return Err(PostgresError::UnsafePermissions {
            name: "PostgreSQL quarantine HBA file",
            path: path.to_owned(),
            mode,
            expected: "not writable by the runtime identity, group, or world",
        });
    }
    if metadata.len() != QUARANTINE_HBA_CONTENT.len() as u64 {
        return Err(PostgresError::InvalidQuarantineHba {
            path: path.to_owned(),
        });
    }
    let contents = fs::read(path).map_err(|source| PostgresError::Read {
        name: "PostgreSQL quarantine HBA file",
        path: path.to_owned(),
        source,
    })?;
    if contents != QUARANTINE_HBA_CONTENT {
        return Err(PostgresError::InvalidQuarantineHba {
            path: path.to_owned(),
        });
    }
    Ok(file_snapshot(&strict_metadata(
        "PostgreSQL quarantine HBA file",
        path,
    )?))
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

/// Invalid opt-in postmaster configuration.
#[derive(Debug, Error)]
pub enum PostgresConfigError {
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
        "PostgreSQL quarantine HBA file {hba_file:?} must not be stored inside PGDATA or the socket directory"
    )]
    MutableHbaFile {
        /// Rejected HBA path.
        hba_file: PathBuf,
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
        "{name} at {path:?} is on mount {actual}, but PGDATA is on mount {expected}; quarantine requires one PGDATA volume"
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
    /// The control file is not in a complete primary state that quarantine may recover.
    #[error("PostgreSQL control-file state {state:?} is not safe for quarantine startup")]
    UnsafeControlState {
        /// Rejected `PostgreSQL` control-file state.
        state: &'static str,
    },
    /// Role-aware standby, archive, or base-backup recovery is outside quarantine mode.
    #[error(
        "PostgreSQL recovery state {path:?} is not supported in quarantine mode; a role-aware orchestrator must handle this state before startup"
    )]
    RecoveryStatePresent {
        /// Recovery marker that prevented process creation.
        path: PathBuf,
    },
    /// User tablespaces escape the single-volume quarantine boundary.
    #[error(
        "PostgreSQL tablespace entry {path:?} is not supported in quarantine mode; Milestone 1 requires all database state inside PGDATA"
    )]
    TablespacePresent {
        /// Entry that prevented process creation.
        path: PathBuf,
    },
    /// The HBA policy did not consist solely of the two deny-all local rules.
    #[error(
        "PostgreSQL quarantine HBA file {path:?} must contain only the built-in deny-all policy"
    )]
    InvalidQuarantineHba {
        /// Rejected HBA path.
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

    use crate::domain::{AgentIdentity, FencingLease};
    use pgshard_types::ShardId;

    use super::*;

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
            validate_hba_file(&hba, geteuid().as_raw()),
            Err(PostgresError::InvalidQuarantineHba { .. })
        ));

        fs::set_permissions(&hba, fs::Permissions::from_mode(0o600)).expect("make HBA replaceable");
        fs::write(&hba, QUARANTINE_HBA_CONTENT).expect("write deny-all HBA");
        assert!(matches!(
            validate_hba_file(&hba, geteuid().as_raw()),
            Err(PostgresError::UnsafePermissions {
                name: "PostgreSQL quarantine HBA file",
                ..
            })
        ));
        fs::set_permissions(&hba, fs::Permissions::from_mode(0o400)).expect("protect deny-all HBA");
        assert!(validate_hba_file(&hba, geteuid().as_raw()).is_ok());

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
            validate_hba_file(&hba, geteuid().as_raw()),
            Err(PostgresError::InvalidQuarantineHba { .. })
        ));
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
            "deny-all HBA policy must override PGDATA configuration"
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
            "fsync=on",
            "full_page_writes=on",
            "ignore_invalid_pages=off",
            "data_sync_retry=off",
            "ignore_checksum_failure=off",
            "zero_damaged_pages=off",
        ] {
            assert!(
                arguments.contains(&OsStr::new(required)),
                "missing {required:?}"
            );
        }
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

        kill_and_reap(&mut child, Some(&pidfd), Some(process_group))
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
