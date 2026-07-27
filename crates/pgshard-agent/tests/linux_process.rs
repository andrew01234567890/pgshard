//! Linux process-level postmaster supervision regression tests.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process, kill_process_group};
use tempfile::TempDir;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);
const HTTP_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const DIAGNOSTIC_LIMIT: usize = 8 * 1024;

struct ChildGuard {
    child: Option<Child>,
    process_group: Option<Pid>,
    stderr_path: PathBuf,
}

impl ChildGuard {
    fn new(child: Child, stderr_path: PathBuf) -> Self {
        let process_group = Pid::from_child(&child);
        Self {
            child: Some(child),
            process_group: Some(process_group),
            stderr_path,
        }
    }

    fn child(&self) -> &Child {
        self.child.as_ref().expect("child remains present")
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child remains present")
    }

    fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let child = self.child.as_mut().expect("child remains present");
        let child_id = child.id();
        match poll_child(child, PROCESS_TIMEOUT)? {
            Some(status) => Ok(status),
            None => Err(std::io::Error::new(
                ErrorKind::TimedOut,
                format!("child {child_id} did not exit within {PROCESS_TIMEOUT:?}"),
            )),
        }
    }

    fn disarm_after_descendants_are_gone(&mut self) {
        self.process_group = None;
        self.child.take();
    }

    fn diagnostics(&self) -> String {
        match fs::read(&self.stderr_path) {
            Ok(stderr) => {
                let start = stderr.len().saturating_sub(DIAGNOSTIC_LIMIT);
                String::from_utf8_lossy(&stderr[start..]).into_owned()
            }
            Err(error) => format!("<cannot read agent stderr: {error}>"),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(process_group) = self.process_group.take() {
            let _ = kill_process_group(process_group, Signal::KILL);
        }
        if let Some(mut child) = self.child.take() {
            let _ = poll_child(&mut child, CLEANUP_TIMEOUT);
        }
    }
}

fn poll_child(
    child: &mut Child,
    limit: Duration,
) -> std::io::Result<Option<std::process::ExitStatus>> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if started.elapsed() >= limit {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn quarantine_process_status_and_sigterm_form_one_supervised_contract() {
    let fixture = AgentFixture::new("while :; do :; done\n");
    let signal_handlers_ready = fixture.root.path().join("signal-handlers-ready");
    fixture.install_postmaster(&format!(
        "trap 'exit 0' TERM\n: > '{}'\nwhile :; do :; done\n",
        signal_handlers_ready.display()
    ));
    let address = reserve_address();
    let mut child = fixture.spawn(address);

    wait_for_supervised_postmaster(&mut child, address, &fixture);
    wait_for_marker(&mut child, &signal_handlers_ready);

    let readiness = request_http(address, "/readyz").expect("request readiness");
    assert!(readiness.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
    assert!(readiness.contains(r#""reason":"postgres_quarantined""#));
    let metrics = request_http(address, "/metrics").expect("request metrics");
    assert!(metrics.contains("pgshard_agent_postgres_process_up 1\n"));

    let postgres_pid = wait_for_only_child(child.child().id());
    let held_request = open_partial_http_request(address);
    kill_process(Pid::from_child(child.child()), Signal::TERM).expect("signal agent");
    assert_http_connection_closes(held_request);
    let status = child.wait().expect("wait for agent");
    assert!(
        status.success(),
        "agent SIGTERM exit was {status}; stderr: {}",
        child.diagnostics()
    );
    assert!(
        !Path::new(&format!("/proc/{postgres_pid}")).exists(),
        "supervised postmaster process {postgres_pid} survived the agent"
    );
    child.disarm_after_descendants_are_gone();
}

#[test]
fn postmaster_crash_aborts_a_held_http_request_within_the_process_bound() {
    let fixture = AgentFixture::new("while :; do :; done\n");
    let address = reserve_address();
    let mut child = fixture.spawn(address);
    wait_for_supervised_postmaster(&mut child, address, &fixture);

    let postgres_pid = wait_for_only_child(child.child().id());
    let held_request = open_partial_http_request(address);
    let postgres = Pid::from_raw(i32::try_from(postgres_pid).expect("child PID fits i32"))
        .expect("positive postmaster PID");
    kill_process(postgres, Signal::KILL).expect("crash postmaster");
    assert_http_connection_closes(held_request);

    let status = child.wait().expect("wait for terminal agent failure");
    assert!(
        !status.success(),
        "postmaster crash unexpectedly succeeded; stderr: {}",
        child.diagnostics()
    );
    assert!(
        !Path::new(&format!("/proc/{postgres_pid}")).exists(),
        "crashed postmaster process {postgres_pid} remained visible"
    );
    child.disarm_after_descendants_are_gone();
}

#[test]
fn setsid_descendant_is_reaped_before_pgdata_can_be_reacquired() {
    let fixture = AgentFixture::new("while :; do sleep 1; done\n");
    let postmaster_marker = fixture.root.path().join("postmaster.pid");
    let descendant_marker = fixture.root.path().join("setsid-descendant.pid");
    fixture.install_postmaster(&format!(
        "printf \"%s\\n\" \"$$\" > '{}'\n/usr/bin/setsid --fork /bin/sh -c 'trap \"\" TERM INT QUIT HUP; printf \"%s\\n\" \"$$\" > \"$1\"; kill -STOP \"$$\"; while :; do sleep 1; done' descendant '{}' &\nwhile :; do sleep 1; done\n",
        postmaster_marker.display(),
        descendant_marker.display()
    ));
    let address = reserve_address();
    let mut first_agent = fixture.spawn(address);
    wait_for_supervised_postmaster(&mut first_agent, address, &fixture);

    let postmaster_pid = wait_for_pid_marker(&mut first_agent, &postmaster_marker);
    let descendant_pid = wait_for_pid_marker(&mut first_agent, &descendant_marker);
    assert_eq!(
        namespace_status_id(descendant_pid, "NSpid:"),
        descendant_pid,
        "fixture PID must be read in the agent namespace"
    );
    assert_eq!(
        namespace_status_id(descendant_pid, "NSpgid:"),
        descendant_pid,
        "fixture must escape the postmaster process group with setsid"
    );
    assert_ne!(descendant_pid, postmaster_pid);

    let postmaster = Pid::from_raw(i32::try_from(postmaster_pid).expect("postmaster PID fits i32"))
        .expect("positive postmaster PID");
    kill_process(postmaster, Signal::KILL).expect("crash postmaster");
    let status = first_agent.wait().expect("wait for terminal agent failure");
    assert!(
        !status.success(),
        "postmaster crash unexpectedly succeeded; stderr: {}",
        first_agent.diagnostics()
    );
    assert!(
        !Path::new(&format!("/proc/{descendant_pid}")).exists(),
        "setsid descendant {descendant_pid} survived the PGDATA fence"
    );
    first_agent.disarm_after_descendants_are_gone();

    let replacement_ready = fixture
        .root
        .path()
        .join("replacement-signal-handlers-ready");
    fixture.install_postmaster(&format!(
        "trap 'exit 0' TERM\n: > '{}'\nwhile :; do :; done\n",
        replacement_ready.display()
    ));
    let replacement_address = reserve_address();
    let mut replacement = fixture.spawn(replacement_address);
    wait_for_supervised_postmaster(&mut replacement, replacement_address, &fixture);
    wait_for_marker(&mut replacement, &replacement_ready);
    kill_process(Pid::from_child(replacement.child()), Signal::TERM)
        .expect("stop replacement agent");
    let replacement_status = replacement.wait().expect("wait for replacement agent");
    assert!(
        replacement_status.success(),
        "PGDATA replacement agent failed after complete descendant cleanup: {replacement_status}; stderr: {}",
        replacement.diagnostics()
    );
    replacement.disarm_after_descendants_are_gone();
}

#[test]
fn occupied_http_listener_prevents_postmaster_spawn() {
    let fixture = AgentFixture::new("while :; do :; done\n");
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve occupied listener");
    let address = listener.local_addr().expect("read occupied address");
    let mut child = fixture.spawn(address);
    let status = child.wait().expect("wait for bind failure");
    assert!(
        !status.success(),
        "occupied bind unexpectedly succeeded; stderr: {}",
        child.diagnostics()
    );
    assert!(
        !fixture.started_marker().exists(),
        "postmaster started even though the control listener could not bind"
    );
    child.disarm_after_descendants_are_gone();
    drop(listener);
}

#[test]
fn a_status_served_by_another_process_does_not_report_a_started_postmaster() {
    let occupant = QuarantineStatusServer::bind();
    let fixture = AgentFixture::new("while :; do sleep 1; done\n");
    let mut child = fixture.spawn(occupant.address());

    let failure = supervised_postmaster_start(&mut child, occupant.address(), &fixture)
        .expect_err("another process answering /status must not satisfy the readiness gate");

    assert!(
        failure.contains("agent exited before its supervised postmaster started"),
        "readiness failed for the wrong reason: {failure}"
    );
    assert!(
        failure.contains("Address already in use"),
        "readiness failure did not name the losing agent's own error: {failure}"
    );
    child.disarm_after_descendants_are_gone();
}

struct AgentFixture {
    root: TempDir,
    data_dir: PathBuf,
    executable: PathBuf,
    started_marker: PathBuf,
    socket_dir: PathBuf,
    hba_file: PathBuf,
    synchronous_conf_file: PathBuf,
}

impl AgentFixture {
    fn new(body: &str) -> Self {
        let root = TempDir::new().expect("create agent fixture");
        let data_dir = root.path().join("data");
        create_pgdata(&data_dir);
        let executable = root.path().join("postgres");
        let started_marker = root.path().join("postmaster-started");
        let controldata = root.path().join("pg_controldata");
        fs::write(
            &controldata,
            "#!/bin/sh\nprintf '%s\\n' 'pg_control version number:            1800' 'Database cluster state:               shut down'\n",
        )
        .expect("write control-data fixture");
        fs::set_permissions(&controldata, fs::Permissions::from_mode(0o500))
            .expect("make control-data fixture executable");
        let socket_dir = root.path().join("socket");
        let hba_file = root.path().join("quarantine.pg_hba.conf");
        let synchronous_conf_file = root.path().join("conf").join("synchronous.conf");
        fs::write(
            &hba_file,
            "local postgres postgres peer\nlocal all all reject\nlocal replication all reject\n",
        )
        .expect("write quarantine HBA fixture");
        fs::set_permissions(&hba_file, fs::Permissions::from_mode(0o400))
            .expect("protect quarantine HBA fixture");
        let fixture = Self {
            root,
            data_dir,
            executable,
            started_marker,
            socket_dir,
            hba_file,
            synchronous_conf_file,
        };
        fixture.install_postmaster(body);
        fixture
    }

    fn started_marker(&self) -> &Path {
        &self.started_marker
    }

    /// Installs a postmaster fixture that announces itself before running `body`.
    ///
    /// The agent reports a running postmaster as soon as it has spawned one, which
    /// carries no claim that the spawned shell has executed a single line. A test
    /// that needs the fixture's own behaviour has to wait for the fixture, so every
    /// script says so itself rather than leaving the test to infer it.
    fn install_postmaster(&self, body: &str) {
        if self.executable.exists() {
            fs::set_permissions(&self.executable, fs::Permissions::from_mode(0o700))
                .expect("make postmaster fixture replaceable");
        }
        let script = format!("#!/bin/sh\n: > '{}'\n{body}", self.started_marker.display());
        fs::write(&self.executable, script).expect("write postmaster fixture");
        fs::set_permissions(&self.executable, fs::Permissions::from_mode(0o500))
            .expect("protect postmaster fixture");
    }

    fn spawn(&self, address: SocketAddr) -> ChildGuard {
        match fs::remove_file(&self.started_marker) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => panic!("clear the postmaster start marker: {error}"),
        }
        let stderr_path = self
            .root
            .path()
            .join(format!("agent-{}-stderr.log", address.port()));
        let stderr = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&stderr_path)
            .expect("create agent stderr capture");
        let child = Command::new(env!("CARGO_BIN_EXE_pgshard-agent"))
            .env_clear()
            .env("PGSHARD_HTTP_BIND", address.to_string())
            .env("PGSHARD_CLUSTER_ID", "cluster-1")
            .env("PGSHARD_SHARD_ID", "0")
            .env("PGSHARD_INSTANCE_ID", "cluster-1-shard-0-0")
            .env("PGSHARD_POSTGRES_MODE", "quarantine")
            .env("PGDATA", &self.data_dir)
            .env("PGSHARD_POSTGRES_BIN", &self.executable)
            .env("PGSHARD_POSTGRES_SOCKET_DIR", &self.socket_dir)
            .env("PGSHARD_POSTGRES_HBA_FILE", &self.hba_file)
            .env(
                "PGSHARD_POSTGRES_SYNCHRONOUS_CONF_FILE",
                &self.synchronous_conf_file,
            )
            .env("PGSHARD_POSTGRES_SMART_SHUTDOWN_MS", "500")
            .env("PGSHARD_POSTGRES_FAST_SHUTDOWN_MS", "500")
            .env("PGSHARD_POSTGRES_IMMEDIATE_SHUTDOWN_MS", "500")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .process_group(0)
            .spawn()
            .expect("spawn agent");
        ChildGuard::new(child, stderr_path)
    }
}

/// Holds an address and answers every request with a running quarantine status.
///
/// This is what an agent that loses the race to bind a reserved port leaves
/// behind for the agent that lost it: an address whose status describes someone
/// else's postmaster.
struct QuarantineStatusServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl QuarantineStatusServer {
    fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind quarantine status server");
        let address = listener.local_addr().expect("read status server address");
        listener
            .set_nonblocking(true)
            .expect("poll the status server without blocking its shutdown");
        let stop = Arc::new(AtomicBool::new(false));
        let worker = thread::spawn({
            let stop = Arc::clone(&stop);
            move || {
                const BODY: &str = r#"{"postgres_process":"running_quarantined"}"#;
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            // Closing on an unread request would reset the
                            // connection and lose the response the caller has
                            // to be offered before it can refuse it.
                            let mut request = [0_u8; 1024];
                            let _ = stream.set_read_timeout(Some(HTTP_CLOSE_TIMEOUT));
                            let _ = stream.read(&mut request);
                            let response = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{BODY}",
                                BODY.len()
                            );
                            let _ = stream.write_all(response.as_bytes());
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => return,
                    }
                }
            }
        });
        Self {
            address,
            stop,
            worker: Some(worker),
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for QuarantineStatusServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn create_pgdata(path: &Path) {
    fs::create_dir(path).expect("create PGDATA");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("secure PGDATA");
    for directory in ["global", "pg_wal", "pg_tblspc"] {
        fs::create_dir(path.join(directory)).expect("create data subdirectory");
        fs::set_permissions(path.join(directory), fs::Permissions::from_mode(0o700))
            .expect("secure data subdirectory");
    }
    fs::write(path.join("PG_VERSION"), "18\n").expect("write PG_VERSION");
    fs::set_permissions(path.join("PG_VERSION"), fs::Permissions::from_mode(0o600))
        .expect("protect PG_VERSION");
    fs::write(
        path.join("postgresql.conf"),
        b"# fixture standing in for what initdb writes\n",
    )
    .expect("write postgresql.conf");
    fs::set_permissions(
        path.join("postgresql.conf"),
        fs::Permissions::from_mode(0o600),
    )
    .expect("protect postgresql.conf");
    let control = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path.join("global/pg_control"))
        .expect("create pg_control");
    control.set_len(8_192).expect("size pg_control");
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve HTTP address");
    let address = listener.local_addr().expect("read HTTP address");
    drop(listener);
    address
}

fn request_http(address: SocketAddr, path: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes(),
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn open_partial_http_request(address: SocketAddr) -> TcpStream {
    let mut stream =
        TcpStream::connect_timeout(&address, Duration::from_millis(100)).expect("connect HTTP");
    stream
        .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\nX-Held: ")
        .expect("write partial HTTP headers");
    thread::sleep(Duration::from_millis(100));
    stream
}

fn assert_http_connection_closes(mut stream: TcpStream) {
    let started = Instant::now();
    let mut buffer = [0_u8; 256];
    loop {
        let remaining = HTTP_CLOSE_TIMEOUT.saturating_sub(started.elapsed());
        assert!(!remaining.is_zero(), "agent HTTP connection did not close");
        stream
            .set_read_timeout(Some(remaining))
            .expect("bound held HTTP read");
        match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::BrokenPipe
                        | ErrorKind::UnexpectedEof
                ) =>
            {
                return;
            }
            Err(error) => panic!("agent HTTP connection did not close cleanly: {error}"),
        }
    }
}

fn wait_for_supervised_postmaster(
    child: &mut ChildGuard,
    address: SocketAddr,
    fixture: &AgentFixture,
) {
    if let Err(failure) = supervised_postmaster_start(child, address, fixture) {
        panic!("{failure}");
    }
}

/// Waits until this agent's own postmaster fixture is running under quarantine.
///
/// A quarantine status alone proves neither that the answering agent is this
/// one — an agent that loses the race to bind a reserved port leaves its address
/// to whichever agent won it — nor that the postmaster it spawned has run. The
/// start marker lives in this fixture's directory, so only this agent's
/// postmaster can raise it.
fn supervised_postmaster_start(
    child: &mut ChildGuard,
    address: SocketAddr,
    fixture: &AgentFixture,
) -> Result<(), String> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.child_mut().try_wait().expect("inspect agent") {
            return Err(format!(
                "agent exited before its supervised postmaster started: {status}; stderr: {}",
                child.diagnostics()
            ));
        }
        if fixture.started_marker().exists()
            && let Ok(status) = request_http(address, "/status")
            && status.contains(r#""postgres_process":"running_quarantined""#)
        {
            return Ok(());
        }
        if started.elapsed() >= PROCESS_TIMEOUT {
            return Err(format!(
                "supervised postmaster did not start within {PROCESS_TIMEOUT:?}; postmaster \
                 started: {}; agent stderr: {}",
                fixture.started_marker().exists(),
                child.diagnostics()
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_only_child(parent_pid: u32) -> u32 {
    let started = Instant::now();
    loop {
        let children = read_children(parent_pid);
        if let [child] = children.as_slice() {
            return *child;
        }
        assert!(
            started.elapsed() < PROCESS_TIMEOUT,
            "expected exactly one supervised child, found {children:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_pid_marker(child: &mut ChildGuard, path: &Path) -> u32 {
    let started = Instant::now();
    loop {
        if let Ok(value) = fs::read_to_string(path)
            && let Ok(pid) = value.trim().parse()
        {
            return pid;
        }
        assert_marker_is_still_coming(child, path, started, "populated");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_marker(child: &mut ChildGuard, path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert_marker_is_still_coming(child, path, started, "created");
        thread::sleep(Duration::from_millis(10));
    }
}

/// Fails a marker wait as soon as nothing is left that could raise the marker.
///
/// Only the supervised postmaster writes these markers, so once the agent has
/// gone the marker never arrives; reporting that as a timeout spends the whole
/// bound and then names the marker instead of the exit that stranded it.
fn assert_marker_is_still_coming(
    child: &mut ChildGuard,
    path: &Path,
    started: Instant,
    verb: &str,
) {
    if let Some(status) = child.child_mut().try_wait().expect("inspect agent") {
        panic!(
            "agent exited before process marker {} was {verb}: {status}; stderr: {}",
            path.display(),
            child.diagnostics()
        );
    }
    assert!(
        started.elapsed() < PROCESS_TIMEOUT,
        "process marker {} was not {verb}; agent stderr: {}",
        path.display(),
        child.diagnostics()
    );
}

fn namespace_status_id(pid: u32, field: &str) -> u32 {
    let status =
        fs::read_to_string(format!("/proc/{pid}/status")).expect("read fixture process status");
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|ids| ids.split_ascii_whitespace().next_back())
        .and_then(|id| id.parse().ok())
        .expect("read namespace process identifier")
}

fn read_children(parent_pid: u32) -> Vec<u32> {
    fs::read_to_string(format!("/proc/{parent_pid}/task/{parent_pid}/children"))
        .unwrap_or_default()
        .split_ascii_whitespace()
        .map(|value| value.parse().expect("kernel child PID"))
        .collect()
}
