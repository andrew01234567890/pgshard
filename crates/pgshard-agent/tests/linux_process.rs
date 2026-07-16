//! Linux process-level postmaster supervision regression tests.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process, kill_process_group};
use tempfile::TempDir;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);
const HTTP_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

struct ChildGuard {
    child: Option<Child>,
    process_group: Option<Pid>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        let process_group = Pid::from_child(&child);
        Self {
            child: Some(child),
            process_group: Some(process_group),
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
    let fixture = AgentFixture::new("#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do :; done\n");
    let address = reserve_address();
    let child = fixture.spawn(address);
    let mut child = ChildGuard::new(child);

    wait_for_quarantine(&mut child, address);

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
    assert!(status.success(), "agent SIGTERM exit was {status}");
    assert!(
        !Path::new(&format!("/proc/{postgres_pid}")).exists(),
        "supervised postmaster process {postgres_pid} survived the agent"
    );
    child.disarm_after_descendants_are_gone();
}

#[test]
fn postmaster_crash_aborts_a_held_http_request_within_the_process_bound() {
    let fixture = AgentFixture::new("#!/bin/sh\nwhile :; do :; done\n");
    let address = reserve_address();
    let child = fixture.spawn(address);
    let mut child = ChildGuard::new(child);
    wait_for_quarantine(&mut child, address);

    let postgres_pid = wait_for_only_child(child.child().id());
    let held_request = open_partial_http_request(address);
    let postgres = Pid::from_raw(i32::try_from(postgres_pid).expect("child PID fits i32"))
        .expect("positive postmaster PID");
    kill_process(postgres, Signal::KILL).expect("crash postmaster");
    assert_http_connection_closes(held_request);

    let status = child.wait().expect("wait for terminal agent failure");
    assert!(!status.success(), "postmaster crash unexpectedly succeeded");
    assert!(
        !Path::new(&format!("/proc/{postgres_pid}")).exists(),
        "crashed postmaster process {postgres_pid} remained visible"
    );
    child.disarm_after_descendants_are_gone();
}

#[test]
fn setsid_descendant_is_reaped_before_pgdata_can_be_reacquired() {
    let fixture = AgentFixture::new("#!/bin/sh\nwhile :; do sleep 1; done\n");
    let postmaster_marker = fixture.root.path().join("postmaster.pid");
    let descendant_marker = fixture.root.path().join("setsid-descendant.pid");
    fixture.replace_executable(&format!(
        "#!/bin/sh\nprintf \"%s\\n\" \"$$\" > '{}'\n/usr/bin/setsid --fork /bin/sh -c 'trap \"\" TERM INT QUIT HUP; printf \"%s\\n\" \"$$\" > \"$1\"; kill -STOP \"$$\"; while :; do sleep 1; done' descendant '{}' &\nwhile :; do sleep 1; done\n",
        postmaster_marker.display(),
        descendant_marker.display()
    ));
    let address = reserve_address();
    let mut first_agent = ChildGuard::new(fixture.spawn(address));
    wait_for_quarantine(&mut first_agent, address);

    let postmaster_pid = wait_for_pid_marker(&postmaster_marker);
    let descendant_pid = wait_for_pid_marker(&descendant_marker);
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
    assert!(!status.success(), "postmaster crash unexpectedly succeeded");
    assert!(
        !Path::new(&format!("/proc/{descendant_pid}")).exists(),
        "setsid descendant {descendant_pid} survived the PGDATA fence"
    );
    first_agent.disarm_after_descendants_are_gone();

    fixture.replace_executable("#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do :; done\n");
    let replacement_address = reserve_address();
    let mut replacement = ChildGuard::new(fixture.spawn(replacement_address));
    wait_for_quarantine(&mut replacement, replacement_address);
    kill_process(Pid::from_child(replacement.child()), Signal::TERM)
        .expect("stop replacement agent");
    let replacement_status = replacement.wait().expect("wait for replacement agent");
    assert!(
        replacement_status.success(),
        "PGDATA replacement agent failed after complete descendant cleanup: {replacement_status}"
    );
    replacement.disarm_after_descendants_are_gone();
}

#[test]
fn occupied_http_listener_prevents_postmaster_spawn() {
    let fixture = AgentFixture::new("#!/bin/sh\nwhile :; do :; done\n");
    let marker = fixture.root.path().join("postmaster-started");
    fixture.replace_executable(&format!(
        "#!/bin/sh\n: > '{}'\nwhile :; do :; done\n",
        marker.display()
    ));
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve occupied listener");
    let address = listener.local_addr().expect("read occupied address");
    let mut child = ChildGuard::new(fixture.spawn(address));
    let status = child.wait().expect("wait for bind failure");
    assert!(!status.success(), "occupied bind unexpectedly succeeded");
    assert!(
        !marker.exists(),
        "postmaster started even though the control listener could not bind"
    );
    child.disarm_after_descendants_are_gone();
    drop(listener);
}

struct AgentFixture {
    root: TempDir,
    data_dir: PathBuf,
    executable: PathBuf,
    socket_dir: PathBuf,
    hba_file: PathBuf,
}

impl AgentFixture {
    fn new(script: &str) -> Self {
        let root = TempDir::new().expect("create agent fixture");
        let data_dir = root.path().join("data");
        create_pgdata(&data_dir);
        let executable = root.path().join("postgres");
        fs::write(&executable, script).expect("write postmaster fixture");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500))
            .expect("make postmaster fixture executable");
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
        fs::write(
            &hba_file,
            "local all all reject\nlocal replication all reject\n",
        )
        .expect("write quarantine HBA fixture");
        fs::set_permissions(&hba_file, fs::Permissions::from_mode(0o400))
            .expect("protect quarantine HBA fixture");
        Self {
            root,
            data_dir,
            executable,
            socket_dir,
            hba_file,
        }
    }

    fn replace_executable(&self, script: &str) {
        fs::set_permissions(&self.executable, fs::Permissions::from_mode(0o700))
            .expect("make postmaster fixture replaceable");
        fs::write(&self.executable, script).expect("replace postmaster fixture");
        fs::set_permissions(&self.executable, fs::Permissions::from_mode(0o500))
            .expect("protect replacement postmaster fixture");
    }

    fn spawn(&self, address: SocketAddr) -> Child {
        Command::new(env!("CARGO_BIN_EXE_pgshard-agent"))
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
            .env("PGSHARD_POSTGRES_SMART_SHUTDOWN_MS", "500")
            .env("PGSHARD_POSTGRES_FAST_SHUTDOWN_MS", "500")
            .env("PGSHARD_POSTGRES_IMMEDIATE_SHUTDOWN_MS", "500")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("spawn agent")
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

fn wait_for_quarantine(child: &mut ChildGuard, address: SocketAddr) {
    let started = Instant::now();
    loop {
        if let Ok(status) = request_http(address, "/status")
            && status.contains(r#""postgres_process":"running_quarantined""#)
        {
            return;
        }
        if let Some(status) = child.child_mut().try_wait().expect("inspect agent") {
            panic!("agent exited before quarantine status was visible: {status}");
        }
        assert!(
            started.elapsed() < PROCESS_TIMEOUT,
            "quarantine startup timed out"
        );
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

fn wait_for_pid_marker(path: &Path) -> u32 {
    let started = Instant::now();
    loop {
        if let Ok(value) = fs::read_to_string(path)
            && let Ok(pid) = value.trim().parse()
        {
            return pid;
        }
        assert!(
            started.elapsed() < PROCESS_TIMEOUT,
            "process marker {} was not populated",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
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
