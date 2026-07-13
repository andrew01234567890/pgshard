//! Linux process-level control-runtime regression tests.

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rustix::process::{Pid, Signal, kill_process};
use tempfile::TempDir;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(5);

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

#[test]
fn env_file_http_and_sigterm_form_one_clean_process_contract() {
    let temporary = TempDir::new().expect("create process-test directory");
    let dsn_path = temporary.path().join("shardschema.dsn");
    fs::write(
        &dsn_path,
        b"postgresql://postgres@127.0.0.1:1/shardschema?sslmode=disable&target_session_attrs=read-write\n",
    )
    .expect("write process-test DSN");
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve HTTP address");
    let address = reservation.local_addr().expect("reserved HTTP address");
    drop(reservation);

    let child = Command::new(env!("CARGO_BIN_EXE_pgshard-pooler"))
        .env_clear()
        .env("PGSHARD_HTTP_BIND", address.to_string())
        .env("PGSHARD_SHARDSCHEMA_DSN_FILE", &dsn_path)
        .env("PGSHARD_CATALOG_POLL_INTERVAL_MS", "999")
        .arg("--catalog-poll-interval-ms=1000")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pooler process");
    let mut child = ChildGuard(child);

    let started = Instant::now();
    let response = loop {
        if let Ok(response) = request_health(address) {
            break response;
        }
        if let Some(status) = child.0.try_wait().expect("inspect pooler process") {
            panic!("pooler exited before HTTP became ready: {status}");
        }
        assert!(
            started.elapsed() < PROCESS_TIMEOUT,
            "HTTP startup timed out"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains(r#"{"status":"alive""#));

    kill_process(Pid::from_child(&child.0), Signal::TERM).expect("send SIGTERM");
    let signalled = Instant::now();
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("wait for pooler process") {
            break status;
        }
        assert!(
            signalled.elapsed() < PROCESS_TIMEOUT,
            "SIGTERM shutdown timed out"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "pooler SIGTERM exit was {status}");
}

fn request_health(address: SocketAddr) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}
