//! Linux process-level runtime regression tests.

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pgshard_pgwire::{ProtocolVersion, encode_startup};
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
fn env_file_http_postgresql_rejection_and_sigterm_form_one_process_contract() {
    let temporary = TempDir::new().expect("create process-test directory");
    let dsn_path = temporary.path().join("shardschema.dsn");
    fs::write(
        &dsn_path,
        b"postgresql://postgres@127.0.0.1:1/shardschema?sslmode=disable&target_session_attrs=read-write\n",
    )
    .expect("write process-test DSN");
    let http_address = reserve_address("HTTP");
    let read_write_address = reserve_address("PostgreSQL read-write");

    let child = Command::new(env!("CARGO_BIN_EXE_pgshard-pooler"))
        .env_clear()
        .env("PGSHARD_HTTP_BIND", http_address.to_string())
        .env("PGSHARD_RW_BIND", read_write_address.to_string())
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
        if let Ok(response) = request_http(http_address, "/healthz") {
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

    let rejection = request_postgresql(read_write_address)
        .expect("PostgreSQL handshake boundary rejects a regular startup");
    assert_eq!(rejection.first(), Some(&b'E'));
    assert!(rejection.windows(6).any(|bytes| bytes == b"C57P03"));

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

#[test]
fn explicit_bootstrap_mode_is_healthy_unready_and_credential_free() {
    let http_address = reserve_address("bootstrap HTTP");
    let read_write_address = reserve_address("bootstrap PostgreSQL read-write");

    let child = Command::new(env!("CARGO_BIN_EXE_pgshard-pooler"))
        .env_clear()
        .env("PGSHARD_HTTP_BIND", http_address.to_string())
        .env("PGSHARD_RW_BIND", read_write_address.to_string())
        .env("PGSHARD_CATALOG_MODE", "bootstrap-unavailable")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn bootstrap pooler process");
    let mut child = ChildGuard(child);

    let started = Instant::now();
    let health = loop {
        if let Ok(response) = request_http(http_address, "/healthz") {
            break response;
        }
        if let Some(status) = child.0.try_wait().expect("inspect bootstrap process") {
            panic!("bootstrap pooler exited before HTTP became ready: {status}");
        }
        assert!(
            started.elapsed() < PROCESS_TIMEOUT,
            "bootstrap HTTP startup timed out"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(health.starts_with("HTTP/1.1 200 OK\r\n"));

    let readiness = request_http(http_address, "/readyz").expect("request readiness");
    assert!(readiness.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
    assert!(readiness.contains(r#"{"ready":false,"reason":"catalog_not_configured"}"#));

    let status = request_http(http_address, "/status").expect("request status");
    assert!(status.contains(r#""phase":"not_configured""#));
    assert!(status.contains(r#""connect_attempts":"0""#));

    let rejection = request_postgresql(read_write_address)
        .expect("bootstrap PostgreSQL boundary rejects startup");
    assert_eq!(rejection.first(), Some(&b'E'));
    assert!(rejection.windows(6).any(|bytes| bytes == b"C57P03"));

    kill_process(Pid::from_child(&child.0), Signal::TERM).expect("send bootstrap SIGTERM");
    let signalled = Instant::now();
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("wait for bootstrap process") {
            break status;
        }
        assert!(
            signalled.elapsed() < PROCESS_TIMEOUT,
            "bootstrap SIGTERM shutdown timed out"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "bootstrap pooler SIGTERM exit was {status}"
    );
}

fn reserve_address(description: &str) -> SocketAddr {
    let reservation = TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("reserve {description} address: {error}"));
    let address = reservation
        .local_addr()
        .unwrap_or_else(|error| panic!("read reserved {description} address: {error}"));
    drop(reservation);
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

fn request_postgresql(address: SocketAddr) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut request = [0_u8; 128];
    let length = encode_startup(
        ProtocolVersion::new(3, 2),
        &[(b"user".as_slice(), b"postgres".as_slice())],
        &mut request,
    )
    .expect("bounded process-test startup");
    stream.write_all(&request[..length])?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    Ok(response)
}
