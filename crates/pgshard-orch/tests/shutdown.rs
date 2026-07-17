//! Linux process shutdown coverage for stalled coordination I/O.

#![cfg(target_os = "linux")]

use std::process::Stdio;
use std::time::Duration;

use tokio::sync::oneshot;

#[tokio::test]
async fn sigterm_cancels_a_stalled_etcd_endpoint_cycle() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled etcd endpoint");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("stalled endpoint address")
    );
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let stalled_server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept etcd request");
        let _ = accepted_tx.send(());
        std::future::pending::<()>().await;
    });

    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_pgshard-orch"))
        .args([
            "--http-bind",
            "127.0.0.1:0",
            "--cluster-id",
            "shutdown-test",
            "--cluster-uid",
            "shutdown-cluster-uid",
            "--orchestrator-id",
            "shutdown-orchestrator",
            "--etcd-endpoints",
            &endpoint,
            "--etcd-session-ttl-seconds",
            "15",
            "--etcd-request-timeout-ms",
            "5000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start orchestrator process");
    let process_id = child.id().expect("orchestrator process ID");
    tokio::time::timeout(Duration::from_secs(5), accepted_rx)
        .await
        .expect("orchestrator did not contact stalled endpoint")
        .expect("stalled endpoint stopped before request");

    let signal_status = tokio::process::Command::new("kill")
        .args(["-TERM", &process_id.to_string()])
        .status()
        .await
        .expect("send SIGTERM");
    assert!(signal_status.success(), "kill exited {signal_status}");

    let output = tokio::time::timeout(Duration::from_secs(2), child.wait_with_output())
        .await
        .expect("orchestrator did not cancel stalled coordination on SIGTERM")
        .expect("wait for orchestrator process");
    assert!(
        output.status.success(),
        "orchestrator exited {}; stdout={}; stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    stalled_server.abort();
}
