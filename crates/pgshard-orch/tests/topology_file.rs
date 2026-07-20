//! Process-boundary tests for projected topology files.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pgshard_orch::topology::{
    AgentStatusCollectionState, ExpectedTopologyIdentity, MAXIMUM_TOPOLOGY_PAYLOAD_BYTES,
    TopologyError, TopologyV1,
};
use uuid::Uuid;

const PAYLOAD: &str = r#"{"schemaVersion":"pgshard.topology.v1","cluster":"demo","clusterObjectUID":"cluster-uid","namespace":"database","durability":"Asynchronous","membersPerShard":1,"listeners":[{"mode":"rw","service":"demo-rw","targetPort":5432},{"mode":"ro","service":"demo-ro","targetPort":5433},{"mode":"r","service":"demo-r","targetPort":5434}],"shards":[{"id":0,"service":"demo-shard-0000","writableLease":{"namespace":"database","name":"demo-shard-0000-term","uid":"lease-uid"},"members":[{"ordinal":0,"instanceId":"demo-shard-0000-0","dnsName":"demo-shard-0000-0.demo-shard-0000.database.svc","postgresqlPort":5432,"agentHttpPort":8080,"physicalSlot":"pgshard_member_0000"}]}],"backup":{"type":"S3","bucket":"backups","endpoint":"https://objects.example.invalid/storage","region":"region-1","prefix":"demo","credentialsSecret":"backup-auth"},"observability":{"prometheus":true,"serviceMonitorRequested":true,"openTelemetryEndpoint":"https://telemetry.example.invalid"}}
"#;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("pgshard-topology-{}", Uuid::new_v4()));
        fs::create_dir(&path).expect("create isolated test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn expected() -> ExpectedTopologyIdentity<'static> {
    ExpectedTopologyIdentity {
        cluster_id: "demo",
        cluster_uid: "cluster-uid",
        namespace: "database",
    }
}

#[test]
fn loads_a_bounded_configmap_style_symlink_without_collecting_agent_status() {
    let directory = TestDirectory::new();
    let revision = directory.path().join("revision");
    fs::create_dir(&revision).expect("create projected revision");
    fs::write(revision.join("cluster.json"), PAYLOAD).expect("write topology");
    std::os::unix::fs::symlink(
        "revision/cluster.json",
        directory.path().join("cluster.json"),
    )
    .expect("create projected symlink");

    let topology =
        TopologyV1::load(directory.path().join("cluster.json"), expected()).expect("load topology");
    let diagnostics = topology.diagnostics();
    assert_eq!(diagnostics.shard_count, 1);
    assert_eq!(diagnostics.member_count, 1);
    assert_eq!(
        diagnostics.agent_status_collection,
        AgentStatusCollectionState::DisabledPodIdentityRequired
    );
    let target = topology
        .agent_observation_targets()
        .pop()
        .expect("one discovered target");
    assert_eq!(
        target.dns_name(),
        "demo-shard-0000-0.demo-shard-0000.database.svc"
    );
    assert_eq!(target.agent_http_port(), 8080);
}

#[test]
fn rejects_an_oversized_file_before_json_decoding() {
    let directory = TestDirectory::new();
    let path = directory.path().join("cluster.json");
    let file = fs::File::create(&path).expect("create topology");
    file.set_len((MAXIMUM_TOPOLOGY_PAYLOAD_BYTES + 1) as u64)
        .expect("extend topology");

    assert!(matches!(
        TopologyV1::load(path, expected()),
        Err(TopologyError::PayloadTooLarge { .. })
    ));
}

#[test]
fn rejects_a_fifo_without_waiting_for_a_writer() {
    let directory = TestDirectory::new();
    let path = directory.path().join("cluster.json");
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        &path,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .expect("create FIFO topology");

    let started = Instant::now();
    assert!(matches!(
        TopologyV1::load(path, expected()),
        Err(TopologyError::NotRegularFile(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn rejects_a_directory_and_a_different_cluster_incarnation() {
    let directory = TestDirectory::new();
    assert!(matches!(
        TopologyV1::load(directory.path(), expected()),
        Err(TopologyError::NotRegularFile(_))
    ));

    let path = directory.path().join("cluster.json");
    fs::write(&path, PAYLOAD).expect("write topology");
    assert!(matches!(
        TopologyV1::load(
            path,
            ExpectedTopologyIdentity {
                cluster_id: "demo",
                cluster_uid: "replacement-uid",
                namespace: "database",
            }
        ),
        Err(TopologyError::Invalid(_))
    ));
}
