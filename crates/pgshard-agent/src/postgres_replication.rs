//! Continuous non-serving physical-replication evidence collection.

use std::path::PathBuf;
use std::time::Duration;

use pgshard_types::writable_generation::DurableWritableGeneration;
use thiserror::Error;
#[cfg(test)]
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

use crate::domain::{
    AgentState, ReplicationEvidence, SourceReplicationEvidence, StandbyReplicationEvidence,
};
use crate::postgres_generation::{
    self, GenerationDurability, PostgresGenerationError, ReplicationEvidenceSession,
};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const OBSERVATION_INTERVAL: Duration = Duration::from_millis(250);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
struct MonitorPolicy {
    initial_reconnect_delay: Duration,
    max_reconnect_delay: Duration,
    observation_interval: Duration,
    operation_timeout: Duration,
}

const PRODUCTION_POLICY: MonitorPolicy = MonitorPolicy {
    initial_reconnect_delay: INITIAL_RECONNECT_DELAY,
    max_reconnect_delay: MAX_RECONNECT_DELAY,
    observation_interval: OBSERVATION_INTERVAL,
    operation_timeout: OPERATION_TIMEOUT,
};

trait EvidenceObserver {
    type Evidence;

    async fn observe(&mut self) -> Result<Self::Evidence, PostgresGenerationError>;
}

trait EvidenceConnector {
    type Observer: EvidenceObserver;

    async fn connect(&mut self) -> Result<Self::Observer, PostgresGenerationError>;
}

struct SourceConnector {
    socket_dir: PathBuf,
    generation: DurableWritableGeneration,
    durability: GenerationDurability,
}

struct SourceObserver {
    session: ReplicationEvidenceSession,
    generation: DurableWritableGeneration,
    durability: GenerationDurability,
}

impl EvidenceConnector for SourceConnector {
    type Observer = SourceObserver;

    async fn connect(&mut self) -> Result<Self::Observer, PostgresGenerationError> {
        Ok(SourceObserver {
            session: postgres_generation::connect_replication_evidence(&self.socket_dir).await?,
            generation: self.generation.clone(),
            durability: self.durability.clone(),
        })
    }
}

impl EvidenceObserver for SourceObserver {
    type Evidence = SourceReplicationEvidence;

    async fn observe(&mut self) -> Result<Self::Evidence, PostgresGenerationError> {
        self.session
            .observe_source(&self.generation, &self.durability)
            .await
    }
}

struct StandbyConnector {
    socket_dir: PathBuf,
    member_slot_name: String,
}

struct StandbyObserver {
    session: ReplicationEvidenceSession,
    member_slot_name: String,
}

impl EvidenceConnector for StandbyConnector {
    type Observer = StandbyObserver;

    async fn connect(&mut self) -> Result<Self::Observer, PostgresGenerationError> {
        Ok(StandbyObserver {
            session: postgres_generation::connect_replication_evidence(&self.socket_dir).await?,
            member_slot_name: self.member_slot_name.clone(),
        })
    }
}

impl EvidenceObserver for StandbyObserver {
    type Evidence = StandbyReplicationEvidence;

    async fn observe(&mut self) -> Result<Self::Evidence, PostgresGenerationError> {
        self.session.observe_standby(&self.member_slot_name).await
    }
}

/// Continuously samples an exact source generation barrier and candidate
/// walsender progress over one retained private peer-authenticated session.
///
/// Before the first coherent sample, connection or observation failures drop
/// the unusable session and reconnect with bounded exponential backoff. After
/// confirmation, the same session is reused and any failure is terminal.
pub(crate) async fn monitor_source_replication_evidence(
    state: AgentState,
    socket_dir: PathBuf,
    generation: DurableWritableGeneration,
    durability: GenerationDurability,
) -> Result<(), ReplicationEvidenceError> {
    #[cfg(test)]
    if let Some(observations) = take_test_replication_evidence_observations(&socket_dir) {
        let evidence = SourceReplicationEvidence {
            observed_at_unix_ms: 1,
            system_identifier: 1,
            timeline: 1,
            in_recovery: false,
            generation_identity: String::from_utf8(generation.canonical_bytes())
                .expect("canonical generation is UTF-8"),
            generation_barrier_lsn: pgshard_types::PgLsn(1),
            durability: durability.evidence(),
            candidates: Vec::new(),
        };
        return monitor_test_replication_evidence(
            state,
            observations,
            ReplicationEvidence::Source(evidence),
        )
        .await;
    }
    monitor_replication_evidence(
        state,
        SourceConnector {
            socket_dir,
            generation,
            durability,
        },
        ReplicationEvidence::Source,
        "source",
        PRODUCTION_POLICY,
    )
    .await
}

/// Continuously samples exact local recovery, generation, member-slot, and WAL
/// progress evidence over one retained private peer-authenticated session.
pub(crate) async fn monitor_standby_replication_evidence(
    state: AgentState,
    socket_dir: PathBuf,
    member_slot_name: String,
) -> Result<(), ReplicationEvidenceError> {
    #[cfg(test)]
    if let Some(observations) = take_test_replication_evidence_observations(&socket_dir) {
        let generation = DurableWritableGeneration::new(
            "test-cluster".to_owned(),
            "test-cluster-uid".to_owned(),
            pgshard_types::ShardId(0),
            "test-database".to_owned(),
            "test-lease".to_owned(),
            "test-lease-uid".to_owned(),
            "test-holder".to_owned(),
            1,
        )
        .expect("valid test generation");
        let evidence = StandbyReplicationEvidence {
            observed_at_unix_ms: 1,
            system_identifier: 1,
            timeline: 1,
            in_recovery: true,
            generation_identity: String::from_utf8(generation.canonical_bytes())
                .expect("canonical generation is UTF-8"),
            member_slot_name,
            receive_lsn: pgshard_types::PgLsn(1),
            replay_lsn: pgshard_types::PgLsn(1),
        };
        return monitor_test_replication_evidence(
            state,
            observations,
            ReplicationEvidence::Standby(evidence),
        )
        .await;
    }
    monitor_replication_evidence(
        state,
        StandbyConnector {
            socket_dir,
            member_slot_name,
        },
        ReplicationEvidence::Standby,
        "standby",
        PRODUCTION_POLICY,
    )
    .await
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestReplicationEvidenceObservation {
    Pending,
    Confirmed,
    Failed,
}

#[cfg(test)]
static TEST_REPLICATION_EVIDENCE_OBSERVATIONS: std::sync::Mutex<
    std::collections::BTreeMap<PathBuf, watch::Receiver<TestReplicationEvidenceObservation>>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
pub(crate) fn set_test_replication_evidence_observations(
    socket_dir: PathBuf,
    observations: watch::Receiver<TestReplicationEvidenceObservation>,
) {
    let mut slot = TEST_REPLICATION_EVIDENCE_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        slot.insert(socket_dir, observations).is_none(),
        "test replication-evidence monitor already installed"
    );
}

#[cfg(test)]
pub(crate) fn remove_test_replication_evidence_observations(socket_dir: &std::path::Path) -> bool {
    TEST_REPLICATION_EVIDENCE_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(socket_dir)
        .is_some()
}

#[cfg(test)]
fn take_test_replication_evidence_observations(
    socket_dir: &std::path::Path,
) -> Option<watch::Receiver<TestReplicationEvidenceObservation>> {
    let mut slot = TEST_REPLICATION_EVIDENCE_OBSERVATIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    slot.remove(socket_dir)
}

#[cfg(test)]
async fn monitor_test_replication_evidence(
    state: AgentState,
    mut observations: watch::Receiver<TestReplicationEvidenceObservation>,
    evidence: ReplicationEvidence,
) -> Result<(), ReplicationEvidenceError> {
    state.clear_replication_evidence();
    loop {
        match *observations.borrow_and_update() {
            TestReplicationEvidenceObservation::Pending => {}
            TestReplicationEvidenceObservation::Confirmed => {
                state.set_replication_evidence(evidence.clone());
            }
            TestReplicationEvidenceObservation::Failed => {
                state.clear_replication_evidence();
                return Err(ReplicationEvidenceError::Observation {
                    source: PostgresGenerationError::InvalidReplicationEvidence,
                });
            }
        }
        if observations.changed().await.is_err() {
            state.clear_replication_evidence();
            return Err(ReplicationEvidenceError::Observation {
                source: PostgresGenerationError::InvalidReplicationEvidence,
            });
        }
    }
}

async fn monitor_replication_evidence<C, W>(
    state: AgentState,
    mut connector: C,
    wrap: W,
    role: &'static str,
    policy: MonitorPolicy,
) -> Result<(), ReplicationEvidenceError>
where
    C: EvidenceConnector,
    W: Fn(<C::Observer as EvidenceObserver>::Evidence) -> ReplicationEvidence,
{
    state.clear_replication_evidence();
    let mut reconnect_delay = policy.initial_reconnect_delay;
    loop {
        let connection = timeout(policy.operation_timeout, connector.connect()).await;
        let mut observer = match connection {
            Ok(Ok(observer)) => observer,
            Ok(Err(source)) => {
                state.clear_replication_evidence();
                tracing::debug!(reason = %source, role, "waiting for replication evidence session");
                sleep(reconnect_delay).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay, policy.max_reconnect_delay);
                continue;
            }
            Err(_) => {
                state.clear_replication_evidence();
                sleep(reconnect_delay).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay, policy.max_reconnect_delay);
                continue;
            }
        };

        let mut confirmed = false;
        loop {
            match timeout(policy.operation_timeout, observer.observe()).await {
                Ok(Ok(evidence)) => {
                    state.set_replication_evidence(wrap(evidence));
                    confirmed = true;
                }
                Ok(Err(source)) if confirmed => {
                    state.clear_replication_evidence();
                    return Err(ReplicationEvidenceError::Observation { source });
                }
                Err(_) if confirmed => {
                    state.clear_replication_evidence();
                    return Err(ReplicationEvidenceError::OperationTimeout(
                        policy.operation_timeout,
                    ));
                }
                Ok(Err(source)) => {
                    state.clear_replication_evidence();
                    tracing::debug!(reason = %source, role, "waiting for coherent replication evidence");
                    break;
                }
                Err(_) => {
                    state.clear_replication_evidence();
                    break;
                }
            }
            sleep(policy.observation_interval).await;
        }

        // This path is reachable only before the first confirmation. A
        // confirmed session never reconnects after losing coherent evidence.
        sleep(reconnect_delay).await;
        reconnect_delay = next_reconnect_delay(reconnect_delay, policy.max_reconnect_delay);
    }
}

fn next_reconnect_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

/// Loss of bounded coherent physical-replication evidence.
#[derive(Debug, Error)]
pub(crate) enum ReplicationEvidenceError {
    /// A previously working SQL observation failed or became incoherent.
    #[error("PostgreSQL replication evidence observation failed: {source}")]
    Observation {
        /// Fail-closed observation error without row data or connection secrets.
        #[source]
        source: PostgresGenerationError,
    },
    /// A previously working SQL observation exceeded its fixed deadline.
    #[error("PostgreSQL replication evidence observation exceeded {0:?}")]
    OperationTimeout(Duration),
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pgshard_types::{PgLsn, ShardId};

    use super::*;
    use crate::domain::{
        GenerationDurabilityEvidence, PostgresProcessState, SourceReplicationCandidateEvidence,
    };

    const TEST_POLICY: MonitorPolicy = MonitorPolicy {
        initial_reconnect_delay: Duration::from_millis(1),
        max_reconnect_delay: Duration::from_millis(4),
        observation_interval: Duration::from_millis(1),
        operation_timeout: Duration::from_millis(10),
    };

    enum ObservationPlan {
        Evidence,
        Failure,
        Pending,
    }

    enum ConnectionPlan {
        Failure,
        Session(VecDeque<ObservationPlan>),
    }

    struct FakeConnector {
        plans: VecDeque<ConnectionPlan>,
        connects: Arc<AtomicUsize>,
        observations: Arc<AtomicUsize>,
    }

    struct FakeObserver {
        plans: VecDeque<ObservationPlan>,
        observations: Arc<AtomicUsize>,
    }

    impl EvidenceConnector for FakeConnector {
        type Observer = FakeObserver;

        async fn connect(&mut self) -> Result<Self::Observer, PostgresGenerationError> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            match self.plans.pop_front().expect("fake connection plan") {
                ConnectionPlan::Failure => Err(PostgresGenerationError::InvalidReplicationEvidence),
                ConnectionPlan::Session(plans) => Ok(FakeObserver {
                    plans,
                    observations: Arc::clone(&self.observations),
                }),
            }
        }
    }

    impl EvidenceObserver for FakeObserver {
        type Evidence = SourceReplicationEvidence;

        async fn observe(&mut self) -> Result<Self::Evidence, PostgresGenerationError> {
            self.observations.fetch_add(1, Ordering::SeqCst);
            match self.plans.pop_front().expect("fake observation plan") {
                ObservationPlan::Evidence => Ok(source_evidence()),
                ObservationPlan::Failure => {
                    Err(PostgresGenerationError::InvalidReplicationEvidence)
                }
                ObservationPlan::Pending => pending().await,
            }
        }
    }

    fn source_evidence() -> SourceReplicationEvidence {
        SourceReplicationEvidence {
            observed_at_unix_ms: 10_000,
            system_identifier: 42,
            timeline: 3,
            in_recovery: false,
            generation_identity: String::from_utf8(
                DurableWritableGeneration::new(
                    "cluster-1".to_owned(),
                    "cluster-uid".to_owned(),
                    ShardId(0),
                    "database".to_owned(),
                    "writable".to_owned(),
                    "lease-uid".to_owned(),
                    "holder".to_owned(),
                    1,
                )
                .expect("valid generation")
                .canonical_bytes(),
            )
            .expect("canonical generation is UTF-8"),
            generation_barrier_lsn: PgLsn(100),
            durability: GenerationDurabilityEvidence::RemoteApplyAnyOne {
                candidates: vec![
                    "pgshard_member_0001".to_owned(),
                    "pgshard_member_0002".to_owned(),
                ],
            },
            candidates: Vec::<SourceReplicationCandidateEvidence>::new(),
        }
    }

    fn source_state() -> AgentState {
        let state = AgentState::default();
        state.set_postgres_process(PostgresProcessState::RunningReplicationBootstrap);
        state
    }

    fn fake_connector(
        plans: impl IntoIterator<Item = ConnectionPlan>,
    ) -> (FakeConnector, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let connects = Arc::new(AtomicUsize::new(0));
        let observations = Arc::new(AtomicUsize::new(0));
        (
            FakeConnector {
                plans: plans.into_iter().collect(),
                connects: Arc::clone(&connects),
                observations: Arc::clone(&observations),
            },
            connects,
            observations,
        )
    }

    #[tokio::test]
    async fn monitor_reuses_one_session_and_clears_on_subsequent_failure() {
        let state = source_state();
        let (connector, connects, observations) = fake_connector([ConnectionPlan::Session(
            [
                ObservationPlan::Evidence,
                ObservationPlan::Evidence,
                ObservationPlan::Failure,
            ]
            .into_iter()
            .collect(),
        )]);
        let result = monitor_replication_evidence(
            state.clone(),
            connector,
            ReplicationEvidence::Source,
            "test-source",
            TEST_POLICY,
        )
        .await;
        assert!(matches!(
            result,
            Err(ReplicationEvidenceError::Observation { .. })
        ));
        assert_eq!(connects.load(Ordering::SeqCst), 1);
        assert_eq!(observations.load(Ordering::SeqCst), 3);
        assert!(state.snapshot().replication_evidence.is_none());
    }

    #[tokio::test]
    async fn monitor_reconnects_only_before_first_confirmation() {
        let state = source_state();
        let (connector, connects, observations) = fake_connector([
            ConnectionPlan::Failure,
            ConnectionPlan::Session([ObservationPlan::Failure].into_iter().collect()),
            ConnectionPlan::Session(
                [ObservationPlan::Evidence, ObservationPlan::Failure]
                    .into_iter()
                    .collect(),
            ),
        ]);
        let result = monitor_replication_evidence(
            state.clone(),
            connector,
            ReplicationEvidence::Source,
            "test-source",
            TEST_POLICY,
        )
        .await;
        assert!(matches!(
            result,
            Err(ReplicationEvidenceError::Observation { .. })
        ));
        assert_eq!(connects.load(Ordering::SeqCst), 3);
        assert_eq!(observations.load(Ordering::SeqCst), 3);
        assert!(state.snapshot().replication_evidence.is_none());
    }

    #[tokio::test]
    async fn monitor_clears_and_terminates_on_timeout_after_confirmation() {
        let state = source_state();
        let (connector, connects, observations) = fake_connector([ConnectionPlan::Session(
            [ObservationPlan::Evidence, ObservationPlan::Pending]
                .into_iter()
                .collect(),
        )]);
        let result = monitor_replication_evidence(
            state.clone(),
            connector,
            ReplicationEvidence::Source,
            "test-source",
            TEST_POLICY,
        )
        .await;
        assert!(matches!(
            result,
            Err(ReplicationEvidenceError::OperationTimeout(duration))
                if duration == TEST_POLICY.operation_timeout
        ));
        assert_eq!(connects.load(Ordering::SeqCst), 1);
        assert_eq!(observations.load(Ordering::SeqCst), 2);
        assert!(state.snapshot().replication_evidence.is_none());
    }

    #[test]
    fn reconnect_delay_is_exponential_and_bounded() {
        assert_eq!(
            next_reconnect_delay(Duration::from_millis(100), Duration::from_secs(2)),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_reconnect_delay(Duration::from_millis(1600), Duration::from_secs(2)),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_reconnect_delay(Duration::from_secs(2), Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }
}
