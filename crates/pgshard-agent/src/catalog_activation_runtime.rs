//! Dormant binding of catalog activation inputs to one writable runtime.
//!
//! This stage is deliberately non-serving and non-mutating. It can publish an
//! opaque private handoff only while the exact durable request, verified static
//! inputs, attempt-private writable authority, postmaster incarnation, and a
//! peer-authenticated local `PostgreSQL` identity all agree. It adds no secrets,
//! SQL writes, HBA changes, readiness, routing, or serving authority.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::catalog_activation_static_inputs::{
    CatalogMaterializationHandoff, ValidatedCatalogStaticInputs, exact_acceptance,
};
use crate::postgres_generation::{
    CatalogRuntimeIdentity, CatalogRuntimeSession, connect_catalog_runtime,
};
use crate::writable::{DurableWritableGeneration, WritableAuthorityObserver};

const RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// Retained factory for attempt-private runtime binders.
///
/// This value contains no authority itself. Each writable attempt receives a
/// fresh observer tied to the same private input and output watches.
pub struct CatalogRuntimeBinding {
    inputs: watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
    output: watch::Sender<Option<Arc<ValidatedCatalogRuntime>>>,
}

/// One attempt-private input/output channel pair.
pub struct CatalogRuntimeBindingAttempt {
    inputs: watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
    output: watch::Sender<Option<Arc<ValidatedCatalogRuntime>>>,
}

/// Opaque retained receiver for the exact writable-runtime handoff.
///
/// There is intentionally no observer API. A later independently reviewed SQL
/// materializer must consume this move-only handoff before the bound session or
/// verified static snapshots can be used.
#[must_use = "dropping the runtime handoff closes its private watch"]
pub struct CatalogRuntimeHandoff {
    #[allow(dead_code, reason = "retained for the later dormant SQL materializer")]
    receiver: watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
}

/// Exact request, inputs, process incarnation, authority generation, and local
/// `PostgreSQL` session. This capability is deliberately private, move-only,
/// non-debuggable, and non-serializable.
struct ValidatedCatalogRuntime {
    _inputs: Arc<ValidatedCatalogStaticInputs>,
    _session: RetainedCatalogRuntimeSession,
    _generation: DurableWritableGeneration,
    _postmaster_pid: u32,
    _boot_id: String,
    _identity: CatalogRuntimeIdentity,
}

/// The retained local session behind a validated runtime capability.
///
/// The test variant exists only to exercise the complete asynchronous
/// publication state machine without requiring a live `PostgreSQL` server.
enum RetainedCatalogRuntimeSession {
    Postgres(CatalogRuntimeSession),
    #[cfg(test)]
    Test(TestCatalogRuntimeSession),
}

impl RetainedCatalogRuntimeSession {
    fn driver_ended(&self) -> watch::Receiver<bool> {
        match self {
            Self::Postgres(session) => session.driver_ended(),
            #[cfg(test)]
            Self::Test(session) => session.driver_ended.clone(),
        }
    }

    #[cfg(test)]
    fn mark_published(&self) {
        if let Self::Test(session) = self {
            session
                .published
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

#[cfg(test)]
struct TestCatalogRuntimeSession {
    driver_ended: watch::Receiver<bool>,
    published: Arc<std::sync::atomic::AtomicBool>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl Drop for TestCatalogRuntimeSession {
    fn drop(&mut self) {
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Converts the static-input handoff into a reusable attempt binder and a
/// private move-only runtime output handoff.
pub fn prepare_catalog_runtime_binding(
    materialization: CatalogMaterializationHandoff,
) -> (CatalogRuntimeBinding, CatalogRuntimeHandoff) {
    let (output, receiver) = watch::channel(None);
    (
        CatalogRuntimeBinding {
            inputs: materialization.into_receiver(),
            output,
        },
        CatalogRuntimeHandoff { receiver },
    )
}

impl CatalogRuntimeBinding {
    /// Creates the one binder scoped to the next writable supervision attempt.
    #[must_use]
    pub fn attempt(&self) -> CatalogRuntimeBindingAttempt {
        CatalogRuntimeBindingAttempt {
            inputs: self.inputs.clone(),
            output: self.output.clone(),
        }
    }
}

struct OutputWithdrawalGuard(watch::Sender<Option<Arc<ValidatedCatalogRuntime>>>);

impl Drop for OutputWithdrawalGuard {
    fn drop(&mut self) {
        self.0.send_replace(None);
    }
}

/// Supervises the fault-isolated binding for one exact running source.
///
/// This future intentionally never completes. Its owning postmaster select
/// drops it on process exit, authority loss, target-fence failure, or shutdown;
/// the drop guard synchronously withdraws the private output first.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn supervise_catalog_runtime_binding(
    attempt: CatalogRuntimeBindingAttempt,
    socket_dir: &Path,
    generation: &DurableWritableGeneration,
    authority: WritableAuthorityObserver,
    required_margin: Duration,
    postmaster_pid: u32,
    boot_id: Option<String>,
) {
    Box::pin(supervise_catalog_runtime_binding_with_connector(
        attempt,
        socket_dir.to_path_buf(),
        generation,
        authority,
        required_margin,
        postmaster_pid,
        boot_id,
        |socket_dir, generation| async move {
            match connect_catalog_runtime(&socket_dir, &generation).await {
                Ok((session, identity)) => {
                    Some((RetainedCatalogRuntimeSession::Postgres(session), identity))
                }
                Err(error) => {
                    tracing::warn!(reason = %error,
                        "catalog-activation runtime identity unavailable; retrying independently");
                    None
                }
            }
        },
    ))
    .await;
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn supervise_catalog_runtime_binding_with_connector<C, F>(
    mut attempt: CatalogRuntimeBindingAttempt,
    socket_dir: PathBuf,
    generation: &DurableWritableGeneration,
    authority: WritableAuthorityObserver,
    required_margin: Duration,
    postmaster_pid: u32,
    boot_id: Option<String>,
    connect: C,
) where
    C: Fn(PathBuf, DurableWritableGeneration) -> F,
    F: Future<Output = Option<(RetainedCatalogRuntimeSession, CatalogRuntimeIdentity)>>,
{
    let _withdraw_on_exit = OutputWithdrawalGuard(attempt.output.clone());
    attempt.output.send_replace(None);
    let Some(boot_id) = boot_id else {
        std::future::pending::<()>().await;
        return;
    };

    loop {
        attempt.output.send_replace(None);
        if attempt.output.is_closed() {
            std::future::pending::<()>().await;
            return;
        }
        if authority.generation_valid_for(required_margin).as_ref() != Some(generation) {
            // The owning postmaster select fences this attempt. Remaining
            // pending here avoids an immediately-ready retry loop competing
            // with that higher-level authority-loss branch.
            std::future::pending::<()>().await;
            return;
        }
        let Some(current) = attempt.inputs.borrow().clone() else {
            wait_for_input_or_authority_loss(
                &mut attempt.inputs,
                authority.clone(),
                required_margin,
            )
            .await;
            continue;
        };
        if !request_matches_runtime(&current, generation, postmaster_pid, &boot_id, None) {
            wait_for_retry_input_or_authority_loss(
                &mut attempt.inputs,
                authority.clone(),
                required_margin,
            )
            .await;
            continue;
        }

        let connection = connect(socket_dir.clone(), generation.clone());
        tokio::pin!(connection);
        let authority_lost = authority
            .clone()
            .wait_until_current_generation_invalid(required_margin);
        tokio::pin!(authority_lost);
        let connected = tokio::select! {
            result = &mut connection => Some(result),
            changed = attempt.inputs.changed() => {
                if changed.is_err() {
                    std::future::pending::<()>().await;
                }
                None
            }
            () = &mut authority_lost => None,
        };
        let Some(connected) = connected else {
            continue;
        };
        let Some((session, identity)) = connected else {
            wait_for_retry_input_or_authority_loss(
                &mut attempt.inputs,
                authority.clone(),
                required_margin,
            )
            .await;
            continue;
        };
        if !request_matches_runtime(
            &current,
            generation,
            postmaster_pid,
            &boot_id,
            Some(identity),
        ) {
            wait_for_retry_input_or_authority_loss(
                &mut attempt.inputs,
                authority.clone(),
                required_margin,
            )
            .await;
            continue;
        }
        let mut driver_ended = session.driver_ended();
        if *driver_ended.borrow() {
            continue;
        }
        if !publish_if_current(
            &attempt.inputs,
            &attempt.output,
            &current,
            session,
            &driver_ended,
            generation,
            &authority,
            required_margin,
            postmaster_pid,
            &boot_id,
            identity,
        ) {
            continue;
        }

        let authority_lost = authority
            .clone()
            .wait_until_current_generation_invalid(required_margin);
        tokio::pin!(authority_lost);
        tokio::select! {
            changed = attempt.inputs.changed() => {
                if changed.is_err() {
                    attempt.output.send_replace(None);
                    std::future::pending::<()>().await;
                }
            }
            () = &mut authority_lost => {}
            () = wait_for_driver_end(&mut driver_ended) => {}
            () = attempt.output.closed() => {
                attempt.output.send_replace(None);
                std::future::pending::<()>().await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_if_current(
    inputs: &watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
    output: &watch::Sender<Option<Arc<ValidatedCatalogRuntime>>>,
    current: &Arc<ValidatedCatalogStaticInputs>,
    session: RetainedCatalogRuntimeSession,
    driver_ended: &watch::Receiver<bool>,
    generation: &DurableWritableGeneration,
    authority: &WritableAuthorityObserver,
    required_margin: Duration,
    postmaster_pid: u32,
    boot_id: &str,
    identity: CatalogRuntimeIdentity,
) -> bool {
    // Keep the driver guard through publication so connection termination
    // cannot interleave after its final check and leak a stale session token.
    let driver_guard = driver_ended.borrow();
    if *driver_guard {
        return false;
    }
    let input_guard = inputs.borrow();
    let still_current = input_guard
        .as_ref()
        .is_some_and(|observed| exact_acceptance(&observed.accepted, &current.accepted));
    if !still_current || output.is_closed() {
        return false;
    }
    let mut session = Some(session);
    authority
        .publish_while_generation_current(generation, required_margin, || {
            let session = session.take().expect("session published once");
            #[cfg(test)]
            session.mark_published();
            output.send_replace(Some(Arc::new(ValidatedCatalogRuntime {
                _inputs: Arc::clone(current),
                _session: session,
                _generation: generation.clone(),
                _postmaster_pid: postmaster_pid,
                _boot_id: boot_id.to_owned(),
                _identity: identity,
            })));
        })
        .is_some()
}

fn request_matches_runtime(
    inputs: &ValidatedCatalogStaticInputs,
    generation: &DurableWritableGeneration,
    postmaster_pid: u32,
    boot_id: &str,
    identity: Option<CatalogRuntimeIdentity>,
) -> bool {
    let accepted = &inputs.accepted;
    let request = accepted.request();
    let generation_identity = generation.canonical_bytes();
    if request.source.generation_identity.as_bytes() != generation_identity
        || request.source.postmaster_pid != postmaster_pid
        || request.source.boot_id != boot_id
        || request.source.pod_name != accepted.target_pod_name()
        || request.source.pod_uid != accepted.target_pod_uid()
    {
        return false;
    }
    identity.is_none_or(|identity| {
        request.source.system_identifier == identity.system_identifier.to_string()
            && request.source.timeline == identity.timeline
    })
}

async fn wait_for_input_or_authority_loss(
    inputs: &mut watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
    authority: WritableAuthorityObserver,
    required_margin: Duration,
) {
    let authority_lost = authority.wait_until_current_generation_invalid(required_margin);
    tokio::pin!(authority_lost);
    tokio::select! {
        changed = inputs.changed() => {
            if changed.is_err() {
                std::future::pending::<()>().await;
            }
        }
        () = &mut authority_lost => {}
    }
}

async fn wait_for_retry_input_or_authority_loss(
    inputs: &mut watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
    authority: WritableAuthorityObserver,
    required_margin: Duration,
) {
    let authority_lost = authority.wait_until_current_generation_invalid(required_margin);
    tokio::pin!(authority_lost);
    tokio::select! {
        () = tokio::time::sleep(RETRY_INTERVAL) => {}
        changed = inputs.changed() => {
            if changed.is_err() {
                std::future::pending::<()>().await;
            }
        }
        () = &mut authority_lost => {}
    }
}

async fn wait_for_driver_end(driver_ended: &mut watch::Receiver<bool>) {
    while !*driver_ended.borrow_and_update() {
        if driver_ended.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    use crate::boottime::{BoottimeClock as _, BoottimeInstant, FakeBoottimeClock};
    use crate::catalog_activation_consumer::tests as consumer_tests;
    use crate::writable::{
        WritableLeaseAttempt, durable_generation_for_test,
        writable_attempt_pair_with_clock_for_test,
    };

    const TEST_BOOT_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const TEST_POSTMASTER_PID: u32 = 100;
    const REQUIRED_MARGIN: Duration = Duration::from_secs(1);

    type StaticInputSender = watch::Sender<Option<Arc<ValidatedCatalogStaticInputs>>>;
    type RuntimeOutputReceiver = watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>;

    fn fixture() -> (ValidatedCatalogStaticInputs, DurableWritableGeneration) {
        let request = consumer_tests::request();
        let generation = DurableWritableGeneration::parse_canonical(
            request.source.generation_identity.as_bytes(),
        )
        .expect("fixture generation");
        let accepted = consumer_tests::accepted(request);
        (
            ValidatedCatalogStaticInputs {
                accepted,
                migration: b"migration".to_vec().into_boxed_slice(),
                genesis: b"genesis".to_vec().into_boxed_slice(),
                preflight: b"preflight".to_vec().into_boxed_slice(),
            },
            generation,
        )
    }

    fn fixture_with_postmaster_pid(
        postmaster_pid: u32,
    ) -> (ValidatedCatalogStaticInputs, DurableWritableGeneration) {
        let mut request = consumer_tests::request();
        request.source.postmaster_pid = postmaster_pid;
        let generation = DurableWritableGeneration::parse_canonical(
            request.source.generation_identity.as_bytes(),
        )
        .expect("fixture generation");
        let accepted = consumer_tests::accepted(request);
        (
            ValidatedCatalogStaticInputs {
                accepted,
                migration: b"migration".to_vec().into_boxed_slice(),
                genesis: b"genesis".to_vec().into_boxed_slice(),
                preflight: b"preflight".to_vec().into_boxed_slice(),
            },
            generation,
        )
    }

    fn channels(
        initial: Arc<ValidatedCatalogStaticInputs>,
    ) -> (
        StaticInputSender,
        CatalogRuntimeBindingAttempt,
        RuntimeOutputReceiver,
    ) {
        let (inputs, input_receiver) = watch::channel(Some(initial));
        let (output, output_receiver) = watch::channel(None);
        (
            inputs,
            CatalogRuntimeBindingAttempt {
                inputs: input_receiver,
                output,
            },
            output_receiver,
        )
    }

    fn authority(
        generation: &DurableWritableGeneration,
    ) -> (
        Arc<FakeBoottimeClock>,
        WritableLeaseAttempt,
        WritableAuthorityObserver,
    ) {
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let (lease, postgres) = writable_attempt_pair_with_clock_for_test(clock.clone());
        lease.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_secs(10))
                .expect("test deadline"),
            generation.clone(),
        );
        (clock, lease, postgres.authority_observer())
    }

    #[derive(Clone)]
    struct TestSessionSignals {
        driver_ended: watch::Sender<bool>,
        published: Arc<AtomicBool>,
        dropped: Arc<AtomicUsize>,
    }

    impl TestSessionSignals {
        fn new() -> Self {
            let (driver_ended, _) = watch::channel(false);
            Self {
                driver_ended,
                published: Arc::new(AtomicBool::new(false)),
                dropped: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn connection(&self) -> (RetainedCatalogRuntimeSession, CatalogRuntimeIdentity) {
            (
                RetainedCatalogRuntimeSession::Test(TestCatalogRuntimeSession {
                    driver_ended: self.driver_ended.subscribe(),
                    published: Arc::clone(&self.published),
                    dropped: Arc::clone(&self.dropped),
                }),
                CatalogRuntimeIdentity {
                    system_identifier: 12_345_678_901_234_567_890,
                    timeline: 3,
                },
            )
        }
    }

    fn immediate_connector(
        signals: TestSessionSignals,
    ) -> impl Fn(
        PathBuf,
        DurableWritableGeneration,
    ) -> std::future::Ready<
        Option<(RetainedCatalogRuntimeSession, CatalogRuntimeIdentity)>,
    > + Send
    + 'static {
        move |_, _| std::future::ready(Some(signals.connection()))
    }

    fn spawn_test_runtime<C, F>(
        attempt: CatalogRuntimeBindingAttempt,
        generation: DurableWritableGeneration,
        authority: WritableAuthorityObserver,
        connect: C,
    ) -> JoinHandle<()>
    where
        C: Fn(PathBuf, DurableWritableGeneration) -> F + Send + 'static,
        F: Future<Output = Option<(RetainedCatalogRuntimeSession, CatalogRuntimeIdentity)>>
            + Send
            + 'static,
    {
        tokio::spawn(async move {
            supervise_catalog_runtime_binding_with_connector(
                attempt,
                PathBuf::from("/unused-test-socket"),
                &generation,
                authority,
                REQUIRED_MARGIN,
                TEST_POSTMASTER_PID,
                Some(TEST_BOOT_ID.to_owned()),
                connect,
            )
            .await;
        })
    }

    async fn wait_for_output(receiver: &mut RuntimeOutputReceiver, present: bool) {
        timeout(Duration::from_secs(1), async {
            loop {
                if receiver.borrow_and_update().is_some() == present {
                    return;
                }
                receiver
                    .changed()
                    .await
                    .expect("runtime output remains open");
            }
        })
        .await
        .expect("runtime output transition");
    }

    async fn wait_for_drop(signals: &TestSessionSignals, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while signals.dropped.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained runtime session drop");
    }

    #[test]
    fn request_requires_exact_process_generation_and_database_identity() {
        let (inputs, generation) = fixture();
        let boot_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let identity = CatalogRuntimeIdentity {
            system_identifier: 12_345_678_901_234_567_890,
            timeline: 3,
        };
        assert!(request_matches_runtime(
            &inputs,
            &generation,
            100,
            boot_id,
            Some(identity),
        ));
        assert!(!request_matches_runtime(
            &inputs,
            &generation,
            101,
            boot_id,
            Some(identity),
        ));
        assert!(!request_matches_runtime(
            &inputs,
            &generation,
            100,
            "ffffffff-1111-2222-3333-444444444444",
            Some(identity),
        ));
        assert!(!request_matches_runtime(
            &inputs,
            &generation,
            100,
            boot_id,
            Some(CatalogRuntimeIdentity {
                system_identifier: identity.system_identifier,
                timeline: 4,
            }),
        ));
    }

    #[test]
    fn request_rejects_another_writable_generation() {
        let (inputs, generation) = fixture();
        let mut canonical = generation.canonical_bytes();
        let term = canonical
            .windows(b"term=9\n".len())
            .position(|window| window == b"term=9\n")
            .expect("term field");
        canonical[term + 5] = b'8';
        let other =
            DurableWritableGeneration::parse_canonical(&canonical).expect("other generation");
        assert!(!request_matches_runtime(
            &inputs,
            &other,
            100,
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            None,
        ));
    }

    #[tokio::test]
    async fn runtime_withdraws_for_acceptance_loss_replacement_and_channel_close() {
        let (inputs, generation) = fixture();
        let current = Arc::new(inputs);
        let (input, attempt, mut output) = channels(Arc::clone(&current));
        let (_clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let task = spawn_test_runtime(
            attempt,
            generation,
            authority,
            immediate_connector(signals.clone()),
        );

        wait_for_output(&mut output, true).await;
        input.send_replace(None);
        wait_for_output(&mut output, false).await;

        input.send_replace(Some(Arc::clone(&current)));
        wait_for_output(&mut output, true).await;
        let (replacement, _) = fixture_with_postmaster_pid(TEST_POSTMASTER_PID + 1);
        input.send_replace(Some(Arc::new(replacement)));
        wait_for_output(&mut output, false).await;

        input.send_replace(Some(current));
        wait_for_output(&mut output, true).await;
        drop(input);
        wait_for_output(&mut output, false).await;
        assert!(signals.dropped.load(Ordering::Acquire) >= 3);

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn same_generation_renewal_retains_output_then_generation_change_withdraws_it() {
        let (inputs, generation) = fixture();
        let (_input, attempt, mut output) = channels(Arc::new(inputs));
        let (clock, lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let task = spawn_test_runtime(
            attempt,
            generation.clone(),
            authority,
            immediate_connector(signals.clone()),
        );

        wait_for_output(&mut output, true).await;
        lease.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_secs(20))
                .expect("renewed deadline"),
            generation,
        );
        tokio::task::yield_now().await;
        assert!(output.borrow().is_some());
        assert!(!output.has_changed().expect("output remains open"));

        lease.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_secs(20))
                .expect("replacement deadline"),
            durable_generation_for_test(10),
        );
        wait_for_output(&mut output, false).await;
        wait_for_drop(&signals, 1).await;

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn authority_expiry_withdraws_the_runtime() {
        let (inputs, generation) = fixture();
        let (_input, attempt, mut output) = channels(Arc::new(inputs));
        let (clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let task = spawn_test_runtime(
            attempt,
            generation,
            authority,
            immediate_connector(signals.clone()),
        );

        wait_for_output(&mut output, true).await;
        clock
            .advance(Duration::from_secs(9))
            .expect("advance to required-margin cutoff");
        wait_for_output(&mut output, false).await;
        wait_for_drop(&signals, 1).await;

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn driver_termination_withdraws_the_runtime() {
        let (inputs, generation) = fixture();
        let (_input, attempt, mut output) = channels(Arc::new(inputs));
        let (_clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let task = spawn_test_runtime(
            attempt,
            generation,
            authority,
            immediate_connector(signals.clone()),
        );

        wait_for_output(&mut output, true).await;
        signals.driver_ended.send_replace(true);
        wait_for_output(&mut output, false).await;
        wait_for_drop(&signals, 1).await;

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn owning_process_or_shutdown_cancellation_withdraws_the_runtime() {
        let (inputs, generation) = fixture();
        let (_input, attempt, mut output) = channels(Arc::new(inputs));
        let (_clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let task = spawn_test_runtime(
            attempt,
            generation,
            authority,
            immediate_connector(signals.clone()),
        );

        wait_for_output(&mut output, true).await;
        task.abort();
        let _ = task.await;
        wait_for_output(&mut output, false).await;
        assert_eq!(signals.dropped.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn dropping_the_private_handoff_drops_its_retained_session() {
        let (inputs, generation) = fixture();
        let (_input, attempt, mut output) = channels(Arc::new(inputs));
        let (_clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let task = spawn_test_runtime(
            attempt,
            generation,
            authority,
            immediate_connector(signals.clone()),
        );

        wait_for_output(&mut output, true).await;
        drop(output);
        wait_for_drop(&signals, 1).await;
        task.abort();
        let _ = task.await;
    }

    struct ConnectionAttemptCompletion(watch::Sender<bool>);

    impl Drop for ConnectionAttemptCompletion {
        fn drop(&mut self) {
            self.0.send_replace(true);
        }
    }

    #[test]
    fn completed_connect_cannot_publish_after_acceptance_loss() {
        let (inputs, generation) = fixture();
        let current = Arc::new(inputs);
        let (input, attempt, output) = channels(Arc::clone(&current));
        let (_clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let (session, identity) = signals.connection();
        let driver_ended = session.driver_ended();

        input.send_replace(None);
        assert!(!publish_if_current(
            &attempt.inputs,
            &attempt.output,
            &current,
            session,
            &driver_ended,
            &generation,
            &authority,
            REQUIRED_MARGIN,
            TEST_POSTMASTER_PID,
            TEST_BOOT_ID,
            identity,
        ));
        assert!(output.borrow().is_none());
        assert!(!signals.published.load(Ordering::Acquire));
        assert_eq!(signals.dropped.load(Ordering::Acquire), 1);
    }

    #[test]
    fn completed_connect_cannot_publish_after_generation_change() {
        let (inputs, generation) = fixture();
        let current = Arc::new(inputs);
        let (_input, attempt, output) = channels(Arc::clone(&current));
        let (clock, lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let (session, identity) = signals.connection();
        let driver_ended = session.driver_ended();

        lease.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_secs(10))
                .expect("replacement deadline"),
            durable_generation_for_test(10),
        );
        assert!(!publish_if_current(
            &attempt.inputs,
            &attempt.output,
            &current,
            session,
            &driver_ended,
            &generation,
            &authority,
            REQUIRED_MARGIN,
            TEST_POSTMASTER_PID,
            TEST_BOOT_ID,
            identity,
        ));
        assert!(output.borrow().is_none());
        assert!(!signals.published.load(Ordering::Acquire));
        assert_eq!(signals.dropped.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn acceptance_loss_cancels_an_inflight_connection() {
        let (inputs, generation) = fixture();
        let (input, attempt, output) = channels(Arc::new(inputs));
        let (_clock, _lease, authority) = authority(&generation);
        let signals = TestSessionSignals::new();
        let (release, released) = oneshot::channel();
        let released = Arc::new(Mutex::new(Some(released)));
        let (started, mut started_receiver) = watch::channel(false);
        let (finished, mut finished_receiver) = watch::channel(false);
        let connector_signals = signals.clone();
        let connector_release = Arc::clone(&released);
        let connector = move |_: PathBuf, _: DurableWritableGeneration| {
            let released = connector_release.lock().expect("release lock").take();
            let started = started.clone();
            let finished = finished.clone();
            let signals = connector_signals.clone();
            async move {
                let Some(released) = released else {
                    std::future::pending::<()>().await;
                    unreachable!("pending connector completed");
                };
                started.send_replace(true);
                let _completion = ConnectionAttemptCompletion(finished);
                let _ = released.await;
                Some(signals.connection())
            }
        };
        let task = spawn_test_runtime(attempt, generation, authority, connector);

        timeout(Duration::from_secs(1), async {
            while !*started_receiver.borrow_and_update() {
                started_receiver
                    .changed()
                    .await
                    .expect("connector start watch");
            }
        })
        .await
        .expect("connector started");
        input.send_replace(None);
        timeout(Duration::from_secs(1), async {
            while !*finished_receiver.borrow_and_update() {
                finished_receiver
                    .changed()
                    .await
                    .expect("connector completion watch");
            }
        })
        .await
        .expect("inflight connector was cancelled");
        assert!(
            release.send(()).is_err(),
            "connector still held its release gate"
        );
        assert!(output.borrow().is_none());
        assert!(!signals.published.load(Ordering::Acquire));

        task.abort();
        let _ = task.await;
    }
}
