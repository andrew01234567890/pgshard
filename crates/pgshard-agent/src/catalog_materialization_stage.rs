//! Drives the SQL materializer from one exact writable-runtime capability.
//!
//! This stage owns *whether* the verified program runs. It never decides what
//! runs: the program, the sealed shard count, and the allow-empty policy all
//! arrive inside the capability the runtime binding published, and that
//! capability already agrees with the exact durable request, the verified
//! static inputs, the attempt-private writable authority, the postmaster
//! incarnation, and the local `PostgreSQL` identity.
//!
//! The binding withdraws and republishes its capability whenever any of those
//! change, and each publication is a fresh `Arc`, so pointer identity against
//! the captured capability observes revocation without having to re-derive it.
//!
//! What that observation is and is not. It is a snapshot, not a linearization
//! point: it is taken, it releases its guard, and only then does the executor
//! dispatch `COMMIT`. A withdrawal landing in that gap does not un-queue the
//! commit. What makes the gap safe is not this predicate but the generation
//! fence the executor holds across it: publishing a writable generation takes
//! `SHARE ROW EXCLUSIVE` on the generation relation, which is the same lock the
//! fence holds, so no competing generation can become authoritative while a
//! fenced commit is in flight. A fence lost with a commit already dispatched is
//! reported as `AmbiguousCatalogCommit` and reconciled against the catalog
//! rather than guessed at. This predicate's job is narrower: end an attempt
//! promptly once its capability is gone, and refuse to publish a proof for one.
//!
//! Withdrawal of a proof is likewise asynchronous: this supervisor has to be
//! scheduled to retract one, so a published proof can briefly outlive the
//! capability it rests on. That is why this module publishes a proof and
//! nothing else. Observing currency and then acting on the answer is the same
//! race in a different place — no in-process guard can close it, because the
//! caller can always defer the action past whatever the guard held. The stage
//! that consumes this handoff has to authorize at its action's own
//! linearization point, and introduces that API together with the action it
//! authorizes, where the two can be reviewed against each other.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::watch;

use crate::catalog_activation_runtime::{CatalogRuntimeHandoff, ValidatedCatalogRuntime};
use crate::catalog_materializer::materialize_and_verify;
use crate::postgres_generation::PostgresGenerationError;

/// Opaque retained receiver for the exact materialized-catalog handoff.
///
/// There is intentionally no observer API, and no committer API either. A
/// commit-under-guard API was tried and withdrawn: holding the binding watch
/// across a caller-supplied closure blocks capability withdrawal for however
/// long that closure runs, and lets a closure that re-enters the same watch
/// deadlock against the writer already waiting on it. Withdrawal is a security
/// operation and cannot be made to wait on unbounded caller code.
///
/// Releasing the guard instead is the race this module already documents. Both
/// shapes are wrong because the question itself is: an in-process guard cannot
/// authorize an effect that outlives the call. A later independently reviewed
/// serving-activation stage must consume this move-only handoff, and introduces
/// its authorization API together with the action it authorizes, so the two can
/// be reviewed against each other.
#[must_use = "dropping the materialization handoff closes its private watch"]
pub struct MaterializedCatalogHandoff {
    receiver: watch::Receiver<Option<Arc<MaterializedCatalog>>>,
}

impl MaterializedCatalogHandoff {
    /// Moves the private receiver into the serving-activation stage.
    pub(crate) fn into_receiver(self) -> watch::Receiver<Option<Arc<MaterializedCatalog>>> {
        self.receiver
    }

    /// Wraps a receiver so the consuming stage can be exercised on its own.
    #[cfg(test)]
    pub(crate) fn for_test(receiver: watch::Receiver<Option<Arc<MaterializedCatalog>>>) -> Self {
        Self { receiver }
    }
}

/// Proof that the catalog held the declared state while this exact runtime
/// capability was current. Deliberately private, move-only, non-debuggable,
/// and non-serializable.
pub(crate) struct MaterializedCatalog {
    /// Retained so the proof cannot outlive the session, authority, and
    /// incarnation evidence it was established against.
    bound: Arc<ValidatedCatalogRuntime>,
    /// Retained so the consumer can observe the binding directly. A published
    /// proof is not the same claim as a current capability: retraction is
    /// asynchronous, so the proof outlives the capability for as long as the
    /// supervisor takes to be scheduled.
    binding: watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
}

impl MaterializedCatalog {
    /// The runtime capability this proof was established against.
    pub(crate) fn bound(&self) -> &Arc<ValidatedCatalogRuntime> {
        &self.bound
    }

    /// Whether the capability behind this proof is still the published one.
    ///
    /// This is what makes observing the proof insufficient on its own: the
    /// proof is withdrawn by a supervisor that has to be scheduled first, so a
    /// consumer holding one must ask the binding directly at the point it acts.
    pub(crate) fn capability_is_current(&self) -> bool {
        still_bound(&self.binding, &self.bound)
    }

    /// Builds a proof around a caller-held binding, so the stage that consumes
    /// one can be exercised without a live server.
    #[cfg(test)]
    pub(crate) fn for_test(
        bound: Arc<ValidatedCatalogRuntime>,
        binding: watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
    ) -> Self {
        Self { bound, binding }
    }
}

/// Starts the materialization stage against the runtime binding's output.
pub fn spawn_catalog_materialization(runtime: CatalogRuntimeHandoff) -> MaterializedCatalogHandoff {
    let (output, receiver) = watch::channel(None);
    tokio::spawn(supervise(
        runtime.into_receiver(),
        output,
        |bound, runtime| async move {
            let inputs = bound.inputs();
            materialize_and_verify(
                bound.socket_dir(),
                &inputs.program,
                inputs.shard_count,
                inputs.allow_empty_database_topology,
                bound.generation(),
                &|| still_bound(&runtime, &bound),
            )
            .await
        },
    ));
    MaterializedCatalogHandoff { receiver }
}

/// Withdraws the published proof whenever this supervisor stops standing
/// behind it, including on drop.
struct OutputWithdrawalGuard(watch::Sender<Option<Arc<MaterializedCatalog>>>);

impl Drop for OutputWithdrawalGuard {
    fn drop(&mut self) {
        self.0.send_replace(None);
    }
}

async fn supervise<M, F>(
    mut runtime: watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
    output: watch::Sender<Option<Arc<MaterializedCatalog>>>,
    materialize: M,
) where
    M: Fn(Arc<ValidatedCatalogRuntime>, watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>) -> F,
    F: Future<Output = Result<(), PostgresGenerationError>>,
{
    let _withdraw_on_exit = OutputWithdrawalGuard(output.clone());
    loop {
        // Nothing this stage published survives the capability it was
        // established against, so the withdrawal precedes every new attempt.
        output.send_replace(None);
        let Some(bound) = runtime.borrow_and_update().clone() else {
            if runtime.changed().await.is_err() {
                return;
            }
            continue;
        };
        if output.is_closed() {
            return;
        }

        match materialize(Arc::clone(&bound), runtime.clone()).await {
            Ok(()) => {
                publish_if_current(&runtime, &output, &bound);
            }
            // The materializer already bounded its own reconciliation. Anything
            // reaching here ends this attempt; a fresh capability starts the
            // next one, which is also what re-establishes the authority this
            // stage would otherwise be acting on stale evidence of.
            Err(error) => {
                tracing::warn!(
                    reason = %error,
                    "catalog materialization did not complete for this runtime capability"
                );
            }
        }

        // Wait for the capability to change rather than retrying against the
        // one that just failed: retrying it would spin, and the binding is what
        // re-establishes agreement.
        if runtime.changed().await.is_err() {
            return;
        }
    }
}

/// Whether this attempt is still acting for the one capability it captured.
///
/// A closed channel retains its last value, so a binding that is gone entirely
/// would otherwise keep reading as current: its drop guard normally withdraws
/// first, but a predicate that depends on that ordering is a predicate that
/// fails open when it does not hold.
fn still_bound(
    runtime: &watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
    bound: &Arc<ValidatedCatalogRuntime>,
) -> bool {
    runtime.has_changed().is_ok()
        && runtime
            .borrow()
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, bound))
}

/// Publishes the proof only while the capability it was established against is
/// still the current one, with nothing awaited between the check and the send.
fn publish_if_current(
    runtime: &watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
    output: &watch::Sender<Option<Arc<MaterializedCatalog>>>,
    bound: &Arc<ValidatedCatalogRuntime>,
) -> bool {
    if runtime.has_changed().is_err() || output.is_closed() {
        return false;
    }
    // Held across the send: a republication landing between the check and the
    // send would otherwise publish a proof for a capability already withdrawn.
    let current = runtime.borrow();
    if !current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, bound))
    {
        return false;
    }
    output.send_replace(Some(Arc::new(MaterializedCatalog {
        bound: Arc::clone(bound),
        binding: runtime.clone(),
    })));
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::catalog_activation_consumer::tests as consumer_tests;
    use crate::catalog_activation_static_inputs::ValidatedCatalogStaticInputs;
    use crate::catalog_materialization_program::compile_catalog_materialization_program;
    use crate::writable::DurableWritableGeneration;

    type RuntimeSender = watch::Sender<Option<Arc<ValidatedCatalogRuntime>>>;
    type ProofReceiver = watch::Receiver<Option<Arc<MaterializedCatalog>>>;

    /// The stage is indifferent to the program's contents; it only decides
    /// whether the materializer may run it.
    fn capability() -> Arc<ValidatedCatalogRuntime> {
        let request = consumer_tests::request();
        let generation = DurableWritableGeneration::parse_canonical(
            request.source.generation_identity.as_bytes(),
        )
        .expect("fixture generation");
        Arc::new(ValidatedCatalogRuntime::for_test(
            Arc::new(ValidatedCatalogStaticInputs {
                accepted: consumer_tests::accepted(request),
                program: compile_catalog_materialization_program(
                    b"BEGIN;\nSELECT 1;\nCOMMIT;\n",
                    b"SELECT 1;\n",
                    b"SELECT 2;\n",
                    b"SELECT 3;\n",
                )
                .expect("fixture program"),
                shard_count: 1,
                allow_empty_database_topology: false,
            }),
            generation,
        ))
    }

    fn start<M, F>(runtime: &RuntimeSender, materialize: M) -> ProofReceiver
    where
        M: Fn(
                Arc<ValidatedCatalogRuntime>,
                watch::Receiver<Option<Arc<ValidatedCatalogRuntime>>>,
            ) -> F
            + Send
            + 'static,
        F: Future<Output = Result<(), PostgresGenerationError>> + Send + 'static,
    {
        let (output, proof) = watch::channel(None);
        tokio::spawn(supervise(runtime.subscribe(), output, materialize));
        proof
    }

    /// Gives the stage many turns. Nothing here awaits real I/O, so a stage
    /// that parks makes no further progress and a stage that spins makes a
    /// great deal -- which is what the attempt count then distinguishes.
    const TURNS: usize = 64;

    async fn settle() {
        for _ in 0..TURNS {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn a_capability_that_materializes_is_proved_against_that_capability() {
        let (runtime, _keep) = watch::channel(None);
        let proof = start(&runtime, |_, _| async { Ok(()) });
        let bound = capability();
        runtime.send_replace(Some(Arc::clone(&bound)));

        settle().await;
        let published = proof.borrow().clone().expect("the catalog was proved");
        assert!(
            Arc::ptr_eq(&published.bound, &bound),
            "the proof is not pinned to the capability it was established against"
        );
    }

    /// The whole point of the stage: a proof may never outlive the capability
    /// it was established against, including one replaced mid-materialization.
    #[tokio::test]
    async fn a_capability_replaced_while_materializing_is_never_proved() {
        let (runtime, _keep) = watch::channel(None);
        let replace_with = runtime.clone();
        let proof = start(&runtime, move |_, _| {
            // Exactly the race the predicate exists for: authority moved on
            // after the last commit and before publication.
            replace_with.send_replace(Some(capability()));
            async { Ok(()) }
        });
        runtime.send_replace(Some(capability()));

        settle().await;
        assert!(
            proof.borrow().is_none(),
            "a capability replaced during materialization was still proved"
        );
    }

    #[tokio::test]
    async fn a_failed_materialization_proves_nothing_and_waits_for_a_new_capability() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let (runtime, _keep) = watch::channel(None);
        let proof = start(&runtime, move |_, _| {
            counted.fetch_add(1, Ordering::AcqRel);
            async { Err(PostgresGenerationError::CatalogNotMaterialized) }
        });
        runtime.send_replace(Some(capability()));

        settle().await;
        assert!(
            proof.borrow().is_none(),
            "a failed attempt published a proof"
        );
        assert_eq!(
            attempts.load(Ordering::Acquire),
            1,
            "the stage retried the capability that just failed instead of waiting"
        );

        // A fresh capability, not a retry of the old one, is what starts the
        // next attempt.
        runtime.send_replace(Some(capability()));
        settle().await;
        assert_eq!(
            attempts.load(Ordering::Acquire),
            2,
            "a new capability did not start a new attempt"
        );
    }

    /// A closed channel keeps its last value, so a binding that is gone must
    /// not keep reading as present.
    #[test]
    fn a_capability_whose_binding_is_gone_is_not_current() {
        let bound = capability();
        let (runtime, receiver) = watch::channel(Some(Arc::clone(&bound)));
        assert!(still_bound(&receiver, &bound));
        drop(runtime);
        assert!(
            !still_bound(&receiver, &bound),
            "a capability outlived the binding that published it"
        );
    }

    #[tokio::test]
    async fn withdrawing_the_capability_withdraws_the_proof() {
        let (runtime, _keep) = watch::channel(None);
        let proof = start(&runtime, |_, _| async { Ok(()) });
        runtime.send_replace(Some(capability()));
        settle().await;
        assert!(proof.borrow().is_some(), "the catalog was not proved");

        runtime.send_replace(None);
        settle().await;
        assert!(
            proof.borrow().is_none(),
            "the proof outlived the capability it was established against"
        );
    }
}
