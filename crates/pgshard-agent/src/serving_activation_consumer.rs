//! Consumes the move-only materialized-catalog handoff and runs at most one
//! serving activation per capability.
//!
//! The materialization stage deliberately publishes a proof and nothing else,
//! and says why: an in-process guard cannot authorize an effect that outlives
//! the call, so the stage that acts has to authorize at its own action's
//! linearization point. This is that stage, and
//! [`CapturedMaterialization::is_current`] is that authorization API. It is
//! synchronous, it asks the binding rather than the proof, and it is re-derived
//! inside the dispatch step rather than around it.
//!
//! Why asking the proof is not enough. A published proof is withdrawn by a
//! supervisor that has to be scheduled first, so the proof outlives the
//! capability it rests on for as long as that takes. The captured proof
//! therefore answers two questions at once: is this still the published proof,
//! and is the capability it retained still the published capability.
//!
//! One capability gets one attempt. A capability whose activation fenced is not
//! retried, because the thing that failed is the incarnation, and the next
//! attempt has to be a different incarnation with its own non-serving start.

use std::future::Future;
use std::sync::Arc;

use pgshard_types::writable_generation::DurableWritableGeneration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::catalog_materialization_stage::{MaterializedCatalog, MaterializedCatalogHandoff};
use crate::serving_activation::{
    FencedPostmaster, ServingActivationError, ServingActivationOutcome, ServingProof,
};

/// One captured materialization proof, plus the means to re-derive whether it
/// is still current.
///
/// This is the authorization API the materialization stage deferred. It is
/// deliberately synchronous: an answer taken before an `await` is evidence
/// about then, and the caller must take it inside the same synchronous step
/// that dispatches the reload.
pub struct CapturedMaterialization {
    proof: Arc<MaterializedCatalog>,
    proofs: watch::Receiver<Option<Arc<MaterializedCatalog>>>,
}

impl CapturedMaterialization {
    /// Whether this exact proof is still published *and* the capability it
    /// retained is still the published capability.
    ///
    /// Both are required. The proof answers whether this stage's own input is
    /// still current; the capability answers whether the evidence underneath it
    /// is. Withdrawal happens in that order, so a check that asked only the
    /// first would accept a proof whose capability was already gone.
    #[must_use]
    pub fn is_current(&self) -> bool {
        if self.proofs.has_changed().is_err() {
            return false;
        }
        let published = self.proofs.borrow();
        published
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.proof))
            && self.proof.capability_is_current()
    }

    /// The writable generation the proof was established under, which is the
    /// generation the reload must still be fenced by.
    #[must_use]
    pub fn generation(&self) -> &DurableWritableGeneration {
        self.proof.bound().generation()
    }
}

/// What the supervisor does with each attempt's conclusion.
///
/// Both arms exist because both are actions. A proved activation is what admits
/// application traffic; a fence is a postmaster that must be stopped. Neither
/// is a value this stage may keep to itself.
pub trait ServingActivationSink: Send + Sync + 'static {
    /// Accepts a proved activation for one incarnation.
    fn serving(&self, proof: ServingProof);
    /// Stops the exact incarnation that may hold an unproved serving policy.
    fn fence(&self, fenced: FencedPostmaster);
}

/// Starts the serving-activation stage against the materialization handoff.
///
/// `attempt` performs one activation for one captured proof. It receives the
/// authorization API rather than a decision, because the decision has to be
/// taken at the dispatch boundary inside it.
pub fn spawn_serving_activation<A, F, S>(
    materialized: MaterializedCatalogHandoff,
    attempt: A,
    sink: S,
) -> JoinHandle<()>
where
    A: Fn(CapturedMaterialization) -> F + Send + 'static,
    F: Future<Output = Result<ServingActivationOutcome, ServingActivationError>> + Send + 'static,
    S: ServingActivationSink,
{
    tokio::spawn(supervise(materialized.into_receiver(), attempt, sink))
}

async fn supervise<A, F, S>(
    mut proofs: watch::Receiver<Option<Arc<MaterializedCatalog>>>,
    attempt: A,
    sink: S,
) where
    A: Fn(CapturedMaterialization) -> F,
    F: Future<Output = Result<ServingActivationOutcome, ServingActivationError>>,
    S: ServingActivationSink,
{
    loop {
        let Some(proof) = proofs.borrow_and_update().clone() else {
            if proofs.changed().await.is_err() {
                return;
            }
            continue;
        };
        let captured = CapturedMaterialization {
            proof,
            proofs: proofs.clone(),
        };
        // The capability can already be gone: the proof's own withdrawal is
        // asynchronous, so a published proof is not evidence that the runtime
        // behind it survived. Starting an attempt on one would install a sealed
        // policy for authority nobody holds.
        if !captured.is_current() {
            if proofs.changed().await.is_err() {
                return;
            }
            continue;
        }

        match attempt(captured).await {
            Ok(ServingActivationOutcome::Serving(proof)) => sink.serving(proof),
            Ok(ServingActivationOutcome::Fenced(fenced)) => sink.fence(fenced),
            // Failures here are strictly before the sealed policy reached the
            // disk, so nothing needs undoing and nothing needs fencing.
            Err(error) => tracing::warn!(
                reason = %error,
                "serving activation did not start for this materialization proof"
            ),
        }

        // One capability, one attempt. Retrying the same proof would repeat a
        // transition against an incarnation whose activation already concluded.
        if proofs.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::catalog_activation_consumer::tests as consumer_tests;
    use crate::catalog_activation_runtime::ValidatedCatalogRuntime;
    use crate::catalog_activation_static_inputs::ValidatedCatalogStaticInputs;
    use crate::catalog_materialization_program::compile_catalog_materialization_program;
    use crate::serving_activation::{FenceReason, PostmasterIncarnation};
    use crate::writable::DurableWritableGeneration;

    type Binding = watch::Sender<Option<Arc<ValidatedCatalogRuntime>>>;
    type Proofs = watch::Sender<Option<Arc<MaterializedCatalog>>>;

    /// Gives the stage many turns. Nothing here awaits real I/O, so a stage
    /// that parks makes no further progress and a stage that spins makes a
    /// great deal.
    const TURNS: usize = 64;

    async fn settle() {
        for _ in 0..TURNS {
            tokio::task::yield_now().await;
        }
    }

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

    /// Publishes a capability and a proof that rests on it.
    fn published() -> (Binding, Proofs, Arc<MaterializedCatalog>) {
        let bound = capability();
        let (binding, binding_receiver) = watch::channel(Some(Arc::clone(&bound)));
        let proof = Arc::new(MaterializedCatalog::for_test(bound, binding_receiver));
        let (proofs, _) = watch::channel(Some(Arc::clone(&proof)));
        (binding, proofs, proof)
    }

    #[derive(Default)]
    struct RecordingSink {
        served: Mutex<Vec<String>>,
        fenced: Mutex<Vec<PostmasterIncarnation>>,
    }

    impl ServingActivationSink for Arc<RecordingSink> {
        fn serving(&self, proof: ServingProof) {
            self.served
                .lock()
                .expect("sink is not poisoned")
                .push(proof.policy_sha256().to_owned());
        }

        fn fence(&self, fenced: FencedPostmaster) {
            self.fenced
                .lock()
                .expect("sink is not poisoned")
                .push(fenced.incarnation().clone());
        }
    }

    fn incarnation(pid: u32) -> PostmasterIncarnation {
        PostmasterIncarnation {
            boot_id: "fixture-boot".to_owned(),
            pid,
            start_ticks: u64::from(pid) * 10,
        }
    }

    fn start<A, F>(
        proofs: &Proofs,
        attempt: A,
        sink: &Arc<RecordingSink>,
    ) -> tokio::task::JoinHandle<()>
    where
        A: Fn(CapturedMaterialization) -> F + Send + 'static,
        F: Future<Output = Result<ServingActivationOutcome, ServingActivationError>>
            + Send
            + 'static,
    {
        tokio::spawn(supervise(proofs.subscribe(), attempt, Arc::clone(sink)))
    }

    #[tokio::test]
    async fn a_published_proof_starts_exactly_one_attempt_that_reaches_the_sink() {
        let (_binding, proofs, _proof) = published();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async {
                    Ok(ServingActivationOutcome::Serving(ServingProof::for_test(
                        incarnation(11),
                        "digest".to_owned(),
                    )))
                }
            },
            &sink,
        );

        settle().await;
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        assert_eq!(
            sink.served.lock().expect("sink").as_slice(),
            ["digest".to_owned()]
        );
    }

    /// The invariant the materialization stage documented but could not
    /// enforce: a published proof outlives its capability, so a consumer that
    /// trusted the proof alone would act on evidence that is already gone.
    #[tokio::test]
    async fn a_proof_whose_capability_is_gone_starts_no_attempt() {
        let (binding, proofs, _proof) = published();
        binding.send_replace(None);
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async { panic!("an attempt started for a withdrawn capability") }
            },
            &sink,
        );

        settle().await;
        assert_eq!(
            attempts.load(Ordering::Acquire),
            0,
            "the proof was still published, so only the binding could refuse this"
        );
    }

    #[tokio::test]
    async fn a_proof_whose_binding_channel_is_gone_starts_no_attempt() {
        let (binding, proofs, _proof) = published();
        // A closed watch keeps its last value, so a binding that is gone
        // entirely would otherwise keep reading as current.
        drop(binding);
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async { panic!("an attempt started for an abandoned binding") }
            },
            &sink,
        );

        settle().await;
        assert_eq!(attempts.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn a_withdrawn_proof_starts_no_attempt() {
        let (_binding, proofs, _proof) = published();
        proofs.send_replace(None);
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async { panic!("an attempt started for a withdrawn proof") }
            },
            &sink,
        );

        settle().await;
        assert_eq!(attempts.load(Ordering::Acquire), 0);
    }

    /// A fence is a conclusion about one incarnation. Retrying it would install
    /// the sealed policy again for a postmaster that is already being stopped.
    #[tokio::test]
    async fn a_fence_reaches_the_sink_and_the_same_proof_is_not_retried() {
        let (_binding, proofs, _proof) = published();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async {
                    Ok(ServingActivationOutcome::Fenced(
                        FencedPostmaster::for_test(incarnation(77), FenceReason::UnprovedReload),
                    ))
                }
            },
            &sink,
        );

        settle().await;
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        assert_eq!(
            sink.fenced.lock().expect("sink").as_slice(),
            [incarnation(77)]
        );

        settle().await;
        assert_eq!(
            attempts.load(Ordering::Acquire),
            1,
            "the stage retried a capability whose activation already concluded"
        );
    }

    #[tokio::test]
    async fn a_new_capability_starts_a_new_attempt() {
        let (_binding, proofs, _proof) = published();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async {
                    Ok(ServingActivationOutcome::Fenced(
                        FencedPostmaster::for_test(incarnation(1), FenceReason::UnprovedReload),
                    ))
                }
            },
            &sink,
        );
        settle().await;
        assert_eq!(attempts.load(Ordering::Acquire), 1);

        let (_next_binding, _next_proofs, next) = published();
        proofs.send_replace(Some(next));
        settle().await;
        assert_eq!(
            attempts.load(Ordering::Acquire),
            2,
            "a fresh capability did not start a fresh attempt"
        );
    }

    #[tokio::test]
    async fn a_failed_attempt_fences_nothing_and_waits_for_a_new_capability() {
        let (_binding, proofs, _proof) = published();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let sink = Arc::new(RecordingSink::default());
        let _task = start(
            &proofs,
            move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                async { Err(ServingActivationError::PolicyDigestMismatch) }
            },
            &sink,
        );

        settle().await;
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        assert!(sink.served.lock().expect("sink").is_empty());
        assert!(
            sink.fenced.lock().expect("sink").is_empty(),
            "a failure before the policy reached the disk fenced a postmaster"
        );
    }

    /// The public entry point, wired the way production would wire it, so the
    /// handoff really is consumed rather than only the loop behind it.
    #[tokio::test]
    async fn the_handoff_itself_is_what_the_stage_consumes() {
        let (_binding, proofs, _proof) = published();
        let sink = Arc::new(RecordingSink::default());
        let task = spawn_serving_activation(
            MaterializedCatalogHandoff::for_test(proofs.subscribe()),
            |captured| async move {
                assert!(captured.is_current());
                Ok(ServingActivationOutcome::Serving(ServingProof::for_test(
                    incarnation(3),
                    "digest".to_owned(),
                )))
            },
            Arc::clone(&sink),
        );

        settle().await;
        assert_eq!(
            sink.served.lock().expect("sink").as_slice(),
            ["digest".to_owned()]
        );
        drop(proofs);
        task.await.expect("the stage ends when its handoff closes");
    }

    #[tokio::test]
    async fn closing_the_handoff_ends_the_stage() {
        let (_binding, proofs, _proof) = published();
        let sink = Arc::new(RecordingSink::default());
        let task = start(
            &proofs,
            |_| async {
                Ok(ServingActivationOutcome::Serving(ServingProof::for_test(
                    incarnation(5),
                    "digest".to_owned(),
                )))
            },
            &sink,
        );
        settle().await;
        drop(proofs);
        task.await.expect("the stage ends when its input closes");
    }

    /// The authorization API on its own: a proof that is still published but
    /// whose capability has been withdrawn is not current.
    #[test]
    fn the_authorization_api_asks_the_binding_and_not_only_the_proof() {
        let (binding, proofs, proof) = published();
        let captured = CapturedMaterialization {
            proof: Arc::clone(&proof),
            proofs: proofs.subscribe(),
        };
        assert!(captured.is_current());

        binding.send_replace(None);
        assert!(
            !captured.is_current(),
            "the proof was still published, so the capability check is the only thing that \
             could have refused"
        );
    }

    #[test]
    fn the_authorization_api_refuses_a_proof_that_was_replaced() {
        let (_binding, proofs, proof) = published();
        let captured = CapturedMaterialization {
            proof,
            proofs: proofs.subscribe(),
        };
        assert!(captured.is_current());

        let (_next_binding, _next_proofs, next) = published();
        proofs.send_replace(Some(next));
        assert!(
            !captured.is_current(),
            "a replaced proof was still reported as current"
        );
    }

    #[test]
    fn the_authorization_api_refuses_a_proof_whose_publisher_is_gone() {
        let (_binding, proofs, proof) = published();
        let captured = CapturedMaterialization {
            proof,
            proofs: proofs.subscribe(),
        };
        assert!(captured.is_current());
        drop(proofs);
        assert!(
            !captured.is_current(),
            "a closed watch keeps its last value, so this is the check that must refuse"
        );
    }

    #[test]
    fn the_captured_generation_is_the_one_the_capability_was_bound_to() {
        let (_binding, proofs, proof) = published();
        let expected = proof.bound().generation().clone();
        let captured = CapturedMaterialization {
            proof,
            proofs: proofs.subscribe(),
        };
        assert_eq!(captured.generation(), &expected);
    }
}
