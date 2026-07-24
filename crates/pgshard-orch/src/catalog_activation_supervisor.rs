//! Fail-closed composition of catalog-activation proof, challenge, and publish.
//!
//! Every network observation precedes a fresh in-process authority issue. The
//! final authority check is synchronous and immediately precedes the only
//! parent-resource `PUT`. Once that `PUT` starts it is never cancelled by the
//! supervisor and remains bounded below the process shutdown grace.

use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;

use crate::catalog_activation_challenge::{
    CatalogActivationChallengeClient, CatalogActivationChallengeError,
};
use crate::catalog_activation_live_objects::{
    AuthoritativeCatalogActivationLiveObjectReader, CatalogActivationLiveObjectError,
};
use crate::catalog_activation_publisher::{
    CatalogActivationPublicationOutcome, CatalogActivationPublisher,
    CatalogActivationPublisherError, DefinitiveCatalogActivationRejection,
    PendingCatalogActivationPublication,
};
use crate::catalog_candidate::{
    AuthoritativeCandidateReader, BoundCandidateSet, CatalogCandidateError,
};
use crate::catalog_materialization::{
    CatalogActivationLiveObjectProofs, CatalogActivationPreparationError,
    CatalogActivationPublicationTarget, CatalogBootstrapDispatch, PreparedCatalogActivationRequest,
    catalog_activation_publication_target, prepare_catalog_activation_request,
    rebind_catalog_activation_live_objects,
};
use crate::domain::OrchState;
use crate::topology::CatalogCandidateObservationPlan;

// The two independent proof collectors retain their last published proof
// through every refresh bracket; a proof leaves only when its replacement
// swaps in, a failure is recorded, or its own freshness deadline expires.
// Once both collectors have published, the proof overlap is therefore
// continuously observable at any poll instant regardless of collector
// phasing. This shorter local-only poll merely shortens the wait for the
// first overlap without increasing Kubernetes traffic before initial
// authority exists.
pub(crate) const AUTHORITY_POLL_PERIOD: Duration = Duration::from_millis(100);

/// Runs the one-shot catalog-activation publisher until shutdown.
///
/// Safe pre-publication failures and definitive API rejections are retried at
/// the bounded observation cadence. Every retry constructs a fresh proof
/// attempt. Any nonempty carrier, successful publication, or ambiguous write is
/// terminal for this process incarnation and waits for shutdown without a
/// second write attempt.
pub async fn supervise(
    plan: CatalogCandidateObservationPlan,
    state: OrchState,
    mut shutdown: watch::Receiver<bool>,
    request_timeout: Duration,
    retry_period: Duration,
    evidence_timeout: Duration,
) {
    let candidates = match AuthoritativeCandidateReader::new(
        plan.clone(),
        request_timeout,
        evidence_timeout,
    ) {
        Ok(reader) => reader,
        Err(error) => {
            tracing::error!(reason = %error, "catalog-activation publisher configuration is invalid");
            wait_until_shutdown(&mut shutdown).await;
            return;
        }
    };
    let challenge = match CatalogActivationChallengeClient::new(
        &plan.cluster_id,
        &plan.namespace,
        evidence_timeout,
    ) {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(reason = %error, "catalog-activation challenge configuration is unavailable");
            wait_until_shutdown(&mut shutdown).await;
            return;
        }
    };

    let mut logged_authority_wait = false;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let retry_delay = match Box::pin(attempt(
            &candidates,
            &challenge,
            &state,
            &mut shutdown,
            request_timeout,
            evidence_timeout,
        ))
        .await
        {
            AttemptDisposition::Stopped => return,
            AttemptDisposition::Retry(error) => {
                if let Some(rejection) = error.definitive_publication_rejection() {
                    tracing::warn!(
                        http_status = rejection.status_code(),
                        "catalog-activation publication was definitively rejected; retrying from fresh proof"
                    );
                } else if error.is_authority_unavailable() {
                    if logged_authority_wait {
                        tracing::debug!(reason = %error, "catalog-activation authority overlap remains unavailable");
                    } else {
                        tracing::warn!(reason = %error, "catalog-activation authority overlap is unavailable; polling for fresh proof overlap");
                        logged_authority_wait = true;
                    }
                } else {
                    tracing::warn!(reason = %error, "catalog-activation publication preconditions unavailable");
                }
                error.retry_delay(retry_period)
            }
            AttemptDisposition::Terminal(outcome) => {
                match outcome {
                    CatalogActivationPublicationOutcome::Installed => tracing::info!(
                        "catalog-activation request publication is installed; no further writes will be attempted"
                    ),
                    CatalogActivationPublicationOutcome::ForeignPublication => tracing::error!(
                        "catalog-activation carrier is nonempty or foreign; no write will be attempted"
                    ),
                    CatalogActivationPublicationOutcome::DefinitiveRejection(_) => unreachable!(
                        "definitive publication rejections are retryable attempt results"
                    ),
                    CatalogActivationPublicationOutcome::Indeterminate => tracing::error!(
                        "catalog-activation publication outcome is indeterminate; no retry will be attempted"
                    ),
                }
                wait_until_shutdown(&mut shutdown).await;
                return;
            }
        };
        if wait_or_stop(&mut shutdown, retry_delay).await {
            return;
        }
    }
}

async fn attempt(
    candidate_reader: &AuthoritativeCandidateReader,
    challenge: &CatalogActivationChallengeClient,
    state: &OrchState,
    shutdown: &mut watch::Receiver<bool>,
    request_timeout: Duration,
    evidence_timeout: Duration,
) -> AttemptDisposition {
    let mut attempt = RuntimeAttempt {
        candidate_reader,
        challenge,
        state,
        request_timeout,
        evidence_timeout,
        dispatch: None,
        target: None,
        candidates: None,
        live: None,
        prepared: None,
        publisher: None,
        pending: None,
        fresh_dispatch: None,
        fresh_target: None,
    };
    Box::pin(run_attempt(&mut attempt, shutdown)).await
}

trait CatalogActivationAttemptDriver {
    fn acquire_initial_authority(&mut self) -> Result<(), AttemptDisposition>;
    async fn read_candidates(&mut self) -> Result<(), AttemptDisposition>;
    async fn read_live_objects(&mut self) -> Result<(), AttemptDisposition>;
    fn prepare_request(&mut self) -> Result<(), AttemptDisposition>;
    async fn preflight_publication(&mut self) -> Result<(), AttemptDisposition>;
    async fn challenge(&mut self) -> Result<(), AttemptDisposition>;
    fn acquire_fresh_authority(&mut self) -> Result<(), AttemptDisposition>;
    fn rebind_fresh_evidence(&mut self) -> Result<(), AttemptDisposition>;
    fn final_revalidate(&mut self) -> Result<(), AttemptDisposition>;
    async fn publish(&mut self) -> CatalogActivationPublicationOutcome;
}

struct RuntimeAttempt<'a> {
    candidate_reader: &'a AuthoritativeCandidateReader,
    challenge: &'a CatalogActivationChallengeClient,
    state: &'a OrchState,
    request_timeout: Duration,
    evidence_timeout: Duration,
    dispatch: Option<CatalogBootstrapDispatch>,
    target: Option<CatalogActivationPublicationTarget>,
    candidates: Option<BoundCandidateSet>,
    live: Option<CatalogActivationLiveObjectProofs>,
    prepared: Option<PreparedCatalogActivationRequest>,
    publisher: Option<CatalogActivationPublisher>,
    pending: Option<PendingCatalogActivationPublication>,
    fresh_dispatch: Option<CatalogBootstrapDispatch>,
    fresh_target: Option<CatalogActivationPublicationTarget>,
}

impl CatalogActivationAttemptDriver for RuntimeAttempt<'_> {
    fn acquire_initial_authority(&mut self) -> Result<(), AttemptDisposition> {
        let capability = self
            .state
            .catalog_materialization_capability()
            .ok_or_else(|| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::AuthorityUnavailable)
            })?;
        let dispatch = self
            .state
            .catalog_bootstrap_dispatch(capability)
            .ok_or_else(|| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::AuthorityUnavailable)
            })?;
        let target = catalog_activation_publication_target(&dispatch).ok_or_else(|| {
            AttemptDisposition::Terminal(CatalogActivationPublicationOutcome::ForeignPublication)
        })?;
        self.dispatch = Some(dispatch);
        self.target = Some(target);
        Ok(())
    }

    async fn read_candidates(&mut self) -> Result<(), AttemptDisposition> {
        self.candidates = Some(self.candidate_reader.read().await.map_err(|error| {
            AttemptDisposition::Retry(CatalogActivationAttemptError::Candidates(error))
        })?);
        Ok(())
    }

    async fn read_live_objects(&mut self) -> Result<(), AttemptDisposition> {
        let target = self.target.as_ref().ok_or_else(missing_attempt_state)?;
        let dispatch = self.dispatch.as_ref().ok_or_else(missing_attempt_state)?;
        let candidates = self.candidates.as_ref().ok_or_else(missing_attempt_state)?;
        let reader = AuthoritativeCatalogActivationLiveObjectReader::new(
            target,
            self.request_timeout,
            self.evidence_timeout,
        )
        .map_err(classify_live_error)?;
        self.live = Some(
            reader
                .read(dispatch, candidates)
                .await
                .map_err(classify_live_error)?,
        );
        Ok(())
    }

    fn prepare_request(&mut self) -> Result<(), AttemptDisposition> {
        let dispatch = self.dispatch.as_ref().ok_or_else(missing_attempt_state)?;
        let live = self.live.as_ref().ok_or_else(missing_attempt_state)?;
        self.prepared = Some(prepare_catalog_activation_request(dispatch, live).map_err(
            |error| AttemptDisposition::Retry(CatalogActivationAttemptError::Preparation(error)),
        )?);
        self.publisher = Some(
            CatalogActivationPublisher::new(
                self.target.as_ref().ok_or_else(missing_attempt_state)?,
            )
            .map_err(classify_publisher_error)?,
        );
        Ok(())
    }

    async fn preflight_publication(&mut self) -> Result<(), AttemptDisposition> {
        let publisher = self.publisher.as_ref().ok_or_else(missing_attempt_state)?;
        let live = self.live.as_ref().ok_or_else(missing_attempt_state)?;
        let prepared = self.prepared.as_ref().ok_or_else(missing_attempt_state)?;
        self.pending = Some(
            publisher
                .prepare(live.carrier_resource_version(), prepared)
                .await
                .map_err(classify_publisher_error)?,
        );
        Ok(())
    }

    async fn challenge(&mut self) -> Result<(), AttemptDisposition> {
        let target = self.target.as_ref().ok_or_else(missing_attempt_state)?;
        let prepared = self.prepared.as_ref().ok_or_else(missing_attempt_state)?;
        self.challenge
            .challenge(target.target_agent_dns_name(), prepared)
            .await
            .map_err(|error| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::Challenge(error))
            })?;
        Ok(())
    }

    fn acquire_fresh_authority(&mut self) -> Result<(), AttemptDisposition> {
        // Network challenge success grants no authority. Issue a new move-only
        // capability and seal a new dispatch before reusing any observation.
        let capability = self
            .state
            .catalog_materialization_capability()
            .ok_or_else(|| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::AuthorityUnavailable)
            })?;
        let dispatch = self
            .state
            .catalog_bootstrap_dispatch(capability)
            .ok_or_else(|| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::AuthorityUnavailable)
            })?;
        let target = catalog_activation_publication_target(&dispatch).ok_or_else(|| {
            AttemptDisposition::Retry(CatalogActivationAttemptError::EvidenceChanged)
        })?;
        self.fresh_dispatch = Some(dispatch);
        self.fresh_target = Some(target);
        Ok(())
    }

    fn rebind_fresh_evidence(&mut self) -> Result<(), AttemptDisposition> {
        let dispatch = self
            .fresh_dispatch
            .as_ref()
            .ok_or_else(missing_attempt_state)?;
        let candidates = self.candidates.as_ref().ok_or_else(missing_attempt_state)?;
        let live = self.live.as_ref().ok_or_else(missing_attempt_state)?;
        let fresh_live = rebind_catalog_activation_live_objects(dispatch, candidates, live)
            .ok_or_else(|| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::EvidenceChanged)
            })?;
        let fresh_prepared =
            prepare_catalog_activation_request(dispatch, &fresh_live).map_err(|error| {
                AttemptDisposition::Retry(CatalogActivationAttemptError::Preparation(error))
            })?;
        if self.fresh_target.as_ref() != self.target.as_ref()
            || Some(&fresh_prepared) != self.prepared.as_ref()
        {
            return Err(AttemptDisposition::Retry(
                CatalogActivationAttemptError::EvidenceChanged,
            ));
        }
        Ok(())
    }

    fn final_revalidate(&mut self) -> Result<(), AttemptDisposition> {
        let dispatch = self
            .fresh_dispatch
            .as_ref()
            .ok_or_else(missing_attempt_state)?;
        if !self.state.revalidate_catalog_bootstrap_dispatch(dispatch) {
            return Err(AttemptDisposition::Retry(
                CatalogActivationAttemptError::AuthorityUnavailable,
            ));
        }
        Ok(())
    }

    async fn publish(&mut self) -> CatalogActivationPublicationOutcome {
        let publisher = self
            .publisher
            .take()
            .expect("publication sequencing retains its publisher");
        let pending = self
            .pending
            .take()
            .expect("publication sequencing retains its pending replacement");
        publisher.publish(pending).await
    }
}

fn missing_attempt_state() -> AttemptDisposition {
    AttemptDisposition::Retry(CatalogActivationAttemptError::InternalState)
}

async fn run_attempt<D: CatalogActivationAttemptDriver>(
    attempt: &mut D,
    shutdown: &mut watch::Receiver<bool>,
) -> AttemptDisposition {
    if let Err(disposition) = run_prewrite_sync(shutdown, || attempt.acquire_initial_authority()) {
        return disposition;
    }
    if let Err(disposition) = run_prewrite(shutdown, attempt.read_candidates()).await {
        return disposition;
    }
    if let Err(disposition) = run_prewrite(shutdown, attempt.read_live_objects()).await {
        return disposition;
    }
    if let Err(disposition) = run_prewrite_sync(shutdown, || attempt.prepare_request()) {
        return disposition;
    }
    if let Err(disposition) = run_prewrite(shutdown, attempt.preflight_publication()).await {
        return disposition;
    }
    if let Err(disposition) = run_prewrite(shutdown, attempt.challenge()).await {
        return disposition;
    }
    if let Err(disposition) = run_prewrite_sync(shutdown, || attempt.acquire_fresh_authority()) {
        return disposition;
    }
    if let Err(disposition) = run_prewrite_sync(shutdown, || attempt.rebind_fresh_evidence()) {
        return disposition;
    }
    if let Err(disposition) = run_prewrite_sync(shutdown, || attempt.final_revalidate()) {
        return disposition;
    }

    // The final synchronous gate above is followed by one last direct watch
    // check. Once `publish` is polled, its one PUT may already be in flight, so
    // it must be drained rather than cancelled. The publisher owns an absolute
    // eight-second bound, below the process ten-second shutdown grace.
    if shutdown_requested(shutdown) {
        return AttemptDisposition::Stopped;
    }
    match attempt.publish().await {
        CatalogActivationPublicationOutcome::DefinitiveRejection(rejection) => {
            AttemptDisposition::Retry(CatalogActivationAttemptError::DefinitiveRejection(
                rejection,
            ))
        }
        outcome => AttemptDisposition::Terminal(outcome),
    }
}

fn run_prewrite_sync(
    shutdown: &mut watch::Receiver<bool>,
    operation: impl FnOnce() -> Result<(), AttemptDisposition>,
) -> Result<(), AttemptDisposition> {
    if shutdown_requested(shutdown) {
        return Err(AttemptDisposition::Stopped);
    }
    let result = operation();
    if shutdown_requested(shutdown) {
        Err(AttemptDisposition::Stopped)
    } else {
        result
    }
}

async fn run_prewrite<F>(
    shutdown: &mut watch::Receiver<bool>,
    operation: F,
) -> Result<(), AttemptDisposition>
where
    F: Future<Output = Result<(), AttemptDisposition>>,
{
    if shutdown_requested(shutdown) {
        return Err(AttemptDisposition::Stopped);
    }
    tokio::select! {
        biased;
        () = wait_for_shutdown_request(shutdown) => Err(AttemptDisposition::Stopped),
        result = operation => {
            if shutdown_requested(shutdown) {
                Err(AttemptDisposition::Stopped)
            } else {
                result
            }
        }
    }
}

fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    match shutdown.has_changed() {
        Ok(true) => *shutdown.borrow_and_update(),
        Ok(false) => false,
        Err(_) => true,
    }
}

async fn wait_for_shutdown_request(shutdown: &mut watch::Receiver<bool>) {
    if shutdown_requested(shutdown) {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() || *shutdown.borrow() {
            return;
        }
    }
}

fn classify_live_error(error: CatalogActivationLiveObjectError) -> AttemptDisposition {
    if matches!(
        error,
        CatalogActivationLiveObjectError::InvalidCarrier
            | CatalogActivationLiveObjectError::InvalidObjectMetadata
    ) {
        AttemptDisposition::Terminal(CatalogActivationPublicationOutcome::ForeignPublication)
    } else {
        AttemptDisposition::Retry(CatalogActivationAttemptError::LiveObjects(error))
    }
}

fn classify_publisher_error(error: CatalogActivationPublisherError) -> AttemptDisposition {
    if matches!(
        error,
        CatalogActivationPublisherError::InvalidTarget
            | CatalogActivationPublisherError::InvalidResourceVersion
            | CatalogActivationPublisherError::InvalidCarrierIdentity
            | CatalogActivationPublisherError::InvalidCarrierBody
            | CatalogActivationPublisherError::CarrierNotEmpty
    ) {
        AttemptDisposition::Terminal(CatalogActivationPublicationOutcome::ForeignPublication)
    } else {
        AttemptDisposition::Retry(CatalogActivationAttemptError::Publisher(error))
    }
}

enum AttemptDisposition {
    Stopped,
    Retry(CatalogActivationAttemptError),
    Terminal(CatalogActivationPublicationOutcome),
}

#[derive(Debug, Error)]
enum CatalogActivationAttemptError {
    #[error("catalog-activation attempt sequencing state is incomplete")]
    InternalState,
    #[error("catalog materialization authority is unavailable")]
    AuthorityUnavailable,
    #[error("catalog-activation evidence changed after the challenge")]
    EvidenceChanged,
    #[error("authoritative candidate read failed: {0}")]
    Candidates(#[source] CatalogCandidateError),
    #[error("authoritative live-object read failed: {0}")]
    LiveObjects(#[source] CatalogActivationLiveObjectError),
    #[error("catalog-activation request preparation failed: {0}")]
    Preparation(#[source] CatalogActivationPreparationError),
    #[error("catalog-activation challenge failed: {0}")]
    Challenge(#[source] CatalogActivationChallengeError),
    #[error("catalog-activation publication preflight failed: {0}")]
    Publisher(#[source] CatalogActivationPublisherError),
    #[error("catalog-activation publication was definitively rejected")]
    DefinitiveRejection(DefinitiveCatalogActivationRejection),
}

impl CatalogActivationAttemptError {
    fn is_authority_unavailable(&self) -> bool {
        matches!(self, Self::AuthorityUnavailable)
    }

    fn retry_delay(&self, configured_retry_period: Duration) -> Duration {
        if self.is_authority_unavailable() {
            configured_retry_period.min(AUTHORITY_POLL_PERIOD)
        } else {
            configured_retry_period
        }
    }

    fn definitive_publication_rejection(&self) -> Option<&DefinitiveCatalogActivationRejection> {
        match self {
            Self::DefinitiveRejection(rejection) => Some(rejection),
            _ => None,
        }
    }
}

async fn wait_or_stop(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        biased;
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        () = tokio::time::sleep(duration) => *shutdown.borrow(),
    }
}

async fn wait_until_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;
    use tokio::time::Instant;

    use crate::catalog_activation_publisher::PUBLICATION_ATTEMPT_TIMEOUT;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Event {
        InitialAuthority,
        Candidates,
        LiveObjects,
        Prepare,
        Preflight,
        Challenge,
        FreshAuthority,
        RebindEvidence,
        FinalRevalidate,
        Put,
    }

    const COMPLETE_ORDER: [Event; 10] = [
        Event::InitialAuthority,
        Event::Candidates,
        Event::LiveObjects,
        Event::Prepare,
        Event::Preflight,
        Event::Challenge,
        Event::FreshAuthority,
        Event::RebindEvidence,
        Event::FinalRevalidate,
        Event::Put,
    ];

    #[test]
    fn authority_wait_breaks_collector_cadence_lock_without_accelerating_other_failures() {
        let configured = Duration::from_secs(2);
        assert_eq!(
            CatalogActivationAttemptError::AuthorityUnavailable.retry_delay(configured),
            AUTHORITY_POLL_PERIOD
        );
        assert_eq!(
            CatalogActivationAttemptError::EvidenceChanged.retry_delay(configured),
            configured
        );
        let already_faster = Duration::from_millis(50);
        assert_eq!(
            CatalogActivationAttemptError::AuthorityUnavailable.retry_delay(already_faster),
            already_faster
        );
    }

    struct FakeAttempt {
        events: Arc<Mutex<Vec<Event>>>,
        fail_at: Option<Event>,
        pause_at: Option<Event>,
        paused: Arc<Notify>,
        shutdown_at: Option<Event>,
        shutdown: Option<watch::Sender<bool>>,
        publication_outcome: CatalogActivationPublicationOutcome,
        publication_duration: Duration,
    }

    impl FakeAttempt {
        fn new(outcome: CatalogActivationPublicationOutcome) -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                fail_at: None,
                pause_at: None,
                paused: Arc::new(Notify::new()),
                shutdown_at: None,
                shutdown: None,
                publication_outcome: outcome,
                publication_duration: Duration::ZERO,
            }
        }

        fn record(&self, event: Event) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
            if self.shutdown_at == Some(event) {
                self.shutdown
                    .as_ref()
                    .expect("shutdown injection has a sender")
                    .send(true)
                    .expect("request shutdown");
            }
        }

        fn recorded(&self) -> Vec<Event> {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn synchronous_stage(&self, event: Event) -> Result<(), AttemptDisposition> {
            self.record(event);
            if self.fail_at != Some(event) {
                return Ok(());
            }
            let error = match event {
                Event::RebindEvidence => CatalogActivationAttemptError::EvidenceChanged,
                _ => CatalogActivationAttemptError::AuthorityUnavailable,
            };
            Err(AttemptDisposition::Retry(error))
        }

        async fn asynchronous_stage(&self, event: Event) -> Result<(), AttemptDisposition> {
            self.record(event);
            if self.pause_at == Some(event) {
                self.paused.notify_one();
                pending::<()>().await;
            }
            Ok(())
        }
    }

    impl CatalogActivationAttemptDriver for FakeAttempt {
        fn acquire_initial_authority(&mut self) -> Result<(), AttemptDisposition> {
            self.synchronous_stage(Event::InitialAuthority)
        }

        async fn read_candidates(&mut self) -> Result<(), AttemptDisposition> {
            self.asynchronous_stage(Event::Candidates).await
        }

        async fn read_live_objects(&mut self) -> Result<(), AttemptDisposition> {
            self.asynchronous_stage(Event::LiveObjects).await
        }

        fn prepare_request(&mut self) -> Result<(), AttemptDisposition> {
            self.synchronous_stage(Event::Prepare)
        }

        async fn preflight_publication(&mut self) -> Result<(), AttemptDisposition> {
            self.asynchronous_stage(Event::Preflight).await
        }

        async fn challenge(&mut self) -> Result<(), AttemptDisposition> {
            self.asynchronous_stage(Event::Challenge).await
        }

        fn acquire_fresh_authority(&mut self) -> Result<(), AttemptDisposition> {
            self.synchronous_stage(Event::FreshAuthority)
        }

        fn rebind_fresh_evidence(&mut self) -> Result<(), AttemptDisposition> {
            self.synchronous_stage(Event::RebindEvidence)
        }

        fn final_revalidate(&mut self) -> Result<(), AttemptDisposition> {
            self.synchronous_stage(Event::FinalRevalidate)
        }

        async fn publish(&mut self) -> CatalogActivationPublicationOutcome {
            self.record(Event::Put);
            self.paused.notify_one();
            tokio::time::sleep(self.publication_duration).await;
            self.publication_outcome.clone()
        }
    }

    #[tokio::test]
    async fn exact_stage_order_reaches_each_terminal_outcome_with_one_put() {
        for outcome in [
            CatalogActivationPublicationOutcome::Installed,
            CatalogActivationPublicationOutcome::ForeignPublication,
            CatalogActivationPublicationOutcome::Indeterminate,
        ] {
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let mut attempt = FakeAttempt::new(outcome.clone());
            let disposition = run_attempt(&mut attempt, &mut shutdown_rx).await;
            assert!(matches!(
                disposition,
                AttemptDisposition::Terminal(actual) if actual == outcome
            ));
            assert_eq!(attempt.recorded(), COMPLETE_ORDER);
            assert_eq!(
                attempt
                    .recorded()
                    .iter()
                    .filter(|event| **event == Event::Put)
                    .count(),
                1
            );
            drop(shutdown_tx);
        }
    }

    #[tokio::test]
    async fn definitive_rejection_retries_only_through_a_fresh_full_attempt() {
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let rejection = DefinitiveCatalogActivationRejection::UnprocessableEntity;
        let mut rejected_attempt = FakeAttempt::new(
            CatalogActivationPublicationOutcome::DefinitiveRejection(rejection),
        );
        let disposition = run_attempt(&mut rejected_attempt, &mut shutdown_rx).await;
        assert!(matches!(
            disposition,
            AttemptDisposition::Retry(CatalogActivationAttemptError::DefinitiveRejection(actual))
                if actual == rejection
        ));
        assert_eq!(rejected_attempt.recorded(), COMPLETE_ORDER);

        let mut fresh_attempt = FakeAttempt::new(CatalogActivationPublicationOutcome::Installed);
        assert!(matches!(
            run_attempt(&mut fresh_attempt, &mut shutdown_rx).await,
            AttemptDisposition::Terminal(CatalogActivationPublicationOutcome::Installed)
        ));
        assert_eq!(fresh_attempt.recorded(), COMPLETE_ORDER);
        for attempt in [&rejected_attempt, &fresh_attempt] {
            assert_eq!(
                attempt
                    .recorded()
                    .iter()
                    .filter(|event| **event == Event::Put)
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn post_challenge_authority_or_full_evidence_drift_cannot_put() {
        for failure in [Event::FreshAuthority, Event::RebindEvidence] {
            let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let mut attempt = FakeAttempt::new(CatalogActivationPublicationOutcome::Installed);
            attempt.fail_at = Some(failure);
            let disposition = run_attempt(&mut attempt, &mut shutdown_rx).await;
            assert!(matches!(disposition, AttemptDisposition::Retry(_)));
            let stop = COMPLETE_ORDER
                .iter()
                .position(|event| *event == failure)
                .expect("failure stage belongs to the exact order");
            assert_eq!(attempt.recorded(), COMPLETE_ORDER[..=stop]);
            assert!(!attempt.recorded().contains(&Event::Put));
        }
    }

    #[tokio::test]
    async fn failed_final_revalidation_cannot_put() {
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut attempt = FakeAttempt::new(CatalogActivationPublicationOutcome::Installed);
        attempt.fail_at = Some(Event::FinalRevalidate);
        let disposition = run_attempt(&mut attempt, &mut shutdown_rx).await;
        assert!(matches!(
            disposition,
            AttemptDisposition::Retry(CatalogActivationAttemptError::AuthorityUnavailable)
        ));
        assert_eq!(attempt.recorded(), COMPLETE_ORDER[..9]);
    }

    #[tokio::test]
    async fn shutdown_cancels_every_awaited_prewrite_stage_without_a_put() {
        for paused_stage in [
            Event::Candidates,
            Event::LiveObjects,
            Event::Preflight,
            Event::Challenge,
        ] {
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let mut attempt = FakeAttempt::new(CatalogActivationPublicationOutcome::Installed);
            attempt.pause_at = Some(paused_stage);
            let paused = Arc::clone(&attempt.paused);
            let mut run = Box::pin(run_attempt(&mut attempt, &mut shutdown_rx));
            tokio::select! {
                biased;
                () = paused.notified() => {}
                disposition = &mut run => panic!("attempt stopped before {paused_stage:?}: {}", disposition_name(&disposition)),
            }
            shutdown_tx.send(true).expect("request shutdown");
            assert!(matches!(run.as_mut().await, AttemptDisposition::Stopped));
            drop(run);
            assert!(!attempt.recorded().contains(&Event::Put));
        }
    }

    #[tokio::test]
    async fn shutdown_observed_after_final_revalidation_but_before_put_refuses_write() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut attempt = FakeAttempt::new(CatalogActivationPublicationOutcome::Installed);
        attempt.shutdown_at = Some(Event::FinalRevalidate);
        attempt.shutdown = Some(shutdown_tx);
        assert!(matches!(
            run_attempt(&mut attempt, &mut shutdown_rx).await,
            AttemptDisposition::Stopped
        ));
        assert_eq!(attempt.recorded(), COMPLETE_ORDER[..9]);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_after_put_dispatch_drains_the_bounded_publication() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut attempt = FakeAttempt::new(CatalogActivationPublicationOutcome::Installed);
        attempt.publication_duration = PUBLICATION_ATTEMPT_TIMEOUT;
        let put_started = Arc::clone(&attempt.paused);
        let started = Instant::now();
        let mut run = Box::pin(run_attempt(&mut attempt, &mut shutdown_rx));
        tokio::select! {
            biased;
            () = put_started.notified() => {}
            disposition = &mut run => panic!("publication finished before PUT observation: {}", disposition_name(&disposition)),
        }
        shutdown_tx.send(true).expect("request shutdown");

        tokio::time::advance(
            PUBLICATION_ATTEMPT_TIMEOUT
                .checked_sub(Duration::from_millis(1))
                .expect("publication bound exceeds one millisecond"),
        )
        .await;
        tokio::select! {
            biased;
            disposition = &mut run => panic!("shutdown cancelled or shortened the PUT: {}", disposition_name(&disposition)),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            run.as_mut().await,
            AttemptDisposition::Terminal(CatalogActivationPublicationOutcome::Installed)
        ));
        drop(run);
        assert_eq!(started.elapsed(), PUBLICATION_ATTEMPT_TIMEOUT);
        assert!(PUBLICATION_ATTEMPT_TIMEOUT < Duration::from_secs(10));
        assert_eq!(attempt.recorded(), COMPLETE_ORDER);
    }

    fn disposition_name(disposition: &AttemptDisposition) -> &'static str {
        match disposition {
            AttemptDisposition::Stopped => "stopped",
            AttemptDisposition::Retry(_) => "retry",
            AttemptDisposition::Terminal(_) => "terminal",
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_wait_stops_immediately_on_shutdown() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let wait = wait_or_stop(&mut shutdown_rx, Duration::from_secs(30));
        tokio::pin!(wait);
        shutdown_tx.send(true).expect("request shutdown");
        assert!(wait.await);
    }

    #[tokio::test]
    async fn terminal_wait_observes_an_already_requested_shutdown() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("request shutdown");
        wait_until_shutdown(&mut shutdown_rx).await;
    }
}
