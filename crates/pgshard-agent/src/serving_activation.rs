//! Installs one sealed serving access policy and proves the postmaster loaded
//! it, or fences the postmaster that may hold it unproved.
//!
//! Every postmaster incarnation starts under the non-serving policy the agent
//! materializes before each spawn. This module performs the only transition out
//! of that state, and it is deliberately a reload rather than a restart: a
//! restart would lose the writable generation the agent fences against.
//!
//! Why the reload needs a proof at all. `process_pm_reload_request` in
//! `src/backend/postmaster/postmaster.c` calls `load_hba`, and `load_hba` in
//! `src/backend/libpq/hba.c` keeps the previously parsed list and returns false
//! when the new file does not parse — the postmaster logs "was not reloaded"
//! and carries on with the rules it already had. Nothing is reported to
//! whoever sent the signal, so a dispatched `SIGHUP` proves only that a signal
//! was delivered.
//!
//! Why the proof cannot be `pg_hba_file_rules`. That view reports the current
//! contents of the file rather than what the server last loaded, which its own
//! documentation states in `doc/src/sgml/system-views.sgml`. Reading it back
//! would prove the agent's own write, which is not in question. The only sound
//! proof is an authentication outcome the two policies disagree about, so what
//! this module classifies is an observed outcome and never a file.
//!
//! What happens when the proof does not arrive is the whole safety argument.
//! The sealed policy is on disk under a live postmaster, and any later reload
//! from any source would put it into effect without passing these checks, so
//! anything other than a proof fences that exact incarnation. Rolling the file
//! back instead was rejected: making a rollback take effect needs the same
//! reload this module has just failed to observe.

use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rustix::fs::{
    AtFlags, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, flock, fstat, mkdirat, open,
    openat, renameat_with, statat, unlinkat,
};
use rustix::io::Errno;
use rustix::process::{
    Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, geteuid, pidfd_open, pidfd_send_signal, waitid,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::writable::DurableWritableGeneration;

const STATE_FILE: &str = "state";
const STATE_STAGING_FILE: &str = ".state.tmp";
const STATE_SCHEMA_VERSION: &str = "pgshard.serving-activation-state.v1";
const FSYNC_PERSISTENCE: &str = "fsync";
const MAX_STATE_RECORD_BYTES: u64 = 8 * 1024;
/// Largest accepted serving policy. An access policy is a handful of lines, so
/// anything approaching this is a projection mistake rather than a policy.
const MAX_SERVING_POLICY_BYTES: usize = 64 * 1024;
const SERVING_POLICY_STAGING_FILE: &str = ".pg_hba.serving.staging";

/// The exact postmaster an activation is bound to.
///
/// A pid is not on its own an identity — pids are reused within a boot — so it
/// is paired with the boot it was observed in, and the dispatch goes through a
/// retained pidfd rather than the pid, which is what makes the signal
/// incarnation-exact regardless of reuse.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostmasterIncarnation {
    /// Canonical Linux boot identifier the pid was observed in.
    pub boot_id: String,
    /// Process identifier of the supervised postmaster.
    pub pid: u32,
}

/// A postmaster incarnation the caller still holds open.
///
/// Holding the pidfd is what makes the liveness answer and the signal refer to
/// the same process: a reused pid names a different process, but a pidfd names
/// the one it was opened for or nothing at all.
#[derive(Debug)]
pub struct RetainedPostmaster {
    pidfd: OwnedFd,
    incarnation: PostmasterIncarnation,
}

impl RetainedPostmaster {
    /// Adopts a pidfd the supervisor already holds for this incarnation.
    #[must_use]
    pub fn adopt(pidfd: OwnedFd, incarnation: PostmasterIncarnation) -> Self {
        Self { pidfd, incarnation }
    }

    /// Opens a pidfd for a child the caller supervises.
    ///
    /// # Errors
    ///
    /// Returns the raw `pidfd_open` failure, which is `ESRCH` for a process
    /// that has already been reaped.
    pub fn open(pid: u32, boot_id: String) -> Result<Self, Errno> {
        let raw = i32::try_from(pid).map_err(|_| Errno::SRCH)?;
        let handle = Pid::from_raw(raw).ok_or(Errno::SRCH)?;
        let pidfd = pidfd_open(handle, PidfdFlags::empty())?;
        Ok(Self {
            pidfd,
            incarnation: PostmasterIncarnation { boot_id, pid },
        })
    }

    /// The incarnation this handle names.
    #[must_use]
    pub fn incarnation(&self) -> &PostmasterIncarnation {
        &self.incarnation
    }

    /// Whether the supervised process has neither exited nor been reaped.
    ///
    /// A zombie is not live, which is why this asks `waitid` rather than
    /// sending signal zero: an exited-but-unreaped process still accepts
    /// signals. Any failure answers "not live", because the only failure that
    /// reaches here is a violated precondition — the process is not this
    /// caller's child — and a liveness check must not fail open.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(
            waitid(
                WaitId::PidFd(self.pidfd.as_fd()),
                WaitIdOptions::EXITED | WaitIdOptions::NOWAIT | WaitIdOptions::NOHANG,
            ),
            Ok(None)
        )
    }

    fn dispatch_reload(&self) -> Result<(), Errno> {
        pidfd_send_signal(&self.pidfd, Signal::HUP)
    }
}

/// A serving access policy whose bytes match the digest the request sealed.
///
/// Constructing one is the only way to name the bytes this module installs, so
/// unverified bytes cannot reach the installer by any path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedServingPolicy {
    bytes: Vec<u8>,
    sha256: String,
}

impl SealedServingPolicy {
    /// Seals candidate bytes against the digests the request declared.
    ///
    /// The non-serving digest is required rather than optional: a serving
    /// policy equal to the one every incarnation already starts under would
    /// make activation a transition that changes nothing while still consuming
    /// the proof, and no probe afterwards could tell the two apart.
    ///
    /// # Errors
    ///
    /// Returns a typed refusal for an empty, oversized, mismatched, or
    /// indistinct policy.
    pub fn seal(
        bytes: Vec<u8>,
        declared_sha256: &str,
        non_serving_sha256: &str,
    ) -> Result<Self, ServingActivationError> {
        if bytes.is_empty() || bytes.len() > MAX_SERVING_POLICY_BYTES {
            return Err(ServingActivationError::InvalidPolicySize {
                bytes: bytes.len(),
                maximum: MAX_SERVING_POLICY_BYTES,
            });
        }
        let sha256 = sha256_hex(&bytes);
        if sha256 != declared_sha256 {
            return Err(ServingActivationError::PolicyDigestMismatch);
        }
        if sha256 == non_serving_sha256 {
            return Err(ServingActivationError::IndistinctPolicy);
        }
        Ok(Self { bytes, sha256 })
    }

    /// The sealed digest, which is what the journal records.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// The exact bytes the postmaster is required to load.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Everything one activation attempt is bound to.
#[derive(Clone, Debug)]
pub struct BoundServingAttempt {
    /// The incarnation whose reload this attempt authorizes.
    pub incarnation: PostmasterIncarnation,
    /// The writable generation that must still be current at dispatch.
    pub generation: DurableWritableGeneration,
}

/// The facts a reload rests on, re-derived at the moment of dispatch.
///
/// Every method is synchronous and must stay that way. A capability that was
/// current when an `async fn` last yielded is not evidence about now, and every
/// stage upstream withdraws asynchronously, so observing a published proof is
/// not the same claim as holding the capability behind it.
pub trait ServingReloadAuthority {
    /// Whether the materialization proof this attempt captured, and the runtime
    /// capability that proof rests on, are both still the current ones.
    fn materialization_is_current(&self) -> bool;
    /// The writable generation currently held, if one is held at all.
    fn writable_generation(&self) -> Option<DurableWritableGeneration>;
    /// Whether the statement-admission fence is installed for that generation.
    fn target_fence_is_installed(&self) -> bool;
    /// The retained postmaster, absent once the supervisor has released it.
    fn postmaster(&self) -> Option<&RetainedPostmaster>;
}

/// What an out-of-band authentication probe observed after the reload.
///
/// Deliberately an observed authentication outcome and never a file: the file
/// is what this module just wrote, so reading it back proves nothing about what
/// the postmaster loaded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingReloadProbe {
    /// The discriminating attempt got the outcome only the sealed serving
    /// policy produces.
    ServingRulesInEffect,
    /// The attempt got the outcome only the non-serving policy produces, so the
    /// postmaster is still running the rules it started with.
    NonServingRulesStillInEffect,
    /// The attempt produced neither outcome, or could not be completed.
    Indeterminate,
}

/// Why an incarnation was fenced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceReason {
    /// Authority was lost after the sealed policy reached the disk.
    AuthorityLost,
    /// The signal could not be dispatched to a determinate conclusion.
    IndeterminateReload,
    /// The postmaster is still running the policy it started with, so the
    /// sealed policy is on disk and out of step with what is loaded.
    ReloadRefused,
    /// The probe could not tell which policy is in effect.
    UnprovedReload,
    /// The journal could not record the attempt's own progress.
    UnrecordedProgress,
}

impl FenceReason {
    const fn label(self) -> &'static str {
        match self {
            Self::AuthorityLost => "authority was lost",
            Self::IndeterminateReload => "the reload signal had no determinate outcome",
            Self::ReloadRefused => "the postmaster kept the policy it started with",
            Self::UnprovedReload => "the loaded policy could not be determined",
            Self::UnrecordedProgress => "the journal could not record progress",
        }
    }
}

/// A postmaster that may hold the sealed serving policy without a proof.
///
/// Move-only and `#[must_use]`: the only correct handling is to stop that exact
/// incarnation, and dropping it silently would leave it running.
#[derive(Debug)]
#[must_use = "a fenced postmaster must be stopped, not dropped"]
pub struct FencedPostmaster {
    incarnation: PostmasterIncarnation,
    reason: FenceReason,
}

impl FencedPostmaster {
    /// The exact incarnation that must be stopped.
    #[must_use]
    pub fn incarnation(&self) -> &PostmasterIncarnation {
        &self.incarnation
    }

    /// Why it was fenced.
    #[must_use]
    pub fn reason(&self) -> FenceReason {
        self.reason
    }

    /// Builds a fence without running an attempt, so the stage that consumes
    /// one can be exercised on its own.
    #[cfg(test)]
    pub(crate) fn for_test(incarnation: PostmasterIncarnation, reason: FenceReason) -> Self {
        Self {
            incarnation,
            reason,
        }
    }
}

/// Proof that one incarnation loaded the sealed serving policy.
#[derive(Debug)]
#[must_use = "a serving proof is the only thing that admits application traffic"]
pub struct ServingProof {
    incarnation: PostmasterIncarnation,
    policy_sha256: String,
}

impl ServingProof {
    /// The incarnation the proof was established against.
    #[must_use]
    pub fn incarnation(&self) -> &PostmasterIncarnation {
        &self.incarnation
    }

    /// The sealed digest that incarnation was proved to be running.
    #[must_use]
    pub fn policy_sha256(&self) -> &str {
        &self.policy_sha256
    }

    /// Builds a proof without running an attempt, so the stage that consumes
    /// one can be exercised on its own.
    #[cfg(test)]
    pub(crate) fn for_test(incarnation: PostmasterIncarnation, policy_sha256: String) -> Self {
        Self {
            incarnation,
            policy_sha256,
        }
    }
}

/// How one activation attempt ended.
///
/// There is no third arm on purpose. Once the sealed policy has reached the
/// disk the attempt has exactly two honest conclusions, and an error return
/// would let a caller treat a failure as "nothing happened".
#[derive(Debug)]
#[must_use = "an activation outcome decides whether the postmaster keeps running"]
pub enum ServingActivationOutcome {
    /// The reload was dispatched under authority and proved.
    Serving(ServingProof),
    /// The incarnation must be stopped.
    Fenced(FencedPostmaster),
}

/// What a durable record left by an earlier attempt means for this one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServingActivationRecovery {
    /// No attempt is on record.
    Fresh,
    /// An attempt reached the disk without proving a reload. The sealed policy
    /// may still be installed, so this never resumes: the non-serving policy is
    /// restored before the next spawn and activation starts from the beginning.
    Interrupted {
        /// The incarnation the interrupted attempt was bound to.
        incarnation: PostmasterIncarnation,
    },
    /// An attempt proved the reload for this exact incarnation. It is a proof
    /// about that incarnation only and never about a later one.
    Proved {
        /// The proved incarnation.
        incarnation: PostmasterIncarnation,
        /// The digest that incarnation was proved to be running.
        policy_sha256: String,
    },
    /// An attempt fenced its incarnation, which must not be resumed.
    Fenced {
        /// The fenced incarnation.
        incarnation: PostmasterIncarnation,
    },
}

impl ServingActivationRecovery {
    /// Whether a recorded state authorizes `current` to be serving now.
    ///
    /// Only an exact proof for this exact incarnation does. Every other state,
    /// and every state belonging to a different incarnation, does not — which
    /// is what stops a serving policy left behind by a crashed attempt from
    /// activating a postmaster that never passed these checks.
    #[must_use]
    pub fn admits_serving(&self, current: &PostmasterIncarnation) -> bool {
        matches!(self, Self::Proved { incarnation, .. } if incarnation == current)
    }
}

/// Fail-closed serving-activation failure.
#[derive(Debug, Error)]
pub enum ServingActivationError {
    /// The candidate policy was empty or larger than the accepted bound.
    #[error("serving policy is {bytes} bytes, outside the accepted bound of {maximum}")]
    InvalidPolicySize {
        /// Observed size.
        bytes: usize,
        /// Largest accepted size.
        maximum: usize,
    },
    /// The candidate policy did not match its sealed digest.
    #[error("serving policy does not match its sealed digest")]
    PolicyDigestMismatch,
    /// The serving policy is byte-identical to the non-serving one.
    #[error("the serving policy is indistinguishable from the non-serving policy")]
    IndistinctPolicy,
    /// A path was not absolute and normal.
    #[error("serving activation path is not an absolute normal path: {path}")]
    InvalidPath {
        /// The refused path.
        path: PathBuf,
    },
    /// An object had unsafe ownership, permissions, or type.
    #[error("unsafe serving activation object at {path}: {reason}")]
    UnsafeObject {
        /// The refused path.
        path: PathBuf,
        /// Why it was refused.
        reason: &'static str,
    },
    /// A journal record did not decode.
    #[error("corrupt serving activation record at {path}")]
    CorruptRecord {
        /// The refused path.
        path: PathBuf,
    },
    /// Another handle holds the journal.
    #[error("serving activation journal at {path} is held by another attempt")]
    Busy {
        /// The locked directory.
        path: PathBuf,
    },
    /// A transition was requested from a state that does not precede it.
    #[error("serving activation cannot move from {from} to {to}")]
    OutOfOrder {
        /// Recorded phase.
        from: &'static str,
        /// Requested phase.
        to: &'static str,
    },
    /// A transition was requested for an attempt the recorded state does not
    /// belong to.
    #[error("serving activation state belongs to a different attempt")]
    Conflict,
    /// The recorded state fenced this attempt, which cannot be resumed.
    #[error("serving activation was already fenced for this attempt")]
    AlreadyFenced,
    /// A filesystem operation failed.
    #[error("cannot {operation} at {path}")]
    Io {
        /// What was being attempted.
        operation: &'static str,
        /// The path involved.
        path: PathBuf,
        /// Underlying failure.
        #[source]
        source: std::io::Error,
    },
    /// The persistence clock was not usable.
    #[error("serving activation persistence clock is invalid")]
    InvalidPersistenceClock,
    /// Unit-test crash injection.
    #[cfg(test)]
    #[error("injected serving activation crash")]
    InjectedCrash,
}

/// Result of installing the sealed policy at the runtime path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingPolicyInstall {
    /// The exact sealed bytes were already installed, and the inode was left
    /// alone.
    AlreadyInstalled,
    /// This call installed them.
    Installed,
}

/// Installs the sealed policy at the runtime path the postmaster reads.
///
/// The write is staged in the same directory, flushed, sealed read-only and
/// renamed over the live path, and the directory is flushed after the rename: a
/// reload can arrive at any instant, so the postmaster must observe either the
/// whole previous policy or the whole sealed one, and a rename that has not
/// reached the directory is not a rename a crash preserves.
///
/// It is idempotent against the bytes it is installing now rather than against
/// a constant. Rewriting an already-correct policy would change its inode, and
/// the spawn path validates the prepared state a second time and compares
/// device and inode: a fresh inode there is indistinguishable from tampering.
///
/// # Errors
///
/// Returns a typed filesystem or validation failure. Nothing is left partially
/// installed: the live path holds either the previous policy or the sealed one.
pub fn install_serving_policy(
    path: &Path,
    expected_uid: u32,
    policy: &SealedServingPolicy,
) -> Result<ServingPolicyInstall, ServingActivationError> {
    if installed_exactly(path, expected_uid, policy)? {
        return Ok(ServingPolicyInstall::AlreadyInstalled);
    }
    let parent = path
        .parent()
        .ok_or_else(|| ServingActivationError::InvalidPath {
            path: path.to_owned(),
        })?;
    let name = path
        .file_name()
        .ok_or_else(|| ServingActivationError::InvalidPath {
            path: path.to_owned(),
        })?;
    let directory = open_directory(parent)?;
    // The policy directory is created by the spawn path, which allows group and
    // world read. Only writability is a policy question here.
    validate_directory(parent, &directory, expected_uid, 0o022)?;
    let staging = parent.join(SERVING_POLICY_STAGING_FILE);
    remove_if_present(&directory, SERVING_POLICY_STAGING_FILE, &staging)?;

    let descriptor = openat(
        &directory,
        SERVING_POLICY_STAGING_FILE,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::RUSR | Mode::WUSR,
    )
    .map_err(|source| io_error("create staged serving policy", &staging, source.into()))?;
    let mut file = File::from(descriptor);
    file.write_all(policy.bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| file.set_permissions(fs::Permissions::from_mode(0o400)))
        .map_err(|source| io_error("write staged serving policy", &staging, source))?;
    drop(file);
    #[cfg(test)]
    crash_checkpoint(CrashCheckpoint::PolicyStaged)?;

    renameat_with(
        &directory,
        SERVING_POLICY_STAGING_FILE,
        &directory,
        name,
        RenameFlags::empty(),
    )
    .map_err(|source| io_error("install staged serving policy", path, source.into()))?;
    #[cfg(test)]
    crash_checkpoint(CrashCheckpoint::PolicyRenamed)?;
    #[cfg(test)]
    tamper_with_the_installed_policy_if_requested(path);
    directory
        .sync_all()
        .map_err(|source| io_error("flush serving policy directory", parent, source))?;

    // The postmaster runs as the same identity as the agent, so "this process
    // just wrote it" is not the same claim as "this is what is there now".
    if !installed_exactly(path, expected_uid, policy)? {
        return Err(ServingActivationError::UnsafeObject {
            path: path.to_owned(),
            reason: "the installed serving policy did not survive its own write",
        });
    }
    Ok(ServingPolicyInstall::Installed)
}

/// Runs one activation attempt against one bound incarnation.
///
/// The order is the safety argument. Nothing is installed until the journal has
/// durably recorded that an attempt is starting, so no crash leaves the sealed
/// policy on disk with no record of it. The dispatch record is durable before
/// the signal, so no crash leaves a reload that happened behind a record saying
/// it did not. Every failure after the policy reaches the disk fences rather
/// than returning, because from that instant the sealed policy is on disk under
/// a live postmaster.
///
/// # Errors
///
/// Returns an error only for failures strictly before the sealed policy is
/// installed. After that point the result is an outcome, and one of its two
/// arms is a fence.
pub async fn activate<A, P, F>(
    journal: &mut ServingActivationJournal,
    hba_path: &Path,
    expected_uid: u32,
    policy: &SealedServingPolicy,
    authority: &A,
    bound: &BoundServingAttempt,
    probe: P,
) -> Result<ServingActivationOutcome, ServingActivationError>
where
    A: ServingReloadAuthority,
    P: FnOnce() -> F,
    F: Future<Output = ServingReloadProbe>,
{
    if let Some(proof) = journal.arm(policy, bound)? {
        return Ok(ServingActivationOutcome::Serving(proof));
    }
    install_serving_policy(hba_path, expected_uid, policy)?;

    // From here the sealed policy is on disk under a postmaster that has not
    // been proved to have loaded it, so every exit is an outcome.
    for phase in [Phase::Installed, Phase::Reloading] {
        if let Err(error) = journal.record(phase, policy, bound) {
            return Ok(journal.fence(bound, FenceReason::UnrecordedProgress, &error.to_string()));
        }
    }

    if let Err(reason) = authorize_and_dispatch_reload(authority, bound) {
        return Ok(journal.fence(bound, reason, reason.label()));
    }

    let observed = probe().await;
    match observed {
        ServingReloadProbe::ServingRulesInEffect => {}
        ServingReloadProbe::NonServingRulesStillInEffect => {
            return Ok(journal.fence(
                bound,
                FenceReason::ReloadRefused,
                FenceReason::ReloadRefused.label(),
            ));
        }
        ServingReloadProbe::Indeterminate => {
            return Ok(journal.fence(
                bound,
                FenceReason::UnprovedReload,
                FenceReason::UnprovedReload.label(),
            ));
        }
    }
    // The probe awaited, so the authority it ran under is evidence about then
    // and not about now. A postmaster that is serving without current authority
    // is exactly what must not keep running.
    if !holds_authority(authority, bound) {
        return Ok(journal.fence(
            bound,
            FenceReason::AuthorityLost,
            FenceReason::AuthorityLost.label(),
        ));
    }
    match journal.record(Phase::Serving, policy, bound) {
        Ok(()) => Ok(ServingActivationOutcome::Serving(ServingProof {
            incarnation: bound.incarnation.clone(),
            policy_sha256: policy.sha256().to_owned(),
        })),
        Err(error) => Ok(journal.fence(bound, FenceReason::UnrecordedProgress, &error.to_string())),
    }
}

/// Re-derives every fact the reload rests on and signals, with nothing awaited
/// in between.
///
/// This is synchronous for a reason a comment cannot enforce but a signature
/// can: there is no point inside it at which the task can be descheduled, so
/// the checks and the signal are one step as far as every asynchronous
/// withdrawal upstream is concerned.
///
/// The liveness check that precedes the signal is not what makes the dispatch
/// exact — a process can exit immediately after it. The pidfd is: a signal sent
/// through one reaches the incarnation it was opened for or nothing at all, so
/// a reused pid can never receive this reload.
fn authorize_and_dispatch_reload<A: ServingReloadAuthority>(
    authority: &A,
    bound: &BoundServingAttempt,
) -> Result<(), FenceReason> {
    if !authority.materialization_is_current() {
        return Err(FenceReason::AuthorityLost);
    }
    if authority.writable_generation().as_ref() != Some(&bound.generation) {
        return Err(FenceReason::AuthorityLost);
    }
    if !authority.target_fence_is_installed() {
        return Err(FenceReason::AuthorityLost);
    }
    let Some(postmaster) = authority.postmaster() else {
        return Err(FenceReason::AuthorityLost);
    };
    if postmaster.incarnation() != &bound.incarnation || !postmaster.is_live() {
        return Err(FenceReason::AuthorityLost);
    }
    match postmaster.dispatch_reload() {
        Ok(()) => Ok(()),
        // The process is gone, so nothing was reloaded. That much is
        // determinate, but the sealed policy is still on disk for whatever
        // starts next, and this attempt is not what clears it.
        Err(Errno::SRCH) => Err(FenceReason::AuthorityLost),
        Err(_) => Err(FenceReason::IndeterminateReload),
    }
}

fn holds_authority<A: ServingReloadAuthority>(authority: &A, bound: &BoundServingAttempt) -> bool {
    authority.materialization_is_current()
        && authority.writable_generation().as_ref() == Some(&bound.generation)
        && authority.target_fence_is_installed()
        && authority
            .postmaster()
            .is_some_and(|postmaster| postmaster.incarnation() == &bound.incarnation)
}

/// The durable state of serving activation for one journal directory.
///
/// Exactly one state is recorded at a time and it only moves forward, so a
/// crash at any instant leaves a state this module recognizes rather than a
/// partial set of records that has to be reconciled.
#[derive(Debug)]
pub struct ServingActivationJournal {
    directory: File,
    directory_path: PathBuf,
    expected_uid: u32,
}

impl ServingActivationJournal {
    /// Opens or creates a dedicated journal directory.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error for unsafe paths, ownership,
    /// permissions, contents, or filesystem failures.
    pub fn open_or_create(directory: impl AsRef<Path>) -> Result<Self, ServingActivationError> {
        Self::open_or_create_for_uid(directory.as_ref(), geteuid().as_raw())
    }

    /// Classifies whatever an earlier attempt left behind.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an unreadable or corrupt journal. A journal
    /// that cannot be read is never reported as `Fresh`.
    pub fn recover(&mut self) -> Result<ServingActivationRecovery, ServingActivationError> {
        self.with_exclusive_lock(|journal| {
            journal.validate_entries()?;
            let Some(record) = journal.read_state()? else {
                return Ok(ServingActivationRecovery::Fresh);
            };
            let incarnation = record.incarnation()?;
            Ok(match record.phase()? {
                Phase::Armed | Phase::Installed | Phase::Reloading => {
                    ServingActivationRecovery::Interrupted { incarnation }
                }
                Phase::Serving => ServingActivationRecovery::Proved {
                    incarnation,
                    policy_sha256: record.policy_sha256,
                },
                Phase::Fenced => ServingActivationRecovery::Fenced { incarnation },
            })
        })
    }

    fn open_or_create_for_uid(
        directory_path: &Path,
        expected_uid: u32,
    ) -> Result<Self, ServingActivationError> {
        validate_absolute_normal(directory_path)?;
        let parent =
            directory_path
                .parent()
                .ok_or_else(|| ServingActivationError::InvalidPath {
                    path: directory_path.to_owned(),
                })?;
        let name =
            directory_path
                .file_name()
                .ok_or_else(|| ServingActivationError::InvalidPath {
                    path: directory_path.to_owned(),
                })?;
        let parent_directory = open_directory(parent)?;
        let created = match mkdirat(
            &parent_directory,
            name,
            Mode::RUSR | Mode::WUSR | Mode::XUSR,
        ) {
            Ok(()) => true,
            Err(Errno::EXIST) => false,
            Err(source) => {
                return Err(io_error(
                    "create journal directory",
                    directory_path,
                    source.into(),
                ));
            }
        };
        let descriptor = openat(
            &parent_directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| io_error("open journal directory", directory_path, source.into()))?;
        let directory = File::from(descriptor);
        validate_directory(directory_path, &directory, expected_uid, 0o077)?;
        if created {
            parent_directory
                .sync_all()
                .map_err(|source| io_error("flush journal parent", parent, source))?;
        }
        let journal = Self {
            directory,
            directory_path: directory_path.to_owned(),
            expected_uid,
        };
        journal.validate_entries()?;
        Ok(journal)
    }

    /// Records that an attempt is starting, or replays an exact earlier proof.
    ///
    /// `Ok(Some(proof))` means this exact attempt was already proved, which is
    /// how an interrupted caller reconciles rather than repeating a transition
    /// it cannot repeat. `Ok(None)` means the attempt is armed and may install.
    fn arm(
        &mut self,
        policy: &SealedServingPolicy,
        bound: &BoundServingAttempt,
    ) -> Result<Option<ServingProof>, ServingActivationError> {
        self.with_exclusive_lock(|journal| {
            journal.validate_entries()?;
            if let Some(existing) = journal.read_state()?
                && existing.describes(policy, bound)?
            {
                match existing.phase()? {
                    Phase::Serving => {
                        return Ok(Some(ServingProof {
                            incarnation: bound.incarnation.clone(),
                            policy_sha256: existing.policy_sha256,
                        }));
                    }
                    Phase::Fenced => return Err(ServingActivationError::AlreadyFenced),
                    // An interrupted attempt for this same incarnation replays
                    // from the beginning: every check is repeated and no
                    // recorded progress is inherited.
                    Phase::Armed | Phase::Installed | Phase::Reloading => {}
                }
            }
            journal
                .write_state(Phase::Armed, policy, bound)
                .map(|()| None)
        })
    }

    fn record(
        &mut self,
        phase: Phase,
        policy: &SealedServingPolicy,
        bound: &BoundServingAttempt,
    ) -> Result<(), ServingActivationError> {
        self.with_exclusive_lock(|journal| {
            let existing = journal
                .read_state()?
                .ok_or(ServingActivationError::Conflict)?;
            if !existing.describes(policy, bound)? {
                return Err(ServingActivationError::Conflict);
            }
            let from = existing.phase()?;
            if !from.precedes(phase) {
                return Err(ServingActivationError::OutOfOrder {
                    from: from.label(),
                    to: phase.label(),
                });
            }
            journal.write_state(phase, policy, bound)
        })
    }

    /// Records the fence, then reports it.
    ///
    /// A journal that cannot record the fence still produces one: the
    /// postmaster is stopped either way, and refusing to report a fence because
    /// the record failed would leave it running.
    fn fence(
        &mut self,
        bound: &BoundServingAttempt,
        reason: FenceReason,
        cause: &str,
    ) -> ServingActivationOutcome {
        tracing::warn!(
            reason = ?reason,
            cause,
            pid = bound.incarnation.pid,
            "fencing the postmaster that may hold an unproved serving policy"
        );
        if let Err(error) = self.write_fence(bound) {
            tracing::error!(
                reason = %error,
                "could not record the serving activation fence; fencing anyway"
            );
        }
        ServingActivationOutcome::Fenced(FencedPostmaster {
            incarnation: bound.incarnation.clone(),
            reason,
        })
    }

    fn write_fence(&mut self, bound: &BoundServingAttempt) -> Result<(), ServingActivationError> {
        self.with_exclusive_lock(|journal| {
            let existing = journal
                .read_state()?
                .ok_or(ServingActivationError::Conflict)?;
            if existing.incarnation()? != bound.incarnation {
                return Err(ServingActivationError::Conflict);
            }
            let record = StateRecord {
                schema_version: STATE_SCHEMA_VERSION.to_owned(),
                phase: Phase::Fenced.label().to_owned(),
                persistence: FSYNC_PERSISTENCE.to_owned(),
                persisted_at_unix_ms: persisted_at_unix_ms()?,
                ..existing
            };
            journal.install_state(&record)
        })
    }

    fn write_state(
        &self,
        phase: Phase,
        policy: &SealedServingPolicy,
        bound: &BoundServingAttempt,
    ) -> Result<(), ServingActivationError> {
        let record = StateRecord {
            schema_version: STATE_SCHEMA_VERSION.to_owned(),
            phase: phase.label().to_owned(),
            policy_sha256: policy.sha256().to_owned(),
            boot_id: bound.incarnation.boot_id.clone(),
            postmaster_pid: bound.incarnation.pid.to_string(),
            generation: generation_text(&bound.generation),
            persistence: FSYNC_PERSISTENCE.to_owned(),
            persisted_at_unix_ms: persisted_at_unix_ms()?,
        };
        self.install_state(&record)
    }

    fn install_state(&self, record: &StateRecord) -> Result<(), ServingActivationError> {
        let staging_path = self.directory_path.join(STATE_STAGING_FILE);
        let final_path = self.directory_path.join(STATE_FILE);
        let encoded =
            serde_json::to_vec(record).map_err(|_| ServingActivationError::CorruptRecord {
                path: final_path.clone(),
            })?;
        remove_if_present(&self.directory, STATE_STAGING_FILE, &staging_path)?;
        let descriptor = openat(
            &self.directory,
            STATE_STAGING_FILE,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|source| io_error("create staged state", &staging_path, source.into()))?;
        let mut file = File::from(descriptor);
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .and_then(|()| file.set_permissions(fs::Permissions::from_mode(0o400)))
            .map_err(|source| io_error("write staged state", &staging_path, source))?;
        drop(file);
        #[cfg(test)]
        crash_checkpoint(CrashCheckpoint::StateStaged)?;
        // Replacing rather than refusing to replace is the point: exactly one
        // state exists at a time, and a reader sees the whole previous state or
        // the whole next one.
        renameat_with(
            &self.directory,
            STATE_STAGING_FILE,
            &self.directory,
            STATE_FILE,
            RenameFlags::empty(),
        )
        .map_err(|source| io_error("install state", &final_path, source.into()))?;
        #[cfg(test)]
        crash_checkpoint(CrashCheckpoint::StateRenamed)?;
        self.directory
            .sync_all()
            .map_err(|source| io_error("flush journal directory", &self.directory_path, source))?;
        #[cfg(test)]
        crash_checkpoint(CrashCheckpoint::StateDirectorySynced)?;
        let installed = self
            .read_state()?
            .ok_or_else(|| ServingActivationError::UnsafeObject {
                path: final_path.clone(),
                reason: "the installed state did not survive its own write",
            })?;
        if &installed != record {
            return Err(ServingActivationError::UnsafeObject {
                path: final_path,
                reason: "the installed state does not match what was written",
            });
        }
        Ok(())
    }

    fn read_state(&self) -> Result<Option<StateRecord>, ServingActivationError> {
        let path = self.directory_path.join(STATE_FILE);
        let stat = match statat(&self.directory, STATE_FILE, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(Errno::NOENT) => return Ok(None),
            Err(source) => return Err(io_error("read state metadata", &path, source.into())),
        };
        if stat.st_uid != self.expected_uid && stat.st_uid != 0 {
            return Err(ServingActivationError::UnsafeObject {
                path,
                reason: "the state is not owned by the runtime identity",
            });
        }
        let descriptor = openat(
            &self.directory,
            STATE_FILE,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|source| io_error("open state", &path, source.into()))?;
        let mut file = File::from(descriptor);
        let opened = fstat(&file).map_err(|source| io_error("stat state", &path, source.into()))?;
        if opened.st_ino != stat.st_ino || opened.st_dev != stat.st_dev {
            return Err(ServingActivationError::UnsafeObject {
                path,
                reason: "the state changed between lookup and open",
            });
        }
        if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile {
            return Err(ServingActivationError::UnsafeObject {
                path,
                reason: "the state is not a regular file",
            });
        }
        let mut contents = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take(MAX_STATE_RECORD_BYTES + 1)
            .read_to_end(&mut contents)
            .map_err(|source| io_error("read state", &path, source))?;
        if contents.len() as u64 > MAX_STATE_RECORD_BYTES {
            return Err(ServingActivationError::UnsafeObject {
                path,
                reason: "the state is larger than the accepted bound",
            });
        }
        let record: StateRecord = serde_json::from_slice(&contents)
            .map_err(|_| ServingActivationError::CorruptRecord { path: path.clone() })?;
        // The phase and the pid are checked where they are read instead, by the
        // typed accessors every caller has to go through. Checking them twice
        // adds a branch nothing can reach first.
        if record.schema_version != STATE_SCHEMA_VERSION || record.persistence != FSYNC_PERSISTENCE
        {
            return Err(ServingActivationError::CorruptRecord { path });
        }
        Ok(Some(record))
    }

    fn validate_entries(&self) -> Result<(), ServingActivationError> {
        let mut entries = Dir::read_from(&self.directory).map_err(|source| {
            io_error("read journal entries", &self.directory_path, source.into())
        })?;
        while let Some(entry) = entries.read() {
            let entry = entry.map_err(|source| {
                io_error("read journal entry", &self.directory_path, source.into())
            })?;
            let name = entry.file_name().to_bytes();
            if matches!(name, b"." | b".." | b"state" | b".state.tmp") {
                continue;
            }
            return Err(ServingActivationError::UnsafeObject {
                path: self.directory_path.join(OsStr::from_bytes(name)),
                reason: "unexpected journal entry",
            });
        }
        Ok(())
    }

    fn with_exclusive_lock<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, ServingActivationError>,
    ) -> Result<T, ServingActivationError> {
        flock(&self.directory, FlockOperation::NonBlockingLockExclusive).map_err(|source| {
            if source == Errno::WOULDBLOCK {
                ServingActivationError::Busy {
                    path: self.directory_path.clone(),
                }
            } else {
                io_error("lock journal", &self.directory_path, source.into())
            }
        })?;
        let result = operation(self);
        let unlock = flock(&self.directory, FlockOperation::Unlock)
            .map_err(|source| io_error("unlock journal", &self.directory_path, source.into()));
        match (result, unlock) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Armed,
    Installed,
    Reloading,
    Serving,
    Fenced,
}

impl Phase {
    const fn label(self) -> &'static str {
        match self {
            Self::Armed => "armed",
            Self::Installed => "installed",
            Self::Reloading => "reloading",
            Self::Serving => "serving",
            Self::Fenced => "fenced",
        }
    }

    fn parse(label: &str) -> Option<Self> {
        match label {
            "armed" => Some(Self::Armed),
            "installed" => Some(Self::Installed),
            "reloading" => Some(Self::Reloading),
            "serving" => Some(Self::Serving),
            "fenced" => Some(Self::Fenced),
            _ => None,
        }
    }

    /// Whether `next` may directly follow this phase.
    ///
    /// Serving is reachable only from a dispatched reload, so no path reaches
    /// it by recording progress that was never made. Fenced is absent because
    /// it is written by its own path, which accepts every predecessor.
    const fn precedes(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Armed, Self::Installed)
                | (Self::Installed, Self::Reloading)
                | (Self::Reloading, Self::Serving)
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateRecord {
    schema_version: String,
    phase: String,
    #[serde(rename = "policySHA256")]
    policy_sha256: String,
    #[serde(rename = "bootID")]
    boot_id: String,
    postmaster_pid: String,
    generation: String,
    persistence: String,
    #[serde(rename = "persistedAtUnixMS")]
    persisted_at_unix_ms: String,
}

impl StateRecord {
    fn phase(&self) -> Result<Phase, ServingActivationError> {
        Phase::parse(&self.phase).ok_or(ServingActivationError::CorruptRecord {
            path: PathBuf::from(STATE_FILE),
        })
    }

    fn incarnation(&self) -> Result<PostmasterIncarnation, ServingActivationError> {
        let pid =
            canonical_pid(&self.postmaster_pid).ok_or(ServingActivationError::CorruptRecord {
                path: PathBuf::from(STATE_FILE),
            })?;
        Ok(PostmasterIncarnation {
            boot_id: self.boot_id.clone(),
            pid,
        })
    }

    /// Whether this record belongs to the exact attempt being made now.
    ///
    /// All three of incarnation, policy and generation are compared: a record
    /// that agrees on only some of them describes a different attempt, and
    /// inheriting its progress would carry a proof across a change it was never
    /// established under.
    fn describes(
        &self,
        policy: &SealedServingPolicy,
        bound: &BoundServingAttempt,
    ) -> Result<bool, ServingActivationError> {
        Ok(self.incarnation()? == bound.incarnation
            && self.policy_sha256 == policy.sha256()
            && self.generation == generation_text(&bound.generation))
    }
}

/// Rejects a pid that is not canonical decimal, so one record cannot name two
/// different spellings of the same number.
fn canonical_pid(value: &str) -> Option<u32> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return None;
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn generation_text(generation: &DurableWritableGeneration) -> String {
    String::from_utf8_lossy(&generation.canonical_bytes()).into_owned()
}

fn persisted_at_unix_ms() -> Result<String, ServingActivationError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ServingActivationError::InvalidPersistenceClock)?
            .as_millis(),
    )
    .map(|milliseconds| milliseconds.to_string())
    .map_err(|_| ServingActivationError::InvalidPersistenceClock)
}

fn installed_exactly(
    path: &Path,
    expected_uid: u32,
    policy: &SealedServingPolicy,
) -> Result<bool, ServingActivationError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(io_error("stat serving policy", path, source)),
    };
    if !metadata.is_file() {
        return Err(ServingActivationError::UnsafeObject {
            path: path.to_owned(),
            reason: "the serving policy path is not a regular file",
        });
    }
    if metadata.uid() != expected_uid && metadata.uid() != 0 {
        return Err(ServingActivationError::UnsafeObject {
            path: path.to_owned(),
            reason: "the serving policy is not owned by the runtime identity",
        });
    }
    if metadata.permissions().mode() & 0o7_777 != 0o400
        || metadata.len() != policy.bytes().len() as u64
    {
        return Ok(false);
    }
    let contents =
        fs::read(path).map_err(|source| io_error("read serving policy", path, source))?;
    Ok(contents == policy.bytes())
}

fn open_directory(path: &Path) -> Result<File, ServingActivationError> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|source| io_error("open directory", path, source.into()))?;
    Ok(File::from(descriptor))
}

fn validate_directory(
    path: &Path,
    directory: &File,
    expected_uid: u32,
    forbidden_mode_bits: u32,
) -> Result<(), ServingActivationError> {
    let metadata = directory
        .metadata()
        .map_err(|source| io_error("stat directory", path, source))?;
    if !metadata.is_dir() {
        return Err(ServingActivationError::UnsafeObject {
            path: path.to_owned(),
            reason: "the path is not a directory",
        });
    }
    if metadata.uid() != expected_uid && metadata.uid() != 0 {
        return Err(ServingActivationError::UnsafeObject {
            path: path.to_owned(),
            reason: "the directory is not owned by the runtime identity",
        });
    }
    if metadata.permissions().mode() & forbidden_mode_bits != 0 {
        return Err(ServingActivationError::UnsafeObject {
            path: path.to_owned(),
            reason: "the directory permits access it must not permit",
        });
    }
    Ok(())
}

fn remove_if_present(
    directory: &File,
    name: &str,
    path: &Path,
) -> Result<(), ServingActivationError> {
    match unlinkat(directory, name, AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => Ok(()),
        Err(source) => Err(io_error(
            "remove interrupted staging file",
            path,
            source.into(),
        )),
    }
}

fn validate_absolute_normal(path: &Path) -> Result<(), ServingActivationError> {
    let normal = path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)));
    if normal {
        Ok(())
    } else {
        Err(ServingActivationError::InvalidPath {
            path: path.to_owned(),
        })
    }
}

fn io_error(
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> ServingActivationError {
    ServingActivationError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .into_iter()
        .fold(String::new(), |mut encoded, byte| {
            let _ = write!(encoded, "{byte:02x}");
            encoded
        })
}

/// Every durable boundary a crash can land on.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CrashCheckpoint {
    /// The sealed policy is staged but not renamed over the live path.
    PolicyStaged,
    /// The policy rename happened but the directory has not been flushed.
    PolicyRenamed,
    /// The state record is staged but not renamed.
    StateStaged,
    /// The state rename happened but the directory has not been flushed.
    StateRenamed,
    /// The state is fully durable.
    StateDirectorySynced,
}

#[cfg(test)]
std::thread_local! {
    static INJECTED_CRASH: std::cell::Cell<Option<(CrashCheckpoint, usize)>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn crash_checkpoint(checkpoint: CrashCheckpoint) -> Result<(), ServingActivationError> {
    let injected = INJECTED_CRASH.with(|slot| match slot.get() {
        Some((armed, 0)) if armed == checkpoint => {
            slot.set(None);
            true
        }
        Some((armed, skip)) if armed == checkpoint => {
            slot.set(Some((armed, skip - 1)));
            false
        }
        _ => false,
    });
    if injected {
        Err(ServingActivationError::InjectedCrash)
    } else {
        Ok(())
    }
}

/// Removes any crash still armed, so one test cannot leak into the next.
#[cfg(test)]
pub(crate) struct CrashGuard;

#[cfg(test)]
impl Drop for CrashGuard {
    fn drop(&mut self) {
        INJECTED_CRASH.with(|slot| slot.set(None));
    }
}

#[cfg(test)]
std::thread_local! {
    static TAMPER_AFTER_POLICY_RENAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Stands in for the one actor the install cannot exclude: another writer at
/// the same uid, between the rename and the check that follows it.
#[cfg(test)]
fn tamper_with_the_installed_policy_if_requested(path: &Path) {
    if TAMPER_AFTER_POLICY_RENAME.with(std::cell::Cell::take) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("open the policy");
        fs::write(path, b"local all all trust\n").expect("tamper with the policy");
        fs::set_permissions(path, fs::Permissions::from_mode(0o400)).expect("seal the policy");
    }
}

/// Removes any tampering still armed, so one test cannot leak into the next.
#[cfg(test)]
pub(crate) struct TamperGuard;

#[cfg(test)]
impl Drop for TamperGuard {
    fn drop(&mut self) {
        TAMPER_AFTER_POLICY_RENAME.with(|slot| slot.set(false));
    }
}

/// Arms exactly one replacement of the installed policy after its rename.
#[cfg(test)]
pub(crate) fn tamper_after_the_next_policy_rename() -> TamperGuard {
    TAMPER_AFTER_POLICY_RENAME.with(|slot| {
        assert!(
            !slot.replace(true),
            "test thread already has armed policy tampering"
        );
    });
    TamperGuard
}

/// Arms exactly one crash at the `skip`-th later arrival at that boundary.
///
/// Occurrence counting matters because one activation crosses the same durable
/// boundary once per recorded phase, and a crash at the first crossing is a
/// different surviving state from a crash at the third.
#[cfg(test)]
pub(crate) fn inject_crash(checkpoint: CrashCheckpoint, skip: usize) -> CrashGuard {
    INJECTED_CRASH.with(|slot| {
        assert!(
            slot.replace(Some((checkpoint, skip))).is_none(),
            "test thread already has an injected serving activation crash"
        );
    });
    CrashGuard
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::os::unix::fs::symlink;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use crate::writable::durable_generation_for_test;

    /// The non-serving policy every incarnation starts under, in the shape the
    /// agent already materializes. Only its digest matters here.
    const NON_SERVING: &[u8] =
        b"local postgres postgres peer\nlocal all all reject\nlocal replication all reject\n";
    const SERVING: &[u8] = b"local postgres postgres peer\n\
hostssl shardschema pgshard_pooler_catalog all scram-sha-256\n\
hostssl shardschema all all reject\n\
host all all all reject\n";

    const SETTLE: Duration = Duration::from_millis(300);
    const POLL_LIMIT: Duration = Duration::from_secs(5);

    fn policy() -> SealedServingPolicy {
        SealedServingPolicy::seal(
            SERVING.to_vec(),
            &sha256_hex(SERVING),
            &sha256_hex(NON_SERVING),
        )
        .expect("the fixture policy is sealed")
    }

    struct Fixture {
        _root: TempDir,
        journal_path: PathBuf,
        hba: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().expect("create an activation fixture");
            let hba_directory = root.path().join("hba");
            fs::create_dir(&hba_directory).expect("create the policy directory");
            fs::set_permissions(&hba_directory, fs::Permissions::from_mode(0o700))
                .expect("seal the policy directory");
            Self {
                journal_path: root.path().join("journal"),
                hba: hba_directory.join("pg_hba.conf"),
                _root: root,
            }
        }

        fn journal(&self) -> ServingActivationJournal {
            ServingActivationJournal::open_or_create(&self.journal_path).expect("open the journal")
        }

        /// Writes the non-serving policy the agent materializes before a spawn.
        fn install_non_serving(&self) {
            if self.hba.exists() {
                fs::set_permissions(&self.hba, fs::Permissions::from_mode(0o600))
                    .expect("open the policy");
            }
            fs::write(&self.hba, NON_SERVING).expect("write the non-serving policy");
            fs::set_permissions(&self.hba, fs::Permissions::from_mode(0o400))
                .expect("seal the non-serving policy");
        }

        fn installed(&self) -> Vec<u8> {
            fs::read(&self.hba).expect("read the installed policy")
        }
    }

    fn uid() -> u32 {
        geteuid().as_raw()
    }

    /// A real child that records every `SIGHUP` it receives.
    ///
    /// The dispatch under test is a real signal through a real pidfd, so the
    /// only honest way to assert that a signal was or was not sent is to have a
    /// process that would notice.
    struct SignalledChild {
        child: Child,
        counter: PathBuf,
        _directory: TempDir,
    }

    impl SignalledChild {
        fn start() -> Self {
            let directory = TempDir::new().expect("create a child fixture");
            let counter = directory.path().join("hangups");
            fs::write(&counter, b"").expect("create the hangup counter");
            let script = format!(
                "trap 'printf x >> {counter}' HUP; while :; do sleep 0.02; done",
                counter = counter.display()
            );
            let child = Command::new("/bin/sh")
                .arg("-c")
                .arg(script)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn a signal fixture");
            Self {
                child,
                counter,
                _directory: directory,
            }
        }

        fn retained(&self) -> RetainedPostmaster {
            RetainedPostmaster::open(self.child.id(), "fixture-boot".to_owned())
                .expect("open a pidfd for the fixture child")
        }

        fn hangups(&self) -> usize {
            fs::read(&self.counter)
                .expect("read the hangup counter")
                .len()
        }

        fn wait_for_hangups(&self, expected: usize) {
            let deadline = Instant::now() + POLL_LIMIT;
            while Instant::now() < deadline {
                if self.hangups() >= expected {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!(
                "the fixture child recorded {} hangups, expected at least {expected}",
                self.hangups()
            );
        }
    }

    impl Drop for SignalledChild {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    struct FakeAuthority {
        materialized: Cell<bool>,
        materialized_after_first: Option<bool>,
        generation: Option<DurableWritableGeneration>,
        target_fence: bool,
        postmaster: Option<RetainedPostmaster>,
    }

    impl FakeAuthority {
        fn complete(child: &SignalledChild) -> Self {
            Self {
                materialized: Cell::new(true),
                materialized_after_first: None,
                generation: Some(durable_generation_for_test(7)),
                target_fence: true,
                postmaster: Some(child.retained()),
            }
        }
    }

    impl ServingReloadAuthority for FakeAuthority {
        fn materialization_is_current(&self) -> bool {
            let answer = self.materialized.get();
            if let Some(next) = self.materialized_after_first {
                self.materialized.set(next);
            }
            answer
        }

        fn writable_generation(&self) -> Option<DurableWritableGeneration> {
            self.generation.clone()
        }

        fn target_fence_is_installed(&self) -> bool {
            self.target_fence
        }

        fn postmaster(&self) -> Option<&RetainedPostmaster> {
            self.postmaster.as_ref()
        }
    }

    fn bound(child: &SignalledChild) -> BoundServingAttempt {
        BoundServingAttempt {
            incarnation: child.retained().incarnation().clone(),
            generation: durable_generation_for_test(7),
        }
    }

    #[test]
    fn only_bytes_that_match_their_seal_and_differ_from_the_non_serving_policy_are_installable() {
        let serving = sha256_hex(SERVING);
        let non_serving = sha256_hex(NON_SERVING);
        assert!(SealedServingPolicy::seal(SERVING.to_vec(), &serving, &non_serving).is_ok());

        assert!(matches!(
            SealedServingPolicy::seal(SERVING.to_vec(), &non_serving, &non_serving),
            Err(ServingActivationError::PolicyDigestMismatch)
        ));
        assert!(
            matches!(
                SealedServingPolicy::seal(NON_SERVING.to_vec(), &non_serving, &non_serving),
                Err(ServingActivationError::IndistinctPolicy)
            ),
            "a serving policy equal to the non-serving one is not a transition"
        );
        assert!(matches!(
            SealedServingPolicy::seal(Vec::new(), &sha256_hex(b""), &non_serving),
            Err(ServingActivationError::InvalidPolicySize { .. })
        ));
        let oversized = vec![b'#'; MAX_SERVING_POLICY_BYTES + 1];
        assert!(matches!(
            SealedServingPolicy::seal(oversized.clone(), &sha256_hex(&oversized), &non_serving),
            Err(ServingActivationError::InvalidPolicySize { .. })
        ));
    }

    /// The trap the whole prepare-then-spawn revalidation rests on: the spawn
    /// path compares device and inode against what preparation saw, so a policy
    /// rewritten unconditionally is indistinguishable from a tampered one.
    #[test]
    fn installing_a_policy_that_is_already_exact_leaves_its_inode_alone() {
        let fixture = Fixture::new();
        let policy = policy();
        fixture.install_non_serving();

        assert_eq!(
            install_serving_policy(&fixture.hba, uid(), &policy).expect("install the policy"),
            ServingPolicyInstall::Installed
        );
        let installed = fs::metadata(&fixture.hba).expect("stat the policy");
        assert_eq!(
            installed.permissions().mode() & 0o7_777,
            0o400,
            "a policy the runtime identity can rewrite is one nothing can vouch for"
        );
        assert_eq!(fixture.installed(), SERVING);

        assert_eq!(
            install_serving_policy(&fixture.hba, uid(), &policy).expect("install again"),
            ServingPolicyInstall::AlreadyInstalled
        );
        assert_eq!(
            fs::metadata(&fixture.hba).expect("stat the policy").ino(),
            installed.ino(),
            "re-installing an exact policy replaced its inode"
        );
    }

    #[test]
    fn an_interrupted_staging_file_does_not_block_the_next_install() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let staging = fixture.hba.with_file_name(SERVING_POLICY_STAGING_FILE);
        fs::write(&staging, b"half a policy").expect("leave an interrupted staging file");
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o400)).expect("seal it");

        install_serving_policy(&fixture.hba, uid(), &policy()).expect("install over the remnant");
        assert_eq!(fixture.installed(), SERVING);
        assert!(!staging.exists(), "the staging file outlived its install");
    }

    #[test]
    fn a_symlinked_policy_path_is_refused_rather_than_followed() {
        let fixture = Fixture::new();
        let elsewhere = fixture.hba.with_file_name("elsewhere.conf");
        fs::write(&elsewhere, NON_SERVING).expect("write the target");
        symlink(&elsewhere, &fixture.hba).expect("link the policy path");

        let error = install_serving_policy(&fixture.hba, uid(), &policy())
            .expect_err("a symlinked policy path is refused");
        assert!(
            matches!(error, ServingActivationError::UnsafeObject { .. }),
            "expected an unsafe object, got {error}"
        );
        assert_eq!(
            fs::read(&elsewhere).expect("read the target"),
            NON_SERVING,
            "the refusal wrote through the link"
        );
    }

    #[test]
    fn a_crash_before_the_rename_leaves_the_previous_policy_in_place() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let before = fs::metadata(&fixture.hba).expect("stat the policy").ino();

        let _guard = inject_crash(CrashCheckpoint::PolicyStaged, 0);
        let error = install_serving_policy(&fixture.hba, uid(), &policy())
            .expect_err("the injected crash stops the install");
        assert!(matches!(error, ServingActivationError::InjectedCrash));
        assert_eq!(
            fixture.installed(),
            NON_SERVING,
            "a crash before the rename published a partial transition"
        );
        assert_eq!(
            fs::metadata(&fixture.hba).expect("stat the policy").ino(),
            before
        );
    }

    #[test]
    fn a_crash_after_the_rename_leaves_the_sealed_policy_whole() {
        let fixture = Fixture::new();
        fixture.install_non_serving();

        let _guard = inject_crash(CrashCheckpoint::PolicyRenamed, 0);
        let error = install_serving_policy(&fixture.hba, uid(), &policy())
            .expect_err("the injected crash stops the install");
        assert!(matches!(error, ServingActivationError::InjectedCrash));
        assert_eq!(
            fixture.installed(),
            SERVING,
            "the rename is atomic: a crash after it leaves the whole sealed policy"
        );
    }

    #[tokio::test]
    async fn a_complete_attempt_dispatches_exactly_one_reload_and_records_a_proof() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let authority = FakeAuthority::complete(&child);
        let bound = bound(&child);
        let policy = policy();
        let mut journal = fixture.journal();

        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy,
            &authority,
            &bound,
            || async { ServingReloadProbe::ServingRulesInEffect },
        )
        .await
        .expect("a complete attempt returns an outcome");
        let ServingActivationOutcome::Serving(proof) = outcome else {
            panic!("a complete attempt did not serve");
        };
        assert_eq!(proof.incarnation(), &bound.incarnation);
        assert_eq!(proof.policy_sha256(), policy.sha256());
        assert_eq!(fixture.installed(), SERVING);

        child.wait_for_hangups(1);
        std::thread::sleep(SETTLE);
        assert_eq!(
            child.hangups(),
            1,
            "the reload was dispatched more than once"
        );

        assert_eq!(
            journal.recover().expect("recover"),
            ServingActivationRecovery::Proved {
                incarnation: bound.incarnation.clone(),
                policy_sha256: policy.sha256().to_owned(),
            }
        );
    }

    /// Every stage between the two extremes: whatever an attempt was doing when
    /// it stopped, what is on disk classifies into one recognized state, and
    /// none of the interrupted ones admits serving.
    #[tokio::test]
    async fn every_crash_boundary_leaves_a_recognized_state_that_never_admits_serving() {
        for (checkpoint, skip) in [
            (CrashCheckpoint::StateStaged, 0),
            (CrashCheckpoint::StateRenamed, 0),
            (CrashCheckpoint::StateDirectorySynced, 0),
            (CrashCheckpoint::PolicyStaged, 0),
            (CrashCheckpoint::PolicyRenamed, 0),
            (CrashCheckpoint::StateStaged, 1),
            (CrashCheckpoint::StateRenamed, 1),
            (CrashCheckpoint::StateStaged, 2),
            (CrashCheckpoint::StateRenamed, 2),
            (CrashCheckpoint::StateStaged, 3),
            (CrashCheckpoint::StateRenamed, 3),
            (CrashCheckpoint::StateDirectorySynced, 3),
        ] {
            let fixture = Fixture::new();
            fixture.install_non_serving();
            let child = SignalledChild::start();
            let authority = FakeAuthority::complete(&child);
            let bound = bound(&child);
            let policy = policy();

            {
                let mut journal = fixture.journal();
                let guard = inject_crash(checkpoint, skip);
                let _ = activate(
                    &mut journal,
                    &fixture.hba,
                    uid(),
                    &policy,
                    &authority,
                    &bound,
                    || async { ServingReloadProbe::ServingRulesInEffect },
                )
                .await;
                drop(guard);
            }

            // A fresh handle is what a restarted agent has: nothing in memory.
            let recovery = fixture.journal().recover().expect("recover");
            assert!(
                !recovery.admits_serving(&bound.incarnation),
                "a crash at {checkpoint:?} occurrence {skip} left a state that admits serving \
                 without a proof: {recovery:?}"
            );
            assert!(
                matches!(
                    recovery,
                    ServingActivationRecovery::Fresh
                        | ServingActivationRecovery::Interrupted { .. }
                        | ServingActivationRecovery::Fenced { .. }
                ),
                "a crash at {checkpoint:?} occurrence {skip} left {recovery:?}"
            );
        }
    }

    /// The invariant a later incarnation depends on: a proof is about one
    /// process, and the next process is a different one.
    #[test]
    fn a_proof_never_admits_a_later_incarnation() {
        let proved = PostmasterIncarnation {
            boot_id: "boot-a".to_owned(),
            pid: 4242,
        };
        let recovery = ServingActivationRecovery::Proved {
            incarnation: proved.clone(),
            policy_sha256: sha256_hex(SERVING),
        };
        assert!(recovery.admits_serving(&proved));
        assert!(
            !recovery.admits_serving(&PostmasterIncarnation {
                boot_id: "boot-a".to_owned(),
                pid: 4243,
            }),
            "a proof for one pid admitted another"
        );
        assert!(
            !recovery.admits_serving(&PostmasterIncarnation {
                boot_id: "boot-b".to_owned(),
                pid: 4242,
            }),
            "a proof from one boot admitted a reused pid in another"
        );
        for interrupted in [
            ServingActivationRecovery::Fresh,
            ServingActivationRecovery::Interrupted {
                incarnation: proved.clone(),
            },
            ServingActivationRecovery::Fenced {
                incarnation: proved.clone(),
            },
        ] {
            assert!(
                !interrupted.admits_serving(&proved),
                "{interrupted:?} admitted serving"
            );
        }
    }

    #[tokio::test]
    async fn an_exact_repeat_replays_the_proof_without_repeating_the_transition() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let authority = FakeAuthority::complete(&child);
        let bound = bound(&child);
        let policy = policy();
        let mut journal = fixture.journal();

        let first = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy,
            &authority,
            &bound,
            || async { ServingReloadProbe::ServingRulesInEffect },
        )
        .await
        .expect("the first attempt returns an outcome");
        assert!(matches!(first, ServingActivationOutcome::Serving(_)));
        child.wait_for_hangups(1);

        let second = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy,
            &authority,
            &bound,
            || async { panic!("a replay must not probe again") },
        )
        .await
        .expect("the replay returns an outcome");
        assert!(matches!(second, ServingActivationOutcome::Serving(_)));
        std::thread::sleep(SETTLE);
        assert_eq!(
            child.hangups(),
            1,
            "a replay dispatched the transition a second time"
        );
    }

    #[tokio::test]
    async fn a_fenced_attempt_is_never_resumed() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let authority = FakeAuthority::complete(&child);
        let bound = bound(&child);
        let policy = policy();
        let mut journal = fixture.journal();

        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy,
            &authority,
            &bound,
            || async { ServingReloadProbe::Indeterminate },
        )
        .await
        .expect("the attempt returns an outcome");
        assert!(matches!(outcome, ServingActivationOutcome::Fenced(_)));

        let error = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy,
            &authority,
            &bound,
            || async { ServingReloadProbe::ServingRulesInEffect },
        )
        .await
        .expect_err("a fenced attempt is refused");
        assert!(matches!(error, ServingActivationError::AlreadyFenced));
    }

    /// A record left by a different attempt is history. It must neither block
    /// the next attempt nor be inherited by it.
    #[tokio::test]
    async fn a_record_from_another_attempt_is_replaced_rather_than_inherited() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let policy = policy();
        let mut journal = fixture.journal();

        let stale = BoundServingAttempt {
            incarnation: PostmasterIncarnation {
                boot_id: "an-older-boot".to_owned(),
                pid: 1,
            },
            generation: durable_generation_for_test(6),
        };
        journal
            .with_exclusive_lock(|journal| journal.write_state(Phase::Serving, &policy, &stale))
            .expect("record a stale proof");
        assert!(
            !journal
                .recover()
                .expect("recover")
                .admits_serving(&bound(&child).incarnation),
            "a stale proof admitted the current incarnation"
        );

        let authority = FakeAuthority::complete(&child);
        let bound = bound(&child);
        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy,
            &authority,
            &bound,
            || async { ServingReloadProbe::ServingRulesInEffect },
        )
        .await
        .expect("the new attempt returns an outcome");
        assert!(matches!(outcome, ServingActivationOutcome::Serving(_)));
        child.wait_for_hangups(1);
    }

    #[test]
    fn recorded_progress_only_moves_forward() {
        let fixture = Fixture::new();
        let child = SignalledChild::start();
        let bound = bound(&child);
        let policy = policy();
        let mut journal = fixture.journal();

        assert!(journal.arm(&policy, &bound).expect("arm").is_none());
        let error = journal
            .record(Phase::Serving, &policy, &bound)
            .expect_err("serving is not reachable from armed");
        assert!(
            matches!(
                error,
                ServingActivationError::OutOfOrder {
                    from: "armed",
                    to: "serving"
                }
            ),
            "expected an out-of-order refusal, got {error}"
        );
        journal
            .record(Phase::Installed, &policy, &bound)
            .expect("installed follows armed");
        let error = journal
            .record(Phase::Installed, &policy, &bound)
            .expect_err("a phase does not follow itself");
        assert!(matches!(error, ServingActivationError::OutOfOrder { .. }));
    }

    #[test]
    fn a_corrupt_or_polluted_journal_is_never_reported_as_fresh() {
        let fixture = Fixture::new();
        {
            let mut journal = fixture.journal();
            assert_eq!(
                journal.recover().expect("recover an empty journal"),
                ServingActivationRecovery::Fresh
            );
        }

        let state = fixture.journal_path.join(STATE_FILE);
        fs::write(&state, b"{\"not\":\"a state\"}").expect("corrupt the state");
        let error = fixture
            .journal()
            .recover()
            .expect_err("a corrupt state is refused");
        assert!(
            matches!(error, ServingActivationError::CorruptRecord { .. }),
            "expected a corrupt record, got {error}"
        );

        fs::remove_file(&state).expect("remove the corrupt state");
        fs::write(fixture.journal_path.join("stowaway"), b"").expect("pollute the journal");
        let error = ServingActivationJournal::open_or_create(&fixture.journal_path)
            .expect_err("an unexpected entry is refused before the journal opens");
        assert!(
            matches!(error, ServingActivationError::UnsafeObject { .. }),
            "expected an unsafe object, got {error}"
        );
    }

    /// The agent and the postmaster share a uid, so writing the policy is not
    /// the same as knowing what is at the path afterwards.
    #[test]
    fn a_policy_replaced_between_the_rename_and_the_check_is_refused() {
        let fixture = Fixture::new();
        fixture.install_non_serving();

        let _guard = tamper_after_the_next_policy_rename();
        let error = install_serving_policy(&fixture.hba, uid(), &policy())
            .expect_err("a replaced policy is refused");
        assert!(
            matches!(
                error,
                ServingActivationError::UnsafeObject {
                    reason: "the installed serving policy did not survive its own write",
                    ..
                }
            ),
            "expected a refusal of the installed policy, got {error}"
        );
    }

    /// A record that decodes is not a record this journal wrote. Each field the
    /// journal depends on is corrupted on its own, because a check that only
    /// fires for garbage does not defend against a plausible forgery.
    #[test]
    fn a_structurally_valid_record_this_journal_did_not_write_is_refused() {
        let fixture = Fixture::new();
        let child = SignalledChild::start();
        let bound = bound(&child);
        let policy = policy();
        {
            let mut journal = fixture.journal();
            assert!(journal.arm(&policy, &bound).expect("arm").is_none());
        }
        let state = fixture.journal_path.join(STATE_FILE);
        let genuine: serde_json::Value =
            serde_json::from_slice(&fs::read(&state).expect("read the state"))
                .expect("the genuine record decodes");

        for (field, forged) in [
            ("schemaVersion", "pgshard.serving-activation-state.v2"),
            ("phase", "definitely-serving"),
            ("persistence", "best-effort"),
            ("postmasterPid", "007"),
        ] {
            let mut record = genuine.clone();
            record[field] = serde_json::Value::String(forged.to_owned());
            fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).expect("open the state");
            fs::write(&state, serde_json::to_vec(&record).expect("encode")).expect("forge");
            fs::set_permissions(&state, fs::Permissions::from_mode(0o400)).expect("seal the state");

            let error = fixture
                .journal()
                .recover()
                .expect_err("a forged record is refused");
            assert!(
                matches!(error, ServingActivationError::CorruptRecord { .. }),
                "a forged {field} was accepted: {error}"
            );
        }
    }

    #[test]
    fn a_pid_that_is_not_canonical_decimal_is_refused() {
        assert_eq!(canonical_pid("4242"), Some(4242));
        assert_eq!(canonical_pid("0"), Some(0));
        for spelling in ["", "0042", " 42", "42 ", "+42", "-42", "4_2", "0x2a"] {
            assert!(
                canonical_pid(spelling).is_none(),
                "{spelling:?} was accepted as a pid"
            );
        }
    }

    /// Each fact the reload rests on, removed on its own. None of them may be
    /// missing when the signal goes out, and the proof of that is a real child
    /// that would have recorded one.
    #[tokio::test]
    async fn every_missing_authority_stops_the_dispatch_and_fences() {
        for (name, mutate) in [
            (
                "materialization",
                Box::new(|authority: &mut FakeAuthority| authority.materialized.set(false))
                    as Box<dyn Fn(&mut FakeAuthority)>,
            ),
            (
                "writable generation",
                Box::new(|authority: &mut FakeAuthority| authority.generation = None),
            ),
            (
                "a different writable generation",
                Box::new(|authority: &mut FakeAuthority| {
                    authority.generation = Some(durable_generation_for_test(8));
                }),
            ),
            (
                "target fence",
                Box::new(|authority: &mut FakeAuthority| authority.target_fence = false),
            ),
            (
                "retained postmaster",
                Box::new(|authority: &mut FakeAuthority| authority.postmaster = None),
            ),
        ] {
            let fixture = Fixture::new();
            fixture.install_non_serving();
            let child = SignalledChild::start();
            let mut authority = FakeAuthority::complete(&child);
            mutate(&mut authority);
            let bound = bound(&child);
            let mut journal = fixture.journal();

            let outcome = activate(
                &mut journal,
                &fixture.hba,
                uid(),
                &policy(),
                &authority,
                &bound,
                || async { panic!("a refused dispatch must never be probed") },
            )
            .await
            .expect("the attempt returns an outcome");
            let ServingActivationOutcome::Fenced(fenced) = outcome else {
                panic!("a missing {name} still served");
            };
            assert_eq!(fenced.reason(), FenceReason::AuthorityLost);
            assert_eq!(fenced.incarnation(), &bound.incarnation);

            std::thread::sleep(SETTLE);
            assert_eq!(
                child.hangups(),
                0,
                "a missing {name} still dispatched a reload"
            );
            // The counter itself has to be shown to work, or "no hangups" would
            // also be what a broken fixture reports.
            child.retained().dispatch_reload().expect("signal by hand");
            child.wait_for_hangups(1);

            assert!(matches!(
                fixture.journal().recover().expect("recover"),
                ServingActivationRecovery::Fenced { .. }
            ));
        }
    }

    /// A zombie is the case that separates the liveness check from the signal.
    /// Signals to one still succeed, so a dispatch that relied on the signal's
    /// own failure would reload a postmaster that no longer exists and then go
    /// looking for a proof.
    #[tokio::test]
    async fn a_postmaster_that_has_already_exited_stops_the_dispatch() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn a short-lived fixture");
        let retained = RetainedPostmaster::open(child.id(), "fixture-boot".to_owned())
            .expect("open a pidfd before the child exits");
        let deadline = Instant::now() + POLL_LIMIT;
        while retained.is_live() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!retained.is_live(), "the fixture child never exited");
        assert!(
            retained.dispatch_reload().is_ok(),
            "a zombie that refuses signals does not exercise the liveness check"
        );

        let bound = BoundServingAttempt {
            incarnation: retained.incarnation().clone(),
            generation: durable_generation_for_test(7),
        };
        let authority = FakeAuthority {
            materialized: Cell::new(true),
            materialized_after_first: None,
            generation: Some(durable_generation_for_test(7)),
            target_fence: true,
            postmaster: Some(retained),
        };
        let mut journal = fixture.journal();

        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy(),
            &authority,
            &bound,
            || async { panic!("a postmaster that has already exited must never be probed") },
        )
        .await
        .expect("the attempt returns an outcome");
        let ServingActivationOutcome::Fenced(fenced) = outcome else {
            panic!("an exited postmaster was reported as serving");
        };
        assert_eq!(fenced.reason(), FenceReason::AuthorityLost);
        child.wait().expect("reap the fixture");
    }

    #[tokio::test]
    async fn an_incarnation_that_moved_on_stops_the_dispatch() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let authority = FakeAuthority::complete(&child);
        let mut bound = bound(&child);
        bound.incarnation.pid = bound.incarnation.pid.wrapping_add(1);
        let mut journal = fixture.journal();

        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy(),
            &authority,
            &bound,
            || async { panic!("a refused dispatch must never be probed") },
        )
        .await
        .expect("the attempt returns an outcome");
        assert!(matches!(outcome, ServingActivationOutcome::Fenced(_)));
        std::thread::sleep(SETTLE);
        assert_eq!(
            child.hangups(),
            0,
            "the reload went to a postmaster the attempt was not bound to"
        );
    }

    #[test]
    fn a_reaped_process_is_not_live_even_though_its_pid_can_be_signalled() {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn a short-lived fixture");
        let retained = RetainedPostmaster::open(child.id(), "fixture-boot".to_owned())
            .expect("open a pidfd before the child exits");
        child.wait().expect("reap the fixture");
        assert!(
            !retained.is_live(),
            "a reaped process reported itself as live"
        );
    }

    #[test]
    fn a_zombie_is_not_live_even_though_signals_still_succeed() {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn a short-lived fixture");
        let retained = RetainedPostmaster::open(child.id(), "fixture-boot".to_owned())
            .expect("open a pidfd before the child exits");
        let deadline = Instant::now() + POLL_LIMIT;
        while retained.is_live() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !retained.is_live(),
            "an exited but unreaped process reported itself as live"
        );
        assert!(
            retained.dispatch_reload().is_ok(),
            "the fixture no longer demonstrates why liveness cannot be a signal probe"
        );
        child.wait().expect("reap the fixture");
    }

    #[tokio::test]
    async fn a_probe_that_is_not_an_exact_proof_fences() {
        for (probe, expected) in [
            (
                ServingReloadProbe::NonServingRulesStillInEffect,
                FenceReason::ReloadRefused,
            ),
            (
                ServingReloadProbe::Indeterminate,
                FenceReason::UnprovedReload,
            ),
        ] {
            let fixture = Fixture::new();
            fixture.install_non_serving();
            let child = SignalledChild::start();
            let authority = FakeAuthority::complete(&child);
            let bound = bound(&child);
            let mut journal = fixture.journal();

            let outcome = activate(
                &mut journal,
                &fixture.hba,
                uid(),
                &policy(),
                &authority,
                &bound,
                || async move { probe },
            )
            .await
            .expect("the attempt returns an outcome");
            let ServingActivationOutcome::Fenced(fenced) = outcome else {
                panic!("{probe:?} was accepted as a proof");
            };
            assert_eq!(fenced.reason(), expected);
            child.wait_for_hangups(1);
            assert!(matches!(
                fixture.journal().recover().expect("recover"),
                ServingActivationRecovery::Fenced { .. }
            ));
        }
    }

    /// The reload has already happened by the time the probe answers, so an
    /// authority that is gone afterwards is a postmaster that must stop, not a
    /// result that can be reported.
    #[tokio::test]
    async fn authority_lost_after_a_proved_reload_fences_instead_of_serving() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let mut authority = FakeAuthority::complete(&child);
        authority.materialized_after_first = Some(false);
        let bound = bound(&child);
        let mut journal = fixture.journal();

        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy(),
            &authority,
            &bound,
            || async { ServingReloadProbe::ServingRulesInEffect },
        )
        .await
        .expect("the attempt returns an outcome");
        let ServingActivationOutcome::Fenced(fenced) = outcome else {
            panic!("a proof taken under lapsed authority was reported as serving");
        };
        assert_eq!(fenced.reason(), FenceReason::AuthorityLost);
        child.wait_for_hangups(1);
    }

    /// Once the sealed policy is on the disk there is no "nothing happened"
    /// answer left, so the journal failing must not turn into one.
    #[tokio::test]
    async fn a_journal_failure_after_the_install_fences_rather_than_returning_an_error() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let authority = FakeAuthority::complete(&child);
        let bound = bound(&child);
        let mut journal = fixture.journal();

        // Occurrence zero is the arm record; occurrence one is the first record
        // written after the policy reaches the disk.
        let _guard = inject_crash(CrashCheckpoint::StateStaged, 1);
        let outcome = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy(),
            &authority,
            &bound,
            || async { panic!("a fenced attempt must never be probed") },
        )
        .await
        .expect("a failure after the install is an outcome, not an error");
        let ServingActivationOutcome::Fenced(fenced) = outcome else {
            panic!("an unrecorded install still served");
        };
        assert_eq!(fenced.reason(), FenceReason::UnrecordedProgress);
        assert_eq!(fixture.installed(), SERVING);
        std::thread::sleep(SETTLE);
        assert_eq!(
            child.hangups(),
            0,
            "an attempt that could not record its own progress still reloaded"
        );
    }

    /// Failing before anything is installed is the one place an error is the
    /// honest answer, and the disk must show that nothing happened.
    #[tokio::test]
    async fn a_failure_before_the_install_returns_an_error_and_changes_nothing() {
        let fixture = Fixture::new();
        fixture.install_non_serving();
        let child = SignalledChild::start();
        let authority = FakeAuthority::complete(&child);
        let bound = bound(&child);
        let mut journal = fixture.journal();

        let _guard = inject_crash(CrashCheckpoint::StateStaged, 0);
        let error = activate(
            &mut journal,
            &fixture.hba,
            uid(),
            &policy(),
            &authority,
            &bound,
            || async { panic!("an attempt that never armed must never probe") },
        )
        .await
        .expect_err("an attempt that cannot arm returns an error");
        assert!(matches!(error, ServingActivationError::InjectedCrash));
        assert_eq!(
            fixture.installed(),
            NON_SERVING,
            "an attempt that never armed still installed the sealed policy"
        );
        std::thread::sleep(SETTLE);
        assert_eq!(child.hangups(), 0);
    }

    #[test]
    fn a_second_handle_cannot_race_the_journal() {
        let fixture = Fixture::new();
        let mut held = fixture.journal();
        let mut other = fixture.journal();
        let outcome = held.with_exclusive_lock(|_| {
            Ok::<_, ServingActivationError>(other.recover().expect_err("the journal is held"))
        });
        assert!(
            matches!(
                outcome.expect("the holder completes"),
                ServingActivationError::Busy { .. }
            ),
            "two handles entered the journal at once"
        );
    }
}
