//! Issues genesis authority for one first catalog creation.
//!
//! [`pgshard_types::genesis_intent`] fixes the form permission is expressed in
//! and is explicit that it grants nothing: whoever writes an intent chooses
//! every byte, so checking one against itself authenticates nothing. That
//! module names the obligations it defers to whoever can read the real sources.
//! This is that component. It lives in the agent because the agent is the only
//! process that can reach all four right-hand sides: the mounted `PGDATA`, the
//! writable generation this attempt holds, the live owning cluster object, and
//! a local durable record on the instance's own volume. The control plane
//! cannot see the mount, the types crate can see nothing, and the operator
//! cannot see this attempt's Lease.
//!
//! # Why the sources take no arguments, and why that is not enough
//!
//! [`GenesisSources`] observes without being told what to observe. A source
//! that cannot be given a value cannot be handed one out of the intent, so the
//! tautology the contract warns about has no way to be expressed inside a
//! source: there is no parameter to smuggle a left-hand side through.
//!
//! Argument-free methods are only half of it, because a caller that can supply
//! its own source, or construct an observation, or choose where the durable
//! record lives, reaches the same place through the constructors instead. So
//! every one of those is closed:
//!
//! - [`GenesisSources`] and [`HeldWritableAuthority`] have a private supertrait.
//!   They cannot be implemented outside this crate at all.
//! - [`ObservedIncarnation`] and [`ObservedOwner`] have private fields and no
//!   public constructor. The reader that mints an incarnation is crate private
//!   and derives the uid it trusts from `geteuid` rather than accepting one, so
//!   a caller cannot anchor the executable check to itself and then feed the
//!   reader a program of its own.
//! - [`GenesisAuthorityStore`] has no public constructor and no path parameter.
//!   Its directory is derived from the mounted data directory, so at most once
//!   is a property of the mount rather than of whichever directory a caller
//!   nominated.
//!
//! What remains reachable from outside is [`observe_owning_cluster`], which
//! turns a Kubernetes object into an [`ObservedOwner`]. That mints one of the
//! four right-hand sides and nothing else, and none of the other three, the
//! store, or a source implementation can be obtained to go with it.
//!
//! # The seal, proved from outside
//!
//! Prose asserting a seal is what a reviewer disproved by compiling a caller
//! that walked straight through it, so the seal is pinned by callers that have
//! to fail to compile. Doc tests build as their own crates against this one,
//! which is the same position an attacker's crate is in, and each expects a
//! named error rather than merely failing.
//!
//! First, that the names resolve and the module really is reachable, so the
//! refusals below cannot be passing because something is misspelled:
//!
//! ```
//! use pgshard_agent::genesis_authority::{
//!     GenesisAuthority, GenesisAuthorityStore, GenesisSources, HeldWritableAuthority,
//!     ObservedIncarnation, ObservedOwner, observe_owning_cluster,
//! };
//! ```
//!
//! A source cannot be implemented, because the trait is sealed:
//!
//! ```compile_fail,E0277
//! use pgshard_agent::genesis_authority::{
//!     GenesisEvidenceError, GenesisSources, ObservedIncarnation, ObservedOwner,
//! };
//! use pgshard_types::writable_generation::DurableWritableGeneration;
//!
//! struct Forged;
//! impl GenesisSources for Forged {
//!     fn observe_incarnation(&self) -> Result<ObservedIncarnation, GenesisEvidenceError> {
//!         unimplemented!()
//!     }
//!     fn observe_owning_cluster(&self) -> Result<ObservedOwner, GenesisEvidenceError> {
//!         unimplemented!()
//!     }
//!     fn observe_held_generation(
//!         &self,
//!     ) -> Result<DurableWritableGeneration, GenesisEvidenceError> {
//!         unimplemented!()
//!     }
//! }
//! ```
//!
//! Nor a writable authority, for the same reason:
//!
//! ```compile_fail,E0277
//! use pgshard_agent::genesis_authority::HeldWritableAuthority;
//! use pgshard_types::writable_generation::DurableWritableGeneration;
//!
//! struct Forged;
//! impl HeldWritableAuthority for Forged {
//!     fn generation_valid_for(
//!         &self,
//!         _: std::time::Duration,
//!     ) -> Option<DurableWritableGeneration> {
//!         None
//!     }
//! }
//! ```
//!
//! The reader that mints an incarnation is unreachable, so a caller cannot
//! supply its own control-data program:
//!
//! ```compile_fail,E0603
//! let _ = pgshard_agent::genesis_authority::read_mounted_incarnation(
//!     std::path::Path::new("/pgdata"),
//!     std::path::Path::new("/tmp/forged-pg_controldata"),
//! );
//! ```
//!
//! So is the store, so a caller cannot nominate where at most once applies:
//!
//! ```compile_fail,E0624
//! let _ = pgshard_agent::genesis_authority::GenesisAuthorityStore::for_data_directory(
//!     std::path::Path::new("/var/lib/postgresql/18/docker"),
//! );
//! ```
//!
//! And neither observation can be built as a literal:
//!
//! ```compile_fail,E0451
//! let _ = pgshard_agent::genesis_authority::ObservedIncarnation {
//!     seed_id: String::new(),
//!     system_identifier: 1,
//!     timeline: 1,
//! };
//! ```
//!
//! ```compile_fail,E0451
//! let _ = pgshard_agent::genesis_authority::ObservedOwner { uid: String::new() };
//! ```
//!
//! Every field is pinned one at a time as well, because a literal still fails
//! to compile while any single field stays private, so opening one of them is
//! a slip the two cases above cannot see:
//!
//! ```compile_fail,E0616
//! fn read(observed: &pgshard_agent::genesis_authority::ObservedIncarnation) -> &str {
//!     &observed.seed_id
//! }
//! ```
//!
//! ```compile_fail,E0616
//! fn read(observed: &pgshard_agent::genesis_authority::ObservedIncarnation) -> u64 {
//!     observed.system_identifier
//! }
//! ```
//!
//! ```compile_fail,E0616
//! fn read(observed: &pgshard_agent::genesis_authority::ObservedIncarnation) -> u32 {
//!     observed.timeline
//! }
//! ```
//!
//! ```compile_fail,E0616
//! fn read(observed: &pgshard_agent::genesis_authority::ObservedOwner) -> &str {
//!     &observed.uid
//! }
//! ```
//!
//! # The two records, and which one this module may write
//!
//! [`GenesisAuthorityStore`] reads the authority record and never writes it.
//! Minting authority is [`GenesisAuthorityAcceptance`], a separate type this
//! module's issuer does not hold, because a component that can write the record
//! that grants it permission has no permission at all — a wiped or unreachable
//! store would silently mint its own. Absent, malformed and zero-valued
//! authority all compare as "invalid, refuse"; none of them compares as
//! "unknown, proceed", and none of them is repaired by writing a fresh record.
//!
//! The store does write the *taken* record, which is the freshness gate. It is
//! keyed by the incarnation read off the mount, never by the one the intent
//! names, so a caller who invents a fresh `seedId` cannot invent a fresh key
//! and find nothing recorded against it. It is durable before [`issue`]
//! returns, and it is consumable at most once, so a crash between recording and
//! creating leaves a shard that refuses rather than a catalog that comes back.
//!
//! [`issue`]: GenesisAuthority::issue
//!
//! # What a permit is not
//!
//! A [`GenesisPermit`] is one fence. The contract also requires the destructive
//! act to be fenced a second time inside the engine, and this permit
//! deliberately makes no claim about that: recorded-but-unenforced protection
//! reads like a fence in review, which is worse than none. See
//! [`GenesisPermit`] for the part of that requirement `PostgreSQL` can actually
//! carry.

use std::fs::File;
use std::io::{Read as _, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::DynamicObject;
use pgshard_types::genesis_intent::{CatalogGenesisIntent, GenesisIntentError};
use pgshard_types::writable_generation::DurableWritableGeneration;
use rustix::fs::{
    AtFlags, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, flock, fstat, mkdirat,
    openat, renameat_with, unlinkat,
};
use rustix::io::Errno;
use rustix::process::geteuid;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::writable::WritableAuthorityObserver;

/// Selects the supported durable authority-record encoding.
pub const GENESIS_AUTHORITY_RECORD_VERSION: &str = "pgshard.catalog-genesis-authority.v1";

/// Selects the supported durable taken-record encoding.
pub const GENESIS_TAKEN_RECORD_VERSION: &str = "pgshard.catalog-genesis-taken.v1";

/// `apiVersion` of the object that may own an instance of this cluster.
pub const OWNING_CLUSTER_API_VERSION: &str = "pgshard.io/v1alpha1";

/// `kind` of the object that may own an instance of this cluster.
pub const OWNING_CLUSTER_KIND: &str = "PgshardCluster";

/// Domain separator for the seed derived from the `initdb` nonce, so the seed
/// can never equal the nonce and republishing it leaks no authentication
/// material.
const SEED_DIGEST_DOMAIN: &str = "pgshard-pgdata-seed-v1";

/// Domain separator for the incarnation key that names a taken record.
const INCARNATION_KEY_DIGEST_DOMAIN: &str = "pgshard-pgdata-incarnation-v1";

/// The only `pg_control` format this agent will read.
///
/// `PG_CONTROL_VERSION` in `src/include/catalog/pg_control.h`. Refusing every
/// other value is what stops a layout this code does not understand from being
/// parsed into a confident wrong answer about which incarnation is mounted.
const SUPPORTED_CONTROL_VERSION: &[u8] = b"1800";

const AUTHORITY_RECORD: &str = "authority";
const AUTHORITY_STAGING: &str = ".authority.tmp";
const TAKEN_PREFIX: &str = "taken-";
const MAXIMUM_RECORD_BYTES: u64 = 4 * 1024;
const MAXIMUM_CONTROL_DATA_BYTES: usize = 64 * 1024;
const MAXIMUM_DIRECTORY_ENTRIES: usize = 64;

/// One `PGDATA` incarnation, as observed on the mount.
///
/// Constructible only by [`read_mounted_incarnation`], which reads it, so this
/// type cannot carry a value that came out of an intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedIncarnation {
    seed_id: String,
    system_identifier: u64,
    timeline: u32,
}

impl ObservedIncarnation {
    /// Returns the seed derived from this incarnation's `initdb` nonce.
    #[must_use]
    pub fn seed_id(&self) -> &str {
        &self.seed_id
    }

    /// Returns the `pg_control` system identifier of this incarnation.
    #[must_use]
    pub const fn system_identifier(&self) -> u64 {
        self.system_identifier
    }

    /// Returns the timeline this incarnation is on.
    #[must_use]
    pub const fn timeline(&self) -> u32 {
        self.timeline
    }

    /// Returns the stable key a taken record is filed under.
    ///
    /// All three values are framed into it, so two incarnations that agree on
    /// any two of them still occupy different keys.
    fn key(&self) -> String {
        let mut hash = Sha256::new();
        frame(&mut hash, INCARNATION_KEY_DIGEST_DOMAIN);
        frame(&mut hash, &self.seed_id);
        frame(&mut hash, &self.system_identifier.to_string());
        frame(&mut hash, &self.timeline.to_string());
        lower_hex(&hash.finalize())
    }
}

/// Closes [`GenesisSources`] and [`HeldWritableAuthority`] to this crate.
///
/// Reachable through the public traits and nameable through neither, so an
/// implementation outside this crate cannot satisfy the bound. Sources are
/// evidence; a caller that can write one has already answered the question.
mod sealed {
    /// Private supertrait. Deliberately has no members.
    pub trait Sealed {}
}

/// The cluster observed as the controlling owner of a live Kubernetes object.
///
/// Opaque and privately constructed, so a wiring layer cannot fill the channel
/// this travels on with a value it chose. That is not a hypothetical: a
/// `String` channel filled from the intent's own `clusterUid` would turn the
/// cluster comparison into a tautology in one line, with no fake binary, no
/// trait implementation and no compile error to notice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedOwner {
    uid: String,
}

/// The privileged reads genesis authority is issued from.
///
/// Every method is argument free, and the trait is sealed. See this module's
/// documentation: together those are the structural defence against a
/// right-hand side sourced from the intent, and neither is a stylistic choice.
pub trait GenesisSources: sealed::Sealed {
    /// Observes the incarnation of the mounted data directory.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error when the mount cannot be observed.
    fn observe_incarnation(&self) -> Result<ObservedIncarnation, GenesisEvidenceError>;

    /// Observes the live cluster object this instance runs under.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error when no live owner is observed.
    fn observe_owning_cluster(&self) -> Result<ObservedOwner, GenesisEvidenceError>;

    /// Observes the writable generation this attempt still holds.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error when this attempt holds none.
    fn observe_held_generation(&self) -> Result<DurableWritableGeneration, GenesisEvidenceError>;
}

/// The writable authority an attempt holds, as an argument-free observation.
///
/// Implemented for the agent's own attempt-private observer. It exists so the
/// production source can be built from a crate-private handle without that
/// handle appearing in a public signature.
pub trait HeldWritableAuthority: sealed::Sealed {
    /// Returns the generation this attempt still holds for at least `required`.
    fn generation_valid_for(&self, required: Duration) -> Option<DurableWritableGeneration>;
}

impl sealed::Sealed for WritableAuthorityObserver {}

impl HeldWritableAuthority for WritableAuthorityObserver {
    fn generation_valid_for(&self, required: Duration) -> Option<DurableWritableGeneration> {
        Self::generation_valid_for(self, required)
    }
}

/// Why a source could not be read.
///
/// Every variant is a refusal. None of them means "unknown, proceed".
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GenesisEvidenceError {
    /// The control-data program could not be run, or refused to run.
    #[error("the mounted data directory could not be inspected")]
    DataDirectoryUnreadable,
    /// The control-data output was not the one canonical shape this agent
    /// accepts, including a `pg_control` format it does not understand and a
    /// checksum warning on standard error.
    #[error("the mounted data directory did not report a canonical incarnation")]
    IncarnationNotCanonical,
    /// No live owning cluster object has been observed.
    #[error("no live owning cluster object is observed")]
    OwningClusterUnobserved,
    /// This attempt holds no writable generation.
    #[error("this attempt holds no writable generation")]
    WritableAuthorityAbsent,
}

/// Why genesis authority was not issued.
#[derive(Debug, Error)]
pub enum GenesisAuthorityError {
    /// The intent is not canonical, so it has no digest and names nothing.
    #[error("the genesis intent is not canonical: {0}")]
    NotCanonical(#[from] GenesisIntentError),
    /// A source could not be read, so nothing can be compared against it.
    #[error("genesis evidence could not be read: {0}")]
    EvidenceUnavailable(#[from] GenesisEvidenceError),
    /// No durable authority record exists. This is a refusal, and it is never
    /// repaired by writing one.
    #[error("no durable genesis authority is recorded")]
    AuthorityAbsent,
    /// A durable authority record exists and accepts a different intent.
    #[error("the durable genesis authority does not accept this intent")]
    AuthorityMismatch,
    /// The intent names a generation this attempt does not hold.
    #[error("the genesis intent names a generation this attempt does not hold")]
    GenerationMismatch,
    /// The intent names a cluster other than the live owning object.
    #[error("the genesis intent names a cluster other than the live owning object")]
    ClusterMismatch,
    /// The intent names an incarnation other than the mounted one.
    #[error("the genesis intent names an incarnation other than the mounted one")]
    IncarnationMismatch,
    /// Genesis has already been recorded for the mounted incarnation.
    #[error("genesis has already been recorded for the mounted incarnation")]
    AlreadyTaken,
    /// The durable store could not be read or written.
    #[error("the genesis authority store failed: {0}")]
    Store(#[from] GenesisStoreError),
}

/// Fail-closed durable-store validation or persistence failure.
#[derive(Debug, Error)]
pub enum GenesisStoreError {
    /// The caller supplied a relative, non-normal, or root directory path.
    #[error("genesis authority store path {path:?} must be absolute and normalized")]
    InvalidDirectoryPath {
        /// Rejected caller-provided path.
        path: PathBuf,
    },
    /// A directory did not meet the ownership or permission contract.
    #[error("unsafe genesis authority store directory {path:?}: {reason}")]
    UnsafeDirectory {
        /// Unsafe directory path.
        path: PathBuf,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A store entry was unsafe or unexpected.
    #[error("unsafe genesis authority store object {path:?}: {reason}")]
    UnsafeObject {
        /// Unsafe object path.
        path: PathBuf,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A record did not have the one canonical encoding.
    #[error("corrupt genesis {record} record at {path:?}")]
    CorruptRecord {
        /// Record kind.
        record: &'static str,
        /// Corrupt record path.
        path: PathBuf,
    },
    /// A validated record changed during one operation.
    #[error("genesis authority store state changed while validating {path:?}")]
    StateChanged {
        /// Changed path.
        path: PathBuf,
    },
    /// Another live handle currently owns the exclusive operation lock.
    #[error("genesis authority store is busy at {path:?}")]
    Busy {
        /// Contended store directory.
        path: PathBuf,
    },
    /// A filesystem operation failed before a definite durable result.
    #[error("genesis authority store could not {operation} at {path:?}: {source}")]
    Io {
        /// Stable operation description.
        operation: &'static str,
        /// Operation target.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthorityRecord {
    schema_version: String,
    #[serde(rename = "intentSHA256")]
    intent_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TakenRecord {
    schema_version: String,
    incarnation_key: String,
    #[serde(rename = "intentSHA256")]
    intent_sha256: String,
    seed_id: String,
    system_identifier: String,
    timeline: u32,
}

/// A caller-owned, dedicated genesis authority store directory, opened for
/// reading authority and for recording that genesis was taken.
///
/// This handle deliberately cannot write the authority record. See
/// [`GenesisAuthorityAcceptance`].
#[derive(Debug)]
pub struct GenesisAuthorityStore {
    directory: File,
    directory_path: PathBuf,
    expected_uid: u32,
}

/// Writes the durable authority record.
///
/// Held by whoever independently authenticated the intent against the control
/// plane, and never by the issuer. Splitting the write out of
/// [`GenesisAuthorityStore`] is the whole point: a component holding both could
/// answer its own question. [`GenesisAuthority`] owns a store and has no way to
/// reach one of these, and neither has anything outside this crate.
#[derive(Debug)]
pub struct GenesisAuthorityAcceptance {
    store: GenesisAuthorityStore,
}

/// Proof that genesis was authorized and durably recorded, in that order.
///
/// Move-only, non-serializable and privately constructed. Possessing one means
/// every comparison agreed and the taken record crossed its durability barrier
/// before this value existed.
///
/// It is exactly one fence. `PostgreSQL` must supply the second, and the only
/// part of that requirement the engine can actually carry is recovery state:
/// `StartTransaction` in `src/backend/access/transam/xact.c` sets
/// `XactReadOnly` from `RecoveryInProgress()` at the start of every
/// transaction, so no role can turn it off, and `standard_ProcessUtility` in
/// `src/backend/tcop/utility.c` then refuses `CREATE DATABASE` through
/// `PreventCommandIfReadOnly`. `default_transaction_read_only` is *not* part of
/// that fence: `guc_tables.c` declares it `PGC_USERSET`, so every role may set
/// it off and no privilege can be withheld to stop them.
#[derive(Debug)]
#[must_use = "an issued genesis permit must be handled explicitly"]
pub struct GenesisPermit {
    intent_sha256: String,
    incarnation: ObservedIncarnation,
    generation: DurableWritableGeneration,
}

impl GenesisPermit {
    /// Returns the digest of the exact intent this permit was issued for.
    #[must_use]
    pub fn intent_sha256(&self) -> &str {
        &self.intent_sha256
    }

    /// Returns the incarnation observed on the mount when this was issued.
    #[must_use]
    pub const fn incarnation(&self) -> &ObservedIncarnation {
        &self.incarnation
    }

    /// Returns the generation the creation has to be fenced against.
    #[must_use]
    pub const fn generation(&self) -> &DurableWritableGeneration {
        &self.generation
    }
}

/// The component that issues genesis authority.
///
/// It owns its sources and its store, so there is no way to hand it evidence.
pub struct GenesisAuthority<S> {
    sources: S,
    store: GenesisAuthorityStore,
}

impl<S: GenesisSources> GenesisAuthority<S> {
    /// Binds an issuer to its sources and to the store that belongs to one
    /// mounted data directory.
    ///
    /// There is deliberately no constructor that accepts a store: at most once
    /// has to be a property of the mount, and a caller that nominates the
    /// directory can nominate a second one and take genesis twice for one
    /// incarnation.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error when the store cannot be opened.
    pub fn for_mount(sources: S, data_directory: &Path) -> Result<Self, GenesisStoreError> {
        Ok(Self {
            sources,
            store: GenesisAuthorityStore::for_data_directory(data_directory)?,
        })
    }

    /// Issues authority for one first genesis, or refuses.
    ///
    /// The order is deliberate. Every comparison runs first, and the taken
    /// record is written last, so no refusal can leave genesis recorded; and
    /// the record is durable before this returns, so a crash after it leaves a
    /// shard that refuses rather than a catalog that comes back.
    ///
    /// # Errors
    ///
    /// Returns the first violated obligation. Every error is a refusal.
    pub fn issue(
        &mut self,
        intent: &CatalogGenesisIntent,
    ) -> Result<GenesisPermit, GenesisAuthorityError> {
        let intent_sha256 = intent.sha256()?;

        // Read off the mount before anything else: this is what keys the gate,
        // and the intent must not get a say in which key is looked up.
        let incarnation = self.sources.observe_incarnation()?;

        let authority = self
            .store
            .accepted_authority()?
            .ok_or(GenesisAuthorityError::AuthorityAbsent)?;
        if authority != intent_sha256 {
            return Err(GenesisAuthorityError::AuthorityMismatch);
        }

        let held = self.sources.observe_held_generation()?;
        if intent.generation.as_bytes() != held.canonical_bytes() {
            return Err(GenesisAuthorityError::GenerationMismatch);
        }

        // Independent of the generation above: a cluster object deleted and
        // recreated under the same name keeps the name and changes the UID,
        // and this attempt's Lease would survive that.
        let owning_cluster = self.sources.observe_owning_cluster()?;
        if intent.target.cluster_uid != owning_cluster.uid {
            return Err(GenesisAuthorityError::ClusterMismatch);
        }

        // All three, every time. Two of three matching is a different
        // incarnation that happens to agree.
        if intent.data_directory.seed_id != incarnation.seed_id
            || intent.data_directory.system_identifier != incarnation.system_identifier
            || intent.data_directory.timeline != incarnation.timeline
        {
            return Err(GenesisAuthorityError::IncarnationMismatch);
        }

        self.store.take_genesis(&incarnation, &intent_sha256)?;
        Ok(GenesisPermit {
            intent_sha256,
            incarnation,
            generation: held,
        })
    }
}

/// The production sources: the mounted data directory, the live owning cluster
/// object, and this attempt's writable authority.
pub struct MountedGenesisSources<A> {
    data_directory: PathBuf,
    controldata_executable: PathBuf,
    owning_cluster: tokio::sync::watch::Receiver<Option<ObservedOwner>>,
    authority: A,
    required_validity: Duration,
}

impl<A> sealed::Sealed for MountedGenesisSources<A> {}

impl<A> MountedGenesisSources<A> {
    /// Binds the production sources.
    ///
    /// `owning_cluster` carries whatever [`observe_owning_cluster`] last
    /// observed on a live object, and nothing else can be put on it. It starts
    /// empty, and an empty channel is a refusal rather than a pass.
    pub const fn new(
        data_directory: PathBuf,
        controldata_executable: PathBuf,
        owning_cluster: tokio::sync::watch::Receiver<Option<ObservedOwner>>,
        authority: A,
        required_validity: Duration,
    ) -> Self {
        Self {
            data_directory,
            controldata_executable,
            owning_cluster,
            authority,
            required_validity,
        }
    }
}

impl<A: HeldWritableAuthority> GenesisSources for MountedGenesisSources<A> {
    fn observe_incarnation(&self) -> Result<ObservedIncarnation, GenesisEvidenceError> {
        read_mounted_incarnation(&self.data_directory, &self.controldata_executable)
    }

    fn observe_owning_cluster(&self) -> Result<ObservedOwner, GenesisEvidenceError> {
        self.owning_cluster
            .borrow()
            .clone()
            .ok_or(GenesisEvidenceError::OwningClusterUnobserved)
    }

    fn observe_held_generation(&self) -> Result<DurableWritableGeneration, GenesisEvidenceError> {
        self.authority
            .generation_valid_for(self.required_validity)
            .ok_or(GenesisEvidenceError::WritableAuthorityAbsent)
    }
}

/// Observes the cluster that controls `owned`, an object read from the
/// Kubernetes API.
///
/// `metadata.ownerReferences` is part of the object body and is written by
/// whichever client created or updated the object — this repository does
/// exactly that in `crates/pgshard-orch/src/coordination.rs`. The API server
/// validates the field, notably that at most one reference sets
/// `controller: true`, but it does not author it. So this is not an
/// API-server-attested fact, and whoever can patch the owned object's metadata
/// chooses the answer it gives.
///
/// What it does rest on is survival across a delete and recreate. A cluster
/// object destroyed and recreated under the same name gets a new UID, while
/// the reference on an object that outlived it still names the dead one; the
/// comparison then refuses, which is the case this fence exists for and the
/// one the writable generation cannot see, because the Lease survives it.
///
/// Exactly one owner reference is accepted: a second would leave a choice of
/// answers, and a fence with a choice of answers is not a fence.
#[must_use]
pub fn observe_owning_cluster(owned: &DynamicObject, expected_name: &str) -> Option<ObservedOwner> {
    let references = owned
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default();
    let [reference] = references else {
        return None;
    };
    let OwnerReference {
        api_version,
        block_owner_deletion,
        controller,
        kind,
        name,
        uid,
    } = reference;
    if controller != &Some(true)
        || block_owner_deletion != &Some(true)
        || api_version != OWNING_CLUSTER_API_VERSION
        || kind != OWNING_CLUSTER_KIND
        || name != expected_name
        || uid.is_empty()
        || uid.len() > 253
        || uid.chars().any(char::is_control)
    {
        return None;
    }
    Some(ObservedOwner { uid: uid.clone() })
}

/// Observes the mounted incarnation by running the trusted control-data
/// program against the data directory.
///
/// The program rather than the bytes, deliberately. `pg_control` is a C struct
/// whose field offsets this code would have to reproduce by hand, and a layout
/// that drifted would be parsed into a confident wrong answer about which
/// incarnation is mounted rather than into a failure. `pg_controldata` is
/// version-matched to the server, and refusing any `pg_control version number`
/// other than the one supported turns drift into a refusal.
///
/// Standard error must be empty. `get_controlfile_by_exact_path` in
/// `src/common/controldata_utils.c` verifies the CRC and retries a torn read,
/// and `pg_controldata.c` then reports a surviving mismatch through
/// `pg_log_warning`, which writes to standard error. Requiring it empty is
/// therefore the checksum check, not a tidiness rule.
///
/// The uid the executable is checked against is this process's own effective
/// uid, taken here rather than accepted from a caller. A caller that supplies
/// both the program and the identity it is trusted under has anchored the check
/// to itself, and can hand the reader any program it likes.
///
/// # Errors
///
/// Returns a typed fail-closed error for any failure to observe exactly one
/// canonical incarnation.
pub(crate) fn read_mounted_incarnation(
    data_directory: &Path,
    controldata_executable: &Path,
) -> Result<ObservedIncarnation, GenesisEvidenceError> {
    validate_trusted_executable(controldata_executable)?;
    let output = Command::new(controldata_executable)
        .arg(data_directory)
        .env_clear()
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .env("PG_COLOR", "never")
        .env("TZ", "UTC")
        .stdin(Stdio::null())
        .output()
        .map_err(|_| GenesisEvidenceError::DataDirectoryUnreadable)?;
    if !output.status.success() {
        return Err(GenesisEvidenceError::DataDirectoryUnreadable);
    }
    if !output.stderr.is_empty() || output.stdout.len() > MAXIMUM_CONTROL_DATA_BYTES {
        return Err(GenesisEvidenceError::IncarnationNotCanonical);
    }
    parse_mounted_incarnation(&output.stdout)
}

/// Refuses a control-data program that anybody but its owner could replace.
fn validate_trusted_executable(executable: &Path) -> Result<(), GenesisEvidenceError> {
    let expected_uid = geteuid().as_raw();
    let descriptor = openat(
        rustix::fs::CWD,
        executable,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|_| GenesisEvidenceError::DataDirectoryUnreadable)?;
    let status = fstat(&descriptor).map_err(|_| GenesisEvidenceError::DataDirectoryUnreadable)?;
    let mode = status.st_mode;
    if FileType::from_raw_mode(status.st_mode) != FileType::RegularFile
        || (status.st_uid != expected_uid && status.st_uid != 0)
        || mode & 0o022 != 0
    {
        return Err(GenesisEvidenceError::DataDirectoryUnreadable);
    }
    Ok(())
}

fn parse_mounted_incarnation(output: &[u8]) -> Result<ObservedIncarnation, GenesisEvidenceError> {
    let value = |prefix: &[u8]| unique_control_data_value(output, prefix);
    if value(b"pg_control version number:").ok_or(GenesisEvidenceError::IncarnationNotCanonical)?
        != SUPPORTED_CONTROL_VERSION
    {
        return Err(GenesisEvidenceError::IncarnationNotCanonical);
    }
    let system_identifier = canonical_decimal(
        value(b"Database system identifier:")
            .ok_or(GenesisEvidenceError::IncarnationNotCanonical)?,
    )
    .ok_or(GenesisEvidenceError::IncarnationNotCanonical)?;
    let timeline = canonical_decimal(
        value(b"Latest checkpoint's TimeLineID:")
            .ok_or(GenesisEvidenceError::IncarnationNotCanonical)?,
    )
    .and_then(|value| u32::try_from(value).ok())
    .ok_or(GenesisEvidenceError::IncarnationNotCanonical)?;
    let nonce = value(b"Mock authentication nonce:")
        .ok_or(GenesisEvidenceError::IncarnationNotCanonical)?;

    // A zeroed identifier or nonce is what an unread, zeroed or freshly
    // allocated control file looks like, and neither names an incarnation.
    if system_identifier == 0 {
        return Err(GenesisEvidenceError::IncarnationNotCanonical);
    }
    if nonce.len() != 64
        || !nonce
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        || nonce.iter().all(|byte| *byte == b'0')
    {
        return Err(GenesisEvidenceError::IncarnationNotCanonical);
    }

    // The nonce is `initdb`'s own stamp: `InitControlFile` in
    // `src/backend/access/transam/xlog.c` fills it with `pg_strong_random` and
    // nothing writes it again, so it is unique to one atomic publication and
    // cannot be chosen by whoever writes an intent. It is hashed rather than
    // republished because `auth-scram.c` derives mock SCRAM challenges from it,
    // and a fence must not hand out authentication material to earn its keep.
    let mut hash = Sha256::new();
    frame(&mut hash, SEED_DIGEST_DOMAIN);
    hash.update(u64::try_from(nonce.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(nonce);
    Ok(ObservedIncarnation {
        seed_id: lower_hex(&hash.finalize()),
        system_identifier,
        timeline,
    })
}

/// Returns the single value carried under `prefix`, or nothing.
///
/// A repeated label produces nothing rather than the first match: a program
/// that printed a label twice is not one whose output has a single meaning.
fn unique_control_data_value<'a>(output: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let mut values = output
        .split(|byte| *byte == b'\n')
        .filter_map(|line| line.strip_prefix(prefix).map(<[u8]>::trim_ascii));
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

/// Accepts only the text `u64::to_string` would have produced.
fn canonical_decimal(text: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(text).ok()?;
    let value = text.parse::<u64>().ok()?;
    (value.to_string() == text).then_some(value)
}

impl GenesisAuthorityStore {
    /// Opens, or creates, the store that belongs to one mounted data
    /// directory.
    ///
    /// The location is derived rather than chosen, which is what makes at most
    /// once a property of the mount. Creating the directory grants nothing;
    /// only a record does, and this handle cannot write the record that grants.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error for unsafe paths, metadata,
    /// ownership, permissions, or filesystem failures.
    pub(crate) fn for_data_directory(data_directory: &Path) -> Result<Self, GenesisStoreError> {
        Self::open_or_create_for_uid(&store_directory(data_directory)?, geteuid().as_raw())
    }

    fn open_or_create_for_uid(
        directory_path: &Path,
        expected_uid: u32,
    ) -> Result<Self, GenesisStoreError> {
        validate_normal_absolute_path(directory_path)?;
        let parent_path =
            directory_path
                .parent()
                .ok_or_else(|| GenesisStoreError::InvalidDirectoryPath {
                    path: directory_path.to_owned(),
                })?;
        let name =
            directory_path
                .file_name()
                .ok_or_else(|| GenesisStoreError::InvalidDirectoryPath {
                    path: directory_path.to_owned(),
                })?;
        let parent = openat(
            rustix::fs::CWD,
            parent_path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| io_error("open parent directory", parent_path, source.into()))?;
        let created = match mkdirat(&parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
            Ok(()) => true,
            Err(source) if source == Errno::EXIST => false,
            Err(source) => {
                return Err(io_error("create directory", directory_path, source.into()));
            }
        };
        let descriptor = openat(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| io_error("open directory", directory_path, source.into()))?;
        let directory = File::from(descriptor);
        let store = Self {
            directory,
            directory_path: directory_path.to_owned(),
            expected_uid,
        };
        store.validate_directory()?;
        if created {
            store
                .directory
                .sync_all()
                .map_err(|source| io_error("flush new directory", directory_path, source))?;
            File::from(parent)
                .sync_all()
                .map_err(|source| io_error("flush parent directory", parent_path, source))?;
        }
        store.validate_entries()?;
        Ok(store)
    }

    /// Returns the durably accepted intent digest, or nothing.
    ///
    /// `Ok(None)` is an absent record, which every caller has to treat as a
    /// refusal. This method never creates or repairs the record.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error for any unsafe or non-canonical
    /// state.
    pub fn accepted_authority(&mut self) -> Result<Option<String>, GenesisStoreError> {
        self.with_exclusive_lock(|store| {
            store.validate_entries()?;
            let Some(contents) = store.read_record(AUTHORITY_RECORD)? else {
                return Ok(None);
            };
            let path = store.directory_path.join(AUTHORITY_RECORD);
            let record: AuthorityRecord = parse_canonical(&contents, "authority", &path)?;
            if record.schema_version != GENESIS_AUTHORITY_RECORD_VERSION
                || !is_canonical_digest(&record.intent_sha256)
            {
                return Err(GenesisStoreError::CorruptRecord {
                    record: "authority",
                    path,
                });
            }
            Ok(Some(record.intent_sha256))
        })
    }

    /// Records that genesis has been taken for `incarnation`, at most once.
    ///
    /// Returns [`GenesisStoreError::StateChanged`] wrapped by the caller when
    /// the record is already there; the record is fsynced, installed under an
    /// immutable name, and re-read before this returns, so a successful return
    /// means a later attempt on the same mount finds it.
    fn take_genesis(
        &mut self,
        incarnation: &ObservedIncarnation,
        intent_sha256: &str,
    ) -> Result<(), GenesisAuthorityError> {
        let key = incarnation.key();
        let record = TakenRecord {
            schema_version: GENESIS_TAKEN_RECORD_VERSION.to_owned(),
            incarnation_key: key.clone(),
            intent_sha256: intent_sha256.to_owned(),
            seed_id: incarnation.seed_id.clone(),
            system_identifier: incarnation.system_identifier.to_string(),
            timeline: incarnation.timeline,
        };
        let encoded = encode_canonical(&record)?;
        let final_name = format!("{TAKEN_PREFIX}{key}");
        let staging_name = format!(".{final_name}.tmp");
        self.with_exclusive_lock(|store| {
            store.validate_entries()?;
            if store.read_record(&final_name)?.is_some() {
                return Ok(true);
            }
            store.install(&staging_name, &final_name, &encoded)?;
            Ok(false)
        })
        .map_err(GenesisAuthorityError::Store)
        .and_then(|already| {
            if already {
                Err(GenesisAuthorityError::AlreadyTaken)
            } else {
                Ok(())
            }
        })
    }

    fn with_exclusive_lock<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, GenesisStoreError>,
    ) -> Result<T, GenesisStoreError> {
        flock(&self.directory, FlockOperation::NonBlockingLockExclusive).map_err(|source| {
            if source == Errno::WOULDBLOCK {
                GenesisStoreError::Busy {
                    path: self.directory_path.clone(),
                }
            } else {
                io_error("lock directory", &self.directory_path, source.into())
            }
        })?;
        let result = operation(self);
        let unlock = flock(&self.directory, FlockOperation::Unlock)
            .map_err(|source| io_error("unlock directory", &self.directory_path, source.into()));
        match (result, unlock) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn validate_directory(&self) -> Result<(), GenesisStoreError> {
        let status = fstat(&self.directory).map_err(|source| {
            io_error(
                "read directory metadata",
                &self.directory_path,
                source.into(),
            )
        })?;
        let mode = status.st_mode;
        if FileType::from_raw_mode(status.st_mode) != FileType::Directory {
            return Err(GenesisStoreError::UnsafeDirectory {
                path: self.directory_path.clone(),
                reason: "not a directory",
            });
        }
        if status.st_uid != self.expected_uid {
            return Err(GenesisStoreError::UnsafeDirectory {
                path: self.directory_path.clone(),
                reason: "owned by another user",
            });
        }
        if mode & 0o7_777 != 0o700 {
            return Err(GenesisStoreError::UnsafeDirectory {
                path: self.directory_path.clone(),
                reason: "permissions are not exactly 0700",
            });
        }
        Ok(())
    }

    fn validate_entries(&self) -> Result<(), GenesisStoreError> {
        self.validate_directory()?;
        let mut entries = Dir::read_from(&self.directory).map_err(|source| {
            io_error(
                "open directory entries",
                &self.directory_path,
                source.into(),
            )
        })?;
        let mut seen = 0_usize;
        while let Some(entry) = entries.read() {
            let entry = entry.map_err(|source| {
                io_error(
                    "read directory entries",
                    &self.directory_path,
                    source.into(),
                )
            })?;
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            seen += 1;
            if seen > MAXIMUM_DIRECTORY_ENTRIES {
                return Err(GenesisStoreError::UnsafeDirectory {
                    path: self.directory_path.clone(),
                    reason: "too many entries",
                });
            }
            let name = std::str::from_utf8(name).map_err(|_| GenesisStoreError::UnsafeObject {
                path: self.directory_path.clone(),
                reason: "entry name is not UTF-8",
            })?;
            if !is_known_entry(name) {
                return Err(GenesisStoreError::UnsafeObject {
                    path: self.directory_path.join(name),
                    reason: "unexpected entry",
                });
            }
        }
        Ok(())
    }

    /// Opens one record by name and returns its exact bytes.
    ///
    /// `O_NOFOLLOW` plus an explicit regular-file, single-link, exact-mode and
    /// ownership check: a symlink, a hard link or a group-writable record is
    /// refused rather than read.
    fn read_record(&self, name: &str) -> Result<Option<Vec<u8>>, GenesisStoreError> {
        let path = self.directory_path.join(name);
        let descriptor = match openat(
            &self.directory,
            name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(Errno::NOENT | Errno::LOOP) => return Ok(None),
            Err(source) => return Err(io_error("open record", &path, source.into())),
        };
        let mut file = File::from(descriptor);
        let status = fstat(&file)
            .map_err(|source| io_error("read record metadata", &path, source.into()))?;
        let mode = status.st_mode;
        if FileType::from_raw_mode(status.st_mode) != FileType::RegularFile
            || status.st_nlink != 1
            || status.st_uid != self.expected_uid
            || mode & 0o077 != 0
        {
            return Err(GenesisStoreError::UnsafeObject {
                path,
                reason: "record is not an exclusively owned regular file",
            });
        }
        let size = u64::try_from(status.st_size).unwrap_or(u64::MAX);
        if size > MAXIMUM_RECORD_BYTES {
            return Err(GenesisStoreError::UnsafeObject {
                path,
                reason: "record is oversized",
            });
        }
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|source| io_error("read record", &path, source))?;
        if u64::try_from(contents.len()).unwrap_or(u64::MAX) != size {
            return Err(GenesisStoreError::StateChanged { path });
        }
        Ok(Some(contents))
    }

    /// Writes `contents` durably under `final_name`, or fails without leaving
    /// a partial record visible under that name.
    fn install(
        &self,
        staging_name: &str,
        final_name: &str,
        contents: &[u8],
    ) -> Result<(), GenesisStoreError> {
        let staging_path = self.directory_path.join(staging_name);
        let final_path = self.directory_path.join(final_name);
        // A staging inode left by an interrupted attempt is never adopted: its
        // durability is unknown, and adopting it would install unflushed bytes.
        match unlinkat(&self.directory, staging_name, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(source) => {
                return Err(io_error(
                    "remove interrupted staging record",
                    &staging_path,
                    source.into(),
                ));
            }
        }
        let descriptor = openat(
            &self.directory,
            staging_name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|source| io_error("create staging record", &staging_path, source.into()))?;
        let mut file = File::from(descriptor);
        file.write_all(contents)
            .and_then(|()| file.set_permissions(std::fs::Permissions::from_mode(0o400)))
            .and_then(|()| file.sync_all())
            .map_err(|source| io_error("write staging record", &staging_path, source))?;
        drop(file);
        renameat_with(
            &self.directory,
            staging_name,
            &self.directory,
            final_name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|source| io_error("install record", &final_path, source.into()))?;
        self.directory
            .sync_all()
            .map_err(|source| io_error("flush directory", &self.directory_path, source))?;
        // Re-read from the installed name rather than trusting the write path:
        // the record is only durable evidence once it can be read back exactly.
        let installed =
            self.read_record(final_name)?
                .ok_or_else(|| GenesisStoreError::StateChanged {
                    path: final_path.clone(),
                })?;
        if installed != contents {
            return Err(GenesisStoreError::StateChanged { path: final_path });
        }
        Ok(())
    }
}

impl GenesisAuthorityAcceptance {
    /// Binds the authority writer to a store.
    ///
    /// Restricted by its argument rather than by a visibility modifier, which
    /// is the stronger of the two here: [`GenesisAuthorityStore`] has no public
    /// constructor, so this is unreachable from outside the crate, and the
    /// signature says what the requirement is instead of hiding it.
    #[must_use]
    pub const fn new(store: GenesisAuthorityStore) -> Self {
        Self { store }
    }

    /// Durably accepts one exact intent digest as genesis authority.
    ///
    /// Recording an authority is the act of authenticating the intent against
    /// the control plane, and it belongs to whoever did that. Recording the
    /// same digest again is a replay and succeeds; recording a different one
    /// over a live authority is refused.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error for a non-canonical digest, a
    /// conflicting live authority, or any filesystem failure.
    pub fn accept(&mut self, intent_sha256: &str) -> Result<(), GenesisStoreError> {
        if !is_canonical_digest(intent_sha256) {
            return Err(GenesisStoreError::CorruptRecord {
                record: "authority",
                path: self.store.directory_path.join(AUTHORITY_RECORD),
            });
        }
        let record = AuthorityRecord {
            schema_version: GENESIS_AUTHORITY_RECORD_VERSION.to_owned(),
            intent_sha256: intent_sha256.to_owned(),
        };
        let encoded = encode_canonical(&record)?;
        self.store.with_exclusive_lock(|store| {
            store.validate_entries()?;
            if let Some(existing) = store.read_record(AUTHORITY_RECORD)? {
                return if existing == encoded {
                    Ok(())
                } else {
                    Err(GenesisStoreError::StateChanged {
                        path: store.directory_path.join(AUTHORITY_RECORD),
                    })
                };
            }
            store.install(AUTHORITY_STAGING, AUTHORITY_RECORD, &encoded)
        })
    }
}

/// Accepts only the entry names this store owns.
fn is_known_entry(name: &str) -> bool {
    if name == AUTHORITY_RECORD || name == AUTHORITY_STAGING {
        return true;
    }
    let stem = name
        .strip_prefix('.')
        .and_then(|name| name.strip_suffix(".tmp"))
        .unwrap_or(name);
    stem.strip_prefix(TAKEN_PREFIX)
        .is_some_and(is_canonical_key)
}

fn is_canonical_key(key: &str) -> bool {
    key.len() == 64
        && key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Rejects a digest that is not lower-case hexadecimal SHA-256, and rejects the
/// all-zero digest.
///
/// Sixty-four zeroes is what an unset field, a zeroed record and a forged
/// placeholder all look like. A zero that compares as permissive is how two
/// nodes end up believing they are both primary, so it is refused by name.
fn is_canonical_digest(digest: &str) -> bool {
    is_canonical_key(digest) && digest.bytes().any(|byte| byte != b'0')
}

fn parse_canonical<T: for<'de> Deserialize<'de> + Serialize>(
    contents: &[u8],
    record: &'static str,
    path: &Path,
) -> Result<T, GenesisStoreError> {
    let corrupt = || GenesisStoreError::CorruptRecord {
        record,
        path: path.to_owned(),
    };
    let parsed: T = serde_json::from_slice(contents).map_err(|_| corrupt())?;
    // Round-tripped, so exactly one encoding of one record is accepted: key
    // order, whitespace and duplicate keys all fail here rather than being
    // normalized into a record that was never written.
    if encode_canonical(&parsed)? != contents {
        return Err(corrupt());
    }
    Ok(parsed)
}

fn encode_canonical<T: Serialize>(record: &T) -> Result<Vec<u8>, GenesisStoreError> {
    serde_json::to_vec(record).map_err(|source| GenesisStoreError::Io {
        operation: "encode record",
        path: PathBuf::new(),
        source: source.into(),
    })
}

/// The store directory that belongs to one mounted data directory.
///
/// Beside `PGDATA` rather than inside it, alongside the catalog-activation
/// journal, so nothing this agent owns lands in a directory `PostgreSQL`
/// manages. A data directory republished by a fresh `initdb` carries a fresh
/// nonce and therefore a fresh key, so a record left behind by the incarnation
/// before it blocks nothing.
const STORE_DIRECTORY: &str = ".pgshard-catalog-genesis";

fn store_directory(data_directory: &Path) -> Result<PathBuf, GenesisStoreError> {
    validate_normal_absolute_path(data_directory)?;
    data_directory
        .parent()
        .map(|parent| parent.join(STORE_DIRECTORY))
        .ok_or_else(|| GenesisStoreError::InvalidDirectoryPath {
            path: data_directory.to_owned(),
        })
}

fn validate_normal_absolute_path(path: &Path) -> Result<(), GenesisStoreError> {
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(GenesisStoreError::InvalidDirectoryPath {
            path: path.to_owned(),
        });
    }
    let mut named = 0_usize;
    for component in components {
        if !matches!(component, Component::Normal(_)) {
            return Err(GenesisStoreError::InvalidDirectoryPath {
                path: path.to_owned(),
            });
        }
        named += 1;
    }
    if named == 0 {
        return Err(GenesisStoreError::InvalidDirectoryPath {
            path: path.to_owned(),
        });
    }
    Ok(())
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> GenesisStoreError {
    GenesisStoreError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn frame(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
        text
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use pgshard_types::ShardId;
    use pgshard_types::genesis_intent::{
        GENESIS_INTENT_VERSION, GenesisDataDirectory, GenesisTarget,
    };
    use tempfile::TempDir;

    /// One named mutation applied to an otherwise agreeing fixture.
    type IntentMutation = Box<dyn Fn(&mut CatalogGenesisIntent)>;
    type OwnerMutation = Box<dyn Fn(&mut OwnerReference)>;
    type RefusalPredicate = fn(&GenesisAuthorityError) -> bool;

    const LIVE_CLUSTER_UID: &str = "11111111-2222-3333-4444-555555555555";
    const OTHER_CLUSTER_UID: &str = "99999999-2222-3333-4444-555555555555";
    const MOUNTED_NONCE: &str = "9f1c3a7e5b0d2846af93c15e70b8d4629a5f0e13c7482b6d95a0fe3172c48b5d";
    const OTHER_NONCE: &str = "1122334455667788990011223344556677889900112233445566778899001122";
    const MOUNTED_SYSTEM_IDENTIFIER: u64 = 7_248_119_402_113_558_016;

    fn control_data(
        version: &str,
        system_identifier: &str,
        timeline: &str,
        nonce: &str,
    ) -> Vec<u8> {
        format!(
            "pg_control version number:            {version}\n\
             Catalog version number:               202506291\n\
             Database system identifier:           {system_identifier}\n\
             Database cluster state:               in production\n\
             Latest checkpoint's TimeLineID:       {timeline}\n\
             Latest checkpoint's PrevTimeLineID:   1\n\
             Mock authentication nonce:            {nonce}\n"
        )
        .into_bytes()
    }

    fn mounted_control_data() -> Vec<u8> {
        control_data(
            "1800",
            &MOUNTED_SYSTEM_IDENTIFIER.to_string(),
            "1",
            MOUNTED_NONCE,
        )
    }

    /// Derived by the production reader rather than written out here: a
    /// hand-written seed would let the reader and the fixture drift apart and
    /// still agree.
    fn mounted_incarnation() -> ObservedIncarnation {
        parse_mounted_incarnation(&mounted_control_data()).expect("the fixture control data parses")
    }

    fn held_generation(cluster_uid: &str, term: u64) -> DurableWritableGeneration {
        DurableWritableGeneration::new(
            "demo".to_owned(),
            cluster_uid.to_owned(),
            ShardId(0),
            "database".to_owned(),
            "demo-shard-0000-writable".to_owned(),
            "dddddddd-1111-2222-3333-444444444444".to_owned(),
            "demo-shard-0000-0".to_owned(),
            term,
        )
        .expect("the fixture generation is valid")
    }

    fn generation_text(generation: &DurableWritableGeneration) -> String {
        String::from_utf8(generation.canonical_bytes()).expect("canonical bytes are UTF-8")
    }

    fn digest(seed: u8) -> String {
        (0..32).fold(String::new(), |mut text, index| {
            use std::fmt::Write as _;
            let _ = write!(text, "{:02x}", seed.wrapping_add(index));
            text
        })
    }

    /// An intent that agrees with every source below. Every rejection in this
    /// module is measured against it, so it has to be issuable or the
    /// rejections prove nothing.
    fn agreeing_intent() -> CatalogGenesisIntent {
        let incarnation = mounted_incarnation();
        CatalogGenesisIntent {
            schema_version: GENESIS_INTENT_VERSION.to_owned(),
            request_sha256: digest(1),
            generation: generation_text(&held_generation(LIVE_CLUSTER_UID, 7)),
            target: GenesisTarget {
                cluster_uid: LIVE_CLUSTER_UID.to_owned(),
                shard: 0,
                members: 1,
            },
            data_directory: GenesisDataDirectory {
                seed_id: incarnation.seed_id.clone(),
                system_identifier: incarnation.system_identifier,
                timeline: incarnation.timeline,
            },
        }
    }

    #[derive(Clone)]
    struct TestSources {
        incarnation: Result<ObservedIncarnation, GenesisEvidenceError>,
        owning_cluster: Result<ObservedOwner, GenesisEvidenceError>,
        generation: Result<DurableWritableGeneration, GenesisEvidenceError>,
    }

    // Only reachable because these tests are inside the module that owns the
    // seal. The same implementation in any other crate does not compile, which
    // `tests/genesis_authority_seal.rs` and the module's doc tests pin.
    impl sealed::Sealed for TestSources {}

    impl TestSources {
        /// The live facts the mount, the API and the Lease actually report.
        /// Nothing here is derived from an intent, which is the whole point.
        fn live() -> Self {
            Self {
                incarnation: Ok(mounted_incarnation()),
                owning_cluster: Ok(ObservedOwner {
                    uid: LIVE_CLUSTER_UID.to_owned(),
                }),
                generation: Ok(held_generation(LIVE_CLUSTER_UID, 7)),
            }
        }
    }

    impl GenesisSources for TestSources {
        fn observe_incarnation(&self) -> Result<ObservedIncarnation, GenesisEvidenceError> {
            self.incarnation.clone()
        }

        fn observe_owning_cluster(&self) -> Result<ObservedOwner, GenesisEvidenceError> {
            self.owning_cluster.clone()
        }

        fn observe_held_generation(
            &self,
        ) -> Result<DurableWritableGeneration, GenesisEvidenceError> {
            self.generation.clone()
        }
    }

    struct Fixture {
        root: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                root: TempDir::new().expect("create a store root"),
            }
        }

        /// The mount. The store location is derived from it rather than
        /// chosen, exactly as in production.
        fn data_directory(&self) -> PathBuf {
            self.root.path().join("docker")
        }

        fn directory(&self) -> PathBuf {
            self.root.path().join(STORE_DIRECTORY)
        }

        fn store(&self) -> GenesisAuthorityStore {
            GenesisAuthorityStore::for_data_directory(&self.data_directory())
                .expect("open the store")
        }

        fn accept(&self, intent_sha256: &str) {
            GenesisAuthorityAcceptance::new(self.store())
                .accept(intent_sha256)
                .expect("record durable authority");
        }

        fn issuer(&self, sources: TestSources) -> GenesisAuthority<TestSources> {
            GenesisAuthority::for_mount(sources, &self.data_directory()).expect("open the store")
        }

        fn entries(&self) -> BTreeSet<String> {
            std::fs::read_dir(self.directory())
                .expect("list the store")
                .map(|entry| {
                    entry
                        .expect("read an entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect()
        }

        fn taken_entries(&self) -> BTreeSet<String> {
            self.entries()
                .into_iter()
                .filter(|name| name.starts_with(TAKEN_PREFIX))
                .collect()
        }
    }

    fn refusal(fixture: &Fixture, sources: TestSources) -> GenesisAuthorityError {
        fixture
            .issuer(sources)
            .issue(&agreeing_intent())
            .expect_err("this intent must be refused")
    }

    #[test]
    fn an_intent_that_agrees_with_every_source_is_issued_once() {
        let fixture = Fixture::new();
        let intent = agreeing_intent();
        let expected = intent.sha256().expect("the fixture is canonical");
        fixture.accept(&expected);

        let permit = fixture
            .issuer(TestSources::live())
            .issue(&intent)
            .expect("every source agrees, so authority is issued");

        assert_eq!(permit.intent_sha256(), expected);
        assert_eq!(permit.incarnation(), &mounted_incarnation());
        assert_eq!(permit.generation(), &held_generation(LIVE_CLUSTER_UID, 7));
        assert_eq!(fixture.taken_entries().len(), 1);
    }

    /// The exact forgery [`pgshard_types::genesis_intent`] pins as canonical:
    /// every field chosen by whoever wanted genesis to happen. It passes
    /// `validate`, and it must not pass here.
    #[test]
    fn a_wholly_self_chosen_intent_is_refused_however_canonical_it_is() {
        let self_chosen = CatalogGenesisIntent {
            schema_version: GENESIS_INTENT_VERSION.to_owned(),
            request_sha256: "0".repeat(64),
            generation: generation_text(&held_generation(OTHER_CLUSTER_UID, 1)),
            target: GenesisTarget {
                cluster_uid: OTHER_CLUSTER_UID.to_owned(),
                shard: 0,
                members: 1,
            },
            data_directory: GenesisDataDirectory {
                seed_id: "forged-seed".to_owned(),
                system_identifier: 1,
                timeline: 1,
            },
        };
        assert_eq!(
            self_chosen.validate(),
            Ok(()),
            "the forgery has to be canonical, or this proves nothing about authority"
        );

        let fixture = Fixture::new();
        // Even with its own digest durably accepted, every live source still
        // disagrees with it.
        fixture.accept(&self_chosen.sha256().expect("the forgery is canonical"));
        let error = fixture
            .issuer(TestSources::live())
            .issue(&self_chosen)
            .expect_err("a self-chosen intent must never be issued");
        assert!(matches!(error, GenesisAuthorityError::GenerationMismatch));
        assert!(fixture.taken_entries().is_empty());
    }

    /// If any right-hand side were copied out of the intent instead of read
    /// from its source, the matching case here would pass. Each mutation has
    /// its own durably accepted authority, so the whole-digest check cannot be
    /// what rejects it and each live comparison is reached on its own.
    #[test]
    fn each_live_comparison_is_made_against_its_own_source() {
        let cases: Vec<(&str, IntentMutation, RefusalPredicate)> = vec![
            (
                "generation term",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.generation = generation_text(&held_generation(LIVE_CLUSTER_UID, 8));
                }),
                |error| matches!(error, GenesisAuthorityError::GenerationMismatch),
            ),
            (
                "cluster",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.target.cluster_uid = OTHER_CLUSTER_UID.to_owned();
                    it.generation = generation_text(&held_generation(OTHER_CLUSTER_UID, 7));
                }),
                // The generation moves with the cluster UID, because the
                // contract refuses an intent whose two cluster UIDs disagree.
                |error| matches!(error, GenesisAuthorityError::GenerationMismatch),
            ),
            (
                "seed",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.seed_id = "a".repeat(64);
                }),
                |error| matches!(error, GenesisAuthorityError::IncarnationMismatch),
            ),
            (
                "system identifier",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.system_identifier = MOUNTED_SYSTEM_IDENTIFIER ^ 1;
                }),
                |error| matches!(error, GenesisAuthorityError::IncarnationMismatch),
            ),
        ];

        for (binding, mutate, expected) in cases {
            let mut mutated = agreeing_intent();
            mutate(&mut mutated);
            assert_ne!(
                mutated,
                agreeing_intent(),
                "the {binding} mutation changed nothing"
            );
            let fixture = Fixture::new();
            fixture.accept(&mutated.sha256().expect("the mutation stays canonical"));

            let Err(error) = fixture.issuer(TestSources::live()).issue(&mutated) else {
                panic!("the {binding} mutation must be refused");
            };
            assert!(
                expected(&error),
                "the {binding} mutation was refused for the wrong reason: {error}"
            );
            assert!(fixture.taken_entries().is_empty());
        }
    }

    /// The cluster UID has a source of its own, and it is not the generation:
    /// a cluster object deleted and recreated under the same name keeps the
    /// name, changes the UID, and leaves this attempt's Lease standing.
    #[test]
    fn the_cluster_uid_is_compared_against_the_live_owning_object() {
        let fixture = Fixture::new();
        let intent = agreeing_intent();
        fixture.accept(&intent.sha256().expect("canonical"));

        let mut sources = TestSources::live();
        sources.owning_cluster = Ok(ObservedOwner {
            uid: OTHER_CLUSTER_UID.to_owned(),
        });
        let error = fixture
            .issuer(sources)
            .issue(&intent)
            .expect_err("a recreated cluster object must refuse genesis");
        assert!(matches!(error, GenesisAuthorityError::ClusterMismatch));
        assert!(fixture.taken_entries().is_empty());
    }

    /// The timeline is one of three, and the other two agreeing does not
    /// excuse it: a promoted cluster is not at first genesis.
    #[test]
    fn all_three_incarnation_values_are_compared() {
        let live = mounted_incarnation();
        let variants: [ObservedIncarnation; 3] = [
            ObservedIncarnation {
                seed_id: "b".repeat(64),
                ..live.clone()
            },
            ObservedIncarnation {
                system_identifier: live.system_identifier ^ 1,
                ..live.clone()
            },
            ObservedIncarnation {
                timeline: 2,
                ..live.clone()
            },
        ];

        for observed in variants {
            let fixture = Fixture::new();
            let intent = agreeing_intent();
            fixture.accept(&intent.sha256().expect("canonical"));
            let mut sources = TestSources::live();
            sources.incarnation = Ok(observed.clone());

            let error = fixture
                .issuer(sources)
                .issue(&intent)
                .expect_err("a different mounted incarnation must refuse genesis");
            assert!(
                matches!(error, GenesisAuthorityError::IncarnationMismatch),
                "unexpected refusal for {observed:?}: {error}"
            );
            assert!(fixture.taken_entries().is_empty());
        }
    }

    /// The `members` gap the contract names: it is bound to no live fact, so
    /// only the whole-intent digest can hold it. Every other signed component
    /// rides the same check.
    #[test]
    fn the_whole_intent_digest_is_authenticated_so_no_signed_field_can_vary() {
        let accepted = agreeing_intent();
        let authority = accepted.sha256().expect("canonical");

        let mutations: Vec<(&str, IntentMutation)> = vec![
            (
                "members",
                Box::new(|it: &mut CatalogGenesisIntent| it.target.members = 3),
            ),
            (
                "request digest",
                Box::new(|it: &mut CatalogGenesisIntent| it.request_sha256 = digest(9)),
            ),
            (
                "generation",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.generation = generation_text(&held_generation(LIVE_CLUSTER_UID, 8));
                }),
            ),
            (
                "cluster",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.target.cluster_uid = OTHER_CLUSTER_UID.to_owned();
                    it.generation = generation_text(&held_generation(OTHER_CLUSTER_UID, 7));
                }),
            ),
            (
                "seed",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.seed_id = "c".repeat(64);
                }),
            ),
            (
                "system identifier",
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.system_identifier = MOUNTED_SYSTEM_IDENTIFIER ^ 1;
                }),
            ),
        ];

        for (binding, mutate) in mutations {
            let mut mutated = accepted.clone();
            mutate(&mut mutated);
            let fixture = Fixture::new();
            fixture.accept(&authority);

            let Err(error) = fixture.issuer(TestSources::live()).issue(&mutated) else {
                panic!("the {binding} mutation must be refused");
            };
            assert!(
                matches!(error, GenesisAuthorityError::AuthorityMismatch),
                "the {binding} mutation escaped the whole-intent digest: {error}"
            );
            assert!(fixture.taken_entries().is_empty());
        }
    }

    /// `members` in particular: it agrees with every live source by
    /// construction, because no live source mentions it.
    #[test]
    fn a_varied_member_count_is_refused_by_the_digest_alone() {
        let fixture = Fixture::new();
        fixture.accept(&agreeing_intent().sha256().expect("canonical"));
        let mut mutated = agreeing_intent();
        mutated.target.members = 3;

        let error = fixture
            .issuer(TestSources::live())
            .issue(&mutated)
            .expect_err("an unbound member count must be refused");
        assert!(matches!(error, GenesisAuthorityError::AuthorityMismatch));
    }

    #[test]
    fn absent_authority_is_refused_and_never_repaired() {
        let fixture = Fixture::new();
        let error = refusal(&fixture, TestSources::live());

        assert!(matches!(error, GenesisAuthorityError::AuthorityAbsent));
        assert!(
            fixture.entries().is_empty(),
            "a refusal wrote something into the store: {:?}",
            fixture.entries()
        );
    }

    #[test]
    fn a_zero_valued_authority_digest_is_refused() {
        let fixture = Fixture::new();
        let mut acceptance = GenesisAuthorityAcceptance::new(fixture.store());
        assert!(
            acceptance.accept(&"0".repeat(64)).is_err(),
            "a zero digest must never be accepted as authority"
        );

        // And it is refused on the read path too, in case one was written by
        // an older writer or by a zeroed store.
        write_record(
            &fixture,
            AUTHORITY_RECORD,
            &serde_json::to_vec(&AuthorityRecord {
                schema_version: GENESIS_AUTHORITY_RECORD_VERSION.to_owned(),
                intent_sha256: "0".repeat(64),
            })
            .expect("encode"),
        );
        assert!(matches!(
            refusal(&fixture, TestSources::live()),
            GenesisAuthorityError::Store(GenesisStoreError::CorruptRecord { .. })
        ));
    }

    #[test]
    fn a_non_canonical_authority_record_is_refused() {
        let canonical = serde_json::to_vec(&AuthorityRecord {
            schema_version: GENESIS_AUTHORITY_RECORD_VERSION.to_owned(),
            intent_sha256: digest(3),
        })
        .expect("encode");

        for (reason, contents) in [
            ("empty", Vec::new()),
            ("truncated", canonical[..canonical.len() - 3].to_vec()),
            (
                "another schema version",
                String::from_utf8(canonical.clone())
                    .expect("UTF-8")
                    .replace("authority.v1", "authority.v2")
                    .into_bytes(),
            ),
            (
                "uppercase digest",
                String::from_utf8(canonical.clone())
                    .expect("UTF-8")
                    .replace(&digest(3), &digest(3).to_uppercase())
                    .into_bytes(),
            ),
            (
                "unknown field",
                format!(
                    "{},\"unexpected\":true}}",
                    String::from_utf8(canonical.clone())
                        .expect("UTF-8")
                        .strip_suffix('}')
                        .expect("object JSON")
                )
                .into_bytes(),
            ),
            (
                "reordered keys",
                format!(
                    "{{\"intentSHA256\":\"{}\",\"schemaVersion\":\"{GENESIS_AUTHORITY_RECORD_VERSION}\"}}",
                    digest(3)
                )
                .into_bytes(),
            ),
        ] {
            let fixture = Fixture::new();
            write_record(&fixture, AUTHORITY_RECORD, &contents);
            let error = refusal(&fixture, TestSources::live());
            assert!(
                matches!(
                    error,
                    GenesisAuthorityError::Store(GenesisStoreError::CorruptRecord { .. })
                ),
                "the {reason} authority record was not refused as corrupt: {error}"
            );
            assert!(fixture.taken_entries().is_empty());
        }
    }

    #[test]
    fn authority_for_another_intent_is_refused() {
        let fixture = Fixture::new();
        let mut other = agreeing_intent();
        other.request_sha256 = digest(9);
        fixture.accept(&other.sha256().expect("canonical"));

        let error = refusal(&fixture, TestSources::live());
        assert!(matches!(error, GenesisAuthorityError::AuthorityMismatch));
        assert!(fixture.taken_entries().is_empty());
    }

    #[test]
    fn a_replaced_authority_record_is_refused_rather_than_overwritten() {
        let fixture = Fixture::new();
        fixture.accept(&digest(3));
        let mut acceptance = GenesisAuthorityAcceptance::new(fixture.store());
        // The same digest is a replay of the same decision.
        acceptance.accept(&digest(3)).expect("replay is accepted");
        assert!(
            acceptance.accept(&digest(9)).is_err(),
            "live authority must never be silently replaced"
        );
    }

    #[test]
    fn genesis_is_consumable_at_most_once_for_one_incarnation() {
        let fixture = Fixture::new();
        let intent = agreeing_intent();
        fixture.accept(&intent.sha256().expect("canonical"));

        drop(
            fixture
                .issuer(TestSources::live())
                .issue(&intent)
                .expect("the first genesis is issued"),
        );
        let error = fixture
            .issuer(TestSources::live())
            .issue(&intent)
            .expect_err("a retry of the same intent must stop");
        assert!(matches!(error, GenesisAuthorityError::AlreadyTaken));
        assert_eq!(fixture.taken_entries().len(), 1);
    }

    /// The `DROP DATABASE` case the contract opens with: every comparison
    /// still passes afterwards, because dropping a database touches none of
    /// them. Only the record closes it.
    #[test]
    fn a_dropped_catalog_is_never_silently_re_materialized() {
        let fixture = Fixture::new();
        let intent = agreeing_intent();
        fixture.accept(&intent.sha256().expect("canonical"));
        drop(
            fixture
                .issuer(TestSources::live())
                .issue(&intent)
                .expect("the first genesis is issued"),
        );

        // Nothing about the acceptance, the generation, the cluster or PGDATA
        // has changed, which is exactly the point.
        let error = fixture
            .issuer(TestSources::live())
            .issue(&intent)
            .expect_err("a re-rendered intent must not re-create a dropped catalog");
        assert!(matches!(error, GenesisAuthorityError::AlreadyTaken));
    }

    /// A crash between recording and creating must leave a stuck shard. The
    /// record outlives the process, so a fresh handle finds it.
    #[test]
    fn the_record_is_durable_before_the_permit_exists() {
        let fixture = Fixture::new();
        let intent = agreeing_intent();
        fixture.accept(&intent.sha256().expect("canonical"));
        let permit = fixture
            .issuer(TestSources::live())
            .issue(&intent)
            .expect("the first genesis is issued");
        // Dropped without creating anything, which is what a crash looks like
        // to the next process.
        drop(permit);

        let mut restarted =
            GenesisAuthority::for_mount(TestSources::live(), &fixture.data_directory())
                .expect("reopen the store");
        assert!(matches!(
            restarted
                .issue(&intent)
                .expect_err("a restart must find genesis already recorded"),
            GenesisAuthorityError::AlreadyTaken
        ));
    }

    /// The gate is keyed by the incarnation, not by the shard: a genuinely new
    /// `initdb` on the same volume is a different key and is not blocked by
    /// the old one.
    #[test]
    fn another_incarnation_has_its_own_gate() {
        let fixture = Fixture::new();
        let first = agreeing_intent();
        fixture.accept(&first.sha256().expect("canonical"));
        drop(
            fixture
                .issuer(TestSources::live())
                .issue(&first)
                .expect("the first genesis is issued"),
        );

        let republished = parse_mounted_incarnation(&control_data(
            "1800",
            &MOUNTED_SYSTEM_IDENTIFIER.to_string(),
            "1",
            OTHER_NONCE,
        ))
        .expect("the second fixture parses");
        let mut second = agreeing_intent();
        second.data_directory.seed_id = republished.seed_id.clone();
        // Authority is per intent, so the new incarnation needs its own.
        let mut acceptance = GenesisAuthorityAcceptance::new(fixture.store());
        std::fs::remove_file(fixture.directory().join(AUTHORITY_RECORD)).expect("clear authority");
        acceptance
            .accept(&second.sha256().expect("canonical"))
            .expect("accept the second authority");

        let mut sources = TestSources::live();
        sources.incarnation = Ok(republished);
        drop(
            fixture
                .issuer(sources)
                .issue(&second)
                .expect("a republished data directory is a new incarnation"),
        );
        assert_eq!(fixture.taken_entries().len(), 2);
    }

    /// Filed under what was read off the mount, and readable back as such.
    #[test]
    fn the_taken_record_names_the_observed_incarnation() {
        let fixture = Fixture::new();
        let intent = agreeing_intent();
        let intent_sha256 = intent.sha256().expect("canonical");
        fixture.accept(&intent_sha256);
        drop(
            fixture
                .issuer(TestSources::live())
                .issue(&intent)
                .expect("issued"),
        );

        let observed = mounted_incarnation();
        let name = format!("{TAKEN_PREFIX}{}", observed.key());
        let contents = std::fs::read(fixture.directory().join(&name)).expect("read taken record");
        let record: TakenRecord = serde_json::from_slice(&contents).expect("parse taken record");
        assert_eq!(
            record,
            TakenRecord {
                schema_version: GENESIS_TAKEN_RECORD_VERSION.to_owned(),
                incarnation_key: observed.key(),
                intent_sha256,
                seed_id: observed.seed_id().to_owned(),
                system_identifier: observed.system_identifier().to_string(),
                timeline: observed.timeline(),
            }
        );
    }

    /// Pinned, not merely stable within one build. The key names the record
    /// that says genesis is already taken, so a silently reframed key finds
    /// nothing recorded against a mount that has already been through genesis
    /// and lets the catalog come back.
    #[test]
    fn the_gate_key_is_pinned() {
        assert_eq!(
            mounted_incarnation().key(),
            "a93ec80da43b062023e82bf42c97c42a0ab2d37367ad3217354d6b06d1bd7cf5",
            "the gate key changed; every already-taken incarnation lost its record"
        );
    }

    #[test]
    fn every_incarnation_value_moves_the_gate_key() {
        let live = mounted_incarnation();
        let mut keys = BTreeSet::new();
        keys.insert(live.key());
        keys.insert(
            ObservedIncarnation {
                seed_id: "d".repeat(64),
                ..live.clone()
            }
            .key(),
        );
        keys.insert(
            ObservedIncarnation {
                system_identifier: live.system_identifier ^ 1,
                ..live.clone()
            }
            .key(),
        );
        keys.insert(
            ObservedIncarnation {
                timeline: 2,
                ..live.clone()
            }
            .key(),
        );
        assert_eq!(keys.len(), 4, "the gate key ignores an incarnation value");

        // Length framing, so two values cannot be reflowed into each other.
        assert_ne!(
            ObservedIncarnation {
                seed_id: "e".repeat(63),
                system_identifier: 11,
                timeline: 1,
            }
            .key(),
            ObservedIncarnation {
                seed_id: "e".repeat(64),
                system_identifier: 1,
                timeline: 1,
            }
            .key()
        );
    }

    #[test]
    fn an_unreadable_source_is_a_refusal_and_never_a_pass() {
        let intent = agreeing_intent();
        let intent_sha256 = intent.sha256().expect("canonical");

        for (name, sources) in [
            (
                "incarnation",
                TestSources {
                    incarnation: Err(GenesisEvidenceError::IncarnationNotCanonical),
                    ..TestSources::live()
                },
            ),
            (
                "owning cluster",
                TestSources {
                    owning_cluster: Err(GenesisEvidenceError::OwningClusterUnobserved),
                    ..TestSources::live()
                },
            ),
            (
                "held generation",
                TestSources {
                    generation: Err(GenesisEvidenceError::WritableAuthorityAbsent),
                    ..TestSources::live()
                },
            ),
        ] {
            let fixture = Fixture::new();
            fixture.accept(&intent_sha256);
            let Err(error) = fixture.issuer(sources).issue(&intent) else {
                panic!("an unreadable {name} source must refuse");
            };
            assert!(
                matches!(error, GenesisAuthorityError::EvidenceUnavailable(_)),
                "unexpected refusal for {name}: {error}"
            );
            assert!(fixture.taken_entries().is_empty());
        }
    }

    #[test]
    fn a_non_canonical_intent_is_refused_before_anything_is_read() {
        let fixture = Fixture::new();
        let mut promoted = agreeing_intent();
        promoted.data_directory.timeline = 2;

        let error = fixture
            .issuer(TestSources::live())
            .issue(&promoted)
            .expect_err("a non-canonical intent has no digest");
        assert!(matches!(error, GenesisAuthorityError::NotCanonical(_)));
        assert!(fixture.entries().is_empty());
    }

    fn write_record(fixture: &Fixture, name: &str, contents: &[u8]) {
        let store = fixture.store();
        drop(store);
        let path = fixture.directory().join(name);
        std::fs::write(&path, contents).expect("write a record");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
            .expect("seal a record");
    }

    // ---- the mounted-incarnation reader ------------------------------------

    /// The stub reads its output from files rather than embedding it: the real
    /// labels contain an apostrophe, which a quoted shell literal would eat.
    fn control_data_stub(root: &Path, name: &str, stdout: &str, stderr: &str, code: u8) -> PathBuf {
        let out = root.join(format!("{name}.out"));
        let err = root.join(format!("{name}.err"));
        std::fs::write(&out, stdout).expect("write the stub stdout");
        std::fs::write(&err, stderr).expect("write the stub stderr");
        let path = root.join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\ncat -- {out}\ncat -- {err} >&2\nexit {code}\n",
                out = out.display(),
                err = err.display(),
            ),
        )
        .expect("write the control-data stub");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("make the stub executable");
        path
    }

    #[test]
    fn the_incarnation_is_read_by_running_the_control_data_program() {
        let root = TempDir::new().expect("root");
        let stub = control_data_stub(
            root.path(),
            "pg_controldata",
            &String::from_utf8(mounted_control_data()).expect("UTF-8"),
            "",
            0,
        );
        let observed = read_mounted_incarnation(root.path(), &stub)
            .expect("the stub reports a canonical incarnation");
        assert_eq!(observed, mounted_incarnation());
    }

    /// `pg_controldata` reports a surviving checksum mismatch through
    /// `pg_log_warning`, which writes to standard error. Anything on standard
    /// error therefore means the control file is untrustworthy.
    #[test]
    fn a_checksum_warning_refuses_the_incarnation() {
        let root = TempDir::new().expect("root");
        let stub = control_data_stub(
            root.path(),
            "pg_controldata",
            &String::from_utf8(mounted_control_data()).expect("UTF-8"),
            "pg_controldata: warning: calculated CRC checksum does not match value stored in control file",
            0,
        );
        assert_eq!(
            read_mounted_incarnation(root.path(), &stub),
            Err(GenesisEvidenceError::IncarnationNotCanonical)
        );
    }

    #[test]
    fn a_failing_control_data_program_refuses_the_incarnation() {
        let root = TempDir::new().expect("root");
        let stub = control_data_stub(root.path(), "pg_controldata", "", "", 1);
        assert_eq!(
            read_mounted_incarnation(root.path(), &stub),
            Err(GenesisEvidenceError::DataDirectoryUnreadable)
        );
    }

    #[test]
    fn a_world_writable_control_data_program_is_never_run() {
        let root = TempDir::new().expect("root");
        let stub = control_data_stub(
            root.path(),
            "pg_controldata",
            &String::from_utf8(mounted_control_data()).expect("UTF-8"),
            "",
            0,
        );
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o777))
            .expect("loosen the stub");
        assert_eq!(
            read_mounted_incarnation(root.path(), &stub),
            Err(GenesisEvidenceError::DataDirectoryUnreadable)
        );
    }

    #[test]
    fn only_one_canonical_control_data_shape_is_accepted() {
        let canonical = String::from_utf8(mounted_control_data()).expect("UTF-8");
        for (reason, output) in [
            ("empty", String::new()),
            (
                "another pg_control format",
                canonical.replace("1800", "1700"),
            ),
            (
                "missing system identifier",
                canonical
                    .lines()
                    .filter(|line| !line.starts_with("Database system identifier:"))
                    .fold(String::new(), |text, line| text + line + "\n"),
            ),
            (
                "missing nonce",
                canonical
                    .lines()
                    .filter(|line| !line.starts_with("Mock authentication nonce:"))
                    .fold(String::new(), |text, line| text + line + "\n"),
            ),
            (
                "repeated system identifier",
                format!("{canonical}Database system identifier:           1\n"),
            ),
            (
                "repeated nonce",
                format!("{canonical}Mock authentication nonce:            {OTHER_NONCE}\n"),
            ),
            (
                "zero system identifier",
                canonical.replace(&MOUNTED_SYSTEM_IDENTIFIER.to_string(), "0"),
            ),
            (
                "padded system identifier",
                canonical.replace(
                    &MOUNTED_SYSTEM_IDENTIFIER.to_string(),
                    &format!("0{MOUNTED_SYSTEM_IDENTIFIER}"),
                ),
            ),
            (
                "signed system identifier",
                canonical.replace(
                    &MOUNTED_SYSTEM_IDENTIFIER.to_string(),
                    &format!("+{MOUNTED_SYSTEM_IDENTIFIER}"),
                ),
            ),
            (
                "overflowing system identifier",
                canonical.replace(
                    &MOUNTED_SYSTEM_IDENTIFIER.to_string(),
                    "18446744073709551616",
                ),
            ),
            (
                "overflowing timeline",
                canonical.replace(
                    "Latest checkpoint's TimeLineID:       1",
                    "Latest checkpoint's TimeLineID:       4294967296",
                ),
            ),
            (
                "zero nonce",
                canonical.replace(MOUNTED_NONCE, &"0".repeat(64)),
            ),
            (
                "short nonce",
                canonical.replace(MOUNTED_NONCE, &MOUNTED_NONCE[..62]),
            ),
            (
                "uppercase nonce",
                canonical.replace(MOUNTED_NONCE, &MOUNTED_NONCE.to_uppercase()),
            ),
        ] {
            assert_eq!(
                parse_mounted_incarnation(output.as_bytes()),
                Err(GenesisEvidenceError::IncarnationNotCanonical),
                "the {reason} case was accepted"
            );
        }
    }

    /// The seed is derived from the `initdb` nonce rather than being the
    /// nonce, so a record or a log carrying a seed hands out no SCRAM mock
    /// authentication material. Pinned, because the seed is signed into every
    /// recorded intent.
    #[test]
    fn the_seed_is_a_domain_separated_digest_of_the_initdb_nonce() {
        let observed = mounted_incarnation();
        assert_ne!(observed.seed_id(), MOUNTED_NONCE);
        assert_eq!(
            observed.seed_id(),
            "1143cb816486830585ed924997de78dd8d6cd28b4eb03019789fe6cb3e4d958e"
        );

        let other = parse_mounted_incarnation(&control_data(
            "1800",
            &MOUNTED_SYSTEM_IDENTIFIER.to_string(),
            "1",
            OTHER_NONCE,
        ))
        .expect("parses");
        assert_ne!(observed.seed_id(), other.seed_id());
    }

    // ---- the live owning cluster -------------------------------------------

    fn owned_object(references: Vec<OwnerReference>) -> DynamicObject {
        let mut object = DynamicObject::new(
            "demo-shard-0000-catalog-activation",
            &kube::core::ApiResource::from_gvk(&kube::core::GroupVersionKind::gvk(
                "pgshard.io",
                "v1alpha1",
                "PgshardCatalogActivation",
            )),
        );
        object.metadata.owner_references = Some(references);
        object
    }

    fn controlling_owner() -> OwnerReference {
        OwnerReference {
            api_version: OWNING_CLUSTER_API_VERSION.to_owned(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: OWNING_CLUSTER_KIND.to_owned(),
            name: "demo".to_owned(),
            uid: LIVE_CLUSTER_UID.to_owned(),
        }
    }

    #[test]
    fn exactly_one_controlling_owner_of_the_right_kind_is_observed() {
        assert_eq!(
            observe_owning_cluster(&owned_object(vec![controlling_owner()]), "demo"),
            Some(ObservedOwner {
                uid: LIVE_CLUSTER_UID.to_owned()
            })
        );

        let mutations: Vec<(&str, OwnerMutation)> = vec![
            (
                "not the controller",
                Box::new(|owner: &mut OwnerReference| owner.controller = Some(false)),
            ),
            (
                "no controller flag",
                Box::new(|owner: &mut OwnerReference| owner.controller = None),
            ),
            (
                "deletion not blocked",
                Box::new(|owner: &mut OwnerReference| owner.block_owner_deletion = None),
            ),
            (
                "another API group",
                Box::new(|owner: &mut OwnerReference| owner.api_version = "apps/v1".to_owned()),
            ),
            (
                "another kind",
                Box::new(|owner: &mut OwnerReference| owner.kind = "StatefulSet".to_owned()),
            ),
            (
                "another name",
                Box::new(|owner: &mut OwnerReference| owner.name = "other".to_owned()),
            ),
            (
                "empty UID",
                Box::new(|owner: &mut OwnerReference| owner.uid = String::new()),
            ),
            (
                "control character in the UID",
                Box::new(|owner: &mut OwnerReference| owner.uid = "uid\u{1}".to_owned()),
            ),
        ];
        for (reason, mutate) in mutations {
            let mut owner = controlling_owner();
            mutate(&mut owner);
            assert_eq!(
                observe_owning_cluster(&owned_object(vec![owner]), "demo"),
                None,
                "the {reason} case was observed as a live owner"
            );
        }

        assert_eq!(
            observe_owning_cluster(&owned_object(Vec::new()), "demo"),
            None,
            "an unowned object has no live owning cluster"
        );
        assert_eq!(
            observe_owning_cluster(
                &owned_object(vec![controlling_owner(), controlling_owner()]),
                "demo"
            ),
            None,
            "two owner references leave a choice of answers"
        );
    }

    // ---- the store ----------------------------------------------------------

    /// At most once has to belong to the mount. Nothing chooses this path, so
    /// there is no second store to take genesis in.
    #[test]
    fn the_store_belongs_to_the_mounted_data_directory() {
        assert_eq!(
            store_directory(Path::new("/var/lib/postgresql/18/docker"))
                .expect("a mounted data directory has a store"),
            Path::new("/var/lib/postgresql/18/.pgshard-catalog-genesis"),
        );
        assert_ne!(
            store_directory(Path::new("/mnt/a/docker")).expect("a store"),
            store_directory(Path::new("/mnt/b/docker")).expect("a store"),
            "two mounts shared one gate"
        );
    }

    #[test]
    fn genesis_taken_on_one_mount_does_not_block_another() {
        let first = Fixture::new();
        let second = Fixture::new();
        let intent = agreeing_intent();
        let intent_sha256 = intent.sha256().expect("canonical");
        first.accept(&intent_sha256);
        second.accept(&intent_sha256);

        drop(
            first
                .issuer(TestSources::live())
                .issue(&intent)
                .expect("the first mount takes genesis"),
        );
        drop(
            second
                .issuer(TestSources::live())
                .issue(&intent)
                .expect("another mount has its own gate"),
        );
        assert_eq!(first.taken_entries().len(), 1);
        assert_eq!(second.taken_entries().len(), 1);
    }

    #[test]
    fn a_relative_or_root_store_path_is_refused() {
        for path in ["relative/pgdata", "/", "/tmp/../tmp/pgdata"] {
            assert!(
                matches!(
                    GenesisAuthorityStore::for_data_directory(Path::new(path)),
                    Err(GenesisStoreError::InvalidDirectoryPath { .. })
                ),
                "{path} was accepted as a mounted data directory"
            );
        }
    }

    #[test]
    fn a_store_directory_that_is_not_exactly_private_is_refused() {
        let fixture = Fixture::new();
        fixture.store();
        std::fs::set_permissions(fixture.directory(), std::fs::Permissions::from_mode(0o750))
            .expect("loosen the store");
        assert!(matches!(
            GenesisAuthorityStore::for_data_directory(&fixture.data_directory()),
            Err(GenesisStoreError::UnsafeDirectory { .. })
        ));
    }

    #[test]
    fn an_unexpected_entry_in_the_store_is_refused() {
        let fixture = Fixture::new();
        fixture.store();
        std::fs::write(fixture.directory().join("notes"), b"hello").expect("write a foreign entry");
        assert!(matches!(
            GenesisAuthorityStore::for_data_directory(&fixture.data_directory()),
            Err(GenesisStoreError::UnsafeObject { .. })
        ));
    }

    #[test]
    fn a_record_that_is_not_an_exclusively_owned_regular_file_is_refused() {
        let fixture = Fixture::new();
        fixture.accept(&digest(3));
        let path = fixture.directory().join(AUTHORITY_RECORD);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o404))
            .expect("loosen the record");
        assert!(matches!(
            fixture.store().accepted_authority(),
            Err(GenesisStoreError::UnsafeObject { .. })
        ));
    }

    /// The planted target is a perfectly valid authority record owned by this
    /// user, so following the link would grant authority rather than merely
    /// fail a later ownership check.
    #[test]
    fn a_symlinked_authority_record_is_never_followed() {
        let fixture = Fixture::new();
        drop(fixture.store());
        let planted = fixture.root.path().join("planted-authority");
        std::fs::write(
            &planted,
            serde_json::to_vec(&AuthorityRecord {
                schema_version: GENESIS_AUTHORITY_RECORD_VERSION.to_owned(),
                intent_sha256: digest(3),
            })
            .expect("encode"),
        )
        .expect("plant a valid record");
        std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o400))
            .expect("seal the planted record");
        std::os::unix::fs::symlink(&planted, fixture.directory().join(AUTHORITY_RECORD))
            .expect("plant a symlink");

        assert_eq!(
            fixture
                .store()
                .accepted_authority()
                .expect("a symlink is absence, not a failure"),
            None,
            "a symlinked authority record was followed"
        );
    }

    #[test]
    fn an_installed_record_is_never_replaced_in_place() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let name = format!("{TAKEN_PREFIX}{}", "a".repeat(64));
        let staging = format!(".{name}.tmp");
        store
            .install(&staging, &name, b"first")
            .expect("the first install succeeds");

        let error = store
            .install(&staging, &name, b"second")
            .expect_err("an installed record must never be replaced");
        assert!(matches!(error, GenesisStoreError::Io { .. }));
        assert_eq!(
            std::fs::read(fixture.directory().join(&name)).expect("read the record"),
            b"first"
        );
    }
}
