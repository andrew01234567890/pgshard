//! Crash-safe, inert catalog-activation acceptance journal.
//!
//! Prepared stores the complete canonical validated request and its independently
//! checked carrier digest. Accepted stores exactly the carrier acceptance fields.
//! Neither record, nor the receipt returned by this module, grants serving,
//! routing, SQL, process, readiness, freshness, or execution authority.
//!
//! A later runtime composition is expected to provide
//! `/var/lib/postgresql/18/.pgshard-catalog-activation` as the dedicated
//! directory. This module intentionally accepts the directory from its caller.

use std::ffi::OsStr;
use std::fs::{self, File, Metadata};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use pgshard_types::catalog_activation::{
    CatalogActivationRequest, CatalogActivationRequestError, KubernetesObjectIdentity,
};
use rustix::fs::{
    AtFlags, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, Stat, flock, fstat, mkdirat,
    open, openat, renameat_with, statat, unlinkat,
};
use rustix::io::Errno;
use rustix::process::geteuid;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const PREPARED_FILE: &str = "prepared";
const PREPARED_STAGING_FILE: &str = ".prepared.tmp";
const ACCEPTED_FILE: &str = "accepted";
const ACCEPTED_STAGING_FILE: &str = ".accepted.tmp";
const PREPARED_SCHEMA_VERSION: &str = "pgshard.catalog-activation-journal.prepared.v1";
const ACCEPTED_SCHEMA_VERSION: &str = "pgshard.catalog-activation-acceptance.v1";
const FSYNC_PERSISTENCE: &str = "fsync";
const MAX_PREPARED_RECORD_BYTES: u64 = 64 * 1024;
const MAX_ACCEPTED_RECORD_BYTES: u64 = 2 * 1024;

/// Result of installing or replaying exact Prepared state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogActivationPrepareOutcome {
    /// This call installed the immutable Prepared final name.
    Installed,
    /// The exact Prepared final already existed and its barriers were renewed.
    Replay,
}

/// A caller-owned, dedicated catalog-activation journal directory.
///
/// Methods take `&mut self` so one handle cannot race its own advisory lock.
/// Independent handles and processes serialize on the directory descriptor.
#[derive(Debug)]
pub struct CatalogActivationJournal {
    directory: File,
    directory_path: PathBuf,
    identity: DirectoryIdentity,
    expected_uid: u32,
}

/// Proof that the exact Accepted record crossed its durability barrier.
///
/// This receipt is deliberately move-only, non-serializable, privately
/// constructible, and inert. Possessing it grants no execution authority.
#[derive(Debug)]
#[must_use = "durable catalog activation acceptance must be handled explicitly"]
pub struct DurableCatalogActivationAcceptance {
    carrier_uid: String,
    request_sha256: String,
    target_pod_name: String,
    target_pod_uid: String,
    persisted_at_unix_ms: String,
    _private: (),
}

impl DurableCatalogActivationAcceptance {
    /// Returns the exact carrier UID bound by Accepted.
    #[must_use]
    pub fn carrier_uid(&self) -> &str {
        &self.carrier_uid
    }

    /// Returns the exact request digest bound by Prepared and Accepted.
    #[must_use]
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    /// Returns the target Pod name bound by Accepted.
    #[must_use]
    pub fn target_pod_name(&self) -> &str {
        &self.target_pod_name
    }

    /// Returns the target Pod UID bound by Accepted.
    #[must_use]
    pub fn target_pod_uid(&self) -> &str {
        &self.target_pod_uid
    }

    /// Returns the original diagnostic persistence time.
    ///
    /// This timestamp is not freshness or execution authority.
    #[must_use]
    pub fn persisted_at_unix_ms(&self) -> &str {
        &self.persisted_at_unix_ms
    }
}

/// Fail-closed journal validation or persistence failure.
#[derive(Debug, Error)]
pub enum CatalogActivationJournalError {
    /// The typed activation request was not canonical and fully bound.
    #[error("invalid catalog activation request: {0}")]
    InvalidRequest(#[from] CatalogActivationRequestError),
    /// The independently supplied carrier digest did not match the request.
    #[error("declared catalog activation request digest does not match the canonical request")]
    DeclaredDigestMismatch,
    /// The observed target Pod identity did not match the request target.
    #[error("catalog activation target Pod does not match the exact request")]
    TargetPodMismatch,
    /// The caller supplied a relative, non-normal, or root directory path.
    #[error("catalog activation journal path {path:?} must be absolute and normalized")]
    InvalidDirectoryPath {
        /// Rejected caller-provided path.
        path: PathBuf,
    },
    /// A directory or ancestor did not meet the ownership or permission contract.
    #[error("unsafe catalog activation journal directory {path:?}: {reason}")]
    UnsafeDirectory {
        /// Unsafe directory path.
        path: PathBuf,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A journal entry was unsafe or unexpected.
    #[error("unsafe catalog activation journal object {path:?}: {reason}")]
    UnsafeObject {
        /// Unsafe object path.
        path: PathBuf,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A record exceeded its phase-specific hard read bound.
    #[error("catalog activation journal record {path:?} is {bytes} bytes; maximum is {maximum}")]
    OversizedRecord {
        /// Oversized record path.
        path: PathBuf,
        /// Observed bytes.
        bytes: u64,
        /// Maximum accepted bytes.
        maximum: u64,
    },
    /// A finalized record did not have the one canonical encoding.
    #[error("corrupt catalog activation {record} record at {path:?}")]
    CorruptRecord {
        /// Record phase.
        record: &'static str,
        /// Corrupt record path.
        path: PathBuf,
    },
    /// Immutable state already binds another valid request or target.
    #[error("catalog activation {record} record conflicts with the requested binding")]
    Conflict {
        /// Conflicting record phase.
        record: &'static str,
    },
    /// Accepted cannot exist before a durable Prepared record.
    #[error("catalog activation Accepted state exists without Prepared state")]
    AcceptedWithoutPrepared,
    /// A validated directory or record changed during one operation.
    #[error("catalog activation journal state changed while validating {path:?}")]
    StateChanged {
        /// Changed path.
        path: PathBuf,
    },
    /// The local clock could not provide a canonical persistence timestamp.
    #[error("catalog activation persistence clock is before the Unix epoch or out of range")]
    InvalidPersistenceClock,
    /// Another live journal handle currently owns the exclusive operation lock.
    #[error("catalog activation journal is busy at {path:?}")]
    Busy {
        /// Contended journal directory.
        path: PathBuf,
    },
    /// A canonical record could not be encoded.
    #[error("encode canonical catalog activation {record} record: {source}")]
    EncodeRecord {
        /// Record phase.
        record: &'static str,
        /// JSON encoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// Final installation was dispatched but no durable receipt can be issued.
    #[error(
        "catalog activation {record} persistence outcome is unknown; resolve from the final name: {source}"
    )]
    OutcomeUnknown {
        /// Record phase whose final name must be resolved.
        record: &'static str,
        /// Failure observed after installation dispatch.
        #[source]
        source: Box<CatalogActivationJournalError>,
    },
    /// A filesystem operation failed before a definite durable result.
    #[error("catalog activation journal could not {operation} at {path:?}: {source}")]
    Io {
        /// Stable operation description.
        operation: &'static str,
        /// Operation target.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// Unit-test crash injection after a named persistence checkpoint.
    #[cfg(test)]
    #[error("injected catalog activation journal crash")]
    InjectedCrash,
    /// Unit-test lock-release failure injection.
    #[cfg(test)]
    #[error("injected catalog activation journal unlock failure")]
    InjectedUnlockFailure,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedRecord {
    schema_version: String,
    #[serde(rename = "requestSHA256")]
    request_sha256: String,
    request: CatalogActivationRequest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AcceptedRecord {
    schema_version: String,
    #[serde(rename = "carrierUID")]
    carrier_uid: String,
    #[serde(rename = "requestSHA256")]
    request_sha256: String,
    target_pod_name: String,
    #[serde(rename = "targetPodUID")]
    target_pod_uid: String,
    persistence: String,
    #[serde(rename = "persistedAtUnixMS")]
    persisted_at_unix_ms: String,
}

impl CatalogActivationJournal {
    /// Opens or creates one dedicated caller-provided journal directory.
    ///
    /// A new directory is created as `0700`, flushed, and installed through a
    /// validated parent. Existing directories must be owned by the effective
    /// user, have exact `0700` permissions, and contain only journal entries.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error for unsafe paths, metadata, contents,
    /// ownership, permissions, or filesystem failures.
    pub fn open_or_create(
        directory: impl AsRef<Path>,
    ) -> Result<Self, CatalogActivationJournalError> {
        Self::open_or_create_for_uid(directory.as_ref(), geteuid().as_raw())
    }

    /// Persists the immutable Prepared record for an exact carrier request.
    ///
    /// `declared_request_sha256` is mandatory so a caller cannot accidentally
    /// omit verification of the carrier's independently stored digest. Exact
    /// replay renews the file and directory durability barriers without
    /// changing the final inode or bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, conflict, corruption, or persistence error.
    pub fn prepare(
        &mut self,
        request: &CatalogActivationRequest,
        declared_request_sha256: &str,
    ) -> Result<CatalogActivationPrepareOutcome, CatalogActivationJournalError> {
        let prepared = checked_prepared_record(request, declared_request_sha256)?;
        let expected = encode_canonical(RecordPhase::Prepared, &prepared)?;
        self.with_exclusive_lock(
            |_| Some(RecordPhase::Prepared),
            |journal| {
                journal.validate_entries()?;
                if let Some(existing) = journal.read_record(RecordName::PreparedFinal)? {
                    let existing_record = parse_prepared(&existing.contents, existing.path())?;
                    require_prepared_binding(&existing_record, &prepared)?;
                    journal
                        .complete_record_durability_barrier(RecordName::PreparedFinal, &expected)?;
                    return Ok(CatalogActivationPrepareOutcome::Replay);
                }
                let staging = journal.prepare_staging(&prepared, &expected)?;
                journal.install_staging(RecordPhase::Prepared, &staging.contents)?;
                Ok(CatalogActivationPrepareOutcome::Installed)
            },
        )
    }

    /// Persists Accepted only after exact durable Prepared state exists.
    ///
    /// The receipt is constructed only after Accepted is flushed, the
    /// directory is flushed, and the final is reopened and exactly reread.
    /// The target identity must be independently supplied by the caller and
    /// match the request's exact source Pod.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, target mismatch, missing-Prepared,
    /// conflict, corruption, clock, or persistence error.
    pub fn accept(
        &mut self,
        request: &CatalogActivationRequest,
        declared_request_sha256: &str,
        target: &KubernetesObjectIdentity,
    ) -> Result<DurableCatalogActivationAcceptance, CatalogActivationJournalError> {
        let prepared = checked_prepared_record(request, declared_request_sha256)?;
        validate_target(request, target)?;
        self.with_exclusive_lock(
            |_| Some(RecordPhase::Accepted),
            |journal| {
                journal.validate_entries()?;
                journal.require_durable_prepared(&prepared)?;
                if let Some(existing) = journal.read_record(RecordName::AcceptedFinal)? {
                    let acceptance = parse_accepted(&existing.contents, existing.path())?;
                    require_accepted_binding(&acceptance, &prepared, target)?;
                    journal.complete_record_durability_barrier(
                        RecordName::AcceptedFinal,
                        &existing.contents,
                    )?;
                    return Ok(receipt(acceptance));
                }
                let (acceptance, encoded) = journal.accepted_staging(&prepared, target)?;
                journal.install_staging(RecordPhase::Accepted, &encoded)?;
                let installed_acceptance = (|| {
                    #[cfg(test)]
                    crash_checkpoint(RecordPhase::Accepted, CrashCheckpoint::ReceiptReread)?;
                    let installed =
                        journal
                            .read_record(RecordName::AcceptedFinal)?
                            .ok_or_else(|| CatalogActivationJournalError::StateChanged {
                                path: journal.directory_path.join(ACCEPTED_FILE),
                            })?;
                    let installed_acceptance =
                        parse_accepted(&installed.contents, installed.path())?;
                    if installed_acceptance != acceptance {
                        return Err(CatalogActivationJournalError::StateChanged {
                            path: installed.path().to_owned(),
                        });
                    }
                    Ok(installed_acceptance)
                })()
                .map_err(|error| outcome_unknown(RecordPhase::Accepted, error))?;
                Ok(receipt(installed_acceptance))
            },
        )
    }

    /// Resolves an earlier acceptance outcome using only the Accepted final.
    ///
    /// A staging file is never acceptance. An exact final has its barriers
    /// renewed and is reopened before a new private receipt is returned.
    ///
    /// # Errors
    ///
    /// Returns a typed fail-closed error for any invalid binding or filesystem
    /// state. `Ok(None)` means no Accepted final exists.
    pub fn resolve_acceptance(
        &mut self,
        request: &CatalogActivationRequest,
        declared_request_sha256: &str,
        target: &KubernetesObjectIdentity,
    ) -> Result<Option<DurableCatalogActivationAcceptance>, CatalogActivationJournalError> {
        let prepared = checked_prepared_record(request, declared_request_sha256)?;
        validate_target(request, target)?;
        self.with_exclusive_lock(
            |acceptance: &Option<DurableCatalogActivationAcceptance>| {
                acceptance.as_ref().map(|_| RecordPhase::Accepted)
            },
            |journal| {
                journal.validate_entries()?;
                let Some(existing) = journal.read_record(RecordName::AcceptedFinal)? else {
                    return Ok(None);
                };
                journal.require_durable_prepared(&prepared)?;
                let acceptance = parse_accepted(&existing.contents, existing.path())?;
                require_accepted_binding(&acceptance, &prepared, target)?;
                journal.complete_record_durability_barrier(
                    RecordName::AcceptedFinal,
                    &existing.contents,
                )?;
                Ok(Some(receipt(acceptance)))
            },
        )
    }

    fn open_or_create_for_uid(
        directory_path: &Path,
        expected_uid: u32,
    ) -> Result<Self, CatalogActivationJournalError> {
        validate_normal_absolute_path(directory_path)?;
        let parent_path = directory_path.parent().ok_or_else(|| {
            CatalogActivationJournalError::InvalidDirectoryPath {
                path: directory_path.to_owned(),
            }
        })?;
        let directory_name = directory_path.file_name().ok_or_else(|| {
            CatalogActivationJournalError::InvalidDirectoryPath {
                path: directory_path.to_owned(),
            }
        })?;
        let parent = ValidatedParent::open(parent_path, expected_uid)?;
        let created = match mkdirat(
            &parent.directory,
            directory_name,
            Mode::RUSR | Mode::WUSR | Mode::XUSR,
        ) {
            Ok(()) => true,
            Err(source) if source == rustix::io::Errno::EXIST => false,
            Err(source) => {
                return Err(io_error("create directory", directory_path, source.into()));
            }
        };
        let descriptor = openat(
            &parent.directory,
            directory_name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| io_error("open directory", directory_path, source.into()))?;
        let directory = File::from(descriptor);
        let metadata = directory
            .metadata()
            .map_err(|source| io_error("read directory metadata", directory_path, source))?;
        validate_journal_directory_metadata(directory_path, &metadata, expected_uid)?;
        let journal = Self {
            identity: DirectoryIdentity::from_metadata(&metadata),
            directory,
            directory_path: directory_path.to_owned(),
            expected_uid,
        };
        journal.validate_identity()?;
        if created {
            journal
                .directory
                .sync_all()
                .map_err(|source| io_error("flush new directory", directory_path, source))?;
        }
        // Also flush after an EEXIST race: another opener may observe a newly
        // created directory before its creator reaches the parent barrier.
        parent.sync_after_creation()?;
        journal.validate_identity()?;
        journal.validate_entries()?;
        Ok(journal)
    }

    fn with_exclusive_lock<T>(
        &mut self,
        durable_phase_on_success: impl FnOnce(&T) -> Option<RecordPhase>,
        operation: impl FnOnce(&mut Self) -> Result<T, CatalogActivationJournalError>,
    ) -> Result<T, CatalogActivationJournalError> {
        flock(&self.directory, FlockOperation::NonBlockingLockExclusive).map_err(|source| {
            if source == Errno::WOULDBLOCK {
                CatalogActivationJournalError::Busy {
                    path: self.directory_path.clone(),
                }
            } else {
                io_error("lock directory", &self.directory_path, source.into())
            }
        })?;
        let result = operation(self);
        let unlock = flock(&self.directory, FlockOperation::Unlock)
            .map_err(|source| io_error("unlock directory", &self.directory_path, source.into()));
        #[cfg(test)]
        let unlock = inject_unlock_failure_if_requested(unlock);
        match (result, unlock) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) => Err(error),
            (Ok(value), Err(error)) => match durable_phase_on_success(&value) {
                Some(phase) => Err(outcome_unknown(phase, error)),
                None => Err(error),
            },
        }
    }

    fn require_durable_prepared(
        &self,
        expected: &PreparedRecord,
    ) -> Result<(), CatalogActivationJournalError> {
        let prepared = self
            .read_record(RecordName::PreparedFinal)?
            .ok_or(CatalogActivationJournalError::AcceptedWithoutPrepared)?;
        let actual = parse_prepared(&prepared.contents, prepared.path())?;
        require_prepared_binding(&actual, expected)?;
        self.complete_record_durability_barrier(RecordName::PreparedFinal, &prepared.contents)
    }

    fn prepare_staging(
        &self,
        expected_record: &PreparedRecord,
        expected_bytes: &[u8],
    ) -> Result<ManagedRecord, CatalogActivationJournalError> {
        if let Some(staging) = self.read_record(RecordName::PreparedStaging)? {
            if staging.snapshot.mode & 0o7_777 != 0o400 {
                self.remove_interrupted_staging(RecordName::PreparedStaging, &staging)?;
                return self.create_staging(RecordName::PreparedStaging, expected_bytes);
            }
            match parse_prepared(&staging.contents, staging.path()) {
                Ok(actual) => {
                    require_prepared_binding(&actual, expected_record)?;
                    if staging.contents != expected_bytes {
                        return Err(CatalogActivationJournalError::StateChanged {
                            path: staging.path().to_owned(),
                        });
                    }
                    return Ok(staging);
                }
                Err(CatalogActivationJournalError::CorruptRecord { .. }) => {
                    self.remove_interrupted_staging(RecordName::PreparedStaging, &staging)?;
                }
                Err(error) => return Err(error),
            }
        }
        self.create_staging(RecordName::PreparedStaging, expected_bytes)
    }

    fn accepted_staging(
        &self,
        prepared: &PreparedRecord,
        target: &KubernetesObjectIdentity,
    ) -> Result<(AcceptedRecord, Vec<u8>), CatalogActivationJournalError> {
        if let Some(staging) = self.read_record(RecordName::AcceptedStaging)? {
            if staging.snapshot.mode & 0o7_777 != 0o400 {
                self.remove_interrupted_staging(RecordName::AcceptedStaging, &staging)?;
                return self.new_accepted_staging(prepared, target);
            }
            match parse_accepted(&staging.contents, staging.path()) {
                Ok(acceptance) => {
                    require_accepted_binding(&acceptance, prepared, target)?;
                    return Ok((acceptance, staging.contents));
                }
                Err(CatalogActivationJournalError::CorruptRecord { .. }) => {
                    self.remove_interrupted_staging(RecordName::AcceptedStaging, &staging)?;
                }
                Err(error) => return Err(error),
            }
        }
        self.new_accepted_staging(prepared, target)
    }

    fn new_accepted_staging(
        &self,
        prepared: &PreparedRecord,
        target: &KubernetesObjectIdentity,
    ) -> Result<(AcceptedRecord, Vec<u8>), CatalogActivationJournalError> {
        let persisted_at_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| CatalogActivationJournalError::InvalidPersistenceClock)?
                .as_millis(),
        )
        .map_err(|_| CatalogActivationJournalError::InvalidPersistenceClock)?
        .to_string();
        let acceptance = AcceptedRecord {
            schema_version: ACCEPTED_SCHEMA_VERSION.to_owned(),
            carrier_uid: prepared.request.carrier.uid.clone(),
            request_sha256: prepared.request_sha256.clone(),
            target_pod_name: target.name.clone(),
            target_pod_uid: target.uid.clone(),
            persistence: FSYNC_PERSISTENCE.to_owned(),
            persisted_at_unix_ms,
        };
        let encoded = encode_canonical(RecordPhase::Accepted, &acceptance)?;
        let staging = self.create_staging(RecordName::AcceptedStaging, &encoded)?;
        if staging.contents != encoded {
            return Err(CatalogActivationJournalError::StateChanged {
                path: staging.path().to_owned(),
            });
        }
        Ok((acceptance, encoded))
    }

    fn create_staging(
        &self,
        name: RecordName,
        contents: &[u8],
    ) -> Result<ManagedRecord, CatalogActivationJournalError> {
        let path = self.directory_path.join(name.file_name());
        let descriptor = openat(
            &self.directory,
            name.file_name(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|source| io_error("create staging record", &path, source.into()))?;
        let mut file = File::from(descriptor);
        file.write_all(contents)
            .and_then(|()| file.set_permissions(fs::Permissions::from_mode(0o400)))
            .map_err(|source| io_error("write staging record", &path, source))?;
        #[cfg(test)]
        crash_checkpoint(name.phase(), CrashCheckpoint::StagingWritten)?;
        file.sync_all()
            .map_err(|source| io_error("flush staging record", &path, source))?;
        let written = object_snapshot(
            &fstat(&file).map_err(|source| {
                io_error("read staging descriptor metadata", &path, source.into())
            })?,
            &path,
        )?;
        let reread = self
            .read_record(name)?
            .ok_or_else(|| CatalogActivationJournalError::StateChanged { path: path.clone() })?;
        if reread.contents != contents || reread.snapshot != written {
            return Err(CatalogActivationJournalError::StateChanged { path });
        }
        Ok(reread)
    }

    fn install_staging(
        &self,
        phase: RecordPhase,
        expected: &[u8],
    ) -> Result<(), CatalogActivationJournalError> {
        let source = RecordName::staging(phase);
        let destination = RecordName::final_name(phase);
        // A previous attempt can leave a canonical 0400 staging inode after
        // chmod but before its fsync. Renew and revalidate the complete file
        // and directory barrier on every attempt before it can become final.
        self.complete_record_durability_barrier(source, expected)?;
        #[cfg(test)]
        crash_checkpoint(phase, CrashCheckpoint::StagingSynced)?;
        let install_result = renameat_with(
            &self.directory,
            source.file_name(),
            &self.directory,
            destination.file_name(),
            RenameFlags::NOREPLACE,
        );
        if let Err(source_error) = install_result {
            match self.read_record(destination) {
                Ok(Some(installed)) => {
                    classify_exact_or_conflict(
                        phase,
                        &installed.contents,
                        expected,
                        installed.path(),
                    )?;
                    return self
                        .complete_record_durability_barrier(destination, expected)
                        .map_err(|error| outcome_unknown(phase, error));
                }
                Ok(None) => {}
                Err(error) => return Err(outcome_unknown(phase, error)),
            }
            return Err(outcome_unknown(
                phase,
                io_error(
                    "install immutable record",
                    &self.directory_path.join(destination.file_name()),
                    source_error.into(),
                ),
            ));
        }
        #[cfg(test)]
        crash_checkpoint(phase, CrashCheckpoint::FinalInstalled)
            .map_err(|error| outcome_unknown(phase, error))?;
        self.sync_directory("flush installed record")
            .map_err(|error| outcome_unknown(phase, error))?;
        #[cfg(test)]
        crash_checkpoint(phase, CrashCheckpoint::DirectorySynced)
            .map_err(|error| outcome_unknown(phase, error))?;
        self.complete_record_durability_barrier(destination, expected)
            .map_err(|error| outcome_unknown(phase, error))
    }

    fn remove_interrupted_staging(
        &self,
        name: RecordName,
        staging: &ManagedRecord,
    ) -> Result<(), CatalogActivationJournalError> {
        let path = self.directory_path.join(name.file_name());
        self.revalidate_record(name, staging)?;
        unlinkat(&self.directory, name.file_name(), AtFlags::empty()).map_err(|source| {
            io_error("remove interrupted staging record", &path, source.into())
        })?;
        self.sync_directory("flush interrupted staging cleanup")?;
        if self.read_record(name)?.is_some() {
            return Err(CatalogActivationJournalError::StateChanged { path });
        }
        Ok(())
    }

    fn complete_record_durability_barrier(
        &self,
        name: RecordName,
        expected: &[u8],
    ) -> Result<(), CatalogActivationJournalError> {
        let path = self.directory_path.join(name.file_name());
        let first = self
            .read_record(name)?
            .ok_or_else(|| CatalogActivationJournalError::StateChanged { path: path.clone() })?;
        classify_exact_or_conflict(name.phase(), &first.contents, expected, &path)?;
        first
            .file
            .sync_all()
            .map_err(|source| io_error("flush journal record", &path, source))?;
        self.revalidate_record(name, &first)?;
        self.sync_directory("complete journal record barrier")?;
        let reopened = self
            .read_record(name)?
            .ok_or_else(|| CatalogActivationJournalError::StateChanged { path: path.clone() })?;
        if reopened.contents != expected || reopened.snapshot != first.snapshot {
            return Err(CatalogActivationJournalError::StateChanged { path });
        }
        self.validate_identity()
    }

    fn validate_entries(&self) -> Result<(), CatalogActivationJournalError> {
        self.validate_identity()?;
        let mut entries = Dir::read_from(&self.directory).map_err(|source| {
            io_error(
                "open directory entries",
                &self.directory_path,
                source.into(),
            )
        })?;
        while let Some(entry) = entries.read() {
            let entry = entry.map_err(|source| {
                io_error(
                    "read directory entries",
                    &self.directory_path,
                    source.into(),
                )
            })?;
            let name = entry.file_name().to_bytes();
            if matches!(name, b"." | b"..") {
                continue;
            }
            if RecordName::from_bytes(name).is_none() {
                return Err(CatalogActivationJournalError::UnsafeObject {
                    path: self
                        .directory_path
                        .join(OsStr::from_bytes(entry.file_name().to_bytes())),
                    reason: "unexpected directory entry",
                });
            }
        }

        let prepared = self.read_record(RecordName::PreparedFinal)?;
        let prepared_record = prepared
            .as_ref()
            .map(|record| parse_prepared(&record.contents, record.path()))
            .transpose()?;
        let accepted = self.read_record(RecordName::AcceptedFinal)?;
        let accepted_staging = self.read_record(RecordName::AcceptedStaging)?;
        if prepared.is_none() && (accepted.is_some() || accepted_staging.is_some()) {
            return Err(CatalogActivationJournalError::AcceptedWithoutPrepared);
        }
        if let Some(accepted) = accepted.as_ref() {
            let accepted_record = parse_accepted(&accepted.contents, accepted.path())?;
            let prepared_record = prepared_record
                .as_ref()
                .ok_or(CatalogActivationJournalError::AcceptedWithoutPrepared)?;
            require_accepted_binding(
                &accepted_record,
                prepared_record,
                &KubernetesObjectIdentity {
                    name: prepared_record.request.source.pod_name.clone(),
                    uid: prepared_record.request.source.pod_uid.clone(),
                },
            )?;
        }
        let _ = self.read_record(RecordName::PreparedStaging)?;
        self.validate_identity()
    }

    fn read_record(
        &self,
        name: RecordName,
    ) -> Result<Option<ManagedRecord>, CatalogActivationJournalError> {
        let path = self.directory_path.join(name.file_name());
        let path_stat = match statat(&self.directory, name.file_name(), AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(source) if source == rustix::io::Errno::NOENT => return Ok(None),
            Err(source) => {
                return Err(io_error("read record metadata", &path, source.into()));
            }
        };
        validate_record_stat(
            &path,
            &path_stat,
            self.expected_uid,
            self.identity.device,
            name,
        )?;
        let expected = object_snapshot(&path_stat, &path)?;
        let descriptor = openat(
            &self.directory,
            name.file_name(),
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|source| io_error("open record", &path, source.into()))?;
        let mut file = File::from(descriptor);
        let opened = fstat(&file)
            .map_err(|source| io_error("read record descriptor metadata", &path, source.into()))?;
        validate_record_stat(
            &path,
            &opened,
            self.expected_uid,
            self.identity.device,
            name,
        )?;
        if object_snapshot(&opened, &path)? != expected {
            return Err(CatalogActivationJournalError::StateChanged { path });
        }
        let mut contents = Vec::new();
        Read::by_ref(&mut file)
            .take(name.maximum_bytes() + 1)
            .read_to_end(&mut contents)
            .map_err(|source| io_error("read record", &path, source))?;
        if contents.len() as u64 > name.maximum_bytes() {
            return Err(CatalogActivationJournalError::OversizedRecord {
                path,
                bytes: contents.len() as u64,
                maximum: name.maximum_bytes(),
            });
        }
        let record = ManagedRecord {
            file,
            snapshot: expected,
            contents,
            path,
        };
        self.revalidate_record(name, &record)?;
        Ok(Some(record))
    }

    fn revalidate_record(
        &self,
        name: RecordName,
        record: &ManagedRecord,
    ) -> Result<(), CatalogActivationJournalError> {
        let path_stat = statat(&self.directory, name.file_name(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|source| {
                io_error("revalidate record metadata", record.path(), source.into())
            })?;
        validate_record_stat(
            record.path(),
            &path_stat,
            self.expected_uid,
            self.identity.device,
            name,
        )?;
        let descriptor_stat = fstat(&record.file).map_err(|source| {
            io_error(
                "revalidate record descriptor metadata",
                record.path(),
                source.into(),
            )
        })?;
        if object_snapshot(&path_stat, record.path())? != record.snapshot
            || object_snapshot(&descriptor_stat, record.path())? != record.snapshot
        {
            return Err(CatalogActivationJournalError::StateChanged {
                path: record.path().to_owned(),
            });
        }
        Ok(())
    }

    fn sync_directory(&self, operation: &'static str) -> Result<(), CatalogActivationJournalError> {
        self.validate_identity()?;
        self.directory
            .sync_all()
            .map_err(|source| io_error(operation, &self.directory_path, source))?;
        self.validate_identity()
    }

    fn validate_identity(&self) -> Result<(), CatalogActivationJournalError> {
        let descriptor_metadata = self.directory.metadata().map_err(|source| {
            io_error(
                "read directory descriptor metadata",
                &self.directory_path,
                source,
            )
        })?;
        validate_journal_directory_metadata(
            &self.directory_path,
            &descriptor_metadata,
            self.expected_uid,
        )?;
        let path_metadata = strict_path_metadata(&self.directory_path)?;
        validate_journal_directory_metadata(
            &self.directory_path,
            &path_metadata,
            self.expected_uid,
        )?;
        if DirectoryIdentity::from_metadata(&descriptor_metadata) != self.identity
            || DirectoryIdentity::from_metadata(&path_metadata) != self.identity
        {
            return Err(CatalogActivationJournalError::StateChanged {
                path: self.directory_path.clone(),
            });
        }
        Ok(())
    }
}

fn checked_prepared_record(
    request: &CatalogActivationRequest,
    declared_request_sha256: &str,
) -> Result<PreparedRecord, CatalogActivationJournalError> {
    let computed = request.sha256()?;
    if declared_request_sha256 != computed {
        return Err(CatalogActivationJournalError::DeclaredDigestMismatch);
    }
    Ok(PreparedRecord {
        schema_version: PREPARED_SCHEMA_VERSION.to_owned(),
        request_sha256: computed,
        request: request.clone(),
    })
}

fn validate_target(
    request: &CatalogActivationRequest,
    target: &KubernetesObjectIdentity,
) -> Result<(), CatalogActivationJournalError> {
    if target.name != request.source.pod_name || target.uid != request.source.pod_uid {
        Err(CatalogActivationJournalError::TargetPodMismatch)
    } else {
        Ok(())
    }
}

fn require_prepared_binding(
    actual: &PreparedRecord,
    expected: &PreparedRecord,
) -> Result<(), CatalogActivationJournalError> {
    if actual == expected {
        Ok(())
    } else {
        Err(CatalogActivationJournalError::Conflict { record: "Prepared" })
    }
}

fn require_accepted_binding(
    actual: &AcceptedRecord,
    prepared: &PreparedRecord,
    target: &KubernetesObjectIdentity,
) -> Result<(), CatalogActivationJournalError> {
    if actual.schema_version == ACCEPTED_SCHEMA_VERSION
        && actual.carrier_uid == prepared.request.carrier.uid
        && actual.request_sha256 == prepared.request_sha256
        && actual.target_pod_name == target.name
        && actual.target_pod_uid == target.uid
        && actual.persistence == FSYNC_PERSISTENCE
        && canonical_u64(&actual.persisted_at_unix_ms).is_some()
    {
        Ok(())
    } else {
        Err(CatalogActivationJournalError::Conflict { record: "Accepted" })
    }
}

fn receipt(record: AcceptedRecord) -> DurableCatalogActivationAcceptance {
    DurableCatalogActivationAcceptance {
        carrier_uid: record.carrier_uid,
        request_sha256: record.request_sha256,
        target_pod_name: record.target_pod_name,
        target_pod_uid: record.target_pod_uid,
        persisted_at_unix_ms: record.persisted_at_unix_ms,
        _private: (),
    }
}

fn encode_canonical(
    phase: RecordPhase,
    record: &impl Serialize,
) -> Result<Vec<u8>, CatalogActivationJournalError> {
    let mut encoded = serde_json::to_vec(record).map_err(|source| {
        CatalogActivationJournalError::EncodeRecord {
            record: phase.label(),
            source,
        }
    })?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn parse_prepared(
    contents: &[u8],
    path: &Path,
) -> Result<PreparedRecord, CatalogActivationJournalError> {
    let body = contents
        .strip_suffix(b"\n")
        .ok_or_else(|| corrupt_record(RecordPhase::Prepared, path))?;
    let record: PreparedRecord =
        serde_json::from_slice(body).map_err(|_| corrupt_record(RecordPhase::Prepared, path))?;
    if record.schema_version != PREPARED_SCHEMA_VERSION
        || record.request.validate().is_err()
        || record.request.sha256().ok().as_deref() != Some(record.request_sha256.as_str())
        || encode_canonical(RecordPhase::Prepared, &record)
            .ok()
            .as_deref()
            != Some(contents)
    {
        return Err(corrupt_record(RecordPhase::Prepared, path));
    }
    Ok(record)
}

fn parse_accepted(
    contents: &[u8],
    path: &Path,
) -> Result<AcceptedRecord, CatalogActivationJournalError> {
    let body = contents
        .strip_suffix(b"\n")
        .ok_or_else(|| corrupt_record(RecordPhase::Accepted, path))?;
    let record: AcceptedRecord =
        serde_json::from_slice(body).map_err(|_| corrupt_record(RecordPhase::Accepted, path))?;
    if record.schema_version != ACCEPTED_SCHEMA_VERSION
        || record.carrier_uid.is_empty()
        || record.carrier_uid.len() > 128
        || !record.carrier_uid.bytes().all(is_uid_byte)
        || record.request_sha256.len() != 64
        || !record.request_sha256.bytes().all(is_lower_hex)
        || record.target_pod_name.is_empty()
        || record.target_pod_name.len() > 253
        || !record.target_pod_name.bytes().all(is_graphic_nonspace)
        || record.target_pod_uid.is_empty()
        || record.target_pod_uid.len() > 128
        || !record.target_pod_uid.bytes().all(is_uid_byte)
        || record.persistence != FSYNC_PERSISTENCE
        || canonical_u64(&record.persisted_at_unix_ms).is_none()
        || encode_canonical(RecordPhase::Accepted, &record)
            .ok()
            .as_deref()
            != Some(contents)
    {
        return Err(corrupt_record(RecordPhase::Accepted, path));
    }
    Ok(record)
}

fn classify_exact_or_conflict(
    phase: RecordPhase,
    actual: &[u8],
    expected: &[u8],
    path: &Path,
) -> Result<(), CatalogActivationJournalError> {
    if actual == expected {
        return Ok(());
    }
    let valid = match phase {
        RecordPhase::Prepared => parse_prepared(actual, path).is_ok(),
        RecordPhase::Accepted => parse_accepted(actual, path).is_ok(),
    };
    if valid {
        Err(CatalogActivationJournalError::Conflict {
            record: phase.label(),
        })
    } else {
        Err(corrupt_record(phase, path))
    }
}

fn corrupt_record(phase: RecordPhase, path: &Path) -> CatalogActivationJournalError {
    CatalogActivationJournalError::CorruptRecord {
        record: phase.label(),
        path: path.to_owned(),
    }
}

fn outcome_unknown(
    phase: RecordPhase,
    error: CatalogActivationJournalError,
) -> CatalogActivationJournalError {
    match error {
        CatalogActivationJournalError::Conflict { .. }
        | CatalogActivationJournalError::CorruptRecord { .. }
        | CatalogActivationJournalError::UnsafeObject { .. }
        | CatalogActivationJournalError::OversizedRecord { .. }
        | CatalogActivationJournalError::AcceptedWithoutPrepared
        | CatalogActivationJournalError::OutcomeUnknown { .. } => error,
        _ => CatalogActivationJournalError::OutcomeUnknown {
            record: phase.label(),
            source: Box::new(error),
        },
    }
}

fn canonical_u64(value: &str) -> Option<u64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn is_uid_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn is_graphic_nonspace(byte: u8) -> bool {
    byte.is_ascii_graphic() && !byte.is_ascii_whitespace()
}

#[derive(Debug)]
struct ValidatedParent {
    directory: File,
    path: PathBuf,
    identity: DirectoryIdentity,
    expected_uid: u32,
}

impl ValidatedParent {
    fn open(path: &Path, expected_uid: u32) -> Result<Self, CatalogActivationJournalError> {
        let path_metadata = strict_path_metadata(path)?;
        validate_parent_metadata(path, &path_metadata, expected_uid)?;
        let descriptor = open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| io_error("open parent directory", path, source.into()))?;
        let directory = File::from(descriptor);
        let descriptor_metadata = directory
            .metadata()
            .map_err(|source| io_error("read parent directory metadata", path, source))?;
        validate_parent_metadata(path, &descriptor_metadata, expected_uid)?;
        let identity = DirectoryIdentity::from_metadata(&path_metadata);
        if DirectoryIdentity::from_metadata(&descriptor_metadata) != identity {
            return Err(CatalogActivationJournalError::StateChanged {
                path: path.to_owned(),
            });
        }
        Ok(Self {
            directory,
            path: path.to_owned(),
            identity,
            expected_uid,
        })
    }

    fn sync_after_creation(&self) -> Result<(), CatalogActivationJournalError> {
        self.directory
            .sync_all()
            .map_err(|source| io_error("flush parent directory", &self.path, source))?;
        let path_metadata = strict_path_metadata(&self.path)?;
        let descriptor_metadata = self
            .directory
            .metadata()
            .map_err(|source| io_error("revalidate parent directory", &self.path, source))?;
        validate_parent_metadata(&self.path, &path_metadata, self.expected_uid)?;
        if DirectoryIdentity::from_metadata(&path_metadata) != self.identity
            || DirectoryIdentity::from_metadata(&descriptor_metadata) != self.identity
        {
            return Err(CatalogActivationJournalError::StateChanged {
                path: self.path.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
    owner: u32,
    mode: u32,
}

impl DirectoryIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            owner: metadata.uid(),
            mode: metadata.mode(),
        }
    }
}

#[derive(Debug)]
struct ManagedRecord {
    file: File,
    snapshot: ObjectSnapshot,
    contents: Vec<u8>,
    path: PathBuf,
}

impl ManagedRecord {
    fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ObjectSnapshot {
    device: u64,
    inode: u64,
    size: u64,
    owner: u32,
    mode: u32,
    links: u64,
    modified_seconds: i64,
    modified_nanoseconds: u64,
    changed_seconds: i64,
    changed_nanoseconds: u64,
}

fn object_snapshot(
    stat: &Stat,
    path: &Path,
) -> Result<ObjectSnapshot, CatalogActivationJournalError> {
    Ok(ObjectSnapshot {
        device: stat.st_dev,
        inode: stat.st_ino,
        size: u64::try_from(stat.st_size).map_err(|_| {
            CatalogActivationJournalError::UnsafeObject {
                path: path.to_owned(),
                reason: "negative record size",
            }
        })?,
        owner: stat.st_uid,
        mode: stat.st_mode,
        links: stat.st_nlink,
        modified_seconds: stat.st_mtime,
        modified_nanoseconds: stat.st_mtime_nsec,
        changed_seconds: stat.st_ctime,
        changed_nanoseconds: stat.st_ctime_nsec,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordPhase {
    Prepared,
    Accepted,
}

impl RecordPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::Prepared => "Prepared",
            Self::Accepted => "Accepted",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordName {
    PreparedFinal,
    PreparedStaging,
    AcceptedFinal,
    AcceptedStaging,
}

impl RecordName {
    const fn final_name(phase: RecordPhase) -> Self {
        match phase {
            RecordPhase::Prepared => Self::PreparedFinal,
            RecordPhase::Accepted => Self::AcceptedFinal,
        }
    }

    const fn staging(phase: RecordPhase) -> Self {
        match phase {
            RecordPhase::Prepared => Self::PreparedStaging,
            RecordPhase::Accepted => Self::AcceptedStaging,
        }
    }

    fn from_bytes(name: &[u8]) -> Option<Self> {
        match name {
            b"prepared" => Some(Self::PreparedFinal),
            b".prepared.tmp" => Some(Self::PreparedStaging),
            b"accepted" => Some(Self::AcceptedFinal),
            b".accepted.tmp" => Some(Self::AcceptedStaging),
            _ => None,
        }
    }

    const fn file_name(self) -> &'static str {
        match self {
            Self::PreparedFinal => PREPARED_FILE,
            Self::PreparedStaging => PREPARED_STAGING_FILE,
            Self::AcceptedFinal => ACCEPTED_FILE,
            Self::AcceptedStaging => ACCEPTED_STAGING_FILE,
        }
    }

    const fn phase(self) -> RecordPhase {
        match self {
            Self::PreparedFinal | Self::PreparedStaging => RecordPhase::Prepared,
            Self::AcceptedFinal | Self::AcceptedStaging => RecordPhase::Accepted,
        }
    }

    const fn is_staging(self) -> bool {
        matches!(self, Self::PreparedStaging | Self::AcceptedStaging)
    }

    const fn maximum_bytes(self) -> u64 {
        match self.phase() {
            RecordPhase::Prepared => MAX_PREPARED_RECORD_BYTES,
            RecordPhase::Accepted => MAX_ACCEPTED_RECORD_BYTES,
        }
    }
}

fn validate_normal_absolute_path(path: &Path) -> Result<(), CatalogActivationJournalError> {
    let valid = path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)));
    if valid {
        Ok(())
    } else {
        Err(CatalogActivationJournalError::InvalidDirectoryPath {
            path: path.to_owned(),
        })
    }
}

fn strict_path_metadata(path: &Path) -> Result<Metadata, CatalogActivationJournalError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_error("read path metadata", path, source))?;
    if metadata.file_type().is_symlink() {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "symlinks are not allowed",
        });
    }
    let canonical =
        fs::canonicalize(path).map_err(|source| io_error("canonicalize path", path, source))?;
    if canonical != path {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "symlinked or non-canonical ancestors are not allowed",
        });
    }
    Ok(metadata)
}

fn validate_parent_metadata(
    path: &Path,
    metadata: &Metadata,
    expected_uid: u32,
) -> Result<(), CatalogActivationJournalError> {
    if !metadata.is_dir() {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "parent is not a directory",
        });
    }
    if metadata.uid() != 0 && metadata.uid() != expected_uid {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "parent has an untrusted owner",
        });
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "parent is group- or world-writable",
        });
    }
    Ok(())
}

fn validate_journal_directory_metadata(
    path: &Path,
    metadata: &Metadata,
    expected_uid: u32,
) -> Result<(), CatalogActivationJournalError> {
    if !metadata.is_dir() {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "journal path is not a directory",
        });
    }
    if metadata.uid() != expected_uid {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "journal directory has the wrong owner",
        });
    }
    if metadata.permissions().mode() & 0o7_777 != 0o700 {
        return Err(CatalogActivationJournalError::UnsafeDirectory {
            path: path.to_owned(),
            reason: "journal directory permissions must be 0700",
        });
    }
    Ok(())
}

fn validate_record_stat(
    path: &Path,
    stat: &Stat,
    expected_uid: u32,
    expected_device: u64,
    name: RecordName,
) -> Result<(), CatalogActivationJournalError> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
        return Err(CatalogActivationJournalError::UnsafeObject {
            path: path.to_owned(),
            reason: "record is not a regular file",
        });
    }
    if stat.st_uid != expected_uid {
        return Err(CatalogActivationJournalError::UnsafeObject {
            path: path.to_owned(),
            reason: "record has the wrong owner",
        });
    }
    if stat.st_dev != expected_device {
        return Err(CatalogActivationJournalError::UnsafeObject {
            path: path.to_owned(),
            reason: "record is on a different filesystem from the journal directory",
        });
    }
    let permissions = stat.st_mode & 0o7_777;
    if permissions != 0o400 && !(name.is_staging() && permissions == 0o600) {
        return Err(CatalogActivationJournalError::UnsafeObject {
            path: path.to_owned(),
            reason: "final records must be 0400 and staging records 0400 or 0600",
        });
    }
    if stat.st_nlink != 1 {
        return Err(CatalogActivationJournalError::UnsafeObject {
            path: path.to_owned(),
            reason: "record must have exactly one hard link",
        });
    }
    let bytes =
        u64::try_from(stat.st_size).map_err(|_| CatalogActivationJournalError::UnsafeObject {
            path: path.to_owned(),
            reason: "record has a negative size",
        })?;
    if bytes > name.maximum_bytes() {
        return Err(CatalogActivationJournalError::OversizedRecord {
            path: path.to_owned(),
            bytes,
            maximum: name.maximum_bytes(),
        });
    }
    Ok(())
}

fn io_error(
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> CatalogActivationJournalError {
    CatalogActivationJournalError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashCheckpoint {
    StagingWritten,
    StagingSynced,
    FinalInstalled,
    DirectorySynced,
    ReceiptReread,
}

#[cfg(test)]
std::thread_local! {
    static INJECTED_CRASH: std::cell::Cell<Option<(RecordPhase, CrashCheckpoint)>> = const {
        std::cell::Cell::new(None)
    };
    static INJECTED_UNLOCK_FAILURE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
fn inject_unlock_failure_if_requested(
    unlock: Result<(), CatalogActivationJournalError>,
) -> Result<(), CatalogActivationJournalError> {
    match unlock {
        Err(error) => Err(error),
        Ok(()) if INJECTED_UNLOCK_FAILURE.with(|slot| slot.replace(false)) => {
            Err(CatalogActivationJournalError::InjectedUnlockFailure)
        }
        Ok(()) => Ok(()),
    }
}

#[cfg(test)]
fn crash_checkpoint(
    phase: RecordPhase,
    checkpoint: CrashCheckpoint,
) -> Result<(), CatalogActivationJournalError> {
    let injected = INJECTED_CRASH.with(|slot| {
        if slot.get() == Some((phase, checkpoint)) {
            slot.set(None);
            true
        } else {
            false
        }
    });
    if injected {
        return Err(CatalogActivationJournalError::InjectedCrash);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::sync::{Arc, Barrier};

    use pgshard_types::ShardId;
    use pgshard_types::catalog_activation::{
        CATALOG_ACTIVATION_REQUEST_VERSION, CatalogActivationBootstrap, CatalogActivationCandidate,
        CatalogActivationCluster, CatalogActivationDispatcher, CatalogActivationMaterials,
        CatalogActivationRemoteApplyWitness, CatalogActivationSource,
        CatalogActivationTargetFenceAcknowledgement, CatalogActivationWritableTerm,
        CatalogMaterialIdentity, MaterialIdentity,
    };
    use pgshard_types::writable_generation::DurableWritableGeneration;
    use rustix::fs::{CWD, Mode, mkfifoat};
    use tempfile::TempDir;

    use super::*;

    const SOURCE_HOLDER: &str =
        "demo-shard-0000-member-0000-0/source-pod-uid/0123456789abcdef01234567";
    const DISPATCHER_HOLDER: &str =
        "demo-orchestrator-0/dispatcher-uid/11111111-2222-4333-8444-555555555555";

    fn digest(value: u8) -> String {
        format!("{value:02x}").repeat(32)
    }

    fn generation_identity() -> String {
        String::from_utf8(
            DurableWritableGeneration::new(
                "demo".into(),
                "cluster-uid".into(),
                ShardId(0),
                "database".into(),
                "demo-shard-0000-term".into(),
                "writable-lease-uid".into(),
                SOURCE_HOLDER.into(),
                9,
            )
            .expect("valid generation")
            .canonical_bytes(),
        )
        .expect("canonical generation is UTF-8")
    }

    #[allow(clippy::too_many_lines)]
    fn request() -> CatalogActivationRequest {
        CatalogActivationRequest {
            schema_version: CATALOG_ACTIVATION_REQUEST_VERSION.to_owned(),
            carrier: KubernetesObjectIdentity {
                name: "demo-catalog-activation".into(),
                uid: "carrier-uid".into(),
            },
            cluster: CatalogActivationCluster {
                name: "demo".into(),
                namespace: "database".into(),
                uid: "cluster-uid".into(),
                generation: "7".into(),
                resource_version: "101".into(),
                status_sha256: digest(1),
            },
            dispatcher: CatalogActivationDispatcher {
                pod_name: "demo-orchestrator-0".into(),
                pod_uid: "dispatcher-uid".into(),
                lease_name: "demo-orch-lease".into(),
                lease_uid: "orchestrator-lease-uid".into(),
                lease_resource_version: "102".into(),
                lease_holder: DISPATCHER_HOLDER.into(),
            },
            candidate: CatalogActivationCandidate {
                name: "demo-s0-m0000-cfg-00112233445566778899aabbccddeeff".into(),
                uid: "candidate-uid".into(),
                resource_version: "103".into(),
                payload_sha256: digest(2),
            },
            bootstrap: CatalogActivationBootstrap {
                secret: KubernetesObjectIdentity {
                    name: "bootstrap-secret".into(),
                    uid: "bootstrap-secret-uid".into(),
                },
                pvc: KubernetesObjectIdentity {
                    name: "bootstrap-pvc".into(),
                    uid: "bootstrap-pvc-uid".into(),
                },
            },
            writable_term: CatalogActivationWritableTerm {
                name: "demo-shard-0000-term".into(),
                uid: "writable-lease-uid".into(),
                resource_version: "104".into(),
                holder: SOURCE_HOLDER.into(),
                generation: "9".into(),
            },
            materials: CatalogActivationMaterials {
                replication: MaterialIdentity {
                    name: "replication".into(),
                    uid: "replication-uid".into(),
                    material_sha256: digest(3),
                },
                catalog: CatalogMaterialIdentity {
                    name: "catalog".into(),
                    uid: "catalog-uid".into(),
                    client_sha256: digest(4),
                    server_sha256: digest(5),
                },
                operation_writer: MaterialIdentity {
                    name: "writer".into(),
                    uid: "writer-uid".into(),
                    material_sha256: digest(6),
                },
                postgresql_configuration: MaterialIdentity {
                    name: "configuration".into(),
                    uid: "configuration-uid".into(),
                    material_sha256: digest(7),
                },
                migration_sha256: digest(8),
                genesis_sha256: digest(9),
                preflight_sha256: digest(10),
                serving_hba_version: "pgshard.catalog-serving-hba.v1".into(),
                serving_hba_sha256: digest(11),
                target_template_sha256: digest(12),
            },
            source: CatalogActivationSource {
                cluster_name: "demo".into(),
                cluster_uid: "cluster-uid".into(),
                pod_name: "demo-shard-0000-member-0000-0".into(),
                pod_uid: "source-pod-uid".into(),
                shard: 0,
                member: 0,
                instance_id: "demo-shard-0000-member-0000-0".into(),
                boot_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
                postmaster_pid: 100,
                system_identifier: "12345678901234567890".into(),
                timeline: 3,
                generation_identity: generation_identity(),
                generation_barrier_lsn: "4294967296".into(),
                target_fence_acknowledgement: CatalogActivationTargetFenceAcknowledgement {
                    observed_at_unix_ms: "1700000000000".into(),
                    deadline_boottime_ns: "9000000000".into(),
                    remaining_validity_at_ack_ms: "5000".into(),
                    remaining_validity_at_report_ms: "4500".into(),
                    control_backend_pid: 101,
                },
            },
            remote_apply_witness: CatalogActivationRemoteApplyWitness {
                cluster_name: "demo".into(),
                cluster_uid: "cluster-uid".into(),
                pod_name: "demo-shard-0000-member-0001-0".into(),
                pod_uid: "witness-pod-uid".into(),
                shard: 0,
                member: 1,
                instance_id: "demo-shard-0000-member-0001-0".into(),
                boot_id: "ffffffff-1111-2222-3333-444444444444".into(),
                postmaster_pid: 200,
                member_slot_name: "pgshard_member_0001".into(),
                system_identifier: "12345678901234567890".into(),
                timeline: 3,
                generation_identity: generation_identity(),
                generation_barrier_lsn: "4294967296".into(),
                receive_lsn: "4294967396".into(),
                replay_lsn: "4294967396".into(),
            },
        }
    }

    fn target(request: &CatalogActivationRequest) -> KubernetesObjectIdentity {
        KubernetesObjectIdentity {
            name: request.source.pod_name.clone(),
            uid: request.source.pod_uid.clone(),
        }
    }

    fn journal_path(root: &TempDir) -> PathBuf {
        root.path().join("journal")
    }

    fn write_with_mode(path: &Path, contents: &[u8], mode: u32) {
        fs::write(path, contents).expect("write hostile fixture");
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .expect("set hostile fixture mode");
    }

    fn create_empty_journal(root: &TempDir) -> PathBuf {
        let path = journal_path(root);
        drop(CatalogActivationJournal::open_or_create(&path).expect("create journal"));
        path
    }

    fn retry_busy<T>(
        mut operation: impl FnMut() -> Result<T, CatalogActivationJournalError>,
    ) -> Result<T, CatalogActivationJournalError> {
        for _ in 0..10_000 {
            match operation() {
                Err(CatalogActivationJournalError::Busy { .. }) => std::thread::yield_now(),
                result => return result,
            }
        }
        panic!("journal remained busy throughout the bounded test retry budget");
    }

    struct CrashGuard;

    impl Drop for CrashGuard {
        fn drop(&mut self) {
            INJECTED_CRASH.with(|slot| slot.set(None));
        }
    }

    fn inject_crash(phase: RecordPhase, checkpoint: CrashCheckpoint) -> CrashGuard {
        INJECTED_CRASH.with(|slot| {
            assert!(
                slot.replace(Some((phase, checkpoint))).is_none(),
                "test already has an injected crash"
            );
        });
        CrashGuard
    }

    struct UnlockFailureGuard;

    impl Drop for UnlockFailureGuard {
        fn drop(&mut self) {
            INJECTED_UNLOCK_FAILURE.with(|slot| slot.set(false));
        }
    }

    fn inject_unlock_failure() -> UnlockFailureGuard {
        INJECTED_UNLOCK_FAILURE.with(|slot| {
            assert!(
                !slot.replace(true),
                "test already injects an unlock failure"
            );
        });
        UnlockFailureGuard
    }

    #[test]
    fn prepared_then_accepted_replay_is_inode_stable_and_exact() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = journal_path(&root);
        let request = request();
        let digest = request.sha256().expect("request digest");
        let target = target(&request);
        let mut journal = CatalogActivationJournal::open_or_create(&path).expect("open journal");

        assert_eq!(
            journal.prepare(&request, &digest).expect("prepare"),
            CatalogActivationPrepareOutcome::Installed
        );
        let prepared_metadata = fs::metadata(path.join(PREPARED_FILE)).expect("prepared metadata");
        let prepared_bytes = fs::read(path.join(PREPARED_FILE)).expect("prepared bytes");
        assert_eq!(prepared_metadata.permissions().mode() & 0o7_777, 0o400);
        assert!(
            journal
                .resolve_acceptance(&request, &digest, &target)
                .expect("resolve prepared")
                .is_none()
        );
        assert_eq!(
            journal.prepare(&request, &digest).expect("prepare replay"),
            CatalogActivationPrepareOutcome::Replay
        );
        assert_eq!(
            fs::metadata(path.join(PREPARED_FILE))
                .expect("replayed prepared metadata")
                .ino(),
            prepared_metadata.ino()
        );
        assert_eq!(
            fs::read(path.join(PREPARED_FILE)).expect("replayed prepared bytes"),
            prepared_bytes
        );

        let receipt = journal
            .accept(&request, &digest, &target)
            .expect("accept exact request");
        assert_eq!(receipt.carrier_uid(), request.carrier.uid);
        assert_eq!(receipt.request_sha256(), digest);
        assert_eq!(receipt.target_pod_name(), target.name);
        assert_eq!(receipt.target_pod_uid(), target.uid);
        assert!(canonical_u64(receipt.persisted_at_unix_ms()).is_some());
        let accepted_metadata = fs::metadata(path.join(ACCEPTED_FILE)).expect("accepted metadata");
        let accepted_bytes = fs::read(path.join(ACCEPTED_FILE)).expect("accepted bytes");
        let original_persisted_at = receipt.persisted_at_unix_ms().to_owned();
        assert_eq!(accepted_metadata.permissions().mode() & 0o7_777, 0o400);
        drop(journal);

        let mut reopened = CatalogActivationJournal::open_or_create(&path).expect("reopen journal");
        let resolved = reopened
            .resolve_acceptance(&request, &digest, &target)
            .expect("resolve accepted")
            .expect("accepted final");
        assert_eq!(resolved.persisted_at_unix_ms(), original_persisted_at);
        let replay = reopened
            .accept(&request, &digest, &target)
            .expect("accept replay");
        assert_eq!(replay.persisted_at_unix_ms(), original_persisted_at);
        assert_eq!(
            fs::metadata(path.join(ACCEPTED_FILE))
                .expect("replayed accepted metadata")
                .ino(),
            accepted_metadata.ino()
        );
        assert_eq!(
            fs::read(path.join(ACCEPTED_FILE)).expect("replayed accepted bytes"),
            accepted_bytes
        );
        assert!(!path.join(PREPARED_STAGING_FILE).exists());
        assert!(!path.join(ACCEPTED_STAGING_FILE).exists());
    }

    #[test]
    fn declared_digest_and_target_are_mandatory() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = journal_path(&root);
        let request = request();
        let digest = request.sha256().expect("request digest");
        let mut journal = CatalogActivationJournal::open_or_create(&path).expect("open journal");
        assert!(matches!(
            journal.prepare(&request, &"0".repeat(64)),
            Err(CatalogActivationJournalError::DeclaredDigestMismatch)
        ));
        assert!(!path.join(PREPARED_FILE).exists());
        journal.prepare(&request, &digest).expect("prepare");
        let wrong_target = KubernetesObjectIdentity {
            name: request.source.pod_name.clone(),
            uid: "other-pod-uid".into(),
        };
        assert!(matches!(
            journal.accept(&request, &digest, &wrong_target),
            Err(CatalogActivationJournalError::TargetPodMismatch)
        ));
        assert!(!path.join(ACCEPTED_FILE).exists());
    }

    #[test]
    fn conflicting_request_never_replaces_prepared() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = journal_path(&root);
        let request = request();
        let request_digest = request.sha256().expect("request digest");
        let mut journal = CatalogActivationJournal::open_or_create(&path).expect("open journal");
        journal.prepare(&request, &request_digest).expect("prepare");
        let before = fs::read(path.join(PREPARED_FILE)).expect("prepared bytes");
        let before_inode = fs::metadata(path.join(PREPARED_FILE))
            .expect("prepared metadata")
            .ino();

        let mut conflicting = request.clone();
        conflicting.candidate.payload_sha256 = digest(99);
        let conflicting_digest = conflicting.sha256().expect("conflicting digest");
        assert!(matches!(
            journal.prepare(&conflicting, &conflicting_digest),
            Err(CatalogActivationJournalError::Conflict { record: "Prepared" })
        ));
        assert_eq!(
            fs::read(path.join(PREPARED_FILE)).expect("prepared bytes"),
            before
        );
        assert_eq!(
            fs::metadata(path.join(PREPARED_FILE))
                .expect("prepared metadata")
                .ino(),
            before_inode
        );
    }

    #[test]
    fn accepted_without_prepared_fails_closed() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = create_empty_journal(&root);
        let request = request();
        let accepted = AcceptedRecord {
            schema_version: ACCEPTED_SCHEMA_VERSION.into(),
            carrier_uid: request.carrier.uid.clone(),
            request_sha256: request.sha256().expect("request digest"),
            target_pod_name: request.source.pod_name.clone(),
            target_pod_uid: request.source.pod_uid.clone(),
            persistence: FSYNC_PERSISTENCE.into(),
            persisted_at_unix_ms: "1700000000000".into(),
        };
        write_with_mode(
            &path.join(ACCEPTED_FILE),
            &encode_canonical(RecordPhase::Accepted, &accepted).expect("encode acceptance"),
            0o400,
        );
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&path),
            Err(CatalogActivationJournalError::AcceptedWithoutPrepared)
        ));
    }

    #[test]
    fn corrupt_oversized_and_unsafe_objects_fail_closed() {
        let corrupt_root = tempfile::tempdir().expect("corrupt tempdir");
        let corrupt_path = create_empty_journal(&corrupt_root);
        write_with_mode(&corrupt_path.join(PREPARED_FILE), b"not-json\n", 0o400);
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&corrupt_path),
            Err(CatalogActivationJournalError::CorruptRecord { .. })
        ));

        let oversized_root = tempfile::tempdir().expect("oversized tempdir");
        let oversized_path = create_empty_journal(&oversized_root);
        write_with_mode(
            &oversized_path.join(PREPARED_FILE),
            &vec![b'x'; usize::try_from(MAX_PREPARED_RECORD_BYTES + 1).expect("test size")],
            0o400,
        );
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&oversized_path),
            Err(CatalogActivationJournalError::OversizedRecord { .. })
        ));

        let symlink_root = tempfile::tempdir().expect("symlink tempdir");
        let symlink_path = create_empty_journal(&symlink_root);
        let external = symlink_root.path().join("external");
        write_with_mode(&external, b"external", 0o400);
        symlink(&external, symlink_path.join(PREPARED_FILE)).expect("symlink record");
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&symlink_path),
            Err(CatalogActivationJournalError::UnsafeObject { .. })
        ));

        let directory_root = tempfile::tempdir().expect("directory tempdir");
        let directory_path = create_empty_journal(&directory_root);
        fs::create_dir(directory_path.join(PREPARED_FILE)).expect("directory record");
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&directory_path),
            Err(CatalogActivationJournalError::UnsafeObject { .. })
        ));

        let fifo_root = tempfile::tempdir().expect("fifo tempdir");
        let fifo_path = create_empty_journal(&fifo_root);
        mkfifoat(CWD, fifo_path.join(PREPARED_FILE), Mode::RUSR | Mode::WUSR).expect("fifo record");
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&fifo_path),
            Err(CatalogActivationJournalError::UnsafeObject { .. })
        ));

        let hardlink_root = tempfile::tempdir().expect("hardlink tempdir");
        let hardlink_path = create_empty_journal(&hardlink_root);
        write_with_mode(&hardlink_path.join(PREPARED_FILE), b"hardlinked", 0o400);
        fs::hard_link(
            hardlink_path.join(PREPARED_FILE),
            hardlink_root.path().join("outside-alias"),
        )
        .expect("hardlink record");
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&hardlink_path),
            Err(CatalogActivationJournalError::UnsafeObject { .. })
        ));

        let mode_root = tempfile::tempdir().expect("mode tempdir");
        let mode_path = create_empty_journal(&mode_root);
        write_with_mode(&mode_path.join(PREPARED_FILE), b"unsafe mode", 0o600);
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&mode_path),
            Err(CatalogActivationJournalError::UnsafeObject { .. })
        ));

        let unknown_root = tempfile::tempdir().expect("unknown tempdir");
        let unknown_path = create_empty_journal(&unknown_root);
        write_with_mode(&unknown_path.join("unknown"), b"unknown", 0o400);
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&unknown_path),
            Err(CatalogActivationJournalError::UnsafeObject { .. })
        ));

        let accepted_root = tempfile::tempdir().expect("accepted tempdir");
        let accepted_path = journal_path(&accepted_root);
        let request = request();
        let request_digest = request.sha256().expect("request digest");
        let target = target(&request);
        let mut journal =
            CatalogActivationJournal::open_or_create(&accepted_path).expect("open journal");
        journal.prepare(&request, &request_digest).expect("prepare");
        drop(
            journal
                .accept(&request, &request_digest, &target)
                .expect("accept"),
        );
        fs::set_permissions(
            accepted_path.join(ACCEPTED_FILE),
            fs::Permissions::from_mode(0o600),
        )
        .expect("make accepted writable");
        write_with_mode(&accepted_path.join(ACCEPTED_FILE), b"corrupt\n", 0o400);
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&accepted_path),
            Err(CatalogActivationJournalError::CorruptRecord { .. })
        ));
    }

    #[test]
    fn unsafe_directory_paths_and_permissions_fail_closed() {
        assert!(matches!(
            CatalogActivationJournal::open_or_create("relative-journal"),
            Err(CatalogActivationJournalError::InvalidDirectoryPath { .. })
        ));

        let mode_root = tempfile::tempdir().expect("mode tempdir");
        let mode_path = create_empty_journal(&mode_root);
        fs::set_permissions(&mode_path, fs::Permissions::from_mode(0o750))
            .expect("change journal mode");
        assert!(matches!(
            CatalogActivationJournal::open_or_create(&mode_path),
            Err(CatalogActivationJournalError::UnsafeDirectory { .. })
        ));

        let symlink_root = tempfile::tempdir().expect("symlink tempdir");
        let real_path = create_empty_journal(&symlink_root);
        let alias_path = symlink_root.path().join("journal-alias");
        symlink(&real_path, &alias_path).expect("symlink journal");
        assert!(CatalogActivationJournal::open_or_create(&alias_path).is_err());

        let owner_root = tempfile::tempdir().expect("owner tempdir");
        assert!(matches!(
            CatalogActivationJournal::open_or_create_for_uid(
                &journal_path(&owner_root),
                geteuid().as_raw().saturating_add(1)
            ),
            Err(CatalogActivationJournalError::UnsafeDirectory { .. })
        ));
    }

    #[test]
    fn safe_partial_staging_is_recovered_but_never_authoritative() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = create_empty_journal(&root);
        let request = request();
        let digest = request.sha256().expect("request digest");
        let target = target(&request);
        write_with_mode(&path.join(PREPARED_STAGING_FILE), b"partial", 0o600);
        let mut journal = CatalogActivationJournal::open_or_create(&path).expect("reopen journal");
        journal
            .prepare(&request, &digest)
            .expect("recover prepared staging");
        write_with_mode(&path.join(ACCEPTED_STAGING_FILE), b"partial", 0o600);
        assert!(
            journal
                .resolve_acceptance(&request, &digest, &target)
                .expect("resolve staging only")
                .is_none()
        );
        drop(
            journal
                .accept(&request, &digest, &target)
                .expect("recover accepted staging"),
        );
        assert!(path.join(ACCEPTED_FILE).exists());
        assert!(!path.join(ACCEPTED_STAGING_FILE).exists());
    }

    #[test]
    fn every_crash_checkpoint_converges_from_final_state() {
        for phase in [RecordPhase::Prepared, RecordPhase::Accepted] {
            for checkpoint in [
                CrashCheckpoint::StagingWritten,
                CrashCheckpoint::StagingSynced,
                CrashCheckpoint::FinalInstalled,
                CrashCheckpoint::DirectorySynced,
            ] {
                let root = tempfile::tempdir().expect("tempdir");
                let path = journal_path(&root);
                let request = request();
                let digest = request.sha256().expect("request digest");
                let target = target(&request);
                let mut journal =
                    CatalogActivationJournal::open_or_create(&path).expect("open journal");
                if phase == RecordPhase::Accepted {
                    journal.prepare(&request, &digest).expect("prepare first");
                }
                let guard = inject_crash(phase, checkpoint);
                let crashed = match phase {
                    RecordPhase::Prepared => journal.prepare(&request, &digest).map(|_| ()),
                    RecordPhase::Accepted => journal.accept(&request, &digest, &target).map(drop),
                };
                match checkpoint {
                    CrashCheckpoint::StagingWritten | CrashCheckpoint::StagingSynced => assert!(
                        matches!(crashed, Err(CatalogActivationJournalError::InjectedCrash))
                    ),
                    CrashCheckpoint::FinalInstalled | CrashCheckpoint::DirectorySynced => {
                        assert!(matches!(
                            crashed,
                            Err(CatalogActivationJournalError::OutcomeUnknown {
                                record,
                                source,
                            }) if record == phase.label()
                                && matches!(*source, CatalogActivationJournalError::InjectedCrash)
                        ));
                    }
                    CrashCheckpoint::ReceiptReread => {
                        unreachable!("receipt reread has its own Accepted-only regression")
                    }
                }
                drop(guard);
                drop(journal);

                let mut restarted =
                    CatalogActivationJournal::open_or_create(&path).expect("restart journal");
                match phase {
                    RecordPhase::Prepared => {
                        restarted
                            .prepare(&request, &digest)
                            .expect("restart prepare");
                    }
                    RecordPhase::Accepted => {
                        let resolution = restarted
                            .resolve_acceptance(&request, &digest, &target)
                            .expect("resolve acceptance");
                        if matches!(
                            checkpoint,
                            CrashCheckpoint::StagingWritten | CrashCheckpoint::StagingSynced
                        ) {
                            assert!(resolution.is_none(), "staging resolved as acceptance");
                            drop(
                                restarted
                                    .accept(&request, &digest, &target)
                                    .expect("finish staged acceptance"),
                            );
                        } else {
                            assert!(resolution.is_some(), "installed final was not resolved");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn accepted_receipt_reread_failure_is_outcome_unknown_and_resolvable() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = journal_path(&root);
        let request = request();
        let digest = request.sha256().expect("request digest");
        let target = target(&request);
        let mut journal = CatalogActivationJournal::open_or_create(&path).expect("open journal");
        journal.prepare(&request, &digest).expect("prepare");

        let guard = inject_crash(RecordPhase::Accepted, CrashCheckpoint::ReceiptReread);
        assert!(matches!(
            journal.accept(&request, &digest, &target),
            Err(CatalogActivationJournalError::OutcomeUnknown {
                record: "Accepted",
                source,
            }) if matches!(*source, CatalogActivationJournalError::InjectedCrash)
        ));
        drop(guard);
        assert!(
            journal
                .resolve_acceptance(&request, &digest, &target)
                .expect("resolve installed acceptance")
                .is_some()
        );
    }

    #[test]
    fn unlock_failure_preserves_phase_aware_success_semantics() {
        {
            let root = tempfile::tempdir().expect("prepare tempdir");
            let path = journal_path(&root);
            let request = request();
            let digest = request.sha256().expect("request digest");
            let mut journal =
                CatalogActivationJournal::open_or_create(&path).expect("open prepare journal");
            let guard = inject_unlock_failure();
            assert!(matches!(
                journal.prepare(&request, &digest),
                Err(CatalogActivationJournalError::OutcomeUnknown {
                    record: "Prepared",
                    source,
                }) if matches!(*source, CatalogActivationJournalError::InjectedUnlockFailure)
            ));
            drop(guard);
            assert_eq!(
                journal.prepare(&request, &digest).expect("replay prepared"),
                CatalogActivationPrepareOutcome::Replay
            );
        }

        {
            let root = tempfile::tempdir().expect("accept tempdir");
            let path = journal_path(&root);
            let request = request();
            let digest = request.sha256().expect("request digest");
            let target = target(&request);
            let mut journal =
                CatalogActivationJournal::open_or_create(&path).expect("open accept journal");
            journal.prepare(&request, &digest).expect("prepare");
            let guard = inject_unlock_failure();
            assert!(matches!(
                journal.accept(&request, &digest, &target),
                Err(CatalogActivationJournalError::OutcomeUnknown {
                    record: "Accepted",
                    source,
                }) if matches!(*source, CatalogActivationJournalError::InjectedUnlockFailure)
            ));
            drop(guard);
            assert!(
                journal
                    .resolve_acceptance(&request, &digest, &target)
                    .expect("resolve accepted")
                    .is_some()
            );
        }

        {
            let root = tempfile::tempdir().expect("resolve-some tempdir");
            let path = journal_path(&root);
            let request = request();
            let digest = request.sha256().expect("request digest");
            let target = target(&request);
            let mut journal =
                CatalogActivationJournal::open_or_create(&path).expect("open resolve journal");
            journal.prepare(&request, &digest).expect("prepare");
            drop(journal.accept(&request, &digest, &target).expect("accept"));
            let guard = inject_unlock_failure();
            assert!(matches!(
                journal.resolve_acceptance(&request, &digest, &target),
                Err(CatalogActivationJournalError::OutcomeUnknown {
                    record: "Accepted",
                    source,
                }) if matches!(*source, CatalogActivationJournalError::InjectedUnlockFailure)
            ));
            drop(guard);
            assert!(
                journal
                    .resolve_acceptance(&request, &digest, &target)
                    .expect("resolve accepted after unlock failure")
                    .is_some()
            );
        }

        {
            let root = tempfile::tempdir().expect("resolve-none tempdir");
            let path = journal_path(&root);
            let request = request();
            let digest = request.sha256().expect("request digest");
            let target = target(&request);
            let mut journal =
                CatalogActivationJournal::open_or_create(&path).expect("open empty journal");
            let guard = inject_unlock_failure();
            assert!(matches!(
                journal.resolve_acceptance(&request, &digest, &target),
                Err(CatalogActivationJournalError::InjectedUnlockFailure)
            ));
            drop(guard);
            assert!(
                journal
                    .resolve_acceptance(&request, &digest, &target)
                    .expect("resolve absent after unlock failure")
                    .is_none()
            );
        }
    }

    #[test]
    fn concurrent_exact_writers_converge() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = journal_path(&root);
        let request = Arc::new(request());
        let digest = Arc::new(request.sha256().expect("request digest"));
        let target = Arc::new(target(&request));
        let barrier = Arc::new(Barrier::new(8));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let request = Arc::clone(&request);
            let digest = Arc::clone(&digest);
            let target = Arc::clone(&target);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                let mut journal =
                    CatalogActivationJournal::open_or_create(path).expect("concurrent open");
                barrier.wait();
                retry_busy(|| journal.prepare(&request, &digest)).expect("concurrent prepare");
                retry_busy(|| journal.accept(&request, &digest, &target))
                    .expect("concurrent accept")
                    .persisted_at_unix_ms()
                    .to_owned()
            }));
        }
        let persisted_times: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .collect();
        assert!(
            persisted_times
                .iter()
                .all(|persisted| persisted == &persisted_times[0])
        );
        let mut journal = CatalogActivationJournal::open_or_create(path).expect("final open");
        assert!(
            journal
                .resolve_acceptance(&request, &digest, &target)
                .expect("final resolve")
                .is_some()
        );
    }

    #[test]
    fn live_lock_contention_fails_immediately_without_mutation() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = create_empty_journal(&root);
        let request = request();
        let digest = request.sha256().expect("request digest");
        let first = CatalogActivationJournal::open_or_create(&path).expect("first handle");
        let mut second = CatalogActivationJournal::open_or_create(&path).expect("second handle");

        flock(&first.directory, FlockOperation::NonBlockingLockExclusive)
            .expect("hold first handle lock");
        assert!(matches!(
            second.prepare(&request, &digest),
            Err(CatalogActivationJournalError::Busy { path: busy_path }) if busy_path == path
        ));
        assert!(!path.join(PREPARED_FILE).exists());
        assert!(!path.join(PREPARED_STAGING_FILE).exists());
        flock(&first.directory, FlockOperation::Unlock).expect("release first handle lock");

        assert_eq!(
            second
                .prepare(&request, &digest)
                .expect("prepare after unlock"),
            CatalogActivationPrepareOutcome::Installed
        );
    }

    #[test]
    fn concurrent_conflicting_writers_choose_one_immutable_prepared() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = create_empty_journal(&root);
        let left = request();
        let mut right = left.clone();
        right.candidate.payload_sha256 = digest(42);
        let barrier = Arc::new(Barrier::new(2));
        let workers: Vec<_> = [left, right]
            .into_iter()
            .map(|request| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let digest = request.sha256().expect("request digest");
                    let mut journal =
                        CatalogActivationJournal::open_or_create(path).expect("open journal");
                    barrier.wait();
                    retry_busy(|| journal.prepare(&request, &digest))
                })
            })
            .collect();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(CatalogActivationJournalError::Conflict { .. })
                ))
                .count(),
            1
        );
    }
}
