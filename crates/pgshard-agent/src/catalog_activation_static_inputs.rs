//! Dormant verification of request-sealed static catalog inputs.
//!
//! This stage consumes the private durable-acceptance handoff, opens four
//! bounded regular files, verifies their exact SHA-256 digests, compiles the
//! protocol-safe materialization program, and re-validates the sealed shard
//! count. It has no credentials, SQL, `PostgreSQL`, PGDATA, HBA, readiness,
//! routing, fencing, Lease, process, or serving authority.

use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustix::fs::{Mode, OFlags, open};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::watch;

use crate::catalog_activation_consumer::{
    CatalogActivationHandoff, DurablyAcceptedCatalogActivation,
};
use crate::catalog_materialization_program::{
    CatalogMaterializationProgram, CatalogMaterializationProgramError,
    compile_catalog_materialization_program,
};

const MAXIMUM_STATIC_INPUT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_SHARDS: u32 = 128;
const RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// Exact paths for the four non-secret static activation inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogActivationStaticInputsConfig {
    migration: PathBuf,
    inventory: PathBuf,
    genesis: PathBuf,
    preflight: PathBuf,
}

impl CatalogActivationStaticInputsConfig {
    /// Validates four distinct absolute normalized file paths.
    ///
    /// # Errors
    ///
    /// Returns an error for a relative, non-normalized, root-only, or duplicate
    /// path.
    pub(crate) fn new(
        migration_path: PathBuf,
        inventory_path: PathBuf,
        genesis_path: PathBuf,
        preflight_path: PathBuf,
    ) -> Result<Self, CatalogActivationStaticInputsConfigError> {
        let paths = [
            &migration_path,
            &inventory_path,
            &genesis_path,
            &preflight_path,
        ];
        let all_absolute_normal = paths.iter().all(|path| absolute_normal_file_path(path));
        let all_distinct = paths
            .iter()
            .enumerate()
            .all(|(index, path)| paths[index + 1..].iter().all(|other| other != path));
        if !all_absolute_normal || !all_distinct {
            return Err(CatalogActivationStaticInputsConfigError);
        }
        Ok(Self {
            migration: migration_path,
            inventory: inventory_path,
            genesis: genesis_path,
            preflight: preflight_path,
        })
    }
}

/// Static-input path validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("invalid catalog-activation static-input paths")]
pub struct CatalogActivationStaticInputsConfigError;

/// Opaque retained receiver for verified static input snapshots.
///
/// There is intentionally no observer API. The materializer consumes this
/// private handoff before the snapshots can be used.
#[must_use = "dropping the static-input handoff closes its private watch"]
pub struct CatalogMaterializationHandoff {
    receiver: watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
}

impl CatalogMaterializationHandoff {
    /// Moves the private receiver into the writable-runtime binding.
    pub(crate) fn into_receiver(
        self,
    ) -> watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>> {
        self.receiver
    }
}

/// Exact request-bound materialization program and scalars. This type is
/// deliberately private, move-only, non-serializable, and non-debuggable.
pub(crate) struct ValidatedCatalogStaticInputs {
    pub(crate) accepted: Arc<DurablyAcceptedCatalogActivation>,
    pub(crate) program: CatalogMaterializationProgram,
    pub(crate) shard_count: u32,
    pub(crate) allow_empty_database_topology: bool,
}

#[derive(Clone)]
struct ExpectedStaticInputs {
    migration_sha256: String,
    inventory_sha256: String,
    genesis_sha256: String,
    preflight_sha256: String,
    shard_count: String,
}

impl ExpectedStaticInputs {
    fn from_accepted(accepted: &DurablyAcceptedCatalogActivation) -> Self {
        let materials = &accepted.request().materials;
        Self {
            migration_sha256: materials.migration_sha256.clone(),
            inventory_sha256: materials.inventory_sha256.clone(),
            genesis_sha256: materials.genesis_sha256.clone(),
            preflight_sha256: materials.preflight_sha256.clone(),
            shard_count: materials.shard_count.clone(),
        }
    }
}

struct StaticInputProgram {
    program: CatalogMaterializationProgram,
    shard_count: u32,
}

/// Starts the optional, fault-isolated static-input verifier.
///
/// The verifier never sends global shutdown. It withdraws its private output
/// before every read attempt and whenever the accepted request changes.
pub fn spawn_catalog_activation_static_input_verifier(
    config: Option<CatalogActivationStaticInputsConfig>,
    accepted: CatalogActivationHandoff,
    shutdown: watch::Receiver<bool>,
) -> CatalogMaterializationHandoff {
    let (sender, receiver) = watch::channel(None);
    let handoff = CatalogMaterializationHandoff { receiver };
    let Some(config) = config else {
        return handoff;
    };
    let accepted = accepted.into_receiver();
    tokio::spawn(supervise(config, accepted, sender, shutdown));
    handoff
}

async fn supervise(
    config: CatalogActivationStaticInputsConfig,
    mut accepted: watch::Receiver<Option<Arc<DurablyAcceptedCatalogActivation>>>,
    output: watch::Sender<Option<Arc<ValidatedCatalogStaticInputs>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let _withdraw_on_exit = OutputWithdrawalGuard(output.clone());
    loop {
        output.send_replace(None);
        if *shutdown.borrow() {
            return;
        }
        let Some(current) = accepted.borrow().clone() else {
            tokio::select! {
                changed = accepted.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = shutdown.changed() => return,
            }
            continue;
        };
        let expected = ExpectedStaticInputs::from_accepted(&current);
        let validation_config = config.clone();
        let mut validation = tokio::task::spawn_blocking(move || {
            validate_static_inputs(&validation_config, &expected)
        });
        let result = tokio::select! {
            result = &mut validation => Some(result),
            changed = accepted.changed() => {
                validation.abort();
                if changed.is_err() {
                    return;
                }
                None
            }
            _ = shutdown.changed() => {
                validation.abort();
                return;
            }
        };
        let Some(result) = result else {
            continue;
        };
        let inputs = match result {
            Ok(Ok(inputs)) => inputs,
            Ok(Err(error)) => {
                tracing::warn!(reason = %error,
                    "catalog-activation static inputs unavailable; retrying independently");
                if wait_for_retry_or_change(&mut accepted, &mut shutdown).await {
                    return;
                }
                continue;
            }
            Err(error) => {
                tracing::warn!(reason = %error,
                    "catalog-activation static-input worker failed; retrying independently");
                if wait_for_retry_or_change(&mut accepted, &mut shutdown).await {
                    return;
                }
                continue;
            }
        };
        if !publish_if_current(&accepted, &shutdown, &output, current, inputs) {
            continue;
        }
        tokio::select! {
            changed = accepted.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}

fn publish_if_current(
    accepted: &watch::Receiver<Option<Arc<DurablyAcceptedCatalogActivation>>>,
    shutdown: &watch::Receiver<bool>,
    output: &watch::Sender<Option<Arc<ValidatedCatalogStaticInputs>>>,
    current: Arc<DurablyAcceptedCatalogActivation>,
    inputs: StaticInputProgram,
) -> bool {
    let checked_current = Arc::clone(&current);
    publish_while_guarded(
        accepted.borrow(),
        shutdown.borrow(),
        &checked_current,
        || {
            output.send_replace(Some(Arc::new(ValidatedCatalogStaticInputs {
                accepted: current,
                program: inputs.program,
                shard_count: inputs.shard_count,
                // Strict default: the preflight treats an unset or false
                // policy as "require the sealed topology". The live-catalog
                // derivation that can relax this ships with the executor
                // enablement, not here.
                allow_empty_database_topology: false,
            })));
        },
    )
}

fn publish_while_guarded<Accepted, Shutdown>(
    accepted_guard: Accepted,
    shutdown_guard: Shutdown,
    current: &DurablyAcceptedCatalogActivation,
    publish: impl FnOnce(),
) -> bool
where
    Accepted: Deref<Target = Option<Arc<DurablyAcceptedCatalogActivation>>>,
    Shutdown: Deref<Target = bool>,
{
    // Keep both read guards through the synchronous output update. A sender
    // cannot complete an acceptance revocation or shutdown transition in the
    // interval after these checks but before the snapshot is published.
    let still_current = accepted_guard
        .as_ref()
        .is_some_and(|observed| exact_acceptance(observed, current));
    if !still_current || *shutdown_guard {
        return false;
    }
    publish();
    true
}

async fn wait_for_retry_or_change(
    accepted: &mut watch::Receiver<Option<Arc<DurablyAcceptedCatalogActivation>>>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        () = tokio::time::sleep(RETRY_INTERVAL) => false,
        changed = accepted.changed() => changed.is_err(),
        _ = shutdown.changed() => true,
    }
}

pub(crate) fn exact_acceptance(
    left: &DurablyAcceptedCatalogActivation,
    right: &DurablyAcceptedCatalogActivation,
) -> bool {
    left.request() == right.request()
        && left.carrier_uid() == right.carrier_uid()
        && left.request_sha256() == right.request_sha256()
        && left.target_pod_name() == right.target_pod_name()
        && left.target_pod_uid() == right.target_pod_uid()
        && left.persisted_at_unix_ms() == right.persisted_at_unix_ms()
}

fn validate_static_inputs(
    config: &CatalogActivationStaticInputsConfig,
    expected: &ExpectedStaticInputs,
) -> Result<StaticInputProgram, CatalogActivationStaticInputError> {
    let shard_count = parse_shard_count(&expected.shard_count)?;
    let migration = read_and_verify("migration", &config.migration, &expected.migration_sha256)?;
    let inventory = read_and_verify("inventory", &config.inventory, &expected.inventory_sha256)?;
    let genesis = read_and_verify("genesis", &config.genesis, &expected.genesis_sha256)?;
    let preflight = read_and_verify("preflight", &config.preflight, &expected.preflight_sha256)?;
    let program =
        compile_catalog_materialization_program(&migration, &inventory, &genesis, &preflight)?;
    Ok(StaticInputProgram {
        program,
        shard_count,
    })
}

/// Re-validates the request-sealed shard count independently of the operator:
/// a canonical unsigned decimal within `1..=MAXIMUM_SHARDS`, never SQL.
fn parse_shard_count(sealed: &str) -> Result<u32, CatalogActivationStaticInputError> {
    let canonical = !sealed.is_empty()
        && sealed.bytes().all(|byte| byte.is_ascii_digit())
        && !sealed.starts_with('0');
    let count = canonical
        .then(|| sealed.parse::<u32>().ok())
        .flatten()
        .filter(|count| (1..=MAXIMUM_SHARDS).contains(count));
    count.ok_or(CatalogActivationStaticInputError::InvalidShardCount)
}

fn read_and_verify(
    name: &'static str,
    path: &Path,
    expected_sha256: &str,
) -> Result<Box<[u8]>, CatalogActivationStaticInputError> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|source| CatalogActivationStaticInputError::Open {
        name,
        source: std::io::Error::from(source),
    })?;
    let file = File::from(descriptor);
    let metadata = file
        .metadata()
        .map_err(|source| CatalogActivationStaticInputError::Metadata { name, source })?;
    if !metadata.is_file() {
        return Err(CatalogActivationStaticInputError::NotRegular { name });
    }
    if metadata.len() == 0 || metadata.len() > MAXIMUM_STATIC_INPUT_BYTES {
        return Err(CatalogActivationStaticInputError::InvalidSize { name });
    }
    let capacity = usize::try_from(metadata.len()).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAXIMUM_STATIC_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| CatalogActivationStaticInputError::Read { name, source })?;
    if bytes.is_empty() || bytes.len() as u64 > MAXIMUM_STATIC_INPUT_BYTES {
        return Err(CatalogActivationStaticInputError::InvalidSize { name });
    }
    let actual = sha256_hex(&bytes);
    if actual != expected_sha256 {
        return Err(CatalogActivationStaticInputError::DigestMismatch { name });
    }
    Ok(bytes.into_boxed_slice())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn absolute_normal_file_path(path: &Path) -> bool {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    let mut normal = false;
    for component in components {
        if !matches!(component, Component::Normal(_)) {
            return false;
        }
        normal = true;
    }
    normal
}

#[derive(Debug, Error)]
enum CatalogActivationStaticInputError {
    #[error(transparent)]
    Program(#[from] CatalogMaterializationProgramError),
    #[error("catalog-activation shard count is not a canonical bounded decimal")]
    InvalidShardCount,
    #[error("open catalog-activation {name} input: {source}")]
    Open {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("inspect catalog-activation {name} input: {source}")]
    Metadata {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("catalog-activation {name} input is not a regular file")]
    NotRegular { name: &'static str },
    #[error("catalog-activation {name} input has an invalid size")]
    InvalidSize { name: &'static str },
    #[error("read catalog-activation {name} input: {source}")]
    Read {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("catalog-activation {name} input digest does not match the accepted request")]
    DigestMismatch { name: &'static str },
}

struct OutputWithdrawalGuard(watch::Sender<Option<Arc<ValidatedCatalogStaticInputs>>>);

impl Drop for OutputWithdrawalGuard {
    fn drop(&mut self) {
        self.0.send_replace(None);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::sync::Mutex;

    use rustix::fs::{CWD, Mode, mkfifoat};
    use tempfile::tempdir;
    use tokio::time::timeout;

    use super::*;
    use crate::catalog_activation_consumer::tests as consumer_tests;

    const TEST_MIGRATION: &[u8] = b"BEGIN;\nSELECT 1;\nCOMMIT;\n";
    const TEST_INVENTORY: &[u8] = concat!(
        "SET LOCAL session_replication_role = origin;\n",
        "SELECT pg_catalog.current_setting('pgshard.expected_shard_count')::bigint;\n",
    )
    .as_bytes();
    const TEST_PREFLIGHT: &[u8] = concat!(
        "DO $pgshard_database_topology_preflight$\nBEGIN\n  PERFORM 1;\nEND\n",
        "$pgshard_database_topology_preflight$;\n",
    )
    .as_bytes();

    fn test_genesis(body: &str) -> Vec<u8> {
        format!("SET LOCAL search_path = pg_catalog;\n{body}\n").into_bytes()
    }

    fn digest(bytes: &[u8]) -> String {
        sha256_hex(bytes)
    }

    fn expected_inputs(genesis: &[u8]) -> ExpectedStaticInputs {
        ExpectedStaticInputs {
            migration_sha256: digest(TEST_MIGRATION),
            inventory_sha256: digest(TEST_INVENTORY),
            genesis_sha256: digest(genesis),
            preflight_sha256: digest(TEST_PREFLIGHT),
            shard_count: "4".to_owned(),
        }
    }

    fn fixture() -> (
        tempfile::TempDir,
        CatalogActivationStaticInputsConfig,
        ExpectedStaticInputs,
    ) {
        let directory = tempdir().expect("temporary directory");
        let migration = directory.path().join("migration.sql");
        let inventory = directory.path().join("inventory.sql");
        let genesis = directory.path().join("genesis.sql");
        let preflight = directory.path().join("preflight.sql");
        let genesis_bytes = test_genesis("SELECT 1;");
        fs::write(&migration, TEST_MIGRATION).expect("migration");
        fs::write(&inventory, TEST_INVENTORY).expect("inventory");
        fs::write(&genesis, &genesis_bytes).expect("genesis");
        fs::write(&preflight, TEST_PREFLIGHT).expect("preflight");
        let config =
            CatalogActivationStaticInputsConfig::new(migration, inventory, genesis, preflight)
                .expect("valid paths");
        (directory, config, expected_inputs(&genesis_bytes))
    }

    #[test]
    fn validates_and_compiles_exact_snapshots_with_the_sealed_shard_count() {
        let (_directory, config, expected) = fixture();
        let inputs = validate_static_inputs(&config, &expected).expect("exact inputs");
        assert_eq!(inputs.program.migration.as_bytes(), TEST_MIGRATION);
        assert_eq!(inputs.program.inventory.as_bytes(), TEST_INVENTORY);
        assert_eq!(inputs.program.genesis.as_bytes(), test_genesis("SELECT 1;"));
        assert_eq!(inputs.program.preflight.as_bytes(), TEST_PREFLIGHT);
        assert_eq!(inputs.shard_count, 4);
    }

    #[test]
    fn rejects_digest_matching_inputs_that_are_not_protocol_safe_programs() {
        let (_directory, config, mut expected) = fixture();
        let secret = "do-not-log-static-input-contents";
        let invalid = format!("BEGIN;\n\\echo {secret}\nCOMMIT;\n");
        fs::write(&config.migration, &invalid).expect("invalid migration");
        expected.migration_sha256 = digest(invalid.as_bytes());

        let Err(error) = validate_static_inputs(&config, &expected) else {
            panic!("digest-matching psql program was accepted");
        };
        assert!(matches!(
            error,
            CatalogActivationStaticInputError::Program(_)
        ));
        assert!(error.to_string().contains("migration input"));
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn rejects_a_caller_framed_input_that_carries_transaction_control() {
        let (_directory, config, mut expected) = fixture();
        let framed = test_genesis("BEGIN;\nSELECT 1;\nCOMMIT;");
        fs::write(&config.genesis, &framed).expect("framed genesis");
        expected.genesis_sha256 = digest(&framed);

        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::Program(_))
        ));
    }

    #[test]
    fn revalidates_the_sealed_shard_count_against_its_canonical_bounds() {
        for (sealed, expected_count) in [("1", 1), ("4", 4), ("128", 128)] {
            assert_eq!(
                parse_shard_count(sealed).expect("canonical shard count"),
                expected_count
            );
        }
        for invalid in ["", "0", "007", "129", "4096", "+4", "-1", "4a", " 4", "٤"] {
            assert!(matches!(
                parse_shard_count(invalid),
                Err(CatalogActivationStaticInputError::InvalidShardCount)
            ));
        }
    }

    #[test]
    fn follows_projected_volume_symlinks_but_snapshots_exact_bytes() {
        let (directory, mut config, mut expected) = fixture();
        let first = directory.path().join("..first");
        let second = directory.path().join("..second");
        fs::create_dir(&first).expect("first projection");
        fs::create_dir(&second).expect("second projection");
        let first_bytes = test_genesis("SELECT 'first';");
        let second_bytes = test_genesis("SELECT 'second';");
        fs::write(first.join("genesis.sql"), &first_bytes).expect("first bytes");
        fs::write(second.join("genesis.sql"), &second_bytes).expect("second bytes");
        let projected = directory.path().join("projected-genesis.sql");
        symlink(first.join("genesis.sql"), &projected).expect("projected symlink");
        config.genesis = projected.clone();
        expected.genesis_sha256 = digest(&first_bytes);
        let first_snapshot = validate_static_inputs(&config, &expected).expect("first projection");
        fs::remove_file(&projected).expect("remove old projection");
        symlink(second.join("genesis.sql"), &projected).expect("new projected symlink");
        assert_eq!(first_snapshot.program.genesis.as_bytes(), first_bytes);
        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::DigestMismatch { name: "genesis" })
        ));
    }

    #[test]
    fn rejects_missing_nonregular_oversize_and_digest_mismatch() {
        let (directory, mut config, expected) = fixture();
        fs::remove_file(&config.migration).expect("remove migration");
        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::Open {
                name: "migration",
                ..
            })
        ));

        config.migration = directory.path().to_path_buf();
        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::NotRegular { name: "migration" })
        ));

        let fifo = directory.path().join("fifo.sql");
        mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).expect("FIFO input");
        config.migration = fifo;
        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::NotRegular { name: "migration" })
        ));

        let oversized = directory.path().join("oversized.sql");
        File::create(&oversized)
            .and_then(|file| file.set_len(MAXIMUM_STATIC_INPUT_BYTES + 1))
            .expect("oversized input");
        config.migration = oversized;
        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::InvalidSize { name: "migration" })
        ));

        let mismatch = directory.path().join("mismatch.sql");
        fs::write(&mismatch, b"BEGIN;\nSELECT 2;\nCOMMIT;\n").expect("mismatch input");
        config.migration = mismatch;
        assert!(matches!(
            validate_static_inputs(&config, &expected),
            Err(CatalogActivationStaticInputError::DigestMismatch { name: "migration" })
        ));
    }

    #[test]
    fn rejects_each_individual_digest_mismatch() {
        for name in ["migration", "inventory", "genesis", "preflight"] {
            let (_directory, config, mut expected) = fixture();
            let other = digest(b"other\n");
            match name {
                "migration" => expected.migration_sha256 = other,
                "inventory" => expected.inventory_sha256 = other,
                "genesis" => expected.genesis_sha256 = other,
                "preflight" => expected.preflight_sha256 = other,
                _ => unreachable!(),
            }
            assert!(matches!(
                validate_static_inputs(&config, &expected),
                Err(CatalogActivationStaticInputError::DigestMismatch { name: actual })
                    if actual == name
            ));
        }
    }

    #[test]
    fn paths_are_absolute_normal_and_distinct() {
        assert!(
            CatalogActivationStaticInputsConfig::new(
                PathBuf::from("/migration.sql"),
                PathBuf::from("/inventory.sql"),
                PathBuf::from("/genesis.sql"),
                PathBuf::from("/preflight.sql"),
            )
            .is_ok()
        );
        for paths in [
            [
                "relative",
                "/inventory.sql",
                "/genesis.sql",
                "/preflight.sql",
            ],
            [
                "/a/../migration.sql",
                "/inventory.sql",
                "/genesis.sql",
                "/preflight.sql",
            ],
            [
                "/migration.sql",
                "/migration.sql",
                "/genesis.sql",
                "/preflight.sql",
            ],
            [
                "/migration.sql",
                "/inventory.sql",
                "/genesis.sql",
                "/inventory.sql",
            ],
            ["/", "/inventory.sql", "/genesis.sql", "/preflight.sql"],
        ] {
            assert!(
                CatalogActivationStaticInputsConfig::new(
                    PathBuf::from(paths[0]),
                    PathBuf::from(paths[1]),
                    PathBuf::from(paths[2]),
                    PathBuf::from(paths[3]),
                )
                .is_err()
            );
        }
    }

    async fn wait_for_output(
        receiver: &mut watch::Receiver<Option<Arc<ValidatedCatalogStaticInputs>>>,
        present: bool,
    ) {
        timeout(Duration::from_secs(3), async {
            loop {
                if receiver.borrow().is_some() == present {
                    return;
                }
                receiver.changed().await.expect("static-input verifier");
            }
        })
        .await
        .expect("static-input state transition");
    }

    #[derive(Clone, Copy)]
    enum ConcurrentTransition {
        RevokeAcceptance,
        Shutdown,
    }

    struct InstrumentedGuard<T> {
        value: T,
        drop_event: &'static str,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl<T> Deref for InstrumentedGuard<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.value
        }
    }

    impl<T> Drop for InstrumentedGuard<T> {
        fn drop(&mut self) {
            self.events
                .lock()
                .expect("event lock")
                .push(self.drop_event);
        }
    }

    fn sealed_request(
        genesis: &[u8],
    ) -> pgshard_types::catalog_activation::CatalogActivationRequest {
        let mut request = consumer_tests::request();
        request.materials.migration_sha256 = digest(TEST_MIGRATION);
        request.materials.inventory_sha256 = digest(TEST_INVENTORY);
        request.materials.genesis_sha256 = digest(genesis);
        request.materials.preflight_sha256 = digest(TEST_PREFLIGHT);
        request.materials.shard_count = "4".to_owned();
        request
    }

    fn assert_transition_is_modelled_after_publish(transition: ConcurrentTransition) {
        let request = sealed_request(&test_genesis("SELECT 1;"));
        let current = consumer_tests::accepted(request);
        let events = Arc::new(Mutex::new(Vec::new()));
        let accepted_guard = InstrumentedGuard {
            value: Some(current.clone()),
            drop_event: "acceptance-transition-complete",
            events: Arc::clone(&events),
        };
        let shutdown_guard = InstrumentedGuard {
            value: false,
            drop_event: "shutdown-transition-complete",
            events: Arc::clone(&events),
        };
        assert!(publish_while_guarded(
            accepted_guard,
            shutdown_guard,
            &current,
            || {
                let mut events = events.lock().expect("event lock");
                events.push(match transition {
                    ConcurrentTransition::RevokeAcceptance => "acceptance-transition-attempt",
                    ConcurrentTransition::Shutdown => "shutdown-transition-attempt",
                });
                events.push("publish");
            },
        ));

        let events = events.lock().expect("event lock");
        let transition_complete = match transition {
            ConcurrentTransition::RevokeAcceptance => "acceptance-transition-complete",
            ConcurrentTransition::Shutdown => "shutdown-transition-complete",
        };
        assert_eq!(
            events[0],
            match transition {
                ConcurrentTransition::RevokeAcceptance => "acceptance-transition-attempt",
                ConcurrentTransition::Shutdown => "shutdown-transition-attempt",
            }
        );
        assert_eq!(events[1], "publish");
        assert!(
            events
                .iter()
                .position(|event| *event == transition_complete)
                > events.iter().position(|event| *event == "publish"),
            "modelled transition completed before publication: {events:?}"
        );
    }

    #[test]
    fn acceptance_revocation_is_modelled_after_guarded_publication() {
        assert_transition_is_modelled_after_publish(ConcurrentTransition::RevokeAcceptance);
    }

    #[test]
    fn shutdown_is_modelled_after_guarded_publication() {
        assert_transition_is_modelled_after_publish(ConcurrentTransition::Shutdown);
    }

    #[tokio::test]
    async fn revokes_changed_acceptance_retries_and_withdraws_on_shutdown() {
        let (_directory, config, _expected) = fixture();
        let first = consumer_tests::accepted(sealed_request(&test_genesis("SELECT 1;")));
        let (accepted_sender, accepted_handoff) = consumer_tests::handoff(None);
        let (shutdown_sender, shutdown) = watch::channel(false);
        let CatalogMaterializationHandoff {
            receiver: mut output,
        } = spawn_catalog_activation_static_input_verifier(
            Some(config.clone()),
            accepted_handoff,
            shutdown,
        );

        accepted_sender.send_replace(Some(first.clone()));
        wait_for_output(&mut output, true).await;
        {
            let sealed = output.borrow();
            let sealed = sealed.as_ref().expect("sealed first input");
            assert_eq!(sealed.accepted.request_sha256(), first.request_sha256());
            assert_eq!(sealed.program.migration.as_bytes(), TEST_MIGRATION);
            assert_eq!(sealed.program.inventory.as_bytes(), TEST_INVENTORY);
            assert_eq!(sealed.program.genesis.as_bytes(), test_genesis("SELECT 1;"));
            assert_eq!(sealed.program.preflight.as_bytes(), TEST_PREFLIGHT);
            assert_eq!(sealed.shard_count, 4);
            assert!(!sealed.allow_empty_database_topology);
        }

        let replacement_genesis = test_genesis("SELECT 'replacement';");
        let replacement = consumer_tests::accepted(sealed_request(&replacement_genesis));
        accepted_sender.send_replace(Some(replacement.clone()));
        wait_for_output(&mut output, false).await;

        fs::write(&config.genesis, replacement_genesis).expect("replacement genesis");
        wait_for_output(&mut output, true).await;
        assert_eq!(
            output
                .borrow()
                .as_ref()
                .expect("sealed replacement")
                .accepted
                .request_sha256(),
            replacement.request_sha256()
        );

        shutdown_sender.send_replace(true);
        wait_for_output(&mut output, false).await;
    }
}
