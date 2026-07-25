//! Reads and digests the exact inputs a `ServingPrimary` replacement must come
//! up under.
//!
//! This compiles a [`ServingPreparationPolicy`]: it decides *which* bytes the
//! replacement is required to load, and nothing else. It installs nothing,
//! reloads nothing, and selects no runtime — the stage that installs the
//! serving policy proves the bytes again at the point it installs them, which
//! is the only place that proof means anything.
//!
//! Every read is fail-closed in the same way the static-input verifier already
//! is: a path that is not a regular file, is empty, is larger than the bound,
//! or whose content does not match the digest sealed into the request is
//! refused rather than accepted with a warning. The two access policies are
//! read as separate inputs because they are separate decisions — the
//! replication-only policy every incarnation starts under, and the sealed
//! policy that admits application traffic once every proof has passed.

#![allow(
    dead_code,
    reason = "dormant serving-input compiler; the preparation stage drives it"
)]

use std::fmt::Write as _;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use pgshard_types::serving_preparation::ServingPreparationPolicy;
use rustix::fs::{Mode, OFlags, open};
use sha2::{Digest, Sha256};

/// Largest accepted serving input. Configuration and access policies are
/// small; anything approaching this is a projection mistake, not a workload.
const MAXIMUM_SERVING_INPUT_BYTES: u64 = 1024 * 1024;

/// Where the projected serving inputs are mounted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServingInputPaths {
    /// `PostgreSQL` configuration the replacement must load.
    pub(crate) configuration: PathBuf,
    /// Replication-only policy every postmaster incarnation starts under.
    pub(crate) non_serving_hba: PathBuf,
    /// Sealed policy that admits application traffic.
    pub(crate) serving_hba: PathBuf,
    /// Workload template the replacement is rendered from.
    pub(crate) template: PathBuf,
}

/// The digests the request sealed, which the projected bytes must match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExpectedServingInputs {
    pub(crate) configuration_sha256: String,
    pub(crate) non_serving_hba_sha256: String,
    pub(crate) serving_hba_sha256: String,
    pub(crate) template_sha256: String,
}

/// Why a serving input was refused.
///
/// Every variant names the input, because "a serving input was wrong" is not
/// actionable at three in the morning.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ServingInputError {
    #[error("cannot open the {name} serving input")]
    Open {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot read metadata for the {name} serving input")]
    Metadata {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("the {name} serving input is not a regular file")]
    NotRegular { name: &'static str },
    #[error("the {name} serving input is empty or larger than the accepted bound")]
    InvalidSize { name: &'static str },
    #[error("cannot read the {name} serving input")]
    Read {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("the {name} serving input does not match its sealed digest")]
    DigestMismatch { name: &'static str },
}

/// Reads every serving input and returns the policy they compile to.
///
/// Returns the policy only when all four inputs are present, bounded, regular
/// and digest-exact. A partially verified policy is not a policy.
pub(crate) fn compile_serving_policy(
    paths: &ServingInputPaths,
    expected: &ExpectedServingInputs,
) -> Result<ServingPreparationPolicy, ServingInputError> {
    read_and_verify(
        "configuration",
        &paths.configuration,
        &expected.configuration_sha256,
    )?;
    read_and_verify(
        "non-serving HBA",
        &paths.non_serving_hba,
        &expected.non_serving_hba_sha256,
    )?;
    read_and_verify(
        "serving HBA",
        &paths.serving_hba,
        &expected.serving_hba_sha256,
    )?;
    read_and_verify("template", &paths.template, &expected.template_sha256)?;
    Ok(ServingPreparationPolicy {
        configuration_sha256: expected.configuration_sha256.clone(),
        non_serving_hba_sha256: expected.non_serving_hba_sha256.clone(),
        serving_hba_sha256: expected.serving_hba_sha256.clone(),
        template_sha256: expected.template_sha256.clone(),
    })
}

/// Opens, bounds, reads and digests one input.
///
/// `O_NONBLOCK` so a path that turns out to be a FIFO cannot hang the agent
/// before the regular-file check rejects it, and the size is bounded from the
/// metadata *and* after reading, because the file may change between the two.
fn read_and_verify(
    name: &'static str,
    path: &Path,
    expected_sha256: &str,
) -> Result<Box<[u8]>, ServingInputError> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|source| ServingInputError::Open {
        name,
        source: std::io::Error::from(source),
    })?;
    let file = File::from(descriptor);
    let metadata = file
        .metadata()
        .map_err(|source| ServingInputError::Metadata { name, source })?;
    if !metadata.is_file() {
        return Err(ServingInputError::NotRegular { name });
    }
    if metadata.len() == 0 || metadata.len() > MAXIMUM_SERVING_INPUT_BYTES {
        return Err(ServingInputError::InvalidSize { name });
    }
    let capacity = usize::try_from(metadata.len()).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAXIMUM_SERVING_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| ServingInputError::Read { name, source })?;
    // Deliberately redundant with the metadata check above, and each alone
    // satisfies the tests below — so neither is individually isolable by them.
    // They cover different lies: metadata can over-report (a file truncated
    // between stat and read) or under-report (a file that grows, or a source
    // that reports zero), and only this check sees what was actually read.
    if bytes.is_empty() || bytes.len() as u64 > MAXIMUM_SERVING_INPUT_BYTES {
        return Err(ServingInputError::InvalidSize { name });
    }
    if sha256_hex(&bytes) != expected_sha256 {
        return Err(ServingInputError::DigestMismatch { name });
    }
    Ok(bytes.into_boxed_slice())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .into_iter()
        .fold(String::new(), |mut encoded, byte| {
            let _ = write!(encoded, "{byte:02x}");
            encoded
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(directory: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = directory.join(name);
        std::fs::write(&path, bytes).expect("write a fixture input");
        path
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        paths: ServingInputPaths,
        expected: ExpectedServingInputs,
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let root = directory.path();
        let configuration = b"shared_buffers = '256MB'\n".to_vec();
        let non_serving = b"hostssl replication all all scram-sha-256\n".to_vec();
        let serving = b"hostssl all all all scram-sha-256\n".to_vec();
        let template = b"apiVersion: apps/v1\n".to_vec();
        Fixture {
            paths: ServingInputPaths {
                configuration: write(root, "postgresql.conf", &configuration),
                non_serving_hba: write(root, "non-serving.conf", &non_serving),
                serving_hba: write(root, "serving.conf", &serving),
                template: write(root, "template.yaml", &template),
            },
            expected: ExpectedServingInputs {
                configuration_sha256: sha256_hex(&configuration),
                non_serving_hba_sha256: sha256_hex(&non_serving),
                serving_hba_sha256: sha256_hex(&serving),
                template_sha256: sha256_hex(&template),
            },
            _directory: directory,
        }
    }

    #[test]
    fn exact_inputs_compile_to_their_policy() {
        let fixture = fixture();
        let policy = compile_serving_policy(&fixture.paths, &fixture.expected)
            .expect("exact inputs compile");
        assert_eq!(
            policy.configuration_sha256,
            fixture.expected.configuration_sha256
        );
        assert_eq!(
            policy.non_serving_hba_sha256,
            fixture.expected.non_serving_hba_sha256
        );
        assert_eq!(
            policy.serving_hba_sha256,
            fixture.expected.serving_hba_sha256
        );
        assert_eq!(policy.template_sha256, fixture.expected.template_sha256);
        // The two access policies are separate decisions and must not collapse
        // into one, or activation would not be a transition.
        assert_ne!(policy.non_serving_hba_sha256, policy.serving_hba_sha256);
    }

    /// Each input is named in its own error: "a serving input was wrong" is not
    /// actionable when four of them exist.
    #[test]
    fn every_input_is_refused_on_its_own_digest() {
        for (name, corrupt) in [
            ("configuration", 0usize),
            ("non-serving HBA", 1),
            ("serving HBA", 2),
            ("template", 3),
        ] {
            let fixture = fixture();
            let mut expected = fixture.expected.clone();
            let slot = match corrupt {
                0 => &mut expected.configuration_sha256,
                1 => &mut expected.non_serving_hba_sha256,
                2 => &mut expected.serving_hba_sha256,
                _ => &mut expected.template_sha256,
            };
            *slot = sha256_hex(b"something else entirely");
            let error = compile_serving_policy(&fixture.paths, &expected)
                .expect_err("a mismatched digest is refused");
            assert!(
                matches!(error, ServingInputError::DigestMismatch { name: got } if got == name),
                "expected a {name} digest mismatch, got {error}"
            );
        }
    }

    #[test]
    fn an_empty_input_is_refused_rather_than_digested() {
        let fixture = fixture();
        std::fs::write(&fixture.paths.serving_hba, b"").expect("truncate");
        let mut expected = fixture.expected.clone();
        expected.serving_hba_sha256 = sha256_hex(b"");
        let error = compile_serving_policy(&fixture.paths, &expected)
            .expect_err("an empty input is refused");
        assert!(
            matches!(error, ServingInputError::InvalidSize { name } if name == "serving HBA"),
            "expected an invalid size, got {error}"
        );
    }

    #[test]
    fn an_oversized_input_is_refused_without_being_read_whole() {
        let fixture = fixture();
        let oversized = vec![b'x'; usize::try_from(MAXIMUM_SERVING_INPUT_BYTES).unwrap() + 1];
        std::fs::write(&fixture.paths.configuration, &oversized).expect("write oversized");
        let mut expected = fixture.expected.clone();
        expected.configuration_sha256 = sha256_hex(&oversized);
        let error = compile_serving_policy(&fixture.paths, &expected)
            .expect_err("an oversized input is refused");
        assert!(
            matches!(error, ServingInputError::InvalidSize { name } if name == "configuration"),
            "expected an invalid size, got {error}"
        );
    }

    /// A directory opens successfully, so only the regular-file check rejects
    /// it. Without that check the read would fail with a confusing errno.
    #[test]
    fn a_non_regular_input_is_refused() {
        let fixture = fixture();
        let directory = fixture
            .paths
            .template
            .parent()
            .expect("a parent")
            .to_path_buf();
        let mut paths = fixture.paths.clone();
        paths.template = directory;
        let error =
            compile_serving_policy(&paths, &fixture.expected).expect_err("a directory is refused");
        assert!(
            matches!(error, ServingInputError::NotRegular { name } if name == "template"),
            "expected a non-regular input, got {error}"
        );
    }

    #[test]
    fn a_missing_input_is_refused() {
        let fixture = fixture();
        let mut paths = fixture.paths.clone();
        paths.non_serving_hba = paths.non_serving_hba.with_file_name("absent.conf");
        let error = compile_serving_policy(&paths, &fixture.expected)
            .expect_err("a missing input is refused");
        assert!(
            matches!(error, ServingInputError::Open { name, .. } if name == "non-serving HBA"),
            "expected an open failure, got {error}"
        );
    }
}
