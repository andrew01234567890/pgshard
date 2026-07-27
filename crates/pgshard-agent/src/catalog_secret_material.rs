//! Fail-closed verification of the mounted catalog, replication and
//! `PostgreSQL` configuration material.
//!
//! The compatibility bootstrap shell proves, before it writes any role, that
//! every projected Secret and `ConfigMap` is byte-identical to the material the
//! controller checkpointed into the activation request. This module is that
//! proof in the agent. It performs bounded reads and pure digest comparisons;
//! it grants no SQL, `PostgreSQL`, HBA, serving, routing or process authority,
//! and no error it produces renders any byte it read.
//!
//! The two digest shapes are deliberately different and must stay that way. A
//! credential or certificate is fingerprinted with the shared length-framed
//! HMAC contract in [`pgshard_types::catalog_material`]. The `PostgreSQL`
//! configuration directory is fingerprinted with the controller's sorted
//! `key NUL value NUL` `ConfigMap` digest, because that is the digest the
//! controller pins into the Pod and the shell recomputes from the projected
//! directory.

use std::fmt::Write as _;
use std::fs::{File, read_dir};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use pgshard_types::catalog_activation::CatalogActivationMaterials;
use pgshard_types::catalog_material::{
    CATALOG_CLIENT_DIGEST_DOMAIN, CATALOG_SERVER_DIGEST_DOMAIN, OPERATION_WRITER_DIGEST_DOMAIN,
    POSTGRESQL_REPLICATION_DIGEST_DOMAIN, catalog_material_sha256,
};
use rustix::fs::{Mode, OFlags, open};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Exactly what the controller generates: 32 random bytes rendered as 64
/// lowercase hexadecimal bytes, with no terminator.
///
/// This is deliberately stricter than the shell reader it replaces, which
/// accepts whatever bytes the file holds. The controller is the only producer
/// of these Secrets and validates the same shape on every read of its own, so
/// a credential that fails here is one the controller did not write — a
/// hand-edited or third-party-substituted Secret. Adopting it would install
/// something no controller can reproduce, so it is refused and the cluster
/// waits for an operator instead. The cutover consequence is stated rather
/// than hidden: a deployment whose credential Secret was edited by hand under
/// the shell is refused by the agent rather than carried forward.
const PASSWORD_BYTES: u64 = 64;
const MAXIMUM_CERTIFICATE_BYTES: u64 = 1024 * 1024;
const MAXIMUM_CONFIGURATION_ENTRY_BYTES: u64 = 1024 * 1024;
const MAXIMUM_CONFIGURATION_ENTRIES: usize = 256;

/// The atomic-update symlink a Kubernetes projected volume keeps beside the
/// keys. The controller hashes the `ConfigMap` keys, so hashing this too would
/// count every entry twice.
const PROJECTED_DATA_LINK: &str = "..data";

/// Exact paths for every mounted catalog and replication material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogSecretPaths {
    replication_password: PathBuf,
    catalog_password: PathBuf,
    catalog_ca_certificate: PathBuf,
    catalog_tls_private_key: PathBuf,
    catalog_tls_certificate: PathBuf,
    operation_writer_password: PathBuf,
    postgresql_configuration: PathBuf,
}

impl CatalogSecretPaths {
    /// Validates six distinct absolute normalized file paths and one absolute
    /// normalized configuration directory.
    ///
    /// # Errors
    ///
    /// Returns an error for a relative, non-normalized, root-only or duplicate
    /// path.
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per mounted material; grouping them would hide which path is which"
    )]
    pub(crate) fn new(
        replication_password: PathBuf,
        catalog_password: PathBuf,
        catalog_ca_certificate: PathBuf,
        catalog_tls_private_key: PathBuf,
        catalog_tls_certificate: PathBuf,
        operation_writer_password: PathBuf,
        postgresql_configuration: PathBuf,
    ) -> Result<Self, CatalogSecretPathsError> {
        let paths = [
            &replication_password,
            &catalog_password,
            &catalog_ca_certificate,
            &catalog_tls_private_key,
            &catalog_tls_certificate,
            &operation_writer_password,
            &postgresql_configuration,
        ];
        let all_absolute_normal = paths.iter().all(|path| absolute_normal(path));
        let all_distinct = paths
            .iter()
            .enumerate()
            .all(|(index, path)| paths[index + 1..].iter().all(|other| other != path));
        if !all_absolute_normal || !all_distinct {
            return Err(CatalogSecretPathsError);
        }
        Ok(Self {
            replication_password,
            catalog_password,
            catalog_ca_certificate,
            catalog_tls_private_key,
            catalog_tls_certificate,
            operation_writer_password,
            postgresql_configuration,
        })
    }
}

/// Material path validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("invalid catalog secret material paths")]
pub(crate) struct CatalogSecretPathsError;

/// One credential held in memory for as long as a check needs it.
///
/// Deliberately not `Clone`, `Debug`, `Display` or serializable: a credential
/// that can be formatted is a credential that reaches a log. The buffer is
/// overwritten on drop, mirroring the `unset` the shell performs the moment it
/// is done with one.
pub(crate) struct SecretPassword(Box<[u8]>);

impl SecretPassword {
    /// The credential bytes. Every caller must keep them out of SQL text,
    /// argv, environment and logs.
    pub(crate) const fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretPassword {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Anything [`read_bounded`] read, held so that it is overwritten however the
/// read ends.
///
/// A TLS private key is not a password, and it is not carried out of this
/// module the way a credential is — but the reason a credential is scrubbed
/// applies to it unchanged: a plain buffer survives the check in a freed
/// allocation or a core dump. Deliberately not `Clone`, `Debug`, `Display` or
/// serializable, for the same reason.
///
/// The reader wraps its buffer in this before the buffer can hold a single
/// byte of the file, rather than the caller wrapping the result afterwards.
/// The difference is every path that does not reach the caller: a read that
/// fails partway, and a file that grew past its bound between the `fstat` and
/// the read. Both leave by an early return, and neither one has a wrapper to
/// scrub what was already read.
struct SecretMaterial(Vec<u8>);

impl SecretMaterial {
    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretMaterial {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Every credential whose projected material matched the checkpointed request.
///
/// Holding one of these is the evidence that the digests agreed; the
/// credentials cannot be obtained without producing that evidence first.
pub(crate) struct VerifiedCatalogSecrets {
    pub(crate) replication: SecretPassword,
    pub(crate) catalog: SecretPassword,
    pub(crate) operation_writer: SecretPassword,
}

/// Proves every mounted material equals the digest the request checkpointed.
///
/// Ordered the way the shell orders it: shape first, then fingerprint, so a
/// malformed credential is refused before it is fingerprinted and before any
/// value derived from it exists.
///
/// # Errors
///
/// Returns an error when a material is missing, is not a bounded regular file,
/// has a non-canonical credential shape, or does not match the checkpointed
/// digest. No error renders the material.
pub(crate) fn verify_catalog_secret_material(
    paths: &CatalogSecretPaths,
    materials: &CatalogActivationMaterials,
) -> Result<VerifiedCatalogSecrets, CatalogSecretMaterialError> {
    let replication = read_password("replication", &paths.replication_password)?;
    let catalog = read_password("catalog", &paths.catalog_password)?;
    let operation_writer = read_password("operation-writer", &paths.operation_writer_password)?;
    let ca_certificate = read_bounded(
        "catalog CA certificate",
        &paths.catalog_ca_certificate,
        MAXIMUM_CERTIFICATE_BYTES,
    )?;
    let private_key = read_bounded(
        "catalog server private key",
        &paths.catalog_tls_private_key,
        MAXIMUM_CERTIFICATE_BYTES,
    )?;
    let certificate = read_bounded(
        "catalog server certificate",
        &paths.catalog_tls_certificate,
        MAXIMUM_CERTIFICATE_BYTES,
    )?;

    require_digest(
        "replication",
        &materials.replication.material_sha256,
        &catalog_material_sha256(
            POSTGRESQL_REPLICATION_DIGEST_DOMAIN,
            replication.expose(),
            std::iter::empty(),
        ),
    )?;
    require_digest(
        "catalog client",
        &materials.catalog.client_sha256,
        &catalog_material_sha256(
            CATALOG_CLIENT_DIGEST_DOMAIN,
            catalog.expose(),
            [ca_certificate.expose()],
        ),
    )?;
    require_digest(
        "catalog server",
        &materials.catalog.server_sha256,
        &catalog_material_sha256(
            CATALOG_SERVER_DIGEST_DOMAIN,
            private_key.expose(),
            [certificate.expose()],
        ),
    )?;
    require_digest(
        "operation-writer",
        &materials.operation_writer.material_sha256,
        &catalog_material_sha256(
            OPERATION_WRITER_DIGEST_DOMAIN,
            operation_writer.expose(),
            [ca_certificate.expose()],
        ),
    )?;
    require_digest(
        "PostgreSQL configuration",
        &materials.postgresql_configuration.material_sha256,
        &projected_directory_sha256(&paths.postgresql_configuration)?,
    )?;

    Ok(VerifiedCatalogSecrets {
        replication,
        catalog,
        operation_writer,
    })
}

/// The controller's `ConfigMap` digest recomputed from a projected directory.
///
/// Sorted by key in byte order, each entry framed `key NUL value NUL`. The
/// atomic-update symlink and the timestamped revision directories a projected
/// volume keeps are not keys and are excluded, exactly as the shell excludes
/// them.
///
/// # Errors
///
/// Returns an error when the directory cannot be listed, holds more than the
/// bounded number of entries, or holds an entry that is not a bounded regular
/// file.
fn projected_directory_sha256(directory: &Path) -> Result<String, CatalogSecretMaterialError> {
    const NAME: &str = "PostgreSQL configuration";
    let entries = read_dir(directory).map_err(|source| CatalogSecretMaterialError::Open {
        material: NAME,
        source,
    })?;
    let mut keys = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| CatalogSecretMaterialError::Read {
            material: NAME,
            source,
        })?;
        let name = entry.file_name();
        if name == PROJECTED_DATA_LINK {
            continue;
        }
        // `DirEntry::metadata` does not traverse the link, so this classifies
        // the entry itself. A projected key is a symlink into the current
        // revision and is followed when its value is read below; the revision
        // directory itself is not a key and is skipped here.
        let entry_kind = entry
            .metadata()
            .map_err(|source| CatalogSecretMaterialError::Read {
                material: NAME,
                source,
            })?;
        if entry_kind.is_dir() {
            continue;
        }
        let name = name
            .into_string()
            .map_err(|_| CatalogSecretMaterialError::NonCanonicalConfigurationKey)?;
        keys.push(name);
        if keys.len() > MAXIMUM_CONFIGURATION_ENTRIES {
            return Err(CatalogSecretMaterialError::InvalidSize { material: NAME });
        }
    }
    keys.sort_unstable();

    let mut hash = Sha256::new();
    for key in keys {
        let value = read_bounded(
            NAME,
            &directory.join(&key),
            MAXIMUM_CONFIGURATION_ENTRY_BYTES,
        )?;
        hash.update(key.as_bytes());
        hash.update([0]);
        hash.update(value.expose());
        hash.update([0]);
    }
    Ok(lower_hex(&hash.finalize()))
}

/// Reads one credential and requires the exact canonical credential shape.
///
/// The read buffer is scrubbed by its own destructor on both paths, so the
/// refusal below carries no scrubbing of its own and the acceptance copies
/// into an exactly-sized allocation rather than shrinking the read buffer in
/// place. Shrinking is a reallocation, and a reallocation of a credential
/// leaves the credential in the block it just released.
fn read_password(
    material: &'static str,
    path: &Path,
) -> Result<SecretPassword, CatalogSecretMaterialError> {
    let bytes = read_bounded(material, path, PASSWORD_BYTES)?;
    let canonical = bytes.expose().len() as u64 == PASSWORD_BYTES
        && bytes
            .expose()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    if !canonical {
        return Err(CatalogSecretMaterialError::NonCanonicalCredential { material });
    }
    Ok(SecretPassword(bytes.expose().into()))
}

/// Reads one bounded regular file into a buffer that is overwritten however
/// the read ends.
fn read_bounded(
    material: &'static str,
    path: &Path,
    maximum: u64,
) -> Result<SecretMaterial, CatalogSecretMaterialError> {
    // `NOCTTY` matches every sibling reader in the workspace: a mounted path
    // that resolved to a terminal device must not become this process's
    // controlling terminal, and the size and regular-file checks below run
    // after the open rather than before it.
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOCTTY,
        Mode::empty(),
    )
    .map_err(|source| CatalogSecretMaterialError::Open {
        material,
        source: std::io::Error::from(source),
    })?;
    let file = File::from(descriptor);
    let metadata = file
        .metadata()
        .map_err(|source| CatalogSecretMaterialError::Read { material, source })?;
    if !metadata.is_file() {
        return Err(CatalogSecretMaterialError::NotRegular { material });
    }
    if metadata.len() == 0 || metadata.len() > maximum {
        return Err(CatalogSecretMaterialError::InvalidSize { material });
    }
    let capacity = usize::try_from(metadata.len()).unwrap_or(0);
    // Wrapped before the read rather than after it. This reader serves the TLS
    // private key and all three credentials, and the two returns below are
    // taken with the buffer already holding part of the file: a read that
    // fails partway leaves what it did read, and so does a file that grew past
    // its bound between the `fstat` above and the read. Neither reaches a
    // caller, so neither has a caller's wrapper to scrub it.
    //
    // The scrub covers this buffer, not every copy of these bytes. A file that
    // grew after the `fstat` makes `read_to_end` outgrow the capacity reserved
    // here, and the reallocation copies what was already read into a new block
    // and releases the old one intact; nothing in this process can reach that
    // block afterwards.
    let mut bytes = SecretMaterial(Vec::with_capacity(capacity));
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes.0)
        .map_err(|source| CatalogSecretMaterialError::Read { material, source })?;
    if bytes.0.is_empty() || bytes.0.len() as u64 > maximum {
        return Err(CatalogSecretMaterialError::InvalidSize { material });
    }
    Ok(bytes)
}

/// Compares one observed fingerprint with the checkpointed one.
///
/// The expected value is shape-checked first. A request carrying a
/// non-canonical digest is a malformed request, not a material mismatch, and
/// conflating the two would report the wrong cause for the refusal.
fn require_digest(
    material: &'static str,
    expected: &str,
    observed: &str,
) -> Result<(), CatalogSecretMaterialError> {
    if !canonical_sha256(expected) {
        return Err(CatalogSecretMaterialError::NonCanonicalCheckpointedDigest { material });
    }
    if expected != observed {
        return Err(CatalogSecretMaterialError::DigestMismatch { material });
    }
    Ok(())
}

fn canonical_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn absolute_normal(path: &Path) -> bool {
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

/// Redacted material verification failure. No variant carries material bytes.
#[derive(Debug, Error)]
pub(crate) enum CatalogSecretMaterialError {
    /// The material could not be opened.
    #[error("open catalog {material} material: {source}")]
    Open {
        /// Material whose read failed.
        material: &'static str,
        /// Underlying failure.
        #[source]
        source: std::io::Error,
    },
    /// The material could not be read or inspected.
    #[error("read catalog {material} material: {source}")]
    Read {
        /// Material whose read failed.
        material: &'static str,
        /// Underlying failure.
        #[source]
        source: std::io::Error,
    },
    /// The material did not resolve to a regular file.
    #[error("catalog {material} material is not a regular file")]
    NotRegular {
        /// Rejected material.
        material: &'static str,
    },
    /// The material was empty or exceeded its bound.
    #[error("catalog {material} material is empty or exceeds its bound")]
    InvalidSize {
        /// Rejected material.
        material: &'static str,
    },
    /// A credential was not exactly 64 lowercase hexadecimal bytes.
    #[error("catalog {material} credential is not the canonical generated shape")]
    NonCanonicalCredential {
        /// Rejected material.
        material: &'static str,
    },
    /// A projected configuration key was not representable as UTF-8.
    #[error("PostgreSQL configuration directory holds a non-UTF-8 key")]
    NonCanonicalConfigurationKey,
    /// The request carried a digest that is not a lowercase SHA-256 digest.
    #[error("checkpointed catalog {material} digest is not a canonical SHA-256 digest")]
    NonCanonicalCheckpointedDigest {
        /// Rejected material.
        material: &'static str,
    },
    /// The projected material differs from the checkpointed creation result.
    #[error("catalog {material} material differs from the checkpointed creation result")]
    DigestMismatch {
        /// Rejected material.
        material: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    use pgshard_types::catalog_activation::{CatalogMaterialIdentity, MaterialIdentity};
    use tempfile::TempDir;

    const REPLICATION_PASSWORD: &[u8; 64] =
        b"1111111111111111111111111111111111111111111111111111111111111111";
    const CATALOG_PASSWORD: &[u8; 64] =
        b"2222222222222222222222222222222222222222222222222222222222222222";
    const OPERATION_WRITER_PASSWORD: &[u8; 64] =
        b"3333333333333333333333333333333333333333333333333333333333333333";
    const CA: &[u8] = b"-----BEGIN CERTIFICATE-----\ncatalog-ca\n";
    const KEY: &[u8] = b"-----BEGIN PRIVATE KEY-----\ncatalog-key\n";
    const CERTIFICATE: &[u8] = b"-----BEGIN CERTIFICATE-----\ncatalog-server\n";

    struct Fixture {
        root: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().expect("material fixture");
            fs::write(
                root.path().join("replication-password"),
                REPLICATION_PASSWORD,
            )
            .expect("replication credential");
            fs::write(root.path().join("catalog-password"), CATALOG_PASSWORD)
                .expect("catalog credential");
            fs::write(
                root.path().join("operation-writer-password"),
                OPERATION_WRITER_PASSWORD,
            )
            .expect("operation-writer credential");
            fs::write(root.path().join("ca.crt"), CA).expect("CA");
            fs::write(root.path().join("tls.key"), KEY).expect("private key");
            fs::write(root.path().join("tls.crt"), CERTIFICATE).expect("certificate");
            let configuration = root.path().join("postgresql-source");
            fs::create_dir(&configuration).expect("configuration directory");
            fs::write(
                configuration.join("primary-0000.conf"),
                b"shared_buffers = 128MB\n",
            )
            .expect("configuration entry");
            fs::write(
                configuration.join("standby-0000.conf"),
                b"hot_standby = on\n",
            )
            .expect("configuration entry");
            Self { root }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.root.path().join(name)
        }

        fn paths(&self) -> CatalogSecretPaths {
            CatalogSecretPaths::new(
                self.path("replication-password"),
                self.path("catalog-password"),
                self.path("ca.crt"),
                self.path("tls.key"),
                self.path("tls.crt"),
                self.path("operation-writer-password"),
                self.path("postgresql-source"),
            )
            .expect("absolute distinct fixture paths")
        }

        fn materials(&self) -> CatalogActivationMaterials {
            let configuration = projected_directory_sha256(&self.path("postgresql-source"))
                .expect("fixture configuration digest");
            CatalogActivationMaterials {
                replication: MaterialIdentity {
                    name: "replication".to_owned(),
                    uid: "replication-uid".to_owned(),
                    material_sha256: catalog_material_sha256(
                        POSTGRESQL_REPLICATION_DIGEST_DOMAIN,
                        REPLICATION_PASSWORD,
                        std::iter::empty(),
                    ),
                },
                catalog: CatalogMaterialIdentity {
                    name: "catalog".to_owned(),
                    uid: "catalog-uid".to_owned(),
                    client_sha256: catalog_material_sha256(
                        CATALOG_CLIENT_DIGEST_DOMAIN,
                        CATALOG_PASSWORD,
                        [CA],
                    ),
                    server_sha256: catalog_material_sha256(
                        CATALOG_SERVER_DIGEST_DOMAIN,
                        KEY,
                        [CERTIFICATE],
                    ),
                },
                operation_writer: MaterialIdentity {
                    name: "operation-writer".to_owned(),
                    uid: "operation-writer-uid".to_owned(),
                    material_sha256: catalog_material_sha256(
                        OPERATION_WRITER_DIGEST_DOMAIN,
                        OPERATION_WRITER_PASSWORD,
                        [CA],
                    ),
                },
                postgresql_configuration: MaterialIdentity {
                    name: "configuration".to_owned(),
                    uid: "configuration-uid".to_owned(),
                    material_sha256: configuration,
                },
                migration_sha256: String::new(),
                shard_count: "1".to_owned(),
                inventory_sha256: String::new(),
                genesis_sha256: String::new(),
                preflight_sha256: String::new(),
                serving_hba_version: String::new(),
                serving_hba_sha256: String::new(),
                target_template_sha256: String::new(),
            }
        }
    }

    #[test]
    fn the_checkpointed_material_is_accepted_and_yields_its_credentials() {
        let fixture = Fixture::new();
        let secrets = verify_catalog_secret_material(&fixture.paths(), &fixture.materials())
            .expect("checkpointed material verifies");
        assert_eq!(secrets.replication.expose(), REPLICATION_PASSWORD);
        assert_eq!(secrets.catalog.expose(), CATALOG_PASSWORD);
        assert_eq!(secrets.operation_writer.expose(), OPERATION_WRITER_PASSWORD);
    }

    /// Every material is load-bearing: changing any one of them by a single
    /// byte has to be refused, and named.
    #[test]
    fn every_material_is_checked_against_its_own_checkpointed_digest() {
        for (file, material) in [
            ("replication-password", "replication"),
            ("catalog-password", "catalog client"),
            ("tls.key", "catalog server"),
            ("tls.crt", "catalog server"),
            ("operation-writer-password", "operation-writer"),
        ] {
            let fixture = Fixture::new();
            let materials = fixture.materials();
            let original = fs::read(fixture.path(file)).expect("read fixture material");
            let mut replacement = original.clone();
            // Stays inside the canonical credential alphabet, so this is a
            // digest mismatch rather than a shape rejection.
            replacement[0] = if replacement[0] == b'a' { b'b' } else { b'a' };
            fs::write(fixture.path(file), &replacement).expect("replace fixture material");

            let error = verify_catalog_secret_material(&fixture.paths(), &materials)
                .err()
                .unwrap_or_else(|| panic!("{file} was accepted after being changed"));
            assert!(
                matches!(
                    error,
                    CatalogSecretMaterialError::DigestMismatch { material: named } if named == material
                ),
                "{file} produced {error:?}"
            );
        }
    }

    /// The CA is an input to two different fingerprints, so it has to be
    /// covered by both and not silently by neither.
    #[test]
    fn the_shared_ca_certificate_is_covered_by_the_client_fingerprint() {
        let fixture = Fixture::new();
        let materials = fixture.materials();
        fs::write(
            fixture.path("ca.crt"),
            b"-----BEGIN CERTIFICATE-----\nother-ca\n",
        )
        .expect("replace the CA");
        let error = verify_catalog_secret_material(&fixture.paths(), &materials)
            .err()
            .expect("a replaced CA is refused");
        assert!(
            matches!(
                error,
                CatalogSecretMaterialError::DigestMismatch {
                    material: "catalog client"
                }
            ),
            "a replaced CA produced {error:?}"
        );
    }

    #[test]
    fn a_changed_configuration_entry_is_refused() {
        let fixture = Fixture::new();
        let materials = fixture.materials();
        fs::write(
            fixture.path("postgresql-source").join("primary-0000.conf"),
            b"shared_buffers = 256MB\n",
        )
        .expect("replace a configuration entry");
        let error = verify_catalog_secret_material(&fixture.paths(), &materials)
            .err()
            .expect("a changed configuration is refused");
        assert!(
            matches!(
                error,
                CatalogSecretMaterialError::DigestMismatch {
                    material: "PostgreSQL configuration"
                }
            ),
            "a changed configuration produced {error:?}"
        );
    }

    /// An added or removed key changes the configuration, so the digest has to
    /// cover the key set and not only the concatenated values.
    #[test]
    fn the_configuration_digest_covers_the_key_set_and_not_only_the_values() {
        let fixture = Fixture::new();
        let materials = fixture.materials();
        fs::write(
            fixture.path("postgresql-source").join("extra.conf"),
            b"fsync = off\n",
        )
        .expect("add a configuration entry");
        assert!(
            verify_catalog_secret_material(&fixture.paths(), &materials).is_err(),
            "an added configuration key was accepted"
        );

        // A renamed key keeps every value byte, so only a digest that covers
        // the key names can tell the two directories apart. `PostgreSQL` reads
        // its configuration by file name, so this is a real difference.
        let named = TempDir::new().expect("named fixture");
        fs::write(named.path().join("primary-0000.conf"), b"a = 1\n").expect("write");
        fs::write(named.path().join("standby-0000.conf"), b"b = 2\n").expect("write");
        let renamed = TempDir::new().expect("renamed fixture");
        fs::write(renamed.path().join("primary-0001.conf"), b"a = 1\n").expect("write");
        fs::write(renamed.path().join("standby-0001.conf"), b"b = 2\n").expect("write");
        assert_ne!(
            projected_directory_sha256(named.path()).expect("named digest"),
            projected_directory_sha256(renamed.path()).expect("renamed digest"),
            "the configuration digest does not cover the key names"
        );

        // The framing must also make key and value boundaries unambiguous: two
        // directories whose concatenated bytes agree must not share a digest.
        let split = TempDir::new().expect("split fixture");
        fs::write(split.path().join("ab"), b"c").expect("write");
        let joined = TempDir::new().expect("joined fixture");
        fs::write(joined.path().join("a"), b"bc").expect("write");
        assert_ne!(
            projected_directory_sha256(split.path()).expect("split digest"),
            projected_directory_sha256(joined.path()).expect("joined digest"),
            "the configuration digest is not unambiguously framed"
        );
    }

    /// A projected volume keeps an atomic-update symlink and a timestamped
    /// revision directory beside the keys. Counting either would make the
    /// agent's digest disagree with the controller's for every real Pod.
    #[test]
    fn a_projected_volume_layout_digests_the_same_as_a_plain_directory() {
        let plain = TempDir::new().expect("plain fixture");
        fs::write(plain.path().join("primary-0000.conf"), b"a = 1\n").expect("write");
        fs::write(plain.path().join("standby-0000.conf"), b"b = 2\n").expect("write");

        let projected = TempDir::new().expect("projected fixture");
        let revision = projected.path().join("..2026_07_27_00_00_00.1234");
        fs::create_dir(&revision).expect("revision directory");
        fs::write(revision.join("primary-0000.conf"), b"a = 1\n").expect("write");
        fs::write(revision.join("standby-0000.conf"), b"b = 2\n").expect("write");
        symlink(&revision, projected.path().join(PROJECTED_DATA_LINK)).expect("data link");
        symlink(
            revision.join("primary-0000.conf"),
            projected.path().join("primary-0000.conf"),
        )
        .expect("key link");
        symlink(
            revision.join("standby-0000.conf"),
            projected.path().join("standby-0000.conf"),
        )
        .expect("key link");

        assert_eq!(
            projected_directory_sha256(plain.path()).expect("plain digest"),
            projected_directory_sha256(projected.path()).expect("projected digest"),
            "the projected-volume layout does not digest as the controller hashes it"
        );
    }

    #[test]
    fn a_credential_that_is_not_the_canonical_generated_shape_is_refused() {
        for invalid in [
            &b"111111111111111111111111111111111111111111111111111111111111111"[..],
            b"1111111111111111111111111111111111111111111111111111111111111111\n",
            b"1111111111111111111111111111111111111111111111111111111111111\xffA",
            b"1111111111111111111111111111111111111111111111111111111111111AAA",
        ] {
            let fixture = Fixture::new();
            let materials = fixture.materials();
            fs::write(fixture.path("catalog-password"), invalid).expect("write credential");
            let error = verify_catalog_secret_material(&fixture.paths(), &materials)
                .err()
                .expect("a non-canonical credential is refused");
            assert!(
                matches!(
                    error,
                    CatalogSecretMaterialError::NonCanonicalCredential { .. }
                        | CatalogSecretMaterialError::InvalidSize { .. }
                ),
                "a non-canonical credential produced {error:?}"
            );
        }
    }

    /// A request carrying a malformed digest must be reported as a malformed
    /// request. Accepting one because it happens not to match would make the
    /// check indistinguishable from a real mismatch.
    #[test]
    fn a_non_canonical_checkpointed_digest_is_its_own_refusal() {
        let fixture = Fixture::new();
        let mut materials = fixture.materials();
        materials.replication.material_sha256 =
            materials.replication.material_sha256.to_uppercase();
        let error = verify_catalog_secret_material(&fixture.paths(), &materials)
            .err()
            .expect("a non-canonical checkpointed digest is refused");
        assert!(
            matches!(
                error,
                CatalogSecretMaterialError::NonCanonicalCheckpointedDigest {
                    material: "replication"
                }
            ),
            "a non-canonical checkpointed digest produced {error:?}"
        );
    }

    #[test]
    fn errors_never_render_the_material_they_refused() {
        let fixture = Fixture::new();
        let materials = fixture.materials();
        let credential = "4444444444444444444444444444444444444444444444444444444444444444";
        fs::write(fixture.path("catalog-password"), credential).expect("write credential");
        let error = verify_catalog_secret_material(&fixture.paths(), &materials)
            .err()
            .expect("a replaced credential is refused");
        let rendered = format!("{error}: {error:?}");
        assert!(
            !rendered.contains(credential),
            "an error rendered {rendered}"
        );
        assert!(rendered.contains("catalog client"));
    }

    #[test]
    fn material_paths_must_be_absolute_normalized_and_distinct() {
        let absolute = PathBuf::from("/etc/pgshard/catalog-auth/catalog-password");
        assert!(
            CatalogSecretPaths::new(
                PathBuf::from("etc/relative"),
                absolute.clone(),
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c"),
                PathBuf::from("/d"),
                PathBuf::from("/e"),
            )
            .is_err()
        );
        assert!(
            CatalogSecretPaths::new(
                PathBuf::from("/etc/../etc/x"),
                absolute.clone(),
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c"),
                PathBuf::from("/d"),
                PathBuf::from("/e"),
            )
            .is_err()
        );
        assert!(
            CatalogSecretPaths::new(
                absolute.clone(),
                absolute.clone(),
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c"),
                PathBuf::from("/d"),
                PathBuf::from("/e"),
            )
            .is_err()
        );
        assert!(
            CatalogSecretPaths::new(
                absolute,
                PathBuf::from("/f"),
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/c"),
                PathBuf::from("/d"),
                PathBuf::from("/e"),
            )
            .is_ok()
        );
    }
}
