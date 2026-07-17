//! Validated control-runtime configuration.

use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use pgshard_catalog::{
    CatalogOperationTimeout, CatalogOperationTimeoutError, CatalogPollInterval,
    CatalogPollIntervalError, CatalogSupervisorConfig, CatalogSupervisorConfigError,
    SHARDSCHEMA_DATABASE,
};
use rustix::fs::{Mode, OFlags};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{
    CertificateDer,
    pem::{PemObject, SectionKind},
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::Config;
use tokio_postgres::config::{ChannelBinding, Host, SslMode, TargetSessionAttrs};
use tokio_postgres_rustls::MakeRustlsConnect;

const MAX_SHARDSCHEMA_DSN_BYTES: usize = 16 * 1024;
const MAX_SHARDSCHEMA_CA_BYTES: usize = 64 * 1024;
const SHARDSCHEMA_PASSWORD_BYTES: usize = 64;
const SHARDSCHEMA_PORT: u16 = 5432;
const CATALOG_LOGIN_ROLE: &str = "pgshard_pooler_catalog";
const CATALOG_APPLICATION_NAME: &str = "pgshard-pooler-catalog";
const CATALOG_CLIENT_DIGEST_DOMAIN: &str = "pgshard-catalog-client-v1";

/// Validated configuration for the fail-closed pooler runtime.
pub struct PoolerConfig {
    http_bind: SocketAddr,
    read_write_bind: SocketAddr,
    catalog: Option<SupervisedCatalogConfig>,
}

pub(crate) struct SupervisedCatalogConfig {
    pub(crate) catalog: Config,
    pub(crate) connector: CatalogConnector,
    pub(crate) supervisor: CatalogSupervisorConfig,
}

pub(crate) enum CatalogConnector {
    LocalNoTls,
    OperatorTls(MakeRustlsConnect),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CatalogMode {
    Local,
    OperatorTls,
    BootstrapUnavailable,
}

#[derive(Debug, Parser)]
#[command(
    name = "pgshard-pooler",
    about = "Fail-closed pgshard catalog runtime and PostgreSQL handshake boundary",
    disable_help_subcommand = true
)]
struct RawConfig {
    /// Control HTTP listen address for health, readiness, status, and metrics.
    #[arg(long, env = "PGSHARD_HTTP_BIND", default_value = "0.0.0.0:8080")]
    http_bind: SocketAddr,

    /// Read-write `PostgreSQL` listen address; connections are rejected until the data plane exists.
    #[arg(long, env = "PGSHARD_RW_BIND", default_value = "0.0.0.0:5432")]
    read_write_bind: SocketAddr,

    /// Catalog runtime: local development, operator-authenticated TLS, or explicit unavailable bootstrap.
    #[arg(
        long,
        env = "PGSHARD_CATALOG_MODE",
        value_enum,
        default_value_t = CatalogMode::Local
    )]
    catalog_mode: CatalogMode,

    /// File containing one local-only shardschema database DSN (maximum 16 KiB).
    #[arg(long, env = "PGSHARD_SHARDSCHEMA_DSN_FILE")]
    shardschema_dsn_file: Option<PathBuf>,

    /// Exact lowercase DNS hostname of the operator-provisioned shardschema service.
    #[arg(long, env = "PGSHARD_SHARDSCHEMA_HOST")]
    shardschema_host: Option<String>,

    /// File containing the operator-provisioned catalog login password (exactly 64 lowercase hexadecimal bytes).
    #[arg(long, env = "PGSHARD_SHARDSCHEMA_PASSWORD_FILE")]
    shardschema_password_file: Option<PathBuf>,

    /// File containing exactly one operator-provisioned PEM CA certificate (maximum 64 KiB).
    #[arg(long, env = "PGSHARD_SHARDSCHEMA_CA_FILE")]
    shardschema_ca_file: Option<PathBuf>,

    /// SHA-256 checkpoint binding the exact password and CA Secret projection.
    #[arg(long, env = "PGSHARD_SHARDSCHEMA_CLIENT_SHA256")]
    shardschema_client_sha256: Option<String>,

    /// Authoritative catalog polling interval in milliseconds (1,000..=300,000).
    #[arg(
        long,
        env = "PGSHARD_CATALOG_POLL_INTERVAL_MS",
        default_value_t = 30_000
    )]
    catalog_poll_interval_ms: u64,

    /// Maximum usable catalog age in milliseconds (2,000..=900,000 and greater than the poll interval).
    #[arg(long, env = "PGSHARD_CATALOG_STALE_GRACE_MS", default_value_t = 90_000)]
    catalog_stale_grace_ms: u64,

    /// Initial reconnect window ceiling in milliseconds (10..=5,000).
    #[arg(
        long,
        env = "PGSHARD_CATALOG_INITIAL_RECONNECT_DELAY_MS",
        default_value_t = 100
    )]
    catalog_initial_reconnect_delay_ms: u64,

    /// Maximum reconnect window ceiling in milliseconds (initial..=60,000).
    #[arg(
        long,
        env = "PGSHARD_CATALOG_MAX_RECONNECT_DELAY_MS",
        default_value_t = 5_000
    )]
    catalog_max_reconnect_delay_ms: u64,

    /// Deadline for one catalog connection attempt in milliseconds (100..=30,000).
    #[arg(
        long,
        env = "PGSHARD_CATALOG_CONNECT_TIMEOUT_MS",
        default_value_t = 5_000
    )]
    catalog_connect_timeout_ms: u64,

    /// Deadline for one catalog load and publication in milliseconds (100..=300,000).
    #[arg(
        long,
        env = "PGSHARD_CATALOG_OPERATION_TIMEOUT_MS",
        default_value_t = 30_000
    )]
    catalog_operation_timeout_ms: u64,
}

impl PoolerConfig {
    /// Parses process arguments and environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown arguments, an unreadable or unsafe DSN,
    /// or catalog timing values outside their validated bounds.
    pub fn from_env() -> Result<Self, PoolerConfigError> {
        Self::try_parse_from(std::env::args_os())
    }

    /// Parses a supplied argument iterator.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown arguments, an unreadable or unsafe DSN,
    /// or catalog timing values outside their validated bounds.
    pub fn try_parse_from<I, T>(arguments: I) -> Result<Self, PoolerConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let raw = RawConfig::try_parse_from(arguments)?;
        let supervisor = catalog_supervisor(&raw)?;
        let operator_fields_present = raw.shardschema_host.is_some()
            || raw.shardschema_password_file.is_some()
            || raw.shardschema_ca_file.is_some()
            || raw.shardschema_client_sha256.is_some();
        let catalog = match raw.catalog_mode {
            CatalogMode::Local => {
                if operator_fields_present {
                    return Err(PoolerConfigError::OperatorTlsFieldsForbiddenInLocalMode);
                }
                let Some(path) = raw.shardschema_dsn_file.as_deref() else {
                    return Err(PoolerConfigError::CatalogDsnRequired);
                };
                Some(supervised_catalog(path, supervisor)?)
            }
            CatalogMode::OperatorTls => {
                if raw.shardschema_dsn_file.is_some() {
                    return Err(PoolerConfigError::CatalogDsnForbiddenInOperatorTlsMode);
                }
                let (Some(host), Some(password_path), Some(ca_path), Some(expected_sha256)) = (
                    raw.shardschema_host.as_deref(),
                    raw.shardschema_password_file.as_deref(),
                    raw.shardschema_ca_file.as_deref(),
                    raw.shardschema_client_sha256.as_deref(),
                ) else {
                    return Err(PoolerConfigError::OperatorTlsFieldsRequired);
                };
                Some(operator_tls_catalog(
                    host,
                    password_path,
                    ca_path,
                    expected_sha256,
                    supervisor,
                )?)
            }
            CatalogMode::BootstrapUnavailable => {
                if raw.shardschema_dsn_file.is_some() || operator_fields_present {
                    return Err(PoolerConfigError::CatalogCredentialsForbiddenInBootstrapMode);
                }
                None
            }
        };

        Ok(Self {
            http_bind: raw.http_bind,
            read_write_bind: raw.read_write_bind,
            catalog,
        })
    }

    /// Returns the control-plane HTTP bind address.
    #[must_use]
    pub const fn http_bind(&self) -> SocketAddr {
        self.http_bind
    }

    /// Returns the `PostgreSQL` read-write bind address.
    #[must_use]
    pub const fn read_write_bind(&self) -> SocketAddr {
        self.read_write_bind
    }

    pub(crate) fn into_runtime_parts(self) -> Option<SupervisedCatalogConfig> {
        self.catalog
    }

    #[cfg(test)]
    pub(crate) fn from_runtime_parts(
        http_bind: SocketAddr,
        read_write_bind: SocketAddr,
        catalog: Config,
        supervisor: CatalogSupervisorConfig,
    ) -> Self {
        Self {
            http_bind,
            read_write_bind,
            catalog: Some(SupervisedCatalogConfig {
                catalog,
                connector: CatalogConnector::LocalNoTls,
                supervisor,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn bootstrap_unavailable(
        http_bind: SocketAddr,
        read_write_bind: SocketAddr,
    ) -> Self {
        Self {
            http_bind,
            read_write_bind,
            catalog: None,
        }
    }
}

fn supervised_catalog(
    dsn_path: &Path,
    supervisor: CatalogSupervisorConfig,
) -> Result<SupervisedCatalogConfig, PoolerConfigError> {
    let dsn = read_shardschema_dsn(dsn_path)?;
    let mut catalog: Config = dsn.parse().map_err(|_| PoolerConfigError::InvalidDsn)?;
    validate_catalog_transport(&catalog)?;
    catalog.application_name(CATALOG_APPLICATION_NAME);

    Ok(SupervisedCatalogConfig {
        catalog,
        connector: CatalogConnector::LocalNoTls,
        supervisor,
    })
}

fn operator_tls_catalog(
    host: &str,
    password_path: &Path,
    ca_path: &Path,
    expected_sha256: &str,
    supervisor: CatalogSupervisorConfig,
) -> Result<SupervisedCatalogConfig, PoolerConfigError> {
    validate_catalog_dns_name(host)?;
    let password = read_bounded_regular_file(
        password_path,
        SHARDSCHEMA_PASSWORD_BYTES,
        "shardschema password file",
    )?;
    if password.len() != SHARDSCHEMA_PASSWORD_BYTES
        || !password
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(PoolerConfigError::InvalidCatalogPassword);
    }
    let ca_contents =
        read_bounded_regular_file(ca_path, MAX_SHARDSCHEMA_CA_BYTES, "shardschema CA file")?;
    if !is_lower_hex_sha256(expected_sha256)
        || catalog_material_sha256(
            CATALOG_CLIENT_DIGEST_DOMAIN,
            [&password[..], &ca_contents[..]],
        ) != expected_sha256
    {
        return Err(PoolerConfigError::CatalogMaterialMismatch);
    }
    let ca_certificate = parse_single_ca_certificate(&ca_contents)?;
    let mut roots = RootCertStore::empty();
    roots
        .add(ca_certificate)
        .map_err(|_| PoolerConfigError::InvalidCatalogCa)?;
    let tls =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| PoolerConfigError::InvalidCatalogTlsConfiguration)?
            .with_root_certificates(roots)
            .with_no_client_auth();

    let mut catalog = Config::new();
    catalog
        .host(host)
        .port(SHARDSCHEMA_PORT)
        .user(CATALOG_LOGIN_ROLE)
        .password(password)
        .dbname(SHARDSCHEMA_DATABASE)
        .ssl_mode(SslMode::Require)
        .target_session_attrs(TargetSessionAttrs::ReadWrite)
        .channel_binding(ChannelBinding::Require)
        .application_name(CATALOG_APPLICATION_NAME);

    Ok(SupervisedCatalogConfig {
        catalog,
        connector: CatalogConnector::OperatorTls(MakeRustlsConnect::new(tls)),
        supervisor,
    })
}

fn catalog_material_sha256<'a>(domain: &str, values: impl IntoIterator<Item = &'a [u8]>) -> String {
    let mut hash = Sha256::new();
    hash.update(domain.as_bytes());
    hash.update(b"\n");
    for value in values {
        let component = Sha256::digest(value);
        hash.update(lower_hex(&component).as_bytes());
        hash.update(b"\n");
    }
    lower_hex(&hash.finalize())
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn catalog_supervisor(raw: &RawConfig) -> Result<CatalogSupervisorConfig, PoolerConfigError> {
    let poll_interval =
        CatalogPollInterval::new(Duration::from_millis(raw.catalog_poll_interval_ms))?;
    let operation_timeout =
        CatalogOperationTimeout::new(Duration::from_millis(raw.catalog_operation_timeout_ms))?;
    Ok(CatalogSupervisorConfig::new(
        poll_interval,
        Duration::from_millis(raw.catalog_stale_grace_ms),
        Duration::from_millis(raw.catalog_initial_reconnect_delay_ms),
        Duration::from_millis(raw.catalog_max_reconnect_delay_ms),
    )?
    .with_timeouts(
        Duration::from_millis(raw.catalog_connect_timeout_ms),
        operation_timeout,
    )?)
}

fn validate_catalog_dns_name(host: &str) -> Result<(), PoolerConfigError> {
    if host.is_empty()
        || host.len() > 253
        || host.parse::<IpAddr>().is_ok()
        || host.ends_with('.')
        || host.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    {
        return Err(PoolerConfigError::InvalidCatalogDnsName);
    }
    Ok(())
}

fn read_bounded_regular_file(
    path: &Path,
    maximum: usize,
    description: &'static str,
) -> Result<Vec<u8>, PoolerConfigError> {
    let metadata = fs::metadata(path).map_err(|source| PoolerConfigError::CatalogFile {
        description,
        source,
    })?;
    if !metadata.file_type().is_file() {
        return Err(PoolerConfigError::CatalogFileNotRegular { description });
    }
    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
        Mode::empty(),
    )
    .map_err(|source| PoolerConfigError::CatalogFile {
        description,
        source: source.into(),
    })?;
    let file = fs::File::from(descriptor);
    if !file
        .metadata()
        .map_err(|source| PoolerConfigError::CatalogFile {
            description,
            source,
        })?
        .file_type()
        .is_file()
    {
        return Err(PoolerConfigError::CatalogFileNotRegular { description });
    }
    let mut contents = Vec::with_capacity(maximum.saturating_add(1));
    file.take(maximum.saturating_add(1) as u64)
        .read_to_end(&mut contents)
        .map_err(|source| PoolerConfigError::CatalogFile {
            description,
            source,
        })?;
    if contents.len() > maximum {
        return Err(PoolerConfigError::CatalogFileTooLarge {
            description,
            maximum,
        });
    }
    Ok(contents)
}

fn parse_single_ca_certificate(
    contents: &[u8],
) -> Result<CertificateDer<'static>, PoolerConfigError> {
    if !contents.starts_with(b"-----BEGIN CERTIFICATE-----\n") {
        return Err(PoolerConfigError::InvalidCatalogCa);
    }
    let mut sections = <(SectionKind, Vec<u8>)>::pem_slice_iter(contents);
    let Some(section) = sections.next() else {
        return Err(PoolerConfigError::InvalidCatalogCa);
    };
    let section = section.map_err(|_| PoolerConfigError::InvalidCatalogCa)?;
    if !sections.remainder().is_empty() {
        return Err(PoolerConfigError::InvalidCatalogCa);
    }
    match section {
        (SectionKind::Certificate, der) => Ok(CertificateDer::from(der)),
        _ => Err(PoolerConfigError::InvalidCatalogCa),
    }
}

fn read_shardschema_dsn(path: &Path) -> Result<String, PoolerConfigError> {
    let metadata = fs::metadata(path).map_err(PoolerConfigError::DsnFile)?;
    if !metadata.file_type().is_file() {
        return Err(PoolerConfigError::DsnNotRegularFile);
    }
    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
        Mode::empty(),
    )
    .map_err(|error| PoolerConfigError::DsnFile(error.into()))?;
    let file = fs::File::from(descriptor);
    if !file
        .metadata()
        .map_err(PoolerConfigError::DsnFile)?
        .file_type()
        .is_file()
    {
        return Err(PoolerConfigError::DsnNotRegularFile);
    }
    let mut bytes = Vec::with_capacity(MAX_SHARDSCHEMA_DSN_BYTES + 1);
    file.take((MAX_SHARDSCHEMA_DSN_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(PoolerConfigError::DsnFile)?;
    if bytes.len() > MAX_SHARDSCHEMA_DSN_BYTES {
        return Err(PoolerConfigError::DsnTooLarge {
            maximum: MAX_SHARDSCHEMA_DSN_BYTES,
        });
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    let dsn = String::from_utf8(bytes).map_err(|_| PoolerConfigError::DsnNotUtf8)?;
    if dsn.is_empty() {
        return Err(PoolerConfigError::EmptyDsn);
    }
    if dsn.trim() != dsn {
        return Err(PoolerConfigError::AmbiguousDsnWhitespace);
    }
    Ok(dsn)
}

fn validate_catalog_transport(catalog: &Config) -> Result<(), CatalogDsnPolicyError> {
    if catalog.get_dbname() != Some(SHARDSCHEMA_DATABASE) {
        return Err(CatalogDsnPolicyError::WrongDatabase);
    }
    if catalog.get_ssl_mode() != SslMode::Disable {
        return Err(CatalogDsnPolicyError::TlsModeNotExplicitlyDisabled);
    }
    if catalog.get_target_session_attrs() != TargetSessionAttrs::ReadWrite {
        return Err(CatalogDsnPolicyError::ReadWriteTargetRequired);
    }
    if catalog.get_options().is_some() {
        return Err(CatalogDsnPolicyError::StartupOptionsForbidden);
    }
    if catalog.get_hosts().is_empty() {
        return Err(CatalogDsnPolicyError::HostRequired);
    }
    if catalog.get_hosts().iter().any(|host| match host {
        Host::Tcp(host) => host
            .parse::<IpAddr>()
            .map_or(true, |address| !address.is_loopback()),
        Host::Unix(_) => false,
    }) || catalog
        .get_hostaddrs()
        .iter()
        .any(|address| !address.is_loopback())
    {
        return Err(CatalogDsnPolicyError::RemotePlaintextTransport);
    }
    Ok(())
}

/// Credential-safe pooler configuration failure.
#[derive(Debug, Error)]
pub enum PoolerConfigError {
    /// Command-line parsing failed.
    #[error(transparent)]
    Arguments(#[from] clap::Error),
    /// The DSN file could not be read.
    #[error("could not read the shardschema DSN file: {0}")]
    DsnFile(#[source] std::io::Error),
    /// Local supervision has no DSN file.
    #[error("shardschema DSN file is required in local catalog mode")]
    CatalogDsnRequired,
    /// Local development mode must not silently accept operator credentials.
    #[error("operator TLS catalog fields must be absent in local catalog mode")]
    OperatorTlsFieldsForbiddenInLocalMode,
    /// Operator TLS mode builds its connection policy without a DSN.
    #[error("shardschema DSN file must be absent in operator-tls catalog mode")]
    CatalogDsnForbiddenInOperatorTlsMode,
    /// Operator TLS requires all three explicit Secret projections.
    #[error(
        "shardschema host, password file, CA file, and client SHA-256 are required in operator-tls catalog mode"
    )]
    OperatorTlsFieldsRequired,
    /// Bootstrap mode must not silently retain any catalog credentials.
    #[error(
        "all shardschema credential and connection fields must be absent in bootstrap-unavailable catalog mode"
    )]
    CatalogCredentialsForbiddenInBootstrapMode,
    /// The operator endpoint must use an exact DNS hostname, never an IP.
    #[error("shardschema host must be a lowercase DNS name without a trailing dot")]
    InvalidCatalogDnsName,
    /// A projected operator credential or CA file could not be read.
    #[error("could not read the {description}: {source}")]
    CatalogFile {
        /// Non-sensitive file description.
        description: &'static str,
        /// Underlying file error; paths and contents are never included by us.
        #[source]
        source: std::io::Error,
    },
    /// A projected operator credential or CA target is not a regular file.
    #[error("the {description} must resolve to a regular file")]
    CatalogFileNotRegular {
        /// Non-sensitive file description.
        description: &'static str,
    },
    /// A projected operator credential or CA exceeds its bounded read.
    #[error("the {description} exceeds {maximum} bytes")]
    CatalogFileTooLarge {
        /// Non-sensitive file description.
        description: &'static str,
        /// Maximum accepted file size.
        maximum: usize,
    },
    /// The catalog password must have the operator's canonical random shape.
    #[error("shardschema password must contain exactly 64 lowercase hexadecimal bytes")]
    InvalidCatalogPassword,
    /// The CA projection must be one parseable certificate and nothing else.
    #[error("shardschema CA file must contain exactly one PEM certificate")]
    InvalidCatalogCa,
    /// The projection no longer matches the operator's checkpointed material.
    #[error("shardschema password or CA material differs from the checkpointed creation result")]
    CatalogMaterialMismatch,
    /// The explicit TLS 1.3 client policy could not be constructed.
    #[error("could not construct the shardschema TLS 1.3 client policy")]
    InvalidCatalogTlsConfiguration,
    /// The DSN path does not resolve to a regular file.
    #[error("shardschema DSN path must resolve to a regular file")]
    DsnNotRegularFile,
    /// The DSN file exceeds its memory bound.
    #[error("shardschema DSN file exceeds {maximum} bytes")]
    DsnTooLarge {
        /// Maximum accepted file size.
        maximum: usize,
    },
    /// The DSN file is not UTF-8.
    #[error("shardschema DSN file must contain UTF-8")]
    DsnNotUtf8,
    /// The DSN is empty.
    #[error("shardschema DSN file is empty")]
    EmptyDsn,
    /// Leading or repeated trailing whitespace is ambiguous.
    #[error("shardschema DSN has ambiguous leading or trailing whitespace")]
    AmbiguousDsnWhitespace,
    /// The DSN does not follow tokio-postgres syntax.
    #[error("shardschema DSN is invalid")]
    InvalidDsn,
    /// The parsed DSN violates the temporary local-transport policy.
    #[error(transparent)]
    CatalogPolicy(#[from] CatalogDsnPolicyError),
    /// The poll interval is outside catalog bounds.
    #[error(transparent)]
    PollInterval(#[from] CatalogPollIntervalError),
    /// The catalog operation timeout is outside catalog bounds.
    #[error(transparent)]
    OperationTimeout(#[from] CatalogOperationTimeoutError),
    /// The combined supervisor policy is invalid.
    #[error(transparent)]
    Supervisor(#[from] CatalogSupervisorConfigError),
}

/// Temporary transport-policy rejection for the control-only executable.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CatalogDsnPolicyError {
    /// The dedicated database name is absent or wrong.
    #[error("catalog DSN must name the shardschema database explicitly")]
    WrongDatabase,
    /// The development-only connector requires an explicit no-TLS policy.
    #[error("catalog DSN must set sslmode=disable for the local-only connector")]
    TlsModeNotExplicitlyDisabled,
    /// The catalog must resolve to a writer.
    #[error("catalog DSN must set target_session_attrs=read-write")]
    ReadWriteTargetRequired,
    /// Startup options can bypass the runtime's session policy.
    #[error("catalog DSN must not set PostgreSQL startup options")]
    StartupOptionsForbidden,
    /// At least one transport endpoint is required.
    #[error("catalog DSN must specify a host")]
    HostRequired,
    /// `NoTls` is permitted only over a local kernel transport.
    #[error("catalog DSN must use only loopback IP literals or Unix sockets")]
    RemotePlaintextTransport,
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::time::Instant;

    use super::*;
    use clap::CommandFactory;
    use tempfile::TempDir;

    struct TestDsnFile {
        _directory: TempDir,
        path: PathBuf,
    }

    struct TestOperatorFiles {
        directory: TempDir,
        password_path: PathBuf,
        ca_path: PathBuf,
        client_sha256: String,
    }

    impl TestOperatorFiles {
        fn new(password: &[u8], ca: &[u8]) -> Self {
            let directory = TempDir::new().expect("create operator catalog test directory");
            let password_path = directory.path().join("catalog-password");
            let ca_path = directory.path().join("ca.crt");
            fs::write(&password_path, password).expect("write test catalog password");
            fs::write(&ca_path, ca).expect("write test catalog CA");
            Self {
                directory,
                password_path,
                ca_path,
                client_sha256: catalog_material_sha256(
                    CATALOG_CLIENT_DIGEST_DOMAIN,
                    [password, ca],
                ),
            }
        }

        fn arguments(&self) -> Vec<OsString> {
            operator_arguments(
                "demo-shardschema.default.svc",
                &self.password_path,
                &self.ca_path,
                &self.client_sha256,
            )
        }
    }

    const TEST_CA: &[u8] = b"-----BEGIN CERTIFICATE-----\n\
MIIBiTCCAS+gAwIBAgIUbJ8sOt0ubvJbxhBrRzotX+SslNEwCgYIKoZIzj0EAwIw\n\
GjEYMBYGA1UEAwwPcGdzaGFyZC10ZXN0LWNhMB4XDTI2MDcxNzExMDE1OVoXDTM2\n\
MDcxNDExMDE1OVowGjEYMBYGA1UEAwwPcGdzaGFyZC10ZXN0LWNhMFkwEwYHKoZI\n\
zj0CAQYIKoZIzj0DAQcDQgAEFVjxj/1nOlW3UJlkBSa3fW/nF7sBBOToSP74N+wZ\n\
JAXlBOJvMB80cluAfqVSGiaEe9ypJl/0yOBNW05CBO67aqNTMFEwHQYDVR0OBBYE\n\
FAIHdwZc3gkqjK/SR9b8eTDjesx7MB8GA1UdIwQYMBaAFAIHdwZc3gkqjK/SR9b8\n\
eTDjesx7MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIgMPua5hVn\n\
Q6DR1lrE26RxZR6piU+H0x2k8/8Abe0RyIACIQCQe2yRXSYi6Dau2hvPJ++YKspw\n\
pqAiYB0dKbPxAXdiSQ==\n\
-----END CERTIFICATE-----\n";

    fn operator_arguments(
        host: &str,
        password_path: &Path,
        ca_path: &Path,
        client_sha256: &str,
    ) -> Vec<OsString> {
        vec![
            OsString::from("pgshard-pooler"),
            OsString::from("--catalog-mode=operator-tls"),
            OsString::from(format!("--shardschema-host={host}")),
            OsString::from("--shardschema-password-file"),
            password_path.as_os_str().to_owned(),
            OsString::from("--shardschema-ca-file"),
            ca_path.as_os_str().to_owned(),
            OsString::from(format!("--shardschema-client-sha256={client_sha256}")),
        ]
    }

    impl TestDsnFile {
        fn new(contents: &[u8]) -> Self {
            let directory = TempDir::new().expect("create test DSN directory");
            let path = directory.path().join("shardschema.dsn");
            fs::write(&path, contents).expect("write test DSN");
            Self {
                _directory: directory,
                path,
            }
        }

        fn fifo() -> Self {
            let directory = TempDir::new().expect("create test FIFO directory");
            let path = directory.path().join("shardschema.dsn");
            rustix::fs::mkfifoat(rustix::fs::CWD, &path, Mode::RUSR | Mode::WUSR)
                .expect("create test FIFO");
            Self {
                _directory: directory,
                path,
            }
        }

        fn arguments(&self) -> Vec<OsString> {
            self.arguments_with_poll_interval(30_000)
        }

        fn arguments_with_poll_interval(&self, poll_interval_milliseconds: u64) -> Vec<OsString> {
            vec![
                OsString::from("pgshard-pooler"),
                OsString::from("--http-bind=0.0.0.0:8080"),
                OsString::from("--read-write-bind=0.0.0.0:5432"),
                OsString::from("--shardschema-dsn-file"),
                self.path.as_os_str().to_owned(),
                OsString::from(format!(
                    "--catalog-poll-interval-ms={poll_interval_milliseconds}"
                )),
                OsString::from("--catalog-stale-grace-ms=90000"),
                OsString::from("--catalog-initial-reconnect-delay-ms=100"),
                OsString::from("--catalog-max-reconnect-delay-ms=5000"),
                OsString::from("--catalog-connect-timeout-ms=5000"),
                OsString::from("--catalog-operation-timeout-ms=30000"),
            ]
        }
    }

    const VALID_DSN: &str = "postgresql://postgres@127.0.0.1/shardschema?sslmode=disable&target_session_attrs=read-write";

    fn parse(contents: &[u8]) -> Result<PoolerConfig, PoolerConfigError> {
        let file = TestDsnFile::new(contents);
        PoolerConfig::try_parse_from(file.arguments())
    }

    fn bootstrap_arguments() -> Vec<OsString> {
        vec![
            OsString::from("pgshard-pooler"),
            OsString::from("--http-bind=0.0.0.0:8080"),
            OsString::from("--read-write-bind=0.0.0.0:5432"),
            OsString::from("--catalog-mode=bootstrap-unavailable"),
        ]
    }

    #[test]
    fn accepts_bounded_local_catalog_configuration() {
        let config = parse(format!("{VALID_DSN}\n").as_bytes()).expect("valid pooler config");
        assert_eq!(
            config.http_bind(),
            "0.0.0.0:8080".parse().expect("default HTTP bind")
        );
        assert_eq!(
            config.read_write_bind(),
            "0.0.0.0:5432".parse().expect("default read-write bind")
        );
        let Some(SupervisedCatalogConfig {
            catalog,
            supervisor,
            ..
        }) = &config.catalog
        else {
            panic!("local catalog configuration did not enable supervision");
        };
        assert_eq!(catalog.get_dbname(), Some(SHARDSCHEMA_DATABASE));
        assert_eq!(
            catalog.get_application_name(),
            Some(CATALOG_APPLICATION_NAME)
        );
        assert_eq!(
            supervisor.operation_timeout().get(),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn accepts_operator_authenticated_tls_catalog_configuration() {
        let files = TestOperatorFiles::new(
            b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            TEST_CA,
        );
        let config = PoolerConfig::try_parse_from(files.arguments()).expect("operator TLS config");
        let Some(SupervisedCatalogConfig {
            catalog,
            connector: CatalogConnector::OperatorTls(_),
            ..
        }) = config.catalog
        else {
            panic!("operator TLS catalog configuration did not select rustls");
        };
        assert_eq!(
            catalog.get_hosts(),
            &[Host::Tcp("demo-shardschema.default.svc".into())]
        );
        assert_eq!(catalog.get_ports(), &[SHARDSCHEMA_PORT]);
        assert_eq!(catalog.get_user(), Some(CATALOG_LOGIN_ROLE));
        assert_eq!(catalog.get_dbname(), Some(SHARDSCHEMA_DATABASE));
        assert_eq!(catalog.get_ssl_mode(), SslMode::Require);
        assert_eq!(catalog.get_channel_binding(), ChannelBinding::Require);
        assert_eq!(
            catalog.get_target_session_attrs(),
            TargetSessionAttrs::ReadWrite
        );
        assert_eq!(
            catalog.get_application_name(),
            Some(CATALOG_APPLICATION_NAME)
        );
        assert_eq!(
            catalog.get_password(),
            Some(&b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"[..])
        );
        assert!(catalog.get_options().is_none());
    }

    #[test]
    fn operator_tls_accepts_kubernetes_style_symlink_projections() {
        let files = TestOperatorFiles::new(
            b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            TEST_CA,
        );
        let projected_password = files.directory.path().join("projected-password");
        let projected_ca = files.directory.path().join("projected-ca");
        symlink(&files.password_path, &projected_password).expect("symlink password projection");
        symlink(&files.ca_path, &projected_ca).expect("symlink CA projection");
        PoolerConfig::try_parse_from(operator_arguments(
            "demo-shardschema.default.svc",
            &projected_password,
            &projected_ca,
            &files.client_sha256,
        ))
        .expect("symlinked Kubernetes Secret projections");
    }

    #[test]
    fn operator_tls_rejects_incomplete_or_cross_mode_credentials() {
        let files = TestOperatorFiles::new(
            b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            TEST_CA,
        );
        assert!(matches!(
            PoolerConfig::try_parse_from(["pgshard-pooler", "--catalog-mode=operator-tls"]),
            Err(PoolerConfigError::OperatorTlsFieldsRequired)
        ));

        let dsn = TestDsnFile::new(VALID_DSN.as_bytes());
        let mut local_arguments = dsn.arguments();
        local_arguments.push(OsString::from(
            "--shardschema-host=demo-shardschema.default.svc",
        ));
        assert!(matches!(
            PoolerConfig::try_parse_from(local_arguments),
            Err(PoolerConfigError::OperatorTlsFieldsForbiddenInLocalMode)
        ));

        let mut operator_with_dsn = files.arguments();
        operator_with_dsn.push(OsString::from("--shardschema-dsn-file"));
        operator_with_dsn.push(dsn.path.as_os_str().to_owned());
        assert!(matches!(
            PoolerConfig::try_parse_from(operator_with_dsn),
            Err(PoolerConfigError::CatalogDsnForbiddenInOperatorTlsMode)
        ));

        let mut replaced_material = files.arguments();
        let digest = replaced_material
            .last_mut()
            .expect("operator arguments include the material digest");
        *digest = OsString::from(format!("--shardschema-client-sha256={}", "0".repeat(64)));
        assert!(matches!(
            PoolerConfig::try_parse_from(replaced_material),
            Err(PoolerConfigError::CatalogMaterialMismatch)
        ));

        let mut bootstrap_with_credentials = bootstrap_arguments();
        bootstrap_with_credentials.push(OsString::from(
            "--shardschema-host=demo-shardschema.default.svc",
        ));
        assert!(matches!(
            PoolerConfig::try_parse_from(bootstrap_with_credentials),
            Err(PoolerConfigError::CatalogCredentialsForbiddenInBootstrapMode)
        ));
    }

    #[test]
    fn operator_tls_rejects_unsafe_dns_password_and_ca_shapes_without_leaking_contents() {
        let valid_password = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let files = TestOperatorFiles::new(valid_password, TEST_CA);
        for host in [
            "127.0.0.1",
            "Demo-shardschema.default.svc",
            "demo-shardschema.default.svc.",
            "-demo.default.svc",
            "demo..svc",
        ] {
            assert!(matches!(
                PoolerConfig::try_parse_from(operator_arguments(
                    host,
                    &files.password_path,
                    &files.ca_path,
                    &files.client_sha256,
                )),
                Err(PoolerConfigError::InvalidCatalogDnsName)
            ));
        }

        for password in [
            b"short".as_slice(),
            b"0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF".as_slice(),
            b"SENSITIVE_PASSWORD_MARKER_MUST_NOT_ESCAPE_0123456789abcdefghijkl".as_slice(),
        ] {
            let invalid = TestOperatorFiles::new(password, TEST_CA);
            let error = PoolerConfig::try_parse_from(invalid.arguments())
                .err()
                .expect("invalid password must fail");
            assert!(matches!(error, PoolerConfigError::InvalidCatalogPassword));
            assert!(!format!("{error:?}").contains("SENSITIVE_PASSWORD_MARKER"));
            assert!(!error.to_string().contains("SENSITIVE_PASSWORD_MARKER"));
        }

        for ca in [
            b"not a certificate".as_slice(),
            [TEST_CA, TEST_CA].concat().as_slice(),
            [b"prefix".as_slice(), TEST_CA].concat().as_slice(),
        ] {
            let invalid = TestOperatorFiles::new(valid_password, ca);
            assert!(matches!(
                PoolerConfig::try_parse_from(invalid.arguments()),
                Err(PoolerConfigError::InvalidCatalogCa)
            ));
        }
    }

    #[test]
    fn operator_tls_bounds_files_and_rejects_fifo_without_blocking() {
        let directory = TempDir::new().expect("create operator file-bound directory");
        let password_fifo = directory.path().join("catalog-password");
        let ca_path = directory.path().join("ca.crt");
        rustix::fs::mkfifoat(rustix::fs::CWD, &password_fifo, Mode::RUSR | Mode::WUSR)
            .expect("create password FIFO");
        fs::write(&ca_path, TEST_CA).expect("write CA");
        let started = Instant::now();
        assert!(matches!(
            PoolerConfig::try_parse_from(operator_arguments(
                "demo-shardschema.default.svc",
                &password_fifo,
                &ca_path,
                &"0".repeat(64),
            )),
            Err(PoolerConfigError::CatalogFileNotRegular { .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(1));

        let oversized_password =
            TestOperatorFiles::new(&[b'a'; SHARDSCHEMA_PASSWORD_BYTES + 1], TEST_CA);
        assert!(matches!(
            PoolerConfig::try_parse_from(oversized_password.arguments()),
            Err(PoolerConfigError::CatalogFileTooLarge { .. })
        ));
        let oversized_ca = TestOperatorFiles::new(
            b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &vec![b'x'; MAX_SHARDSCHEMA_CA_BYTES + 1],
        );
        assert!(matches!(
            PoolerConfig::try_parse_from(oversized_ca.arguments()),
            Err(PoolerConfigError::CatalogFileTooLarge { .. })
        ));
    }

    #[test]
    fn accepts_only_explicit_credential_free_bootstrap_mode() {
        let config = PoolerConfig::try_parse_from(bootstrap_arguments())
            .expect("explicit bootstrap-unavailable mode");
        assert!(config.catalog.is_none());

        assert!(matches!(
            PoolerConfig::try_parse_from(["pgshard-pooler"]),
            Err(PoolerConfigError::CatalogDsnRequired)
        ));

        let file = TestDsnFile::new(VALID_DSN.as_bytes());
        let mut arguments = bootstrap_arguments();
        arguments.push(OsString::from("--shardschema-dsn-file"));
        arguments.push(file.path.as_os_str().to_owned());
        assert!(matches!(
            PoolerConfig::try_parse_from(arguments),
            Err(PoolerConfigError::CatalogCredentialsForbiddenInBootstrapMode)
        ));

        let mut arguments = bootstrap_arguments();
        arguments.push(OsString::from("--catalog-poll-interval-ms=999"));
        assert!(matches!(
            PoolerConfig::try_parse_from(arguments),
            Err(PoolerConfigError::PollInterval(_))
        ));
    }

    #[test]
    fn rejects_unsafe_catalog_transport_variants() {
        for (dsn, expected) in [
            (
                "postgresql://postgres@192.0.2.1/shardschema?sslmode=disable&target_session_attrs=read-write",
                CatalogDsnPolicyError::RemotePlaintextTransport,
            ),
            (
                "postgresql://postgres@localhost/shardschema?sslmode=disable&target_session_attrs=read-write",
                CatalogDsnPolicyError::RemotePlaintextTransport,
            ),
            (
                "postgresql://postgres@127.0.0.1/shardschema?target_session_attrs=read-write",
                CatalogDsnPolicyError::TlsModeNotExplicitlyDisabled,
            ),
            (
                "postgresql://postgres@127.0.0.1/postgres?sslmode=disable&target_session_attrs=read-write",
                CatalogDsnPolicyError::WrongDatabase,
            ),
            (
                "postgresql://postgres@127.0.0.1/shardschema?sslmode=disable",
                CatalogDsnPolicyError::ReadWriteTargetRequired,
            ),
            (
                "host=127.0.0.1 hostaddr=192.0.2.1 dbname=shardschema sslmode=disable target_session_attrs=read-write",
                CatalogDsnPolicyError::RemotePlaintextTransport,
            ),
            (
                "host=127.0.0.1 dbname=shardschema sslmode=disable target_session_attrs=read-write options='-c search_path=public'",
                CatalogDsnPolicyError::StartupOptionsForbidden,
            ),
        ] {
            assert!(
                matches!(
                    parse(dsn.as_bytes()),
                    Err(PoolerConfigError::CatalogPolicy(actual)) if actual == expected
                ),
                "unexpected result for policy case {expected:?}"
            );
        }
    }

    #[test]
    fn bounds_and_redacts_dsn_file_failures() {
        assert!(matches!(parse(b""), Err(PoolerConfigError::EmptyDsn)));
        assert!(matches!(
            parse(b"  host=127.0.0.1"),
            Err(PoolerConfigError::AmbiguousDsnWhitespace)
        ));
        assert!(matches!(
            parse(&vec![b'x'; MAX_SHARDSCHEMA_DSN_BYTES + 1]),
            Err(PoolerConfigError::DsnTooLarge { .. })
        ));
        let marker = "SENSITIVE_DSN_MARKER_MUST_NOT_ESCAPE";
        let Err(error) = parse(marker.as_bytes()) else {
            panic!("marker unexpectedly parsed as a valid DSN");
        };
        assert!(!format!("{error:?}").contains(marker));
        assert!(!error.to_string().contains(marker));
    }

    #[test]
    fn rejects_fifo_without_waiting_for_a_writer() {
        let file = TestDsnFile::fifo();
        let started = Instant::now();
        assert!(matches!(
            PoolerConfig::try_parse_from(file.arguments()),
            Err(PoolerConfigError::DsnNotRegularFile)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn generated_help_explains_units_bounds_and_dsn_policy() {
        let help = RawConfig::command().render_long_help().to_string();
        for expected in [
            "PostgreSQL handshake boundary",
            "connections are rejected until the data plane exists",
            "explicit unavailable bootstrap",
            "bootstrap-unavailable",
            "maximum 16 KiB",
            "1,000..=300,000",
            "2,000..=900,000",
            "100..=30,000",
            "100..=300,000",
        ] {
            assert!(help.contains(expected), "missing help text: {expected}");
        }
    }

    #[test]
    fn rejects_out_of_range_runtime_timing() {
        let file = TestDsnFile::new(VALID_DSN.as_bytes());
        assert!(matches!(
            PoolerConfig::try_parse_from(file.arguments_with_poll_interval(999)),
            Err(PoolerConfigError::PollInterval(_))
        ));
    }
}
