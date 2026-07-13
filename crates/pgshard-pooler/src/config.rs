//! Validated control-runtime configuration.

use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use pgshard_catalog::{
    CatalogOperationTimeout, CatalogOperationTimeoutError, CatalogPollInterval,
    CatalogPollIntervalError, CatalogSupervisorConfig, CatalogSupervisorConfigError,
    SHARDSCHEMA_DATABASE,
};
use thiserror::Error;
use tokio_postgres::Config;
use tokio_postgres::config::{Host, SslMode, TargetSessionAttrs};

const MAX_SHARDSCHEMA_DSN_BYTES: usize = 16 * 1024;
const CATALOG_APPLICATION_NAME: &str = "pgshard-pooler-catalog";

/// Validated configuration for the pooler control runtime.
pub struct PoolerConfig {
    http_bind: SocketAddr,
    catalog: Config,
    supervisor: CatalogSupervisorConfig,
}

#[derive(Debug, Parser)]
#[command(name = "pgshard-pooler", disable_help_subcommand = true)]
struct RawConfig {
    #[arg(long, env = "PGSHARD_HTTP_BIND", default_value = "0.0.0.0:8080")]
    http_bind: SocketAddr,

    #[arg(long, env = "PGSHARD_SHARDSCHEMA_DSN_FILE")]
    shardschema_dsn_file: PathBuf,

    #[arg(
        long,
        env = "PGSHARD_CATALOG_POLL_INTERVAL_MS",
        default_value_t = 30_000
    )]
    catalog_poll_interval_ms: u64,

    #[arg(long, env = "PGSHARD_CATALOG_STALE_GRACE_MS", default_value_t = 90_000)]
    catalog_stale_grace_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_CATALOG_INITIAL_RECONNECT_DELAY_MS",
        default_value_t = 100
    )]
    catalog_initial_reconnect_delay_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_CATALOG_MAX_RECONNECT_DELAY_MS",
        default_value_t = 5_000
    )]
    catalog_max_reconnect_delay_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_CATALOG_CONNECT_TIMEOUT_MS",
        default_value_t = 5_000
    )]
    catalog_connect_timeout_ms: u64,

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
        let dsn = read_shardschema_dsn(&raw.shardschema_dsn_file)?;
        let mut catalog: Config = dsn.parse().map_err(|_| PoolerConfigError::InvalidDsn)?;
        validate_catalog_transport(&catalog)?;
        catalog.application_name(CATALOG_APPLICATION_NAME);

        let poll_interval =
            CatalogPollInterval::new(Duration::from_millis(raw.catalog_poll_interval_ms))?;
        let operation_timeout =
            CatalogOperationTimeout::new(Duration::from_millis(raw.catalog_operation_timeout_ms))?;
        let supervisor = CatalogSupervisorConfig::new(
            poll_interval,
            Duration::from_millis(raw.catalog_stale_grace_ms),
            Duration::from_millis(raw.catalog_initial_reconnect_delay_ms),
            Duration::from_millis(raw.catalog_max_reconnect_delay_ms),
        )?
        .with_timeouts(
            Duration::from_millis(raw.catalog_connect_timeout_ms),
            operation_timeout,
        )?;

        Ok(Self {
            http_bind: raw.http_bind,
            catalog,
            supervisor,
        })
    }

    /// Returns the control-plane HTTP bind address.
    #[must_use]
    pub const fn http_bind(&self) -> SocketAddr {
        self.http_bind
    }

    pub(crate) fn into_runtime_parts(self) -> (Config, CatalogSupervisorConfig) {
        (self.catalog, self.supervisor)
    }

    #[cfg(test)]
    pub(crate) fn from_runtime_parts(
        http_bind: SocketAddr,
        catalog: Config,
        supervisor: CatalogSupervisorConfig,
    ) -> Self {
        Self {
            http_bind,
            catalog,
            supervisor,
        }
    }
}

fn read_shardschema_dsn(path: &Path) -> Result<String, PoolerConfigError> {
    let file = fs::File::open(path).map_err(PoolerConfigError::DsnFile)?;
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    struct TestDsnFile(PathBuf);

    impl TestDsnFile {
        fn new(contents: &[u8]) -> Self {
            let sequence = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "pgshard-pooler-config-{}-{sequence}",
                std::process::id()
            ));
            fs::write(&path, contents).expect("write test DSN");
            Self(path)
        }

        fn arguments(&self) -> Vec<OsString> {
            vec![
                OsString::from("pgshard-pooler"),
                OsString::from("--shardschema-dsn-file"),
                self.0.as_os_str().to_owned(),
            ]
        }
    }

    impl Drop for TestDsnFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    const VALID_DSN: &str = "postgresql://postgres@127.0.0.1/shardschema?sslmode=disable&target_session_attrs=read-write";

    fn parse(contents: &[u8]) -> Result<PoolerConfig, PoolerConfigError> {
        let file = TestDsnFile::new(contents);
        PoolerConfig::try_parse_from(file.arguments())
    }

    #[test]
    fn accepts_bounded_local_catalog_configuration() {
        let config = parse(format!("{VALID_DSN}\n").as_bytes()).expect("valid pooler config");
        assert_eq!(
            config.http_bind(),
            "0.0.0.0:8080".parse().expect("default HTTP bind")
        );
        assert_eq!(config.catalog.get_dbname(), Some(SHARDSCHEMA_DATABASE));
        assert_eq!(
            config.catalog.get_application_name(),
            Some(CATALOG_APPLICATION_NAME)
        );
        assert_eq!(
            config.supervisor.operation_timeout().get(),
            Duration::from_secs(30)
        );
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
    fn rejects_out_of_range_runtime_timing() {
        let file = TestDsnFile::new(VALID_DSN.as_bytes());
        let mut arguments = file.arguments();
        arguments.extend([
            OsString::from("--catalog-poll-interval-ms"),
            OsString::from("999"),
        ]);
        assert!(matches!(
            PoolerConfig::try_parse_from(arguments),
            Err(PoolerConfigError::PollInterval(_))
        ));
    }
}
