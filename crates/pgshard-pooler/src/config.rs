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
use rustix::fs::{Mode, OFlags};
use thiserror::Error;
use tokio_postgres::Config;
use tokio_postgres::config::{Host, SslMode, TargetSessionAttrs};

const MAX_SHARDSCHEMA_DSN_BYTES: usize = 16 * 1024;
const CATALOG_APPLICATION_NAME: &str = "pgshard-pooler-catalog";

/// Validated configuration for the fail-closed pooler runtime.
pub struct PoolerConfig {
    http_bind: SocketAddr,
    read_write_bind: SocketAddr,
    catalog: Config,
    supervisor: CatalogSupervisorConfig,
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

    /// File containing one local-only shardschema database DSN (maximum 16 KiB).
    #[arg(long, env = "PGSHARD_SHARDSCHEMA_DSN_FILE")]
    shardschema_dsn_file: PathBuf,

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
            read_write_bind: raw.read_write_bind,
            catalog,
            supervisor,
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

    pub(crate) fn into_runtime_parts(self) -> (Config, CatalogSupervisorConfig) {
        (self.catalog, self.supervisor)
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
            catalog,
            supervisor,
        }
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
    use std::time::Instant;

    use super::*;
    use clap::CommandFactory;
    use tempfile::TempDir;

    struct TestDsnFile {
        _directory: TempDir,
        path: PathBuf,
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
