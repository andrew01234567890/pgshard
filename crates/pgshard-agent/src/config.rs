//! Strict command-line and environment configuration.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use pgshard_types::ShardId;
use thiserror::Error;
use url::Url;

use crate::coordination::WritableLeaseConfig;
use crate::domain::AgentIdentity;
use crate::postgres::{PostgresConfig, PostgresConfigError};
use crate::telemetry::TelemetryConfig;

/// Validated process configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentConfig {
    /// Address for health, readiness, status, and metrics.
    pub http_bind: SocketAddr,
    /// Stable process identity.
    pub identity: AgentIdentity,
    /// Maximum authenticated fencing lease duration accepted by the agent.
    pub max_lease_ttl_ms: u64,
    /// Optional exact per-cell writable-term Lease authority.
    pub writable_lease: Option<WritableLeaseConfig>,
    /// OpenTelemetry configuration placeholder.
    pub telemetry: TelemetryConfig,
    /// Optional client-TCP-quarantined `PostgreSQL` process supervision.
    pub postgres: Option<PostgresConfig>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum PostgresMode {
    #[default]
    Disabled,
    Quarantine,
}

#[derive(Debug, Parser)]
#[command(name = "pgshard-agent", disable_help_subcommand = true)]
struct RawConfig {
    #[arg(long, env = "PGSHARD_HTTP_BIND", default_value = "0.0.0.0:8080")]
    http_bind: SocketAddr,

    #[arg(long, env = "PGSHARD_CLUSTER_ID")]
    cluster_id: String,

    #[arg(long, env = "PGSHARD_SHARD_ID")]
    shard_id: u32,

    #[arg(long, env = "PGSHARD_INSTANCE_ID")]
    instance_id: String,

    #[arg(long, env = "PGSHARD_MAX_LEASE_TTL_MS", default_value_t = 15_000)]
    max_lease_ttl_ms: u64,

    #[arg(long, env = "PGSHARD_CLUSTER_UID")]
    cluster_uid: Option<String>,

    #[arg(long, env = "PGSHARD_POD_UID")]
    pod_uid: Option<String>,

    #[arg(long, env = "PGSHARD_LEASE_NAMESPACE")]
    lease_namespace: Option<String>,

    #[arg(long, env = "PGSHARD_WRITABLE_LEASE_NAME")]
    writable_lease_name: Option<String>,

    #[arg(long, env = "PGSHARD_WRITABLE_LEASE_UID")]
    writable_lease_uid: Option<String>,

    #[arg(long, env = "PGSHARD_WRITABLE_LEASE_DURATION_SECONDS")]
    writable_lease_duration_seconds: Option<u64>,

    #[arg(long, env = "PGSHARD_WRITABLE_LEASE_RENEW_DEADLINE_SECONDS")]
    writable_lease_renew_deadline_seconds: Option<u64>,

    #[arg(long, env = "PGSHARD_WRITABLE_LEASE_RETRY_MS")]
    writable_lease_retry_ms: Option<u64>,

    #[arg(long, env = "PGSHARD_KUBERNETES_REQUEST_TIMEOUT_MS")]
    kubernetes_request_timeout_ms: Option<u64>,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_MODE",
        value_enum,
        default_value_t = PostgresMode::Disabled
    )]
    postgres_mode: PostgresMode,

    #[arg(long, env = "PGDATA")]
    postgres_data_dir: Option<PathBuf>,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_BIN",
        default_value = "/usr/lib/postgresql/18/bin/postgres"
    )]
    postgres_bin: PathBuf,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_SOCKET_DIR",
        default_value = "/run/pgshard/postgres"
    )]
    postgres_socket_dir: PathBuf,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_HBA_FILE",
        default_value = "/etc/pgshard/quarantine.pg_hba.conf"
    )]
    postgres_hba_file: PathBuf,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_SMART_SHUTDOWN_MS",
        default_value_t = 5_000
    )]
    postgres_smart_shutdown_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_FAST_SHUTDOWN_MS",
        default_value_t = 44_000
    )]
    postgres_fast_shutdown_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_POSTGRES_IMMEDIATE_SHUTDOWN_MS",
        default_value_t = 500
    )]
    postgres_immediate_shutdown_ms: u64,
}

impl AgentConfig {
    /// Parses configuration from process arguments and environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown arguments, invalid values, or unsafe
    /// identifiers.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::try_parse_from(std::env::args_os())
    }

    /// Parses configuration from a supplied argument iterator.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown arguments, invalid values, or unsafe
    /// identifiers.
    pub fn try_parse_from<I, T>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let raw = RawConfig::try_parse_from(args)?;
        validate_identifier("cluster ID", &raw.cluster_id)?;
        validate_identifier("instance ID", &raw.instance_id)?;
        if !(1_000..=300_000).contains(&raw.max_lease_ttl_ms) {
            return Err(ConfigError::InvalidLeaseTtl(raw.max_lease_ttl_ms));
        }

        let identity = AgentIdentity {
            cluster_id: raw.cluster_id,
            shard_id: ShardId(raw.shard_id),
            instance_id: raw.instance_id,
        };
        let writable_setting_supplied = raw.cluster_uid.is_some()
            || raw.pod_uid.is_some()
            || raw.lease_namespace.is_some()
            || raw.writable_lease_uid.is_some()
            || raw.writable_lease_duration_seconds.is_some()
            || raw.writable_lease_renew_deadline_seconds.is_some()
            || raw.writable_lease_retry_ms.is_some()
            || raw.kubernetes_request_timeout_ms.is_some();
        let writable_lease = match raw.writable_lease_name {
            Some(lease_name) => {
                let cluster_uid = raw
                    .cluster_uid
                    .ok_or(ConfigError::IncompleteWritableLease)?;
                let lease_uid = raw
                    .writable_lease_uid
                    .ok_or(ConfigError::IncompleteWritableLease)?;
                let pod_uid = raw.pod_uid.ok_or(ConfigError::IncompleteWritableLease)?;
                let namespace = raw
                    .lease_namespace
                    .ok_or(ConfigError::IncompleteWritableLease)?;
                let lease_duration =
                    Duration::from_secs(raw.writable_lease_duration_seconds.unwrap_or(15));
                if duration_millis(lease_duration) > raw.max_lease_ttl_ms {
                    return Err(ConfigError::WritableLeaseExceedsAgentPolicy {
                        requested_ms: duration_millis(lease_duration),
                        maximum_ms: raw.max_lease_ttl_ms,
                    });
                }
                Some(
                    WritableLeaseConfig::new(
                        namespace,
                        lease_name,
                        identity.clone(),
                        cluster_uid,
                        lease_uid,
                        pod_uid,
                        lease_duration,
                        Duration::from_secs(
                            raw.writable_lease_renew_deadline_seconds.unwrap_or(10),
                        ),
                        Duration::from_millis(raw.writable_lease_retry_ms.unwrap_or(2_000)),
                        Duration::from_millis(raw.kubernetes_request_timeout_ms.unwrap_or(2_000)),
                    )
                    .map_err(|_| ConfigError::InvalidWritableLeaseSettings)?,
                )
            }
            None if writable_setting_supplied => {
                return Err(ConfigError::IncompleteWritableLease);
            }
            None => None,
        };

        let otlp_endpoint = raw
            .otlp_endpoint
            .map(|value| validate_otlp_endpoint(&value))
            .transpose()?;
        let postgres = match raw.postgres_mode {
            PostgresMode::Disabled => None,
            PostgresMode::Quarantine => {
                let data_dir = raw
                    .postgres_data_dir
                    .ok_or(ConfigError::PostgresDataDirectoryMissing)?;
                Some(PostgresConfig::new(
                    data_dir,
                    raw.postgres_bin,
                    raw.postgres_socket_dir,
                    raw.postgres_hba_file,
                    Duration::from_millis(raw.postgres_smart_shutdown_ms),
                    Duration::from_millis(raw.postgres_fast_shutdown_ms),
                    Duration::from_millis(raw.postgres_immediate_shutdown_ms),
                )?)
            }
        };
        validate_writable_postgres_pair(writable_lease.as_ref(), postgres.as_ref())?;

        Ok(Self {
            http_bind: raw.http_bind,
            identity,
            max_lease_ttl_ms: raw.max_lease_ttl_ms,
            writable_lease,
            telemetry: TelemetryConfig { otlp_endpoint },
            postgres,
        })
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn validate_writable_postgres_pair(
    writable_lease: Option<&WritableLeaseConfig>,
    postgres: Option<&PostgresConfig>,
) -> Result<(), ConfigError> {
    if writable_lease.is_some() && postgres.is_none() {
        return Err(ConfigError::WritableLeaseRequiresPostgres);
    }
    if let (Some(writable_lease), Some(postgres)) = (writable_lease, postgres) {
        let shutdown_margin = writable_lease.shutdown_margin();
        let target_fence_budget = postgres.target_fence_budget();
        if target_fence_budget >= shutdown_margin {
            return Err(ConfigError::WritableLeaseFenceMarginInsufficient {
                shutdown_margin_ms: duration_millis(shutdown_margin),
                target_fence_budget_ms: duration_millis(target_fence_budget),
            });
        }
    }
    Ok(())
}

fn validate_identifier(name: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty() || value.len() > 63 {
        return Err(ConfigError::InvalidIdentifier {
            name,
            value: value.to_owned(),
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ConfigError::InvalidIdentifier {
            name,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn validate_otlp_endpoint(value: &str) -> Result<Url, ConfigError> {
    if value.trim() != value {
        return Err(ConfigError::UnsafeOtlpEndpoint(value.to_owned()));
    }
    let endpoint = Url::parse(value).map_err(|source| ConfigError::InvalidOtlpEndpoint {
        value: value.to_owned(),
        source,
    })?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(ConfigError::UnsafeOtlpEndpoint(value.to_owned()));
    }
    Ok(endpoint)
}

/// Configuration parsing or validation failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Command-line parsing failed.
    #[error(transparent)]
    Arguments(#[from] clap::Error),
    /// An identifier is empty, too long, or contains unsafe characters.
    #[error("{name} {value:?} must be 1-63 ASCII letters, digits, '.', '_', or '-'")]
    InvalidIdentifier {
        /// Identifier field.
        name: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Lease TTL is outside the bounded safety range.
    #[error("maximum lease TTL {0} ms must be between 1000 and 300000 ms")]
    InvalidLeaseTtl(u64),
    /// Only part of the exact writable-term Lease identity was supplied.
    #[error(
        "writable-term Lease name, namespace, Lease UID, fleet UID, and Pod UID must be supplied together"
    )]
    IncompleteWritableLease,
    /// Writable-term Lease timing or identity is unsafe.
    #[error("writable-term Lease settings are invalid")]
    InvalidWritableLeaseSettings,
    /// The Kubernetes Lease can outlive the agent's accepted fencing policy.
    #[error("writable-term Lease duration {requested_ms} ms exceeds agent maximum {maximum_ms} ms")]
    WritableLeaseExceedsAgentPolicy {
        /// Requested Kubernetes Lease duration.
        requested_ms: u64,
        /// Agent's configured maximum accepted lease duration.
        maximum_ms: u64,
    },
    /// Lease ownership without the supervised postmaster has no safe runtime role.
    #[error("writable-term Lease coordination requires supervised PostgreSQL quarantine mode")]
    WritableLeaseRequiresPostgres,
    /// Target-side fencing cannot finish inside the reserved Lease margin.
    #[error(
        "writable-term Lease shutdown margin {shutdown_margin_ms} ms must exceed PostgreSQL target-fence budget {target_fence_budget_ms} ms"
    )]
    WritableLeaseFenceMarginInsufficient {
        /// Time between the renewal deadline and local Lease expiry.
        shutdown_margin_ms: u64,
        /// Configured immediate-stop and process-tree cleanup budget.
        target_fence_budget_ms: u64,
    },
    /// Endpoint URL parsing failed.
    #[error("invalid OTLP endpoint {value:?}: {source}")]
    InvalidOtlpEndpoint {
        /// Rejected value.
        value: String,
        /// URL parsing error.
        source: url::ParseError,
    },
    /// Endpoint is not an unauthenticated HTTP(S) URL.
    #[error("OTLP endpoint {0:?} must be an HTTP(S) URL without embedded credentials")]
    UnsafeOtlpEndpoint(String),
    /// Quarantine mode requires an explicit durable data directory.
    #[error("PGDATA is required when PostgreSQL quarantine supervision is enabled")]
    PostgresDataDirectoryMissing,
    /// `PostgreSQL` process configuration is unsafe or unbounded.
    #[error(transparent)]
    Postgres(#[from] PostgresConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_args() -> Vec<&'static str> {
        vec![
            "pgshard-agent",
            "--cluster-id",
            "cluster-1",
            "--shard-id",
            "3",
            "--instance-id",
            "cluster-1-shard-3-0",
        ]
    }

    #[test]
    fn accepts_required_identity() {
        let config = AgentConfig::try_parse_from(required_args()).expect("valid config");
        assert_eq!(config.identity.shard_id, ShardId(3));
        assert_eq!(config.max_lease_ttl_ms, 15_000);
        assert!(config.writable_lease.is_none());
        assert!(config.telemetry.otlp_endpoint.is_none());
        assert!(config.postgres.is_none());
    }

    #[test]
    fn rejects_unknown_arguments() {
        let mut args = required_args();
        args.push("--surprise");
        assert!(matches!(
            AgentConfig::try_parse_from(args),
            Err(ConfigError::Arguments(_))
        ));
    }

    #[test]
    fn rejects_unsafe_identity() {
        let mut args = required_args();
        let last = args.len() - 1;
        args[last] = "instance/with/slashes";
        assert!(matches!(
            AgentConfig::try_parse_from(args),
            Err(ConfigError::InvalidIdentifier { .. })
        ));
    }

    #[test]
    fn rejects_unbounded_lease_ttl() {
        let mut args = required_args();
        args.extend(["--max-lease-ttl-ms", "300001"]);
        assert!(matches!(
            AgentConfig::try_parse_from(args),
            Err(ConfigError::InvalidLeaseTtl(300_001))
        ));
    }

    #[test]
    fn accepts_exact_writable_lease_identity() {
        let mut args = required_args();
        args.extend([
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--lease-namespace",
            "database",
            "--writable-lease-name",
            "cluster-1-cell-0003-writable",
            "--writable-lease-uid",
            "99999999-8888-7777-6666-555555555555",
            "--postgres-mode",
            "quarantine",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
        ]);
        let config = AgentConfig::try_parse_from(args).expect("valid writable Lease config");
        assert!(config.writable_lease.is_some());
    }

    #[test]
    fn rejects_writable_lease_without_target_fence_margin() {
        let mut args = required_args();
        args.extend([
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--lease-namespace",
            "database",
            "--writable-lease-name",
            "cluster-1-cell-0003-writable",
            "--writable-lease-uid",
            "99999999-8888-7777-6666-555555555555",
            "--writable-lease-duration-seconds",
            "6",
            "--writable-lease-renew-deadline-seconds",
            "4",
            "--writable-lease-retry-ms",
            "100",
            "--kubernetes-request-timeout-ms",
            "100",
            "--postgres-mode",
            "quarantine",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
        ]);

        assert!(matches!(
            AgentConfig::try_parse_from(args),
            Err(ConfigError::WritableLeaseFenceMarginInsufficient {
                shutdown_margin_ms: 2_000,
                target_fence_budget_ms: 3_500,
            })
        ));
    }

    #[test]
    fn rejects_partial_or_overlong_writable_lease_authority() {
        let mut partial = required_args();
        partial.extend(["--writable-lease-name", "cluster-1-cell-0003-writable"]);
        assert!(matches!(
            AgentConfig::try_parse_from(partial),
            Err(ConfigError::IncompleteWritableLease)
        ));

        let mut timing_without_identity = required_args();
        timing_without_identity.extend(["--writable-lease-retry-ms", "1000"]);
        assert!(matches!(
            AgentConfig::try_parse_from(timing_without_identity),
            Err(ConfigError::IncompleteWritableLease)
        ));

        let mut complete_without_postgres = required_args();
        complete_without_postgres.extend([
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--lease-namespace",
            "database",
            "--writable-lease-name",
            "cluster-1-cell-0003-writable",
            "--writable-lease-uid",
            "99999999-8888-7777-6666-555555555555",
        ]);
        assert!(matches!(
            AgentConfig::try_parse_from(complete_without_postgres),
            Err(ConfigError::WritableLeaseRequiresPostgres)
        ));

        let mut overlong = required_args();
        overlong.extend([
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--lease-namespace",
            "database",
            "--writable-lease-name",
            "cluster-1-cell-0003-writable",
            "--writable-lease-uid",
            "99999999-8888-7777-6666-555555555555",
            "--max-lease-ttl-ms",
            "14000",
        ]);
        assert!(matches!(
            AgentConfig::try_parse_from(overlong),
            Err(ConfigError::WritableLeaseExceedsAgentPolicy {
                requested_ms: 15_000,
                maximum_ms: 14_000,
            })
        ));
    }

    #[test]
    fn rejects_otlp_credentials() {
        let mut args = required_args();
        args.extend([
            "--otlp-endpoint",
            "https://embedded:credential@collector:4317",
        ]);
        assert!(matches!(
            AgentConfig::try_parse_from(args),
            Err(ConfigError::UnsafeOtlpEndpoint(_))
        ));
    }

    #[test]
    fn rejects_otlp_query_fragment_and_whitespace() {
        for endpoint in [
            "https://collector:4317?token=value",
            "https://collector:4317/#fragment",
            " https://collector:4317",
        ] {
            let mut args = required_args();
            args.extend(["--otlp-endpoint", endpoint]);
            assert!(matches!(
                AgentConfig::try_parse_from(args),
                Err(ConfigError::UnsafeOtlpEndpoint(_))
            ));
        }
    }

    #[test]
    fn quarantine_mode_requires_pgdata_and_bounded_shutdown() {
        let mut missing = required_args();
        missing.extend(["--postgres-mode", "quarantine"]);
        assert!(matches!(
            AgentConfig::try_parse_from(missing),
            Err(ConfigError::PostgresDataDirectoryMissing)
        ));

        let mut configured = required_args();
        configured.extend([
            "--postgres-mode",
            "quarantine",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
            "--postgres-smart-shutdown-ms",
            "5000",
            "--postgres-fast-shutdown-ms",
            "40000",
            "--postgres-immediate-shutdown-ms",
            "5000",
        ]);
        let parsed = AgentConfig::try_parse_from(configured).expect("bounded quarantine config");
        assert_eq!(
            parsed.postgres.as_ref().map(PostgresConfig::data_dir),
            Some(std::path::Path::new("/var/lib/postgresql/data"))
        );

        let mut excessive = required_args();
        excessive.extend([
            "--postgres-mode",
            "quarantine",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
            "--postgres-smart-shutdown-ms",
            "10000",
            "--postgres-fast-shutdown-ms",
            "40000",
            "--postgres-immediate-shutdown-ms",
            "10000",
        ]);
        assert!(matches!(
            AgentConfig::try_parse_from(excessive),
            Err(ConfigError::Postgres(
                PostgresConfigError::ShutdownBudgetExceeded { .. }
            ))
        ));
    }
}
