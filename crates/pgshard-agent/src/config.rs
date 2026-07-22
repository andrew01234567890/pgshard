//! Strict command-line and environment configuration.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use pgshard_types::ShardId;
use thiserror::Error;
use url::Url;

use crate::catalog_activation_consumer::CatalogActivationConsumerConfig;
use crate::coordination::WritableLeaseConfig;
use crate::domain::{ActivationConfigEvidence, AgentIdentity};
use crate::postgres::{
    PostgresConfig, PostgresConfigError, PostgresRuntimeRole, PostgresServerTlsConfig,
    PostgresStandbyConfig,
};
use crate::postgres_generation::GenerationDurability;
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
    /// Optional fail-closed `PostgreSQL` process supervision.
    pub postgres: Option<PostgresConfig>,
    /// Optional exact non-serving activation configuration evidence.
    pub activation_config: Option<ActivationConfigEvidence>,
    /// Optional dormant catalog-activation consumer.
    pub catalog_activation_consumer: Option<CatalogActivationConsumerConfig>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum PostgresMode {
    #[default]
    Disabled,
    Quarantine,
    ReplicationBootstrapPrimary,
    ReplicationStandby,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum PostgresGenerationDurabilityMode {
    Local,
    RemoteApplyAnyOne,
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

    #[arg(long, env = "PGSHARD_CATALOG_ACTIVATION_CARRIER_NAMESPACE")]
    catalog_activation_carrier_namespace: Option<String>,

    #[arg(long, env = "PGSHARD_CATALOG_ACTIVATION_CARRIER_NAME")]
    catalog_activation_carrier_name: Option<String>,

    #[arg(long, env = "PGSHARD_CATALOG_ACTIVATION_CARRIER_UID")]
    catalog_activation_carrier_uid: Option<String>,

    #[arg(long, env = "PGSHARD_CATALOG_ACTIVATION_JOURNAL_ROOT")]
    catalog_activation_journal_root: Option<PathBuf>,

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

    #[arg(long, env = "PGSHARD_POSTGRES_PRIMARY_HOST")]
    postgres_primary_host: Option<String>,

    #[arg(long, env = "PGSHARD_POSTGRES_PRIMARY_PORT")]
    postgres_primary_port: Option<u16>,

    #[arg(long, env = "PGSHARD_POSTGRES_PRIMARY_SLOT_NAME")]
    postgres_primary_slot_name: Option<String>,

    #[arg(long, env = "PGSHARD_POSTGRES_PRIMARY_PASSFILE")]
    postgres_primary_passfile: Option<PathBuf>,

    #[arg(long, env = "PGSHARD_POSTGRES_PRIMARY_SSLROOTCERT")]
    postgres_primary_sslrootcert: Option<PathBuf>,

    #[arg(long, env = "PGSHARD_REPLICATION_TLS_CA_SHA256")]
    replication_tls_ca_sha256: Option<String>,

    #[arg(long, env = "PGSHARD_POSTGRES_SERVER_TLS_CERT")]
    postgres_server_tls_cert: Option<PathBuf>,

    #[arg(long, env = "PGSHARD_POSTGRES_SERVER_TLS_KEY")]
    postgres_server_tls_key: Option<PathBuf>,

    #[arg(long, env = "PGSHARD_REPLICATION_TLS_SERVER_SHA256")]
    replication_tls_server_sha256: Option<String>,

    #[arg(long, env = "PGSHARD_POSTGRES_GENERATION_DURABILITY", value_enum)]
    postgres_generation_durability: Option<PostgresGenerationDurabilityMode>,

    #[arg(long, env = "PGSHARD_POSTGRES_SYNCHRONOUS_STANDBY_NAMES")]
    postgres_synchronous_standby_names: Option<String>,

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

impl RawConfig {
    fn postgres_config(&self) -> Result<Option<PostgresConfig>, ConfigError> {
        let standby_setting_supplied = self.postgres_primary_host.is_some()
            || self.postgres_primary_port.is_some()
            || self.postgres_primary_slot_name.is_some()
            || self.postgres_primary_passfile.is_some()
            || self.postgres_primary_sslrootcert.is_some()
            || self.replication_tls_ca_sha256.is_some();
        let server_tls_setting_supplied = self.postgres_server_tls_cert.is_some()
            || self.postgres_server_tls_key.is_some()
            || self.replication_tls_server_sha256.is_some();
        let generation_setting_supplied = self.postgres_generation_durability.is_some()
            || self.postgres_synchronous_standby_names.is_some();
        let (role, standby, server_tls, generation_durability) = match self.postgres_mode {
            PostgresMode::Disabled => {
                if standby_setting_supplied {
                    return Err(ConfigError::ReplicationStandbySettingsRequireMode);
                }
                if server_tls_setting_supplied {
                    return Err(ConfigError::ServerTlsSettingsRequireBootstrapPrimary);
                }
                if generation_setting_supplied {
                    return Err(ConfigError::GenerationDurabilityRequiresBootstrapPrimary);
                }
                return Ok(None);
            }
            PostgresMode::Quarantine => (
                PostgresRuntimeRole::Quarantine,
                None,
                None,
                GenerationDurability::Local,
            ),
            PostgresMode::ReplicationBootstrapPrimary => {
                let mode = self
                    .postgres_generation_durability
                    .ok_or(ConfigError::GenerationDurabilityRequired)?;
                let durability = match mode {
                    PostgresGenerationDurabilityMode::Local => {
                        if self.postgres_synchronous_standby_names.is_some() {
                            return Err(ConfigError::SynchronousStandbyNamesRequireRemoteApply);
                        }
                        GenerationDurability::Local
                    }
                    PostgresGenerationDurabilityMode::RemoteApplyAnyOne => {
                        let names = self
                            .postgres_synchronous_standby_names
                            .as_deref()
                            .ok_or(ConfigError::SynchronousStandbyNamesRequired)?;
                        GenerationDurability::remote_apply_any_one(
                            names.split(',').map(str::to_owned).collect(),
                        )
                        .map_err(|_| ConfigError::InvalidSynchronousStandbySet)?
                    }
                };
                (
                    PostgresRuntimeRole::ReplicationBootstrapPrimary,
                    None,
                    Some(self.bootstrap_server_tls()?),
                    durability,
                )
            }
            PostgresMode::ReplicationStandby => (
                PostgresRuntimeRole::ReplicationStandby,
                Some(self.replication_standby()?),
                None,
                GenerationDurability::Local,
            ),
        };
        if standby_setting_supplied && standby.is_none() {
            return Err(ConfigError::ReplicationStandbySettingsRequireMode);
        }
        if server_tls_setting_supplied && server_tls.is_none() {
            return Err(ConfigError::ServerTlsSettingsRequireBootstrapPrimary);
        }
        if role != PostgresRuntimeRole::ReplicationBootstrapPrimary && generation_setting_supplied {
            return Err(ConfigError::GenerationDurabilityRequiresBootstrapPrimary);
        }
        let data_dir = self
            .postgres_data_dir
            .clone()
            .ok_or(ConfigError::PostgresDataDirectoryMissing)?;
        PostgresConfig::new_for_role(
            role,
            standby,
            server_tls,
            generation_durability,
            data_dir,
            self.postgres_bin.clone(),
            self.postgres_socket_dir.clone(),
            self.postgres_hba_file.clone(),
            Duration::from_millis(self.postgres_smart_shutdown_ms),
            Duration::from_millis(self.postgres_fast_shutdown_ms),
            Duration::from_millis(self.postgres_immediate_shutdown_ms),
        )
        .map(Some)
        .map_err(ConfigError::from)
    }

    fn bootstrap_server_tls(&self) -> Result<PostgresServerTlsConfig, ConfigError> {
        let cert_file = self
            .postgres_server_tls_cert
            .clone()
            .ok_or(ConfigError::IncompleteServerTlsSettings)?;
        let key_file = self
            .postgres_server_tls_key
            .clone()
            .ok_or(ConfigError::IncompleteServerTlsSettings)?;
        let material_sha256 = self
            .replication_tls_server_sha256
            .clone()
            .ok_or(ConfigError::IncompleteServerTlsSettings)?;
        PostgresServerTlsConfig::new(cert_file, key_file, material_sha256)
            .map_err(ConfigError::from)
    }

    fn replication_standby(&self) -> Result<PostgresStandbyConfig, ConfigError> {
        let primary_host = self
            .postgres_primary_host
            .clone()
            .ok_or(ConfigError::IncompleteReplicationStandbySettings)?;
        let primary_slot_name = self
            .postgres_primary_slot_name
            .clone()
            .ok_or(ConfigError::IncompleteReplicationStandbySettings)?;
        let primary_passfile = self
            .postgres_primary_passfile
            .clone()
            .ok_or(ConfigError::IncompleteReplicationStandbySettings)?;
        let primary_sslrootcert = self
            .postgres_primary_sslrootcert
            .clone()
            .ok_or(ConfigError::IncompleteReplicationStandbySettings)?;
        let replication_tls_ca_sha256 = self
            .replication_tls_ca_sha256
            .clone()
            .ok_or(ConfigError::IncompleteReplicationStandbySettings)?;
        PostgresStandbyConfig::new(
            primary_host,
            self.postgres_primary_port.unwrap_or(5432),
            primary_slot_name,
            primary_passfile,
            primary_sslrootcert,
            replication_tls_ca_sha256,
        )
        .map_err(ConfigError::from)
    }
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
        let postgres = raw.postgres_config()?;

        let identity = AgentIdentity {
            cluster_id: raw.cluster_id,
            shard_id: ShardId(raw.shard_id),
            instance_id: raw.instance_id,
        };
        let activation_cluster_uid = raw.cluster_uid.clone();
        let activation_pod_uid = raw.pod_uid.clone();
        let activation_lease_namespace = raw.lease_namespace.clone();
        let activation_request_timeout = raw.kubernetes_request_timeout_ms;
        let writable_setting_supplied = raw.lease_namespace.is_some()
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

        let activation_config = build_activation_config(
            &identity,
            postgres.as_ref(),
            writable_lease.as_ref(),
            activation_cluster_uid.clone(),
            activation_pod_uid.clone(),
        )?;
        let catalog_activation_consumer = build_catalog_activation_consumer(
            &identity,
            postgres.as_ref(),
            activation_cluster_uid.as_deref(),
            activation_pod_uid.as_deref(),
            activation_lease_namespace.as_deref(),
            activation_request_timeout,
            raw.catalog_activation_carrier_namespace,
            raw.catalog_activation_carrier_name,
            raw.catalog_activation_carrier_uid,
            raw.catalog_activation_journal_root,
        )?;

        let otlp_endpoint = raw
            .otlp_endpoint
            .as_deref()
            .map(validate_otlp_endpoint)
            .transpose()?;
        validate_writable_postgres_pair(writable_lease.as_ref(), postgres.as_ref())?;

        Ok(Self {
            http_bind: raw.http_bind,
            identity,
            max_lease_ttl_ms: raw.max_lease_ttl_ms,
            writable_lease,
            telemetry: TelemetryConfig { otlp_endpoint },
            postgres,
            activation_config,
            catalog_activation_consumer,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn build_catalog_activation_consumer(
    identity: &AgentIdentity,
    postgres: Option<&PostgresConfig>,
    cluster_uid: Option<&str>,
    pod_uid: Option<&str>,
    lease_namespace: Option<&str>,
    request_timeout_ms: Option<u64>,
    carrier_namespace: Option<String>,
    carrier_name: Option<String>,
    carrier_uid: Option<String>,
    journal_root: Option<PathBuf>,
) -> Result<Option<CatalogActivationConsumerConfig>, ConfigError> {
    let supplied = carrier_namespace.is_some()
        || carrier_name.is_some()
        || carrier_uid.is_some()
        || journal_root.is_some();
    if !supplied {
        return Ok(None);
    }
    let (Some(carrier_namespace), Some(carrier_name), Some(carrier_uid), Some(journal_root)) =
        (carrier_namespace, carrier_name, carrier_uid, journal_root)
    else {
        return Err(ConfigError::IncompleteCatalogActivationConsumer);
    };
    if identity.shard_id != ShardId(0)
        || !postgres.is_some_and(PostgresConfig::requires_writable_authority)
    {
        return Err(ConfigError::InvalidCatalogActivationConsumerRole);
    }
    let cluster_uid = cluster_uid.ok_or(ConfigError::IncompleteCatalogActivationConsumer)?;
    let pod_uid = pod_uid.ok_or(ConfigError::IncompleteCatalogActivationConsumer)?;
    let lease_namespace =
        lease_namespace.ok_or(ConfigError::IncompleteCatalogActivationConsumer)?;
    if carrier_namespace != lease_namespace {
        return Err(ConfigError::InvalidCatalogActivationConsumerIdentity);
    }
    CatalogActivationConsumerConfig::new(
        identity.clone(),
        cluster_uid.to_owned(),
        pod_uid.to_owned(),
        carrier_namespace,
        carrier_name,
        carrier_uid,
        journal_root,
        Duration::from_millis(request_timeout_ms.unwrap_or(2_000)),
    )
    .map(Some)
    .map_err(|_| ConfigError::InvalidCatalogActivationConsumerIdentity)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn build_activation_config(
    identity: &AgentIdentity,
    postgres: Option<&PostgresConfig>,
    writable_lease: Option<&WritableLeaseConfig>,
    cluster_uid: Option<String>,
    pod_uid: Option<String>,
) -> Result<Option<ActivationConfigEvidence>, ConfigError> {
    if let Some(writable_lease) = writable_lease {
        let postgres = postgres.ok_or(ConfigError::WritableLeaseRequiresPostgres)?;
        return Ok(postgres.requires_writable_authority().then(|| {
            writable_lease.activation_config(
                postgres.generation_durability().evidence(),
                duration_millis(postgres.target_fence_budget()),
            )
        }));
    }
    if postgres.is_some_and(PostgresConfig::is_replication_standby) {
        return match (cluster_uid, pod_uid) {
            (Some(cluster_uid), Some(pod_uid))
                if activation_uid(&cluster_uid) && activation_uid(&pod_uid) =>
            {
                Ok(postgres.and_then(|postgres| {
                    postgres.standby_activation_config(identity.clone(), cluster_uid, pod_uid)
                }))
            }
            (Some(_), Some(_)) => Err(ConfigError::InvalidActivationIdentity),
            (None, None) => Ok(None),
            _ => Err(ConfigError::IncompleteActivationIdentity),
        };
    }
    if cluster_uid.is_some() || pod_uid.is_some() {
        return Err(ConfigError::IncompleteWritableLease);
    }
    Ok(None)
}

fn validate_writable_postgres_pair(
    writable_lease: Option<&WritableLeaseConfig>,
    postgres: Option<&PostgresConfig>,
) -> Result<(), ConfigError> {
    if writable_lease.is_some() && postgres.is_none() {
        return Err(ConfigError::WritableLeaseRequiresPostgres);
    }
    if writable_lease.is_some() && postgres.is_some_and(PostgresConfig::forbids_writable_authority)
    {
        return Err(ConfigError::ReplicationStandbyForbidsWritableLease);
    }
    if postgres.is_some_and(PostgresConfig::requires_writable_authority) && writable_lease.is_none()
    {
        return Err(ConfigError::ReplicationBootstrapPrimaryRequiresWritableLease);
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

fn activation_uid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
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
    #[error("writable-term Lease coordination requires supervised PostgreSQL")]
    WritableLeaseRequiresPostgres,
    /// Replication-bootstrap-primary TCP must never start without exact writable authority.
    #[error(
        "PostgreSQL replication-bootstrap-primary mode requires writable-term Lease coordination"
    )]
    ReplicationBootstrapPrimaryRequiresWritableLease,
    /// Physical standbys must never hold writable-term authority.
    #[error("PostgreSQL replication-standby mode forbids writable-term Lease coordination")]
    ReplicationStandbyForbidsWritableLease,
    /// Standby connection settings are valid only for the standby runtime role.
    #[error("PostgreSQL primary settings require replication-standby mode")]
    ReplicationStandbySettingsRequireMode,
    /// A standby requires one complete upstream identity and credential path.
    #[error("PostgreSQL replication-standby settings are incomplete")]
    IncompleteReplicationStandbySettings,
    /// Server TLS settings are valid only for the bootstrap-source role.
    #[error("PostgreSQL server TLS settings require replication-bootstrap-primary mode")]
    ServerTlsSettingsRequireBootstrapPrimary,
    /// The bootstrap source requires one complete verified server TLS identity.
    #[error("PostgreSQL replication-bootstrap-primary server TLS settings are incomplete")]
    IncompleteServerTlsSettings,
    /// Generation durability is source-only and must not silently change other roles.
    #[error("PostgreSQL generation durability settings require replication-bootstrap-primary mode")]
    GenerationDurabilityRequiresBootstrapPrimary,
    /// A replication bootstrap source must explicitly choose its publication durability.
    #[error(
        "PostgreSQL replication-bootstrap-primary mode requires explicit generation durability"
    )]
    GenerationDurabilityRequired,
    /// Remote-apply publication requires the complete managed candidate set.
    #[error("remote-apply-any-one generation durability requires synchronous standby names")]
    SynchronousStandbyNamesRequired,
    /// Candidate names have no meaning for locally durable publication.
    #[error("PostgreSQL synchronous standby names require remote-apply-any-one durability")]
    SynchronousStandbyNamesRequireRemoteApply,
    /// Candidate names must be one exact complete supported managed topology.
    #[error(
        "PostgreSQL synchronous standby names must be the exact sorted member 1..2 or 1..4 CSV"
    )]
    InvalidSynchronousStandbySet,
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
    /// Standby activation evidence requires both exact Kubernetes object UIDs.
    #[error("PostgreSQL standby activation evidence requires both cluster and Pod UIDs")]
    IncompleteActivationIdentity,
    /// One activation identity is not a bounded canonical Kubernetes UID.
    #[error("PostgreSQL activation cluster and Pod UIDs must be 1-128 safe ASCII characters")]
    InvalidActivationIdentity,
    /// Only part of the dormant carrier consumer identity was supplied.
    #[error(
        "catalog-activation carrier namespace, name, UID, and journal root must be supplied together"
    )]
    IncompleteCatalogActivationConsumer,
    /// The dormant consumer may run only on the shard-zero bootstrap source.
    #[error("catalog-activation consumer requires shard zero replication-bootstrap-primary mode")]
    InvalidCatalogActivationConsumerRole,
    /// Carrier, cluster, Pod, or journal identity is unsafe or inconsistent.
    #[error("catalog-activation consumer identity or journal root is invalid")]
    InvalidCatalogActivationConsumerIdentity,
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
    /// Every supervised `PostgreSQL` role requires an explicit durable data directory.
    #[error("PGDATA is required when PostgreSQL supervision is enabled")]
    PostgresDataDirectoryMissing,
    /// `PostgreSQL` process configuration is unsafe or unbounded.
    #[error(transparent)]
    Postgres(#[from] PostgresConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERVER_TLS_SHA256_FIXTURE: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REPLICATION_CA_SHA256_FIXTURE: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

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

    fn replication_standby_args() -> Vec<&'static str> {
        let mut args = required_args();
        args.extend([
            "--postgres-mode",
            "replication-standby",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
            "--postgres-primary-host",
            "cluster-1-shard-0003-member-0000.database.svc",
            "--postgres-primary-slot-name",
            "pgshard_member_0001",
            "--postgres-primary-passfile",
            "/etc/pgshard/replication/passfile",
            "--postgres-primary-sslrootcert",
            "/run/pgshard/standby-auth/ca.crt",
            "--replication-tls-ca-sha256",
            REPLICATION_CA_SHA256_FIXTURE,
        ]);
        args
    }

    fn server_tls_args() -> [&'static str; 6] {
        [
            "--postgres-server-tls-cert",
            "/run/pgshard/server-tls/tls.crt",
            "--postgres-server-tls-key",
            "/run/pgshard/server-tls/tls.key",
            "--replication-tls-server-sha256",
            SERVER_TLS_SHA256_FIXTURE,
        ]
    }

    fn replication_bootstrap_primary_args() -> Vec<&'static str> {
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
            "replication-bootstrap-primary",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
        ]);
        args.extend(server_tls_args());
        args
    }

    fn catalog_activation_consumer_args() -> Vec<&'static str> {
        vec![
            "pgshard-agent",
            "--cluster-id",
            "cluster-1",
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--shard-id",
            "0",
            "--instance-id",
            "cluster-1-shard-0000-0",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--lease-namespace",
            "database",
            "--writable-lease-name",
            "cluster-1-shard-0000-term",
            "--writable-lease-uid",
            "99999999-8888-7777-6666-555555555555",
            "--postgres-mode",
            "replication-bootstrap-primary",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
            "--postgres-generation-durability",
            "local",
            "--postgres-server-tls-cert",
            "/run/pgshard/server-tls/tls.crt",
            "--postgres-server-tls-key",
            "/run/pgshard/server-tls/tls.key",
            "--replication-tls-server-sha256",
            SERVER_TLS_SHA256_FIXTURE,
            "--catalog-activation-carrier-namespace",
            "database",
            "--catalog-activation-carrier-name",
            "cluster-1-catalog-activation",
            "--catalog-activation-carrier-uid",
            "12121212-3434-5656-7878-909090909090",
            "--catalog-activation-journal-root",
            "/var/lib/pgshard/catalog-activation",
        ]
    }

    fn expected_replication_standby(primary_port: u16) -> PostgresConfig {
        let standby = PostgresStandbyConfig::new(
            "cluster-1-shard-0003-member-0000.database.svc".to_owned(),
            primary_port,
            "pgshard_member_0001".to_owned(),
            PathBuf::from("/etc/pgshard/replication/passfile"),
            PathBuf::from("/run/pgshard/standby-auth/ca.crt"),
            REPLICATION_CA_SHA256_FIXTURE.to_owned(),
        )
        .expect("valid standby identity");
        PostgresConfig::new_replication_standby(
            standby,
            PathBuf::from("/var/lib/postgresql/data"),
            PathBuf::from("/usr/lib/postgresql/18/bin/postgres"),
            PathBuf::from("/run/pgshard/postgres"),
            PathBuf::from("/etc/pgshard/quarantine.pg_hba.conf"),
            Duration::from_secs(5),
            Duration::from_secs(44),
            Duration::from_millis(500),
        )
        .expect("valid standby process configuration")
    }

    #[test]
    fn accepts_required_identity() {
        let config = AgentConfig::try_parse_from(required_args()).expect("valid config");
        assert_eq!(config.identity.shard_id, ShardId(3));
        assert_eq!(config.max_lease_ttl_ms, 15_000);
        assert!(config.writable_lease.is_none());
        assert!(config.telemetry.otlp_endpoint.is_none());
        assert!(config.postgres.is_none());
        assert!(config.activation_config.is_none());
        assert!(config.catalog_activation_consumer.is_none());
    }

    #[test]
    fn catalog_activation_consumer_is_all_or_none_and_source_only() {
        let config = AgentConfig::try_parse_from(catalog_activation_consumer_args())
            .expect("complete shard-zero source consumer");
        assert!(config.catalog_activation_consumer.is_some());

        let mut partial = catalog_activation_consumer_args();
        partial.truncate(partial.len() - 2);
        assert!(matches!(
            AgentConfig::try_parse_from(partial),
            Err(ConfigError::IncompleteCatalogActivationConsumer)
        ));

        let mut foreign_namespace = catalog_activation_consumer_args();
        let namespace = foreign_namespace
            .iter()
            .position(|argument| *argument == "--catalog-activation-carrier-namespace")
            .expect("carrier namespace argument");
        foreign_namespace[namespace + 1] = "other";
        assert!(matches!(
            AgentConfig::try_parse_from(foreign_namespace),
            Err(ConfigError::InvalidCatalogActivationConsumerIdentity)
        ));

        let mut nonzero_shard = catalog_activation_consumer_args();
        let shard = nonzero_shard
            .iter()
            .position(|argument| *argument == "--shard-id")
            .expect("shard argument");
        nonzero_shard[shard + 1] = "1";
        assert!(matches!(
            AgentConfig::try_parse_from(nonzero_shard),
            Err(ConfigError::InvalidCatalogActivationConsumerRole)
        ));
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
        assert!(config.activation_config.is_none());
    }

    #[test]
    fn replication_bootstrap_primary_requires_exact_writable_lease_identity() {
        let mut unleased = required_args();
        unleased.extend([
            "--postgres-mode",
            "replication-bootstrap-primary",
            "--postgres-generation-durability",
            "local",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
        ]);
        unleased.extend(server_tls_args());
        assert!(matches!(
            AgentConfig::try_parse_from(unleased),
            Err(ConfigError::ReplicationBootstrapPrimaryRequiresWritableLease)
        ));

        let mut leased = required_args();
        leased.extend([
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
            "replication-bootstrap-primary",
            "--postgres-generation-durability",
            "local",
            "--postgres-data-dir",
            "/var/lib/postgresql/data",
        ]);
        leased.extend(server_tls_args());
        let config = AgentConfig::try_parse_from(leased).expect("leased replication primary");
        assert!(config.writable_lease.is_some());
        assert!(
            config
                .postgres
                .as_ref()
                .is_some_and(PostgresConfig::requires_writable_authority)
        );
        let activation = config
            .activation_config
            .expect("source activation identity");
        assert_eq!(activation.identity, config.identity);
        assert_eq!(
            activation.cluster_uid,
            "11111111-2222-3333-4444-555555555555"
        );
        assert_eq!(activation.pod_uid, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        assert!(matches!(
            activation.postgres,
            crate::domain::ActivationPostgresConfig::Source {
                lease_namespace,
                lease_name,
                lease_uid,
                durability: crate::domain::GenerationDurabilityEvidence::Local,
                target_fence_required_margin_ms: 3_500,
            } if lease_namespace == "database"
                && lease_name == "cluster-1-cell-0003-writable"
                && lease_uid == "99999999-8888-7777-6666-555555555555"
        ));
    }

    #[test]
    fn replication_bootstrap_primary_requires_explicit_generation_durability() {
        assert!(matches!(
            AgentConfig::try_parse_from(replication_bootstrap_primary_args()),
            Err(ConfigError::GenerationDurabilityRequired)
        ));

        let mut local = replication_bootstrap_primary_args();
        local.extend(["--postgres-generation-durability", "local"]);
        let config = AgentConfig::try_parse_from(local).expect("explicit local durability");
        assert_eq!(
            config
                .postgres
                .as_ref()
                .expect("PostgreSQL source")
                .generation_durability(),
            &GenerationDurability::Local
        );
    }

    #[test]
    fn accepts_only_complete_remote_apply_candidate_sets() {
        for names in [
            "pgshard_member_0001,pgshard_member_0002",
            "pgshard_member_0001,pgshard_member_0002,pgshard_member_0003,pgshard_member_0004",
        ] {
            let mut args = replication_bootstrap_primary_args();
            args.extend([
                "--postgres-generation-durability",
                "remote-apply-any-one",
                "--postgres-synchronous-standby-names",
                names,
            ]);
            let config = AgentConfig::try_parse_from(args).expect("complete remote topology");
            assert_eq!(
                config
                    .postgres
                    .as_ref()
                    .expect("PostgreSQL source")
                    .generation_durability()
                    .synchronous_standby_names_setting(),
                format!("ANY 1 ({})", names.replace(',', ", "))
            );
        }

        for names in [
            "",
            "pgshard_member_0001",
            "pgshard_member_0002,pgshard_member_0001",
            "pgshard_member_0001,pgshard_member_0001",
            "pgshard_member_0001,pgshard_member_0003",
            "pgshard_member_0001, pgshard_member_0002",
        ] {
            let mut args = replication_bootstrap_primary_args();
            args.extend([
                "--postgres-generation-durability",
                "remote-apply-any-one",
                "--postgres-synchronous-standby-names",
                names,
            ]);
            assert!(matches!(
                AgentConfig::try_parse_from(args),
                Err(ConfigError::InvalidSynchronousStandbySet)
            ));
        }
    }

    #[test]
    fn generation_durability_settings_are_source_only_and_consistent() {
        let mut local_with_names = replication_bootstrap_primary_args();
        local_with_names.extend([
            "--postgres-generation-durability",
            "local",
            "--postgres-synchronous-standby-names",
            "pgshard_member_0001,pgshard_member_0002",
        ]);
        assert!(matches!(
            AgentConfig::try_parse_from(local_with_names),
            Err(ConfigError::SynchronousStandbyNamesRequireRemoteApply)
        ));

        let mut remote_without_names = replication_bootstrap_primary_args();
        remote_without_names.extend(["--postgres-generation-durability", "remote-apply-any-one"]);
        assert!(matches!(
            AgentConfig::try_parse_from(remote_without_names),
            Err(ConfigError::SynchronousStandbyNamesRequired)
        ));

        for mode in ["quarantine", "replication-standby"] {
            let mut args = if mode == "replication-standby" {
                replication_standby_args()
            } else {
                let mut args = required_args();
                args.extend([
                    "--postgres-mode",
                    "quarantine",
                    "--postgres-data-dir",
                    "/var/lib/postgresql/data",
                ]);
                args
            };
            args.extend(["--postgres-generation-durability", "local"]);
            assert!(matches!(
                AgentConfig::try_parse_from(args),
                Err(ConfigError::GenerationDurabilityRequiresBootstrapPrimary)
            ));
        }
    }

    #[test]
    fn server_tls_settings_are_bootstrap_primary_only_and_complete() {
        let mut with_durability = replication_bootstrap_primary_args();
        with_durability.extend(["--postgres-generation-durability", "local"]);
        for index in 0..3 {
            let mut incomplete = with_durability.clone();
            let flag = incomplete
                .iter()
                .position(|argument| *argument == server_tls_args()[index * 2])
                .expect("required server TLS setting");
            incomplete.drain(flag..=flag + 1);
            assert!(matches!(
                AgentConfig::try_parse_from(incomplete),
                Err(ConfigError::IncompleteServerTlsSettings)
            ));
        }

        let mut malformed_digest = with_durability;
        let digest = malformed_digest
            .iter()
            .position(|argument| *argument == "--replication-tls-server-sha256")
            .expect("server TLS digest setting");
        malformed_digest[digest + 1] = "UPPERCASE";
        assert!(matches!(
            AgentConfig::try_parse_from(malformed_digest),
            Err(ConfigError::Postgres(
                PostgresConfigError::InvalidMaterialDigest { .. }
            ))
        ));

        for mode in [None, Some("quarantine"), Some("replication-standby")] {
            let mut args = match mode {
                None => required_args(),
                Some("replication-standby") => replication_standby_args(),
                Some(mode) => {
                    let mut args = required_args();
                    args.extend([
                        "--postgres-mode",
                        mode,
                        "--postgres-data-dir",
                        "/var/lib/postgresql/data",
                    ]);
                    args
                }
            };
            args.extend(server_tls_args());
            assert!(matches!(
                AgentConfig::try_parse_from(args),
                Err(ConfigError::ServerTlsSettingsRequireBootstrapPrimary)
            ));
        }
    }

    #[test]
    fn accepts_complete_replication_standby_settings() {
        let mut args = replication_standby_args();
        args.extend(["--postgres-primary-port", "6432"]);

        let config = AgentConfig::try_parse_from(args).expect("complete replication standby");
        assert_eq!(config.postgres, Some(expected_replication_standby(6432)));
        assert!(config.writable_lease.is_none());
    }

    #[test]
    fn replication_standby_defaults_primary_port() {
        let config = AgentConfig::try_parse_from(replication_standby_args())
            .expect("replication standby with default port");
        assert_eq!(config.postgres, Some(expected_replication_standby(5432)));
        assert!(config.activation_config.is_none());
    }

    #[test]
    fn standby_activation_identity_is_exact_complete_and_optional() {
        let mut args = replication_standby_args();
        args.extend([
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        ]);
        let config = AgentConfig::try_parse_from(args).expect("standby activation identity");
        let activation = config.activation_config.expect("standby activation config");
        assert_eq!(activation.identity, config.identity);
        assert!(matches!(
            activation.postgres,
            crate::domain::ActivationPostgresConfig::Standby {
                primary_host,
                primary_port: 5432,
                member_slot_name,
            } if primary_host == "cluster-1-shard-0003-member-0000.database.svc"
                && member_slot_name == "pgshard_member_0001"
        ));

        for setting in ["--cluster-uid", "--pod-uid"] {
            let mut partial = replication_standby_args();
            partial.extend([setting, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"]);
            assert!(matches!(
                AgentConfig::try_parse_from(partial),
                Err(ConfigError::IncompleteActivationIdentity)
            ));
        }

        let mut unsafe_uid = replication_standby_args();
        unsafe_uid.extend([
            "--cluster-uid",
            "cluster/uid",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        ]);
        assert!(matches!(
            AgentConfig::try_parse_from(unsafe_uid),
            Err(ConfigError::InvalidActivationIdentity)
        ));
    }

    #[test]
    fn rejects_incomplete_replication_standby_settings() {
        for missing in [
            "--postgres-primary-host",
            "--postgres-primary-slot-name",
            "--postgres-primary-passfile",
            "--postgres-primary-sslrootcert",
            "--replication-tls-ca-sha256",
        ] {
            let mut args = replication_standby_args();
            let index = args
                .iter()
                .position(|argument| *argument == missing)
                .expect("required standby setting");
            args.drain(index..=index + 1);
            assert!(matches!(
                AgentConfig::try_parse_from(args),
                Err(ConfigError::IncompleteReplicationStandbySettings)
            ));
        }
    }

    #[test]
    fn rejects_replication_standby_settings_in_other_modes() {
        for setting in [
            ["--postgres-primary-host", "primary.database.svc"],
            ["--postgres-primary-port", "5432"],
            ["--postgres-primary-slot-name", "pgshard_member_0001"],
            [
                "--postgres-primary-passfile",
                "/etc/pgshard/replication/passfile",
            ],
            [
                "--postgres-primary-sslrootcert",
                "/run/pgshard/standby-auth/ca.crt",
            ],
            ["--replication-tls-ca-sha256", REPLICATION_CA_SHA256_FIXTURE],
        ] {
            let mut args = required_args();
            args.extend(setting);
            assert!(matches!(
                AgentConfig::try_parse_from(args),
                Err(ConfigError::ReplicationStandbySettingsRequireMode)
            ));
        }

        let mut quarantine = replication_standby_args();
        let mode = quarantine
            .iter()
            .position(|argument| *argument == "replication-standby")
            .expect("standby mode");
        quarantine[mode] = "quarantine";
        assert!(matches!(
            AgentConfig::try_parse_from(quarantine),
            Err(ConfigError::ReplicationStandbySettingsRequireMode)
        ));
    }

    #[test]
    fn replication_standby_forbids_writable_lease_before_fence_margin() {
        let mut args = replication_standby_args();
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
        ]);

        assert!(matches!(
            AgentConfig::try_parse_from(args),
            Err(ConfigError::ReplicationStandbyForbidsWritableLease)
        ));
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
