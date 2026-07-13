//! Strict command-line and environment configuration.

use std::ffi::OsString;
use std::net::SocketAddr;

use clap::Parser;
use pgshard_types::ShardId;
use thiserror::Error;
use url::Url;

use crate::domain::AgentIdentity;
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
    /// OpenTelemetry configuration placeholder.
    pub telemetry: TelemetryConfig,
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

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,
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

        let otlp_endpoint = raw
            .otlp_endpoint
            .map(|value| validate_otlp_endpoint(&value))
            .transpose()?;

        Ok(Self {
            http_bind: raw.http_bind,
            identity: AgentIdentity {
                cluster_id: raw.cluster_id,
                shard_id: ShardId(raw.shard_id),
                instance_id: raw.instance_id,
            },
            max_lease_ttl_ms: raw.max_lease_ttl_ms,
            telemetry: TelemetryConfig { otlp_endpoint },
        })
    }
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
        assert!(config.telemetry.otlp_endpoint.is_none());
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
}
