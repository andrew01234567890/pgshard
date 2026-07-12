//! Strict command-line and environment configuration.

use std::ffi::OsString;
use std::net::SocketAddr;

use clap::Parser;
use thiserror::Error;
use url::Url;

use crate::domain::OrchestratorIdentity;
use crate::telemetry::TelemetryConfig;

/// Validated orchestrator configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrchConfig {
    /// Health, readiness, status, and metrics bind address.
    pub http_bind: SocketAddr,
    /// Stable orchestrator identity.
    pub identity: OrchestratorIdentity,
    /// Default requested lease duration.
    pub lease_ttl_ms: u64,
    /// OpenTelemetry configuration placeholder.
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Parser)]
#[command(name = "pgshard-orch", disable_help_subcommand = true)]
struct RawConfig {
    #[arg(long, env = "PGSHARD_HTTP_BIND", default_value = "0.0.0.0:8080")]
    http_bind: SocketAddr,

    #[arg(long, env = "PGSHARD_CLUSTER_ID")]
    cluster_id: String,

    #[arg(long, env = "PGSHARD_ORCH_ID")]
    orchestrator_id: String,

    #[arg(long, env = "PGSHARD_LEASE_TTL_MS", default_value_t = 15_000)]
    lease_ttl_ms: u64,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,
}

impl OrchConfig {
    /// Parses process arguments and environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown arguments or invalid values.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::try_parse_from(std::env::args_os())
    }

    /// Parses a supplied argument iterator for deterministic tests.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown arguments or invalid values.
    pub fn try_parse_from<I, T>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let raw = RawConfig::try_parse_from(args)?;
        validate_identifier("cluster ID", &raw.cluster_id)?;
        validate_identifier("orchestrator ID", &raw.orchestrator_id)?;
        if !(1_000..=300_000).contains(&raw.lease_ttl_ms) {
            return Err(ConfigError::InvalidLeaseTtl(raw.lease_ttl_ms));
        }
        let otlp_endpoint = raw
            .otlp_endpoint
            .map(|value| validate_otlp_endpoint(&value))
            .transpose()?;

        Ok(Self {
            http_bind: raw.http_bind,
            identity: OrchestratorIdentity {
                cluster_id: raw.cluster_id,
                orchestrator_id: raw.orchestrator_id,
            },
            lease_ttl_ms: raw.lease_ttl_ms,
            telemetry: TelemetryConfig { otlp_endpoint },
        })
    }
}

fn validate_identifier(name: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 63
        || !value
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
    let endpoint = Url::parse(value).map_err(|source| ConfigError::InvalidOtlpEndpoint {
        value: value.to_owned(),
        source,
    })?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
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
    #[error("lease TTL {0} ms must be between 1000 and 300000 ms")]
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

    fn args() -> Vec<&'static str> {
        vec![
            "pgshard-orch",
            "--cluster-id",
            "cluster-1",
            "--orchestrator-id",
            "orch-0",
        ]
    }

    #[test]
    fn accepts_bounded_default_ttl() {
        let config = OrchConfig::try_parse_from(args()).expect("valid config");
        assert_eq!(config.lease_ttl_ms, 15_000);
    }

    #[test]
    fn rejects_dangerously_short_ttl() {
        let mut args = args();
        args.extend(["--lease-ttl-ms", "10"]);
        assert!(matches!(
            OrchConfig::try_parse_from(args),
            Err(ConfigError::InvalidLeaseTtl(10))
        ));
    }

    #[test]
    fn rejects_unknown_arguments() {
        let mut args = args();
        args.push("--unsafe-promote");
        assert!(matches!(
            OrchConfig::try_parse_from(args),
            Err(ConfigError::Arguments(_))
        ));
    }
}
