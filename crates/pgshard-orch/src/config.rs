//! Strict command-line and environment configuration.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::time::Duration;

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
    /// Immutable operator-assigned logical cluster incarnation.
    pub cluster_uid: String,
    /// Default requested lease duration.
    pub lease_ttl_ms: u64,
    /// Ordered, bounded etcd v3 HTTP gateway endpoints.
    pub etcd_endpoints: Vec<Url>,
    /// TTL of the renewable orchestrator-incarnation key.
    pub etcd_session_ttl: Duration,
    /// Per-endpoint coordination request timeout.
    pub etcd_request_timeout: Duration,
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

    #[arg(long, env = "PGSHARD_CLUSTER_UID")]
    cluster_uid: String,

    #[arg(long, env = "PGSHARD_ORCH_ID")]
    orchestrator_id: String,

    #[arg(long, env = "PGSHARD_LEASE_TTL_MS", default_value_t = 15_000)]
    lease_ttl_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_ETCD_ENDPOINTS",
        required = true,
        value_delimiter = ',',
        num_args = 1..=9
    )]
    etcd_endpoints: Vec<String>,

    #[arg(long, env = "PGSHARD_ETCD_SESSION_TTL_SECONDS", default_value_t = 15)]
    etcd_session_ttl_seconds: u64,

    #[arg(long, env = "PGSHARD_ETCD_REQUEST_TIMEOUT_MS", default_value_t = 1_000)]
    etcd_request_timeout_ms: u64,

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
        validate_identifier("cluster UID", &raw.cluster_uid)?;
        validate_identifier("orchestrator ID", &raw.orchestrator_id)?;
        if !(1_000..=300_000).contains(&raw.lease_ttl_ms) {
            return Err(ConfigError::InvalidLeaseTtl(raw.lease_ttl_ms));
        }
        if !(6..=300).contains(&raw.etcd_session_ttl_seconds) {
            return Err(ConfigError::InvalidEtcdSessionTtl(
                raw.etcd_session_ttl_seconds,
            ));
        }
        if !(100..=5_000).contains(&raw.etcd_request_timeout_ms) {
            return Err(ConfigError::InvalidEtcdRequestTimeout(
                raw.etcd_request_timeout_ms,
            ));
        }
        let etcd_endpoints = validate_etcd_endpoints(raw.etcd_endpoints)?;
        let full_cycle_ms = raw
            .etcd_request_timeout_ms
            .saturating_mul(u64::try_from(etcd_endpoints.len()).unwrap_or(u64::MAX));
        if full_cycle_ms > raw.etcd_session_ttl_seconds.saturating_mul(1_000) / 3 {
            return Err(ConfigError::UnsafeEtcdTiming {
                endpoint_count: etcd_endpoints.len(),
                request_timeout_ms: raw.etcd_request_timeout_ms,
                session_ttl_seconds: raw.etcd_session_ttl_seconds,
            });
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
            cluster_uid: raw.cluster_uid,
            lease_ttl_ms: raw.lease_ttl_ms,
            etcd_endpoints,
            etcd_session_ttl: Duration::from_secs(raw.etcd_session_ttl_seconds),
            etcd_request_timeout: Duration::from_millis(raw.etcd_request_timeout_ms),
            telemetry: TelemetryConfig { otlp_endpoint },
        })
    }
}

fn validate_etcd_endpoints(values: Vec<String>) -> Result<Vec<Url>, ConfigError> {
    let mut endpoints = Vec::with_capacity(values.len());
    for value in values {
        if value.trim() != value {
            return Err(ConfigError::UnsafeEtcdEndpoint(value));
        }
        let endpoint = Url::parse(&value)
            .map_err(|source| ConfigError::InvalidEtcdEndpoint { value, source })?;
        if endpoint.scheme() != "http"
            || endpoint.host_str().is_none()
            || endpoint.port().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ConfigError::UnsafeEtcdEndpoint(endpoint.into()));
        }
        if endpoints.contains(&endpoint) {
            return Err(ConfigError::DuplicateEtcdEndpoint(endpoint.into()));
        }
        endpoints.push(endpoint);
    }
    if endpoints.is_empty() {
        return Err(ConfigError::EtcdEndpointsMissing);
    }
    Ok(endpoints)
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
    #[error("lease TTL {0} ms must be between 1000 and 300000 ms")]
    InvalidLeaseTtl(u64),
    /// No coordination endpoint was supplied.
    #[error("at least one etcd endpoint is required")]
    EtcdEndpointsMissing,
    /// An etcd endpoint is not a URL.
    #[error("invalid etcd endpoint {value:?}: {source}")]
    InvalidEtcdEndpoint {
        /// Rejected value.
        value: String,
        /// URL parsing error.
        source: url::ParseError,
    },
    /// An endpoint escapes the current in-cluster plaintext boundary.
    #[error(
        "etcd endpoint {0:?} must be an explicit-port HTTP URL without credentials, path, query, or fragment"
    )]
    UnsafeEtcdEndpoint(String),
    /// Repeating an endpoint does not provide an independent failover target.
    #[error("duplicate etcd endpoint {0:?}")]
    DuplicateEtcdEndpoint(String),
    /// Session TTL cannot safely cover bounded failover.
    #[error("etcd session TTL {0} seconds must be between 6 and 300")]
    InvalidEtcdSessionTtl(u64),
    /// One endpoint attempt is outside the supported bound.
    #[error("etcd request timeout {0} ms must be between 100 and 5000")]
    InvalidEtcdRequestTimeout(u64),
    /// Trying every endpoint could consume too much of the session TTL.
    #[error(
        "{endpoint_count} etcd endpoints at {request_timeout_ms} ms each exceed one third of the {session_ttl_seconds} second session TTL"
    )]
    UnsafeEtcdTiming {
        /// Number of configured endpoints.
        endpoint_count: usize,
        /// Per-endpoint request timeout.
        request_timeout_ms: u64,
        /// Session TTL.
        session_ttl_seconds: u64,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Vec<&'static str> {
        vec![
            "pgshard-orch",
            "--cluster-id",
            "cluster-1",
            "--cluster-uid",
            "11111111-2222-3333-4444-555555555555",
            "--orchestrator-id",
            "orch-0",
            "--etcd-endpoints",
            "http://127.0.0.1:2379",
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

    #[test]
    fn parses_bounded_distinct_etcd_endpoints() {
        let mut values = args();
        let last = values.len() - 1;
        values[last] = "http://127.0.0.1:2379,http://127.0.0.2:2379";
        let config = OrchConfig::try_parse_from(values).expect("valid endpoints");
        assert_eq!(config.etcd_endpoints.len(), 2);
        assert_eq!(config.etcd_session_ttl, Duration::from_secs(15));
        assert_eq!(config.etcd_request_timeout, Duration::from_secs(1));
    }

    #[test]
    fn rejects_unsafe_or_duplicate_etcd_endpoints() {
        for endpoints in [
            "https://127.0.0.1:2379",
            "http://user@127.0.0.1:2379",
            "http://127.0.0.1:2379/path",
            "http://127.0.0.1:2379?query=value",
            "http://127.0.0.1:2379,http://127.0.0.1:2379",
        ] {
            let mut values = args();
            let last = values.len() - 1;
            values[last] = endpoints;
            assert!(OrchConfig::try_parse_from(values).is_err(), "{endpoints}");
        }
    }

    #[test]
    fn rejects_coordination_timing_that_can_exhaust_the_lease() {
        let mut values = args();
        values.extend([
            "--etcd-session-ttl-seconds",
            "6",
            "--etcd-request-timeout-ms",
            "1000",
        ]);
        let last = values.len() - 5;
        values[last] = "http://127.0.0.1:2379,http://127.0.0.2:2379,http://127.0.0.3:2379";
        assert!(matches!(
            OrchConfig::try_parse_from(values),
            Err(ConfigError::UnsafeEtcdTiming { .. })
        ));
    }

    #[test]
    fn rejects_otlp_query_fragment_and_whitespace() {
        for endpoint in [
            "https://collector:4317?token=value",
            "https://collector:4317/#fragment",
            " https://collector:4317",
        ] {
            let mut values = args();
            values.extend(["--otlp-endpoint", endpoint]);
            assert!(matches!(
                OrchConfig::try_parse_from(values),
                Err(ConfigError::UnsafeOtlpEndpoint(_))
            ));
        }
    }
}
