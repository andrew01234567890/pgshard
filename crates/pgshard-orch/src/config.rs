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
    /// Immutable Kubernetes Pod incarnation.
    pub pod_uid: String,
    /// Namespace containing the operator-owned leadership Lease.
    pub lease_namespace: String,
    /// Name of the operator-owned leadership Lease.
    pub lease_name: String,
    /// Default requested operation-lease duration.
    pub lease_ttl_ms: u64,
    /// Duration written into the Kubernetes leadership Lease.
    pub kubernetes_lease_duration: Duration,
    /// Candidate observation and retry cadence.
    pub kubernetes_lease_retry_period: Duration,
    /// Bound for one Kubernetes API request.
    pub kubernetes_request_timeout: Duration,
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

    #[arg(long, env = "PGSHARD_POD_UID")]
    pod_uid: String,

    #[arg(long, env = "PGSHARD_LEASE_NAMESPACE")]
    lease_namespace: String,

    #[arg(long, env = "PGSHARD_LEASE_NAME")]
    lease_name: String,

    #[arg(long, env = "PGSHARD_LEASE_TTL_MS", default_value_t = 15_000)]
    lease_ttl_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_KUBERNETES_LEASE_DURATION_SECONDS",
        default_value_t = 15
    )]
    kubernetes_lease_duration_seconds: u64,

    #[arg(
        long,
        env = "PGSHARD_KUBERNETES_LEASE_RETRY_MS",
        default_value_t = 2_000
    )]
    kubernetes_lease_retry_ms: u64,

    #[arg(
        long,
        env = "PGSHARD_KUBERNETES_REQUEST_TIMEOUT_MS",
        default_value_t = 1_000
    )]
    kubernetes_request_timeout_ms: u64,

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
        validate_dns_label("cluster ID", &raw.cluster_id)?;
        validate_dns_label("orchestrator ID", &raw.orchestrator_id)?;
        validate_dns_label("Lease namespace", &raw.lease_namespace)?;
        validate_dns_label("Lease name", &raw.lease_name)?;
        validate_uid("cluster UID", &raw.cluster_uid)?;
        validate_uid("Pod UID", &raw.pod_uid)?;
        if !(1_000..=300_000).contains(&raw.lease_ttl_ms) {
            return Err(ConfigError::InvalidLeaseTtl(raw.lease_ttl_ms));
        }
        if !(6..=300).contains(&raw.kubernetes_lease_duration_seconds) {
            return Err(ConfigError::InvalidKubernetesLeaseDuration(
                raw.kubernetes_lease_duration_seconds,
            ));
        }
        if !(100..=5_000).contains(&raw.kubernetes_request_timeout_ms) {
            return Err(ConfigError::InvalidKubernetesRequestTimeout(
                raw.kubernetes_request_timeout_ms,
            ));
        }
        if !(100..=30_000).contains(&raw.kubernetes_lease_retry_ms) {
            return Err(ConfigError::InvalidKubernetesLeaseRetry(
                raw.kubernetes_lease_retry_ms,
            ));
        }
        let lease_duration_ms = raw.kubernetes_lease_duration_seconds.saturating_mul(1_000);
        if raw.kubernetes_request_timeout_ms > lease_duration_ms / 3
            || raw.kubernetes_lease_retry_ms > lease_duration_ms / 3
        {
            return Err(ConfigError::UnsafeKubernetesLeaseTiming {
                request_timeout_ms: raw.kubernetes_request_timeout_ms,
                retry_period_ms: raw.kubernetes_lease_retry_ms,
                lease_duration_seconds: raw.kubernetes_lease_duration_seconds,
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
            pod_uid: raw.pod_uid,
            lease_namespace: raw.lease_namespace,
            lease_name: raw.lease_name,
            lease_ttl_ms: raw.lease_ttl_ms,
            kubernetes_lease_duration: Duration::from_secs(raw.kubernetes_lease_duration_seconds),
            kubernetes_lease_retry_period: Duration::from_millis(raw.kubernetes_lease_retry_ms),
            kubernetes_request_timeout: Duration::from_millis(raw.kubernetes_request_timeout_ms),
            telemetry: TelemetryConfig { otlp_endpoint },
        })
    }
}

fn validate_dns_label(name: &'static str, value: &str) -> Result<(), ConfigError> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if !valid {
        return Err(ConfigError::InvalidDnsLabel {
            name,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn validate_uid(name: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ConfigError::InvalidUid {
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
    /// A Kubernetes object name is not a DNS label.
    #[error("{name} {value:?} must be a 1-63 byte lowercase DNS label")]
    InvalidDnsLabel {
        /// Identifier field.
        name: &'static str,
        /// Rejected value.
        value: String,
    },
    /// An API UID is empty, oversized, or contains unsafe characters.
    #[error("{name} {value:?} must be a bounded Kubernetes UID")]
    InvalidUid {
        /// UID field.
        name: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Operation lease TTL is outside the bounded safety range.
    #[error("lease TTL {0} ms must be between 1000 and 300000 ms")]
    InvalidLeaseTtl(u64),
    /// Kubernetes leadership Lease duration is outside the bounded range.
    #[error("Kubernetes Lease duration {0} seconds must be between 6 and 300")]
    InvalidKubernetesLeaseDuration(u64),
    /// One Kubernetes API request is outside the supported bound.
    #[error("Kubernetes request timeout {0} ms must be between 100 and 5000")]
    InvalidKubernetesRequestTimeout(u64),
    /// Candidate polling is outside the supported bound.
    #[error("Kubernetes Lease retry period {0} ms must be between 100 and 30000")]
    InvalidKubernetesLeaseRetry(u64),
    /// Request and retry timing cannot safely fit within the Lease duration.
    #[error(
        "Kubernetes request timeout {request_timeout_ms} ms and retry period {retry_period_ms} ms must each fit within one third of the {lease_duration_seconds} second Lease duration"
    )]
    UnsafeKubernetesLeaseTiming {
        /// Per-request timeout.
        request_timeout_ms: u64,
        /// Candidate retry period.
        retry_period_ms: u64,
        /// Leadership Lease duration.
        lease_duration_seconds: u64,
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
            "cluster-1-orchestrator-abc12",
            "--pod-uid",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--lease-namespace",
            "database",
            "--lease-name",
            "cluster-1-orchestrator-leader",
        ]
    }

    #[test]
    fn accepts_bounded_kubernetes_lease_defaults() {
        let config = OrchConfig::try_parse_from(args()).expect("valid config");
        assert_eq!(config.lease_ttl_ms, 15_000);
        assert_eq!(config.kubernetes_lease_duration, Duration::from_secs(15));
        assert_eq!(config.kubernetes_lease_retry_period, Duration::from_secs(2));
        assert_eq!(config.kubernetes_request_timeout, Duration::from_secs(1));
    }

    #[test]
    fn rejects_dangerously_short_operation_ttl() {
        let mut values = args();
        values.extend(["--lease-ttl-ms", "10"]);
        assert!(matches!(
            OrchConfig::try_parse_from(values),
            Err(ConfigError::InvalidLeaseTtl(10))
        ));
    }

    #[test]
    fn rejects_unknown_arguments() {
        let mut values = args();
        values.push("--unsafe-promote");
        assert!(matches!(
            OrchConfig::try_parse_from(values),
            Err(ConfigError::Arguments(_))
        ));
    }

    #[test]
    fn rejects_non_dns_object_names_and_missing_uids() {
        for (flag, value) in [
            ("--cluster-id", "Cluster_1"),
            ("--lease-namespace", "database."),
            ("--lease-name", "-leader"),
            ("--pod-uid", ""),
        ] {
            let mut values = args();
            let index = values
                .iter()
                .position(|argument| *argument == flag)
                .unwrap()
                + 1;
            values[index] = value;
            assert!(
                OrchConfig::try_parse_from(values).is_err(),
                "{flag}={value}"
            );
        }
    }

    #[test]
    fn rejects_coordination_timing_that_can_exhaust_the_lease() {
        let mut values = args();
        values.extend([
            "--kubernetes-lease-duration-seconds",
            "6",
            "--kubernetes-request-timeout-ms",
            "3000",
        ]);
        assert!(matches!(
            OrchConfig::try_parse_from(values),
            Err(ConfigError::UnsafeKubernetesLeaseTiming { .. })
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
