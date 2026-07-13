//! Honest OpenTelemetry configuration state.
//!
//! Export is intentionally not claimed by this foundation crate. The endpoint
//! is validated and recorded so the actual OTLP pipeline can be added without
//! changing the process configuration contract.

use serde::Serialize;
use url::Url;

/// Validated OpenTelemetry configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelemetryConfig {
    /// OTLP endpoint supplied by the operator.
    pub otlp_endpoint: Option<Url>,
}

impl TelemetryConfig {
    /// Returns the current, deliberately conservative exporter state.
    #[must_use]
    pub fn status(&self) -> TelemetryStatus {
        TelemetryStatus {
            enabled: false,
            endpoint_configured: self.otlp_endpoint.is_some(),
            reason: if self.otlp_endpoint.is_some() {
                "OTLP endpoint configured; exporter is not implemented in this runtime foundation"
            } else {
                "OTLP endpoint not configured"
            },
        }
    }
}

/// Externally reportable telemetry state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct TelemetryStatus {
    /// Whether spans or logs are currently exported.
    pub enabled: bool,
    /// Whether a validated endpoint was supplied.
    pub endpoint_configured: bool,
    /// Human-readable explanation that avoids false observability claims.
    pub reason: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_claims_unimplemented_export() {
        let configured = TelemetryConfig {
            otlp_endpoint: Some(Url::parse("http://collector:4317").expect("valid URL")),
        };
        assert!(!configured.status().enabled);
        assert!(configured.status().endpoint_configured);
    }
}
