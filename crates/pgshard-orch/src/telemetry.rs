//! Honest OpenTelemetry configuration state.

use serde::Serialize;
use url::Url;

/// Validated OpenTelemetry configuration.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelemetryConfig {
    /// OTLP endpoint supplied by the operator.
    pub otlp_endpoint: Option<Url>,
}

impl TelemetryConfig {
    /// Reports configuration without claiming an exporter is running.
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
    /// Whether telemetry is actually exported.
    pub enabled: bool,
    /// Whether a validated endpoint was supplied.
    pub endpoint_configured: bool,
    /// Honest explanation of exporter state.
    pub reason: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_endpoint_does_not_claim_export() {
        let config = TelemetryConfig {
            otlp_endpoint: Some(Url::parse("http://collector:4317").expect("valid URL")),
        };
        assert!(!config.status().enabled);
        assert!(config.status().endpoint_configured);
    }
}
