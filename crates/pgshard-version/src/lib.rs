//! Canonical build identity shared by every pgshard runtime.

/// Exact source release version, or `0.0.0-dev+<sha>` for an untagged build.
pub const VERSION: &str = env!("PGSHARD_BUILD_VERSION");

/// Complete source commit SHA, or `unknown` outside a Git checkout without an
/// explicit `PGSHARD_GIT_SHA` build input.
pub const GIT_SHA: &str = env!("PGSHARD_GIT_SHA");

/// Returns a bounded component identity suitable for metrics and APIs.
#[must_use]
pub const fn version() -> &'static str {
    VERSION
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::*;

    #[test]
    fn build_version_is_semver() {
        Version::parse(VERSION).expect("build script must emit SemVer");
    }

    #[test]
    fn source_identity_is_bounded() {
        assert!(GIT_SHA == "unknown" || GIT_SHA.len() == 40);
    }
}
