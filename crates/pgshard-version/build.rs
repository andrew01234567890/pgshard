//! Derive a reproducible release or development identity from trusted inputs.

use std::env;
use std::process::Command;

use semver::Version;

fn main() {
    println!("cargo::rerun-if-env-changed=PGSHARD_BUILD_VERSION");
    println!("cargo::rerun-if-env-changed=PGSHARD_GIT_SHA");
    println!("cargo::rerun-if-changed=../../.git/HEAD");

    let sha = env::var("PGSHARD_GIT_SHA")
        .ok()
        .filter(|value| full_sha(value))
        .or_else(git_sha)
        .unwrap_or_else(|| "unknown".to_owned());
    let version = env::var("PGSHARD_BUILD_VERSION")
        .ok()
        .map(|value| value.trim_start_matches('v').to_owned())
        .or_else(exact_semver_tag)
        .unwrap_or_else(|| format!("0.0.0-dev+{}", &sha[..sha.len().min(12)]));

    Version::parse(&version).expect("invalid pgshard build version");
    println!("cargo::rustc-env=PGSHARD_BUILD_VERSION={version}");
    println!("cargo::rustc-env=PGSHARD_GIT_SHA={sha}");
}

fn git_sha() -> Option<String> {
    command(&["rev-parse", "HEAD"]).filter(|value| full_sha(value))
}

fn exact_semver_tag() -> Option<String> {
    let mut versions: Vec<String> = command(&["tag", "--points-at", "HEAD"])?
        .lines()
        .filter_map(|tag| tag.strip_prefix('v'))
        .filter(|tag| Version::parse(tag).is_ok())
        .map(str::to_owned)
        .collect();
    versions.sort();
    versions.dedup();
    match versions.len() {
        0 => None,
        1 => versions.pop(),
        _ => panic!("multiple SemVer tags point at the build commit"),
    }
}

fn command(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn full_sha(value: &str) -> bool {
    value.len() == 40 && value.chars().all(|character| character.is_ascii_hexdigit())
}
