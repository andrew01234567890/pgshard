//! Deterministic `SemVer` release and public-repository audit tooling.

use std::env;
use std::ffi::OsStr;
use std::process::{Command, Output};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use semver::Version;
use serde::Serialize;

const FIRST_VERSION: Version = Version::new(0, 1, 0);

#[derive(Debug, Parser)]
#[command(about = "Create deterministic source-only pgshard releases")]
struct Cli {
    #[command(subcommand)]
    command: ReleaseCommand,
}

#[derive(Debug, Subcommand)]
enum ReleaseCommand {
    /// Audit new commits and content for public-repository privacy rules.
    Audit {
        /// Base revision excluded from the audit.
        #[arg(long, default_value = "origin/main")]
        base: String,
        /// Head revision included in the audit.
        #[arg(long, default_value = "HEAD")]
        head: String,
    },
    /// Print the version that the selected commit would receive.
    Next {
        /// Commit to inspect.
        #[arg(long, default_value = "HEAD")]
        sha: String,
    },
    /// Validate a Conventional Commit subject.
    Validate {
        /// Subject to validate. Reads HEAD when omitted.
        #[arg(long)]
        subject: Option<String>,
    },
    /// Create an idempotent tag and source-only GitHub Release.
    Publish {
        /// Exact main-branch commit to release.
        #[arg(long)]
        sha: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Bump {
    Patch,
    Minor,
}

#[derive(Debug, Serialize)]
struct ReleaseSummary<'a> {
    version: &'a str,
    sha: &'a str,
    previous_tag: Option<&'a str>,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        ReleaseCommand::Audit { base, head } => audit(&base, &head)?,
        ReleaseCommand::Next { sha } => {
            let subject = commit_subject(&sha)?;
            let current = highest_version_tag()?;
            println!("{}", next_version(current.as_ref(), &subject)?);
        }
        ReleaseCommand::Validate { subject } => {
            let subject = subject.map_or_else(|| commit_subject("HEAD"), Ok)?;
            parse_bump(&subject)?;
            println!("valid Conventional Commit subject: {subject}");
        }
        ReleaseCommand::Publish { sha } => publish(&sha)?,
    }
    Ok(())
}

fn audit(base: &str, head: &str) -> Result<()> {
    let merge_base = git(&["merge-base", base, head])?;
    let range = format!("{merge_base}..{head}");
    let identities = git(&["log", "--format=%H%x09%ae%x09%ce", &range])?;
    for line in identities.lines() {
        let mut fields = line.split('\t');
        let sha = fields.next().unwrap_or_default();
        let author = fields.next().unwrap_or_default();
        let committer = fields.next().unwrap_or_default();
        ensure!(
            is_noreply(author) && is_noreply(committer),
            "commit {sha} must use noreply author and committer addresses"
        );
    }

    let names = git(&["diff", "--name-only", &range, "--"])?;
    for path in names
        .lines()
        .filter(|path| *path != "Cargo.lock" && *path != "crates/pgshard-release/src/main.rs")
    {
        let diff = git(&["diff", "--unified=0", &range, "--", path])?;
        audit_added_lines(path, &diff)?;
    }
    println!("public repository audit passed for {range}");
    Ok(())
}

fn is_noreply(email: &str) -> bool {
    email == "noreply@github.com" || email.ends_with("@users.noreply.github.com")
}

fn audit_added_lines(path: &str, diff: &str) -> Result<()> {
    const FORBIDDEN: [&str; 6] = [
        "/home/",
        "BEGIN OPENSSH PRIVATE KEY",
        "BEGIN RSA PRIVATE KEY",
        "github_pat_",
        "ghp_",
        "AKIA",
    ];
    for line in diff
        .lines()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
    {
        ensure!(
            !FORBIDDEN.iter().any(|pattern| line.contains(pattern)),
            "new content in {path} matched a forbidden sensitive-data pattern"
        );
    }
    Ok(())
}

fn publish(requested_sha: &str) -> Result<()> {
    ensure!(
        env::var("GITHUB_ACTIONS").as_deref() == Ok("true"),
        "publish may only run in GitHub Actions"
    );
    let sha = git(&["rev-parse", &format!("{requested_sha}^{{commit}}")])?;
    if let Ok(expected) = env::var("GITHUB_SHA") {
        ensure!(sha == expected, "requested SHA does not match GITHUB_SHA");
    }

    if let Some(existing) = semver_tag_at(&sha)? {
        ensure_release_exists(&existing, &sha)?;
        println!("release {existing} already exists for {sha}");
        return Ok(());
    }

    let subject = commit_subject(&sha)?;
    let current = highest_version_tag()?;
    let version = next_version(current.as_ref(), &subject)?;
    let tag = format!("v{version}");

    if let Some(tag_sha) = tag_target(&tag)? {
        ensure!(tag_sha == sha, "tag {tag} already points to another commit");
    }

    let repository = env::var("GITHUB_REPOSITORY").context("GITHUB_REPOSITORY is required")?;
    let notes = release_notes(&repository, &sha, &subject, current.as_ref());
    let title = format!("pgshard {tag}");

    let mut args = vec![
        "release".to_owned(),
        "create".to_owned(),
        tag.clone(),
        "--target".to_owned(),
        sha.clone(),
        "--title".to_owned(),
        title,
        "--notes".to_owned(),
        notes,
    ];
    if version.major == 0 {
        args.push("--prerelease".to_owned());
    }
    run("gh", args.iter().map(String::as_str))?;

    let previous_tag = current.as_ref().map(|(tag, _)| tag.as_str());
    println!(
        "{}",
        serde_json::to_string(&ReleaseSummary {
            version: &tag,
            sha: &sha,
            previous_tag,
        })?
    );
    Ok(())
}

fn next_version(current: Option<&(String, Version)>, subject: &str) -> Result<Version> {
    let bump = parse_bump(subject)?;
    let Some((_, current)) = current else {
        return Ok(FIRST_VERSION);
    };

    let mut next = current.clone();
    match bump {
        Bump::Minor => {
            next.minor += 1;
            next.patch = 0;
        }
        Bump::Patch => next.patch += 1,
    }
    next.pre = semver::Prerelease::EMPTY;
    next.build = semver::BuildMetadata::EMPTY;
    Ok(next)
}

fn parse_bump(subject: &str) -> Result<Bump> {
    let (prefix, description) = subject
        .split_once(": ")
        .context("subject must use `type(scope): description` Conventional Commit syntax")?;
    ensure!(
        !description.trim().is_empty(),
        "commit description must not be empty"
    );

    let breaking = prefix.ends_with('!');
    let prefix = prefix.trim_end_matches('!');
    let kind = prefix.split_once('(').map_or(prefix, |(kind, scope)| {
        if !scope.ends_with(')') || scope.len() == 1 {
            ""
        } else {
            kind
        }
    });
    ensure!(!kind.is_empty(), "invalid Conventional Commit scope");

    let allowed = [
        "build", "chore", "ci", "docs", "feat", "fix", "perf", "refactor", "revert", "test",
    ];
    ensure!(
        allowed.contains(&kind),
        "unsupported Conventional Commit type `{kind}`"
    );

    // Before 1.0, breaking changes intentionally advance the minor release.
    if breaking || kind == "feat" {
        Ok(Bump::Minor)
    } else {
        Ok(Bump::Patch)
    }
}

fn highest_version_tag() -> Result<Option<(String, Version)>> {
    let output = git(&["tag", "--list", "v*"])?;
    Ok(output
        .lines()
        .filter_map(|tag| {
            Version::parse(tag.trim_start_matches('v'))
                .ok()
                .map(|version| (tag.to_owned(), version))
        })
        .max_by(|(_, left), (_, right)| left.cmp(right)))
}

fn semver_tag_at(sha: &str) -> Result<Option<String>> {
    let output = git(&["tag", "--points-at", sha])?;
    Ok(output
        .lines()
        .find(|tag| Version::parse(tag.trim_start_matches('v')).is_ok())
        .map(str::to_owned))
}

fn tag_target(tag: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(["rev-list", "-n", "1", tag])
        .output()
        .context("failed to inspect git tag")?;
    if output.status.success() {
        Ok(Some(String::from_utf8(output.stdout)?.trim().to_owned()))
    } else {
        Ok(None)
    }
}

fn commit_subject(sha: &str) -> Result<String> {
    git(&["show", "-s", "--format=%s", sha])
}

fn release_notes(
    repository: &str,
    sha: &str,
    subject: &str,
    current: Option<&(String, Version)>,
) -> String {
    let short_sha = &sha[..sha.len().min(12)];
    let compare = current.map_or_else(
        || format!("https://github.com/{repository}/commit/{sha}"),
        |(tag, _)| format!("https://github.com/{repository}/compare/{tag}...{sha}"),
    );
    format!(
        "## Change\n\n- {subject}\n- Commit: [`{short_sha}`](https://github.com/{repository}/commit/{sha})\n\n[Compare changes]({compare})\n\nThis prerelease contains source code only. No container images, binaries, charts, or packages are published."
    )
}

fn ensure_release_exists(tag: &str, sha: &str) -> Result<()> {
    let tagged_sha = tag_target(tag)?.context("release tag disappeared")?;
    ensure!(
        tagged_sha == sha,
        "existing release tag points to another commit"
    );
    let status = Command::new("gh")
        .args(["release", "view", tag])
        .status()
        .context("failed to inspect GitHub Release")?;
    ensure!(
        status.success(),
        "tag exists without the required GitHub Release"
    );
    Ok(())
}

fn git(args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("failed to run git {}", args.join(" ")))?;
    output_text("git", output)
}

fn run<I, S>(program: &str, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {program}"))?;
    output_text(program, output)
}

fn output_text(program: &str, output: Output) -> Result<String> {
    if !output.status.success() {
        bail!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_release_is_fixed() {
        assert_eq!(
            next_version(None, "docs: start documentation").unwrap(),
            FIRST_VERSION
        );
    }

    #[test]
    fn pre_one_features_and_breaking_changes_bump_minor() {
        let current = ("v0.4.7".to_owned(), Version::new(0, 4, 7));
        assert_eq!(
            next_version(Some(&current), "feat(router): add ranges").unwrap(),
            Version::new(0, 5, 0)
        );
        assert_eq!(
            next_version(Some(&current), "fix!: replace protocol").unwrap(),
            Version::new(0, 5, 0)
        );
    }

    #[test]
    fn maintenance_changes_bump_patch() {
        let current = ("v0.4.7".to_owned(), Version::new(0, 4, 7));
        assert_eq!(
            next_version(Some(&current), "ci: parallelize tests").unwrap(),
            Version::new(0, 4, 8)
        );
    }

    #[test]
    fn invalid_subject_is_rejected() {
        assert!(parse_bump("not conventional").is_err());
        assert!(parse_bump("unknown: change").is_err());
        assert!(parse_bump("feat(): change").is_err());
    }

    #[test]
    fn noreply_validation_accepts_only_github_noreply_domains() {
        assert!(is_noreply("123+contributor@users.noreply.github.com"));
        assert!(is_noreply("noreply@github.com"));
        assert!(!is_noreply("developer@example.com"));
    }

    #[test]
    fn content_audit_rejects_sensitive_added_lines() {
        assert!(audit_added_lines("safe.md", "+safe public content").is_ok());
        assert!(audit_added_lines("bad.md", "+path from /home/example").is_err());
        assert!(audit_added_lines("bad.md", "+github_pat_example").is_err());
    }
}
