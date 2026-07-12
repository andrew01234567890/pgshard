//! Deterministic `SemVer` release and public-repository audit tooling.

use std::env;
use std::ffi::OsStr;
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use semver::Version;
use serde::{Deserialize, Serialize};

const FIRST_VERSION: Version = Version::new(0, 1, 0);
const RELEASE_MARKER: &str = "crates/pgshard-release/Cargo.toml";
const NOREPLY_EMAIL: &str = "13841202+andrew01234567890@users.noreply.github.com";

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
    /// Safely enable squash auto-merge after a successful CI workflow run.
    DependabotAutomerge {
        /// Repository in owner/name form.
        #[arg(long)]
        repository: String,
        /// Exact successful pull-request head SHA.
        #[arg(long)]
        sha: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Bump {
    Patch,
    Minor,
    Major,
}

#[derive(Debug)]
struct PlannedRelease {
    sha: String,
    message: String,
    version: Version,
    previous_tag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PullRequest {
    number: u64,
    node_id: String,
    title: String,
    state: String,
    user: Login,
    base: PullRef,
    head: PullRef,
}

#[derive(Debug, Deserialize)]
struct Login {
    login: String,
}

#[derive(Debug, Deserialize)]
struct PullRef {
    #[serde(rename = "ref")]
    name: String,
    sha: String,
}

#[derive(Debug, Deserialize)]
struct PullCommit {
    author: Option<Login>,
    commit: CommitData,
}

#[derive(Debug, Deserialize)]
struct CommitData {
    message: String,
    verification: CommitVerification,
}

#[derive(Debug, Deserialize)]
struct CommitVerification {
    verified: bool,
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
            let sha = git(&["rev-parse", &format!("{sha}^{{commit}}")])?;
            if let Some(tag) = semver_tag_at(&sha)? {
                println!("{}", tag.trim_start_matches('v'));
            } else {
                let plan = release_plan(&sha)?;
                let release = plan
                    .last()
                    .context("selected commit is outside the release history")?;
                ensure!(
                    release.sha == sha,
                    "selected commit is not first-parent releasable"
                );
                println!("{}", release.version);
            }
        }
        ReleaseCommand::Validate { subject } => {
            let message = subject.map_or_else(|| commit_message("HEAD"), Ok)?;
            parse_bump(&message)?;
            println!(
                "valid Conventional Commit subject: {}",
                message.lines().next().unwrap_or_default()
            );
        }
        ReleaseCommand::Publish { sha } => publish(&sha)?,
        ReleaseCommand::DependabotAutomerge { repository, sha } => {
            dependabot_automerge(&repository, &sha)?;
        }
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

    let messages = git(&["log", "--format=%B", &range])?;
    audit_content("commit messages", &messages)?;

    let names = git(&["diff", "--diff-filter=ACMR", "--name-only", &range, "--"])?;
    for path in names.lines() {
        audit_content("repository path", path)?;
        let content = git(&["show", &format!("{head}:{path}")])?;
        audit_content(path, &content)?;
    }
    println!("public repository audit passed for {range}");
    Ok(())
}

fn is_noreply(email: &str) -> bool {
    email == "noreply@github.com" || email.ends_with("@users.noreply.github.com")
}

fn audit_content(path: &str, content: &str) -> Result<()> {
    let forbidden = [
        ["/", "home", "/"].concat(),
        ["BEGIN ", "OPENSSH PRIVATE KEY"].concat(),
        ["BEGIN ", "RSA PRIVATE KEY"].concat(),
        ["github", "_pat_"].concat(),
        ["gh", "p_"].concat(),
        ["AK", "IA"].concat(),
    ];
    for line in content.lines() {
        ensure!(
            !forbidden.iter().any(|pattern| line.contains(pattern)),
            "content in {path} matched a forbidden sensitive-data pattern"
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

    let repository = env::var("GITHUB_REPOSITORY").context("GITHUB_REPOSITORY is required")?;
    let plan = release_plan(&sha)?;
    ensure!(!plan.is_empty(), "no releasable first-parent commits found");

    // One workflow for a descendant may run before an ancestor's workflow.
    // Publishing the complete gap oldest-first makes either execution order
    // deterministic; the later job becomes an idempotent verification.
    for release in plan {
        publish_one(&repository, &release)?;
    }
    Ok(())
}

fn publish_one(repository: &str, release: &PlannedRelease) -> Result<()> {
    if let Some(existing) = semver_tag_at(&release.sha)? {
        ensure!(
            existing == format!("v{}", release.version),
            "commit {} already has unexpected release tag {existing}",
            release.sha
        );
        ensure_release_exists(&existing, &release.sha)?;
        return Ok(());
    }

    let tag = format!("v{}", release.version);
    if let Some(tag_sha) = tag_target(&tag)? {
        ensure!(
            tag_sha == release.sha,
            "tag {tag} already points to another commit"
        );
    }

    let subject = release.message.lines().next().unwrap_or_default();
    let notes = release_notes(
        repository,
        &release.sha,
        subject,
        release.previous_tag.as_deref(),
    );
    let mut args = vec![
        "release".to_owned(),
        "create".to_owned(),
        tag.clone(),
        "--target".to_owned(),
        release.sha.clone(),
        "--title".to_owned(),
        format!("pgshard {tag}"),
        "--notes".to_owned(),
        notes,
    ];
    if release.version.major == 0 {
        args.push("--prerelease".to_owned());
    }
    run("gh", args.iter().map(String::as_str))?;

    println!(
        "{}",
        serde_json::to_string(&ReleaseSummary {
            version: &tag,
            sha: &release.sha,
            previous_tag: release.previous_tag.as_deref(),
        })?
    );
    Ok(())
}

fn release_plan(sha: &str) -> Result<Vec<PlannedRelease>> {
    let chain = first_parent_chain(sha)?;
    let mut tagged = None;
    for (index, commit) in chain.iter().enumerate() {
        if let Some(tag) = semver_tag_at(commit)? {
            tagged = Some((index, tag));
            break;
        }
    }

    let (mut current, mut previous_tag, pending): (Option<Version>, Option<String>, Vec<&String>) =
        if let Some((tag_index, tag)) = tagged {
            let version = Version::parse(tag.trim_start_matches('v'))?;
            (
                Some(version),
                Some(tag),
                chain[..tag_index].iter().rev().collect(),
            )
        } else {
            let chronological: Vec<&String> = chain.iter().rev().collect();
            let start = chronological
                .iter()
                .position(|commit| commit_contains(commit, RELEASE_MARKER))
                .context("release marker is absent from first-parent history")?;
            (None, None, chronological[start..].to_vec())
        };

    let mut plan = Vec::with_capacity(pending.len());
    for commit in pending {
        ensure!(
            semver_tag_at(commit)?.is_none(),
            "release history contains a non-nearest tagged gap"
        );
        let message = commit_message(commit)?;
        let version = next_version(current.as_ref(), &message)?;
        plan.push(PlannedRelease {
            sha: commit.clone(),
            message,
            version: version.clone(),
            previous_tag: previous_tag.clone(),
        });
        previous_tag = Some(format!("v{version}"));
        current = Some(version);
    }
    Ok(plan)
}

fn first_parent_chain(sha: &str) -> Result<Vec<String>> {
    Ok(git(&["rev-list", "--first-parent", sha])?
        .lines()
        .map(str::to_owned)
        .collect())
}

fn commit_contains(sha: &str, path: &str) -> bool {
    Command::new("git")
        .args(["cat-file", "-e", &format!("{sha}:{path}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn next_version(current: Option<&Version>, message: &str) -> Result<Version> {
    let bump = parse_bump(message)?;
    let Some(current) = current else {
        return Ok(FIRST_VERSION);
    };

    let mut next = current.clone();
    match bump {
        Bump::Major if next.major == 0 => {
            next.minor += 1;
            next.patch = 0;
        }
        Bump::Major => {
            next.major += 1;
            next.minor = 0;
            next.patch = 0;
        }
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

fn parse_bump(message: &str) -> Result<Bump> {
    let subject = message.lines().next().unwrap_or_default();
    let (prefix, description) = subject
        .split_once(": ")
        .context("subject must use `type(scope): description` Conventional Commit syntax")?;
    ensure!(
        !description.trim().is_empty(),
        "commit description must not be empty"
    );

    ensure!(
        description.trim() == description,
        "commit description must not have surrounding whitespace"
    );

    let breaking_subject = prefix.ends_with('!');
    let prefix = prefix.trim_end_matches('!');
    let kind = if let Some((kind, scope)) = prefix.split_once('(') {
        ensure!(
            scope.ends_with(')')
                && scope.len() > 1
                && !scope[..scope.len() - 1].contains(['(', ')']),
            "invalid Conventional Commit scope"
        );
        kind
    } else {
        ensure!(!prefix.contains(')'), "invalid Conventional Commit scope");
        prefix
    };
    ensure!(
        !kind.is_empty(),
        "Conventional Commit type must not be empty"
    );

    let allowed = [
        "build", "chore", "ci", "docs", "feat", "fix", "perf", "refactor", "revert", "test",
    ];
    ensure!(
        allowed.contains(&kind),
        "unsupported Conventional Commit type `{kind}`"
    );

    let breaking_footer = message.lines().skip(1).any(|line| {
        line.strip_prefix("BREAKING CHANGE: ")
            .or_else(|| line.strip_prefix("BREAKING-CHANGE: "))
            .is_some_and(|description| !description.trim().is_empty())
    });
    if breaking_subject || breaking_footer {
        Ok(Bump::Major)
    } else if kind == "feat" {
        Ok(Bump::Minor)
    } else {
        Ok(Bump::Patch)
    }
}

fn semver_tag_at(sha: &str) -> Result<Option<String>> {
    let output = git(&["tag", "--points-at", sha])?;
    let tags: Vec<&str> = output
        .lines()
        .filter(|tag| release_tag_version(tag).is_some())
        .collect();
    ensure!(
        tags.len() <= 1,
        "multiple SemVer release tags point at commit {sha}"
    );
    Ok(tags.first().map(|tag| (*tag).to_owned()))
}

fn release_tag_version(tag: &str) -> Option<Version> {
    tag.strip_prefix('v')
        .and_then(|value| Version::parse(value).ok())
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

fn commit_message(sha: &str) -> Result<String> {
    git(&["show", "-s", "--format=%B", sha])
}

fn release_notes(repository: &str, sha: &str, subject: &str, previous_tag: Option<&str>) -> String {
    let short_sha = &sha[..sha.len().min(12)];
    let compare = previous_tag.map_or_else(
        || format!("https://github.com/{repository}/commit/{sha}"),
        |tag| format!("https://github.com/{repository}/compare/{tag}...{sha}"),
    );
    format!(
        "## Change\n\n- {subject}\n- Commit: [`{short_sha}`](https://github.com/{repository}/commit/{sha})\n\n[Compare changes]({compare})\n\nThis prerelease contains source code only. No container images, binaries, charts, or packages are published."
    )
}

fn dependabot_automerge(repository: &str, requested_sha: &str) -> Result<()> {
    ensure!(
        env::var("GITHUB_ACTIONS").as_deref() == Ok("true"),
        "Dependabot auto-merge may only run in GitHub Actions"
    );
    ensure!(
        repository.split_once('/').is_some_and(|(owner, name)| {
            !owner.is_empty()
                && !name.is_empty()
                && owner
                    .chars()
                    .chain(name.chars())
                    .all(|character| character.is_ascii_alphanumeric() || "-_.".contains(character))
        }),
        "invalid repository name"
    );
    ensure!(
        requested_sha.len() == 40
            && requested_sha
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
        "head SHA must be a complete hexadecimal object ID"
    );

    let pulls_json = run(
        "gh",
        [
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            &format!("repos/{repository}/commits/{requested_sha}/pulls"),
        ],
    )?;
    let pulls: Vec<PullRequest> = serde_json::from_str(&pulls_json)?;
    let mut eligible = pulls.into_iter().filter(|pull| {
        pull.state == "open"
            && pull.user.login == "dependabot[bot]"
            && pull.base.name == "main"
            && pull.head.sha == requested_sha
    });
    let Some(pull) = eligible.next() else {
        println!("no open Dependabot pull request matches {requested_sha}");
        return Ok(());
    };
    ensure!(
        eligible.next().is_none(),
        "multiple Dependabot pull requests match one head SHA"
    );

    let commits_json = run(
        "gh",
        [
            "api",
            &format!("repos/{repository}/pulls/{}/commits", pull.number),
        ],
    )?;
    let commits: Vec<PullCommit> = serde_json::from_str(&commits_json)?;
    ensure!(
        !commits.is_empty(),
        "Dependabot pull request has no commits"
    );
    ensure!(
        commits.iter().all(|commit| {
            commit.author.as_ref().map(|author| author.login.as_str()) == Some("dependabot[bot]")
                && commit.commit.verification.verified
        }),
        "every auto-merged commit must be verified and authored by Dependabot"
    );
    if !dependabot_patch_only(commits.iter().map(|commit| commit.commit.message.as_str())) {
        println!(
            "Dependabot pull request #{} is not a verified patch-only update",
            pull.number
        );
        return Ok(());
    }

    run(
        "gh",
        [
            "api",
            "graphql",
            "-f",
            "query=mutation($id: ID!, $headline: String!, $authorEmail: String!, $oid: GitObjectID!) { enablePullRequestAutoMerge(input: {pullRequestId: $id, mergeMethod: SQUASH, commitHeadline: $headline, authorEmail: $authorEmail, expectedHeadOid: $oid}) { pullRequest { autoMergeRequest { enabledAt } } } }",
            "-f",
            &format!("id={}", pull.node_id),
            "-f",
            &format!("headline={}", pull.title),
            "-f",
            &format!("authorEmail={NOREPLY_EMAIL}"),
            "-f",
            &format!("oid={requested_sha}"),
        ],
    )?;
    println!(
        "enabled checked squash auto-merge for Dependabot pull request #{}",
        pull.number
    );
    Ok(())
}

fn dependabot_patch_only<'a>(messages: impl IntoIterator<Item = &'a str>) -> bool {
    let mut dependency_count = 0_usize;
    let mut update_types = Vec::new();
    for message in messages {
        for line in message.lines().map(str::trim) {
            if line.starts_with("dependency-name:") || line.starts_with("- dependency-name:") {
                dependency_count += 1;
            }
            if let Some(update_type) = line.strip_prefix("update-type: ") {
                update_types.push(update_type);
            }
        }
    }
    dependency_count > 0
        && dependency_count == update_types.len()
        && update_types
            .iter()
            .all(|update_type| *update_type == "version-update:semver-patch")
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
        let current = Version::new(0, 4, 7);
        assert_eq!(
            next_version(Some(&current), "feat(router): add ranges").unwrap(),
            Version::new(0, 5, 0)
        );
        assert_eq!(
            next_version(Some(&current), "fix!: replace protocol").unwrap(),
            Version::new(0, 5, 0)
        );
        assert_eq!(
            next_version(
                Some(&current),
                "fix: retain compatibility\n\nBREAKING CHANGE: replace wire format"
            )
            .unwrap(),
            Version::new(0, 5, 0)
        );
    }

    #[test]
    fn maintenance_changes_bump_patch() {
        let current = Version::new(0, 4, 7);
        assert_eq!(
            next_version(Some(&current), "ci: parallelize tests").unwrap(),
            Version::new(0, 4, 8)
        );
    }

    #[test]
    fn post_one_breaking_change_bumps_major() {
        let current = Version::new(1, 7, 4);
        assert_eq!(
            next_version(Some(&current), "refactor!: replace protocol").unwrap(),
            Version::new(2, 0, 0)
        );
    }

    #[test]
    fn invalid_subject_is_rejected() {
        assert!(parse_bump("not conventional").is_err());
        assert!(parse_bump("unknown: change").is_err());
        assert!(parse_bump("feat(): change").is_err());
        assert!(parse_bump("feat(foo)bar): change").is_err());
        assert!(parse_bump("feat(foo)): change").is_err());
        assert!(parse_bump("feat:  padded").is_err());
    }

    #[test]
    fn noreply_validation_accepts_only_github_noreply_domains() {
        assert!(is_noreply("123+contributor@users.noreply.github.com"));
        assert!(is_noreply("noreply@github.com"));
        assert!(!is_noreply("developer@example.com"));
    }

    #[test]
    fn content_audit_rejects_sensitive_added_lines() {
        assert!(audit_content("safe.md", "safe public content").is_ok());
        let private_path = format!("path from /{}/example", "home");
        let token = format!("{}{}example", "github", "_pat_");
        assert!(audit_content("bad.md", &private_path).is_err());
        assert!(audit_content("bad.md", &token).is_err());
    }

    #[test]
    fn dependabot_metadata_must_cover_only_patch_updates() {
        let patch = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-patch\n...";
        let mixed = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-patch\n- dependency-name: tokio\n  update-type: version-update:semver-minor\n...";
        let incomplete = "---\nupdated-dependencies:\n- dependency-name: serde\n...";
        assert!(dependabot_patch_only([patch]));
        assert!(!dependabot_patch_only([mixed]));
        assert!(!dependabot_patch_only([incomplete]));
    }
}
