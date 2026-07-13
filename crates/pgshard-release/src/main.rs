//! Deterministic `SemVer` release and public-repository audit tooling.

use std::env;
use std::ffi::OsStr;
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use semver::Version;
use serde::{Deserialize, Serialize};

const FIRST_VERSION: Version = Version::new(0, 1, 0);
const RELEASE_MARKER: &str = "crates/pgshard-release/RELEASE_START";
const RELEASE_HELPER_SOURCE: &str = "crates/pgshard-release/src/main.rs";
const DEPENDABOT_MERGE_QUERY: &str = "query=mutation($id: ID!, $headline: String!, $oid: GitObjectID!) { mergePullRequest(input: {pullRequestId: $id, mergeMethod: SQUASH, commitHeadline: $headline, expectedHeadOid: $oid}) { pullRequest { state mergedAt mergeCommit { oid } } } }";

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
        /// Permit GitHub's squash author while still requiring its noreply committer.
        #[arg(long)]
        allow_github_squash_author: bool,
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
    /// Safely squash-merge a verified patch update after successful CI.
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
struct PullRequestDetails {
    number: u64,
    node_id: String,
    head: PullRef,
    commits: usize,
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
    sha: String,
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

#[derive(Debug, Deserialize)]
struct GitHubCommitDetails {
    sha: String,
    committer: Option<Login>,
    commit: GitHubCommitData,
}

#[derive(Debug, Deserialize)]
struct GitHubCommitData {
    verification: GitHubCommitVerification,
}

#[derive(Debug, Deserialize)]
struct GitHubCommitVerification {
    verified: bool,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct CheckRuns {
    check_runs: Vec<CheckRun>,
}

#[derive(Debug, Deserialize)]
struct CheckRun {
    name: String,
    status: String,
    conclusion: Option<String>,
    app: CheckApp,
}

#[derive(Debug, Deserialize)]
struct CheckApp {
    slug: String,
}

#[derive(Debug, Serialize)]
struct ReleaseSummary<'a> {
    version: &'a str,
    sha: &'a str,
    previous_tag: Option<&'a str>,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        ReleaseCommand::Audit {
            base,
            head,
            allow_github_squash_author,
        } => audit(&base, &head, allow_github_squash_author)?,
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

fn audit(base: &str, head: &str, allow_github_squash_author: bool) -> Result<()> {
    let merge_base = git(&["merge-base", base, head])?;
    let range = format!("{merge_base}..{head}");
    let identities = git(&["log", "--format=%H%x09%ae%x09%cn%x09%ce", &range])?;
    for line in identities.lines() {
        let mut fields = line.split('\t');
        let sha = fields.next().unwrap_or_default();
        let author = fields.next().unwrap_or_default();
        let committer_name = fields.next().unwrap_or_default();
        let committer = fields.next().unwrap_or_default();
        let github_squash_verified = allow_github_squash_author
            && github_squash_identity(committer_name, committer)
            && github_commit_is_verified(sha)?;
        ensure!(
            commit_identity_is_allowed(author, committer, github_squash_verified),
            "commit {sha} must use noreply identities or an explicitly allowed GitHub squash author"
        );
    }

    let messages = git(&["log", "--format=%B", &range])?;
    audit_content("commit messages", &messages)?;

    let commits = git(&["rev-list", "--reverse", &range])?;
    for commit in commits.lines() {
        let names = git(&[
            "diff-tree",
            "--root",
            "-m",
            "--no-commit-id",
            "--name-only",
            "--diff-filter=ACMR",
            "-r",
            commit,
            "--",
        ])?;
        for path in names.lines() {
            audit_repository_path(path)?;
            let content = git(&["show", &format!("{commit}:{path}")])?;
            audit_content(path, &content)?;
        }
    }
    println!("public repository audit passed for {range}");
    Ok(())
}

fn audit_repository_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && path.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-')
            }),
        "repository path contains unsupported characters"
    );
    audit_content("repository path", path)
}

fn is_noreply(email: &str) -> bool {
    email == "noreply@github.com" || email.ends_with("@users.noreply.github.com")
}

fn commit_identity_is_allowed(author: &str, committer: &str, github_squash_verified: bool) -> bool {
    (is_noreply(author) && is_noreply(committer)) || github_squash_verified
}

fn github_squash_identity(committer_name: &str, committer: &str) -> bool {
    committer_name == "GitHub" && committer == "noreply@github.com"
}

fn github_commit_is_verified(sha: &str) -> Result<bool> {
    let repository = env::var("GITHUB_REPOSITORY")
        .context("GITHUB_REPOSITORY is required to verify a GitHub squash commit")?;
    let response = run(
        "gh",
        [
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            &format!("repos/{repository}/commits/{sha}"),
        ],
    )?;
    let details: GitHubCommitDetails = serde_json::from_str(&response)?;
    Ok(github_commit_details_are_verified(&details, sha))
}

fn github_commit_details_are_verified(details: &GitHubCommitDetails, sha: &str) -> bool {
    details.sha == sha
        && details.committer.as_ref().map(|login| login.login.as_str()) == Some("web-flow")
        && details.commit.verification.verified
        && details.commit.verification.reason == "valid"
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
        if let Some(pattern) = forbidden.iter().find(|pattern| line.contains(*pattern)) {
            ensure!(
                is_legacy_scanner_fixture(path, line, pattern),
                "content in {path} matched a forbidden sensitive-data pattern"
            );
        }
    }
    Ok(())
}

fn is_legacy_scanner_fixture(path: &str, line: &str, pattern: &str) -> bool {
    if path != RELEASE_HELPER_SOURCE {
        return false;
    }
    let line = line.trim();
    if line == format!("{pattern:?},") {
        return true;
    }
    let home_test = format!(
        "assert!(audit_added_lines(\"bad.md\", \"+path from {pattern}example\").is_err());"
    );
    let token_test =
        format!("assert!(audit_added_lines(\"bad.md\", \"+{pattern}example\").is_err());");
    line == home_test || line == token_test
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

    ensure_ci_passed(repository, &release.sha)?;

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

fn ensure_ci_passed(repository: &str, sha: &str) -> Result<()> {
    let response = run(
        "gh",
        [
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            &format!(
                "repos/{repository}/commits/{sha}/check-runs?check_name=CI%20aggregate&filter=latest&per_page=10"
            ),
        ],
    )?;
    let checks: CheckRuns = serde_json::from_str(&response)?;
    ensure!(
        ci_passed(&checks),
        "commit {sha} does not have a successful exact-head CI aggregate check"
    );
    Ok(())
}

fn ci_passed(checks: &CheckRuns) -> bool {
    checks.check_runs.iter().any(|check| {
        check.name == "CI aggregate"
            && check.app.slug == "github-actions"
            && check.status == "completed"
            && check.conclusion.as_deref() == Some("success")
    })
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

    let trailing_bangs = prefix
        .chars()
        .rev()
        .take_while(|character| *character == '!')
        .count();
    ensure!(
        trailing_bangs <= 1,
        "Conventional Commit subject permits at most one breaking-change marker"
    );
    let breaking_subject = trailing_bangs == 1;
    let prefix = prefix.strip_suffix('!').unwrap_or(prefix);
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
    validate_dependabot_context(repository, requested_sha)?;
    let Some(pull) = matching_dependabot_pull(repository, requested_sha)? else {
        println!("no open Dependabot pull request matches {requested_sha}");
        return Ok(());
    };
    let commits = load_dependabot_commits(repository, &pull, requested_sha)?;
    ensure!(
        dependabot_commits_verified(&commits, requested_sha),
        "every auto-merged commit must be verified and authored by Dependabot"
    );
    if !dependabot_patch_only(commits.iter().map(|commit| commit.commit.message.as_str())) {
        println!(
            "Dependabot pull request #{} is not a verified patch-only update",
            pull.number
        );
        return Ok(());
    }

    merge_dependabot_pull(&pull, requested_sha)?;
    println!(
        "squash-merged checked Dependabot pull request #{}",
        pull.number
    );
    Ok(())
}

fn validate_dependabot_context(repository: &str, requested_sha: &str) -> Result<()> {
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
    Ok(())
}

fn matching_dependabot_pull(repository: &str, requested_sha: &str) -> Result<Option<PullRequest>> {
    let pulls_json = run(
        "gh",
        [
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            &format!("repos/{repository}/commits/{requested_sha}/pulls?per_page=100"),
        ],
    )?;
    let pulls: Vec<PullRequest> = serde_json::from_str(&pulls_json)?;
    ensure!(
        pulls.len() < 100,
        "associated-pull lookup reached its page limit and is ambiguous"
    );
    let mut eligible = pulls.into_iter().filter(|pull| {
        pull.state == "open"
            && pull.user.login == "dependabot[bot]"
            && pull.base.name == "main"
            && pull.head.sha == requested_sha
    });
    let pull = eligible.next();
    ensure!(
        eligible.next().is_none(),
        "multiple Dependabot pull requests match one head SHA"
    );
    Ok(pull)
}

fn load_dependabot_commits(
    repository: &str,
    pull: &PullRequest,
    requested_sha: &str,
) -> Result<Vec<PullCommit>> {
    let details_json = run(
        "gh",
        ["api", &format!("repos/{repository}/pulls/{}", pull.number)],
    )?;
    let details: PullRequestDetails = serde_json::from_str(&details_json)?;
    ensure!(
        details.number == pull.number
            && details.node_id == pull.node_id
            && details.head.sha == requested_sha,
        "Dependabot pull request changed during verification"
    );
    ensure!(
        details.commits <= 250,
        "Dependabot pull request exceeds the verifiable commit limit"
    );
    let mut commits = Vec::with_capacity(details.commits);
    for page in 1..=3 {
        let commits_json = run(
            "gh",
            [
                "api",
                &format!(
                    "repos/{repository}/pulls/{}/commits?per_page=100&page={page}",
                    pull.number
                ),
            ],
        )?;
        let mut page_commits: Vec<PullCommit> = serde_json::from_str(&commits_json)?;
        let page_len = page_commits.len();
        commits.append(&mut page_commits);
        if page_len < 100 {
            break;
        }
    }
    ensure!(
        commits.len() == details.commits,
        "Dependabot commit pagination was incomplete"
    );
    Ok(commits)
}

fn merge_dependabot_pull(pull: &PullRequest, requested_sha: &str) -> Result<()> {
    run(
        "gh",
        [
            "api",
            "graphql",
            "-f",
            DEPENDABOT_MERGE_QUERY,
            "-f",
            &format!("id={}", pull.node_id),
            "-f",
            &format!("headline={}", pull.title),
            "-f",
            &format!("oid={requested_sha}"),
        ],
    )?;
    Ok(())
}

fn dependabot_commits_verified(commits: &[PullCommit], requested_sha: &str) -> bool {
    commits.last().map(|commit| commit.sha.as_str()) == Some(requested_sha)
        && commits.iter().all(|commit| {
            commit.author.as_ref().map(|author| author.login.as_str()) == Some("dependabot[bot]")
                && commit.commit.verification.verified
        })
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
        assert!(parse_bump("feat!!: change").is_err());
        assert!(parse_bump("feat(scope)!!: change").is_err());
    }

    #[test]
    fn noreply_validation_accepts_only_github_noreply_domains() {
        assert!(is_noreply("123+contributor@users.noreply.github.com"));
        assert!(is_noreply("noreply@github.com"));
        assert!(!is_noreply("developer@example.com"));
        assert!(commit_identity_is_allowed(
            "developer@example.com",
            "noreply@github.com",
            true,
        ));
        assert!(!commit_identity_is_allowed(
            "developer@example.com",
            "noreply@github.com",
            false,
        ));
        assert!(github_squash_identity("GitHub", "noreply@github.com"));
        assert!(!github_squash_identity("maintainer", "noreply@github.com"));
    }

    #[test]
    fn content_audit_rejects_sensitive_added_lines() {
        assert!(audit_content("safe.md", "safe public content").is_ok());
        let private_path = format!("path from /{}/example", "home");
        let token = format!("{}{}example", "github", "_pat_");
        assert!(audit_content("bad.md", &private_path).is_err());
        assert!(audit_content("bad.md", &token).is_err());
        assert!(audit_content(RELEASE_HELPER_SOURCE, include_str!("main.rs")).is_ok());

        let old_pattern = ["github", "_pat_"].concat();
        let old_detector_line = format!("    {old_pattern:?},");
        assert!(audit_content(RELEASE_HELPER_SOURCE, &old_detector_line).is_ok());
        let disguised_leak = format!("let value = \"{old_pattern}actual-value\";");
        assert!(audit_content(RELEASE_HELPER_SOURCE, &disguised_leak).is_err());
    }

    #[test]
    fn dependabot_metadata_must_cover_only_patch_updates() {
        let patch = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-patch\n...";
        let mixed = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-patch\n- dependency-name: tokio\n  update-type: version-update:semver-minor\n...";
        let incomplete = "---\nupdated-dependencies:\n- dependency-name: serde\n...";
        assert!(dependabot_patch_only([patch]));
        assert!(!dependabot_patch_only([mixed]));
        assert!(!dependabot_patch_only([incomplete]));
        assert!(!DEPENDABOT_MERGE_QUERY.contains("authorEmail"));
        assert!(DEPENDABOT_MERGE_QUERY.contains("mergePullRequest"));
        assert!(!DEPENDABOT_MERGE_QUERY.contains("enablePullRequestAutoMerge"));
        assert!(DEPENDABOT_MERGE_QUERY.contains("expectedHeadOid"));
    }

    #[test]
    fn dependabot_version_updates_are_patch_only() {
        let configuration = include_str!("../../../.github/dependabot.yml");
        assert_eq!(
            configuration.matches("version-update:semver-minor").count(),
            4
        );
        assert_eq!(
            configuration.matches("version-update:semver-major").count(),
            4
        );
    }

    #[test]
    fn dependabot_verification_covers_commits_beyond_first_page() {
        let mut commits: Vec<PullCommit> = (0..31)
            .map(|index| PullCommit {
                sha: format!("{index:040x}"),
                author: Some(Login {
                    login: "dependabot[bot]".to_owned(),
                }),
                commit: CommitData {
                    message: "chore: patch dependency".to_owned(),
                    verification: CommitVerification { verified: true },
                },
            })
            .collect();
        let head = commits.last().expect("head commit").sha.clone();
        assert!(dependabot_commits_verified(&commits, &head));
        commits[30].author = Some(Login {
            login: "maintainer".to_owned(),
        });
        assert!(!dependabot_commits_verified(&commits, &head));
    }

    #[test]
    fn github_squash_exception_requires_verified_web_flow_commit() {
        let mut details = GitHubCommitDetails {
            sha: "a".repeat(40),
            committer: Some(Login {
                login: "web-flow".to_owned(),
            }),
            commit: GitHubCommitData {
                verification: GitHubCommitVerification {
                    verified: true,
                    reason: "valid".to_owned(),
                },
            },
        };
        assert!(github_commit_details_are_verified(
            &details,
            &"a".repeat(40)
        ));
        details.committer = Some(Login {
            login: "maintainer".to_owned(),
        });
        assert!(!github_commit_details_are_verified(
            &details,
            &"a".repeat(40)
        ));
        details.committer = Some(Login {
            login: "web-flow".to_owned(),
        });
        details.commit.verification.verified = false;
        assert!(!github_commit_details_are_verified(
            &details,
            &"a".repeat(40)
        ));
        assert!(!github_commit_details_are_verified(
            &details,
            &"b".repeat(40)
        ));
    }

    #[test]
    fn release_requires_successful_github_actions_aggregate() {
        let successful: CheckRuns = serde_json::from_value(serde_json::json!({
            "check_runs": [{
                "name": "CI aggregate",
                "status": "completed",
                "conclusion": "success",
                "app": {"slug": "github-actions"}
            }]
        }))
        .expect("valid checks response");
        assert!(ci_passed(&successful));

        let failed: CheckRuns = serde_json::from_value(serde_json::json!({
            "check_runs": [{
                "name": "CI aggregate",
                "status": "completed",
                "conclusion": "failure",
                "app": {"slug": "github-actions"}
            }]
        }))
        .expect("valid checks response");
        assert!(!ci_passed(&failed));
    }

    #[test]
    fn every_workspace_crate_is_non_publishable() {
        let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("workspace root");
        let output = Command::new("cargo")
            .args(["metadata", "--no-deps", "--format-version", "1"])
            .current_dir(workspace)
            .output()
            .expect("run cargo metadata");
        assert!(output.status.success());
        let metadata: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("metadata JSON");
        for package in metadata["packages"].as_array().expect("package list") {
            assert_eq!(
                package["publish"],
                serde_json::json!([]),
                "{} must set publish = false",
                package["name"]
            );
        }
    }

    #[test]
    fn ci_guards_component_deletion_and_rust_policy_changes() {
        let workflow = include_str!("../../../.github/workflows/ci.yml");
        let makefile = include_str!("../../../Makefile");
        for manifest in [
            "Cargo.toml",
            "buf.yaml",
            "operator/go.mod",
            "website/package.json",
            "ui/package.json",
            "tests/integration/Cargo.toml",
            "deploy/docker-bake.hcl",
            "tests/e2e/Cargo.toml",
            "benchmarks/Cargo.toml",
        ] {
            assert!(
                workflow.contains(&format!("exists_at_head_or_base {manifest}")),
                "CI must check {manifest} at both head and base"
            );
        }
        for policy in ["deny\\.toml", "rustfmt\\.toml", "^\\.cargo/", "^Makefile"] {
            assert!(
                workflow.contains(policy),
                "Rust CI trigger must include {policy}"
            );
        }
        for command in [
            "go mod tidy",
            "go mod verify",
            "go test -race ./...",
            "go vet ./...",
            "go build ./...",
            "go tool govulncheck ./...",
            "go tool controller-gen",
        ] {
            assert!(
                makefile.contains(command),
                "operator CI target must run {command}"
            );
        }
        assert!(workflow.contains("bufbuild/buf-action@fd21066df7214747548607aaa45548ba2b9bc1ff"));
        assert!(!workflow.contains("bufbuild/buf-setup-action"));
        assert!(workflow.contains("run: make go-check"));
    }
}
