//! Deterministic `SemVer` release and public-repository audit tooling.

use std::collections::HashSet;
use std::env;
use std::ffi::OsStr;
use std::hash::Hash;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use semver::Version;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const FIRST_VERSION: Version = Version::new(0, 1, 0);
const RELEASE_MARKER: &str = "crates/pgshard-release/RELEASE_START";
const RELEASE_HELPER_SOURCE: &str = "crates/pgshard-release/src/main.rs";
const RELEASE_CHECK_WAIT_TIMEOUT: Duration = Duration::from_mins(15);
const RELEASE_CHECK_POLL_INTERVAL: Duration = Duration::from_secs(10);
const CI_WORKFLOW_PATH: &str = ".github/workflows/ci.yml";
const CODEQL_WORKFLOW_PATH: &str = "dynamic/github-code-scanning/codeql";
const REQUIRED_CODEQL_ANALYSES: [&str; 4] = [
    "Analyze (actions)",
    "Analyze (go)",
    "Analyze (javascript-typescript)",
    "Analyze (rust)",
];
const UNPRIVILEGED_DEPENDABOT_PATHS: [&str; 4] = [
    "operator/go.mod",
    "operator/go.sum",
    "crates/pgshard-pgwire/fuzz/Cargo.toml",
    "crates/pgshard-pgwire/fuzz/Cargo.lock",
];
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
    state: String,
    merged: bool,
    merge_commit_sha: Option<String>,
    base: PullRef,
    head: PullRef,
    commits: usize,
    changed_files: usize,
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
struct GitReference {
    object: GitReferenceObject,
}

#[derive(Debug, Deserialize)]
struct GitReferenceObject {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct CompareResult {
    status: String,
    behind_by: u64,
    merge_base_commit: CompareCommit,
}

#[derive(Debug, Deserialize)]
struct CompareCommit {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct Workflows {
    total_count: usize,
    workflows: Vec<Workflow>,
}

#[derive(Debug, Deserialize)]
struct Workflow {
    id: u64,
    path: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct WorkflowRuns {
    total_count: usize,
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkflowRun {
    id: u64,
    workflow_id: u64,
    head_branch: String,
    head_sha: String,
    path: String,
    event: String,
    status: String,
    conclusion: Option<String>,
    run_attempt: u64,
}

#[derive(Debug, Deserialize)]
struct WorkflowJobs {
    total_count: usize,
    jobs: Vec<WorkflowJob>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkflowJob {
    id: u64,
    run_id: u64,
    run_attempt: u64,
    head_sha: String,
    name: String,
    status: String,
    conclusion: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowDispatch {
    workflow_run_id: u64,
}

#[derive(Debug, Deserialize)]
struct PullCommit {
    sha: String,
    author: Option<Login>,
    commit: CommitData,
}

#[derive(Debug, Deserialize)]
struct PullFile {
    filename: String,
    status: String,
    previous_filename: Option<String>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateState {
    Passed,
    Pending,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReleaseWorkflows {
    ci: u64,
    codeql: u64,
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
            let content = git_bytes(&["show", &format!("{commit}:{path}")])?;
            audit_content_bytes(path, &content)?;
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

fn audit_content_bytes(path: &str, content: &[u8]) -> Result<()> {
    audit_content(path, &String::from_utf8_lossy(content))
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

    let repository = env::var("GITHUB_REPOSITORY").context("GITHUB_REPOSITORY is required")?;
    ensure!(
        main_contains_commit(&repository, &sha)?,
        "release commit {sha} is not reachable from current main"
    );

    if let Some(existing) = semver_tag_at(&sha)? {
        ensure_release_exists(&existing, &sha)?;
        println!("release {existing} already exists for {sha}");
        return Ok(());
    }

    let plan = release_plan(&sha)?;
    ensure!(!plan.is_empty(), "no releasable first-parent commits found");

    // One workflow for a descendant may run before an ancestor's workflow.
    // Publish the complete gap oldest-first and wait for each exact aggregate;
    // the later job then becomes an idempotent verification.
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

    ensure_release_checks_passed(repository, &release.sha)?;

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

fn ensure_release_checks_passed(repository: &str, sha: &str) -> Result<()> {
    let started = Instant::now();
    loop {
        let workflows = release_workflows(repository)?;
        let ci_runs = exact_workflow_runs(repository, workflows.ci, sha)?;
        let codeql_runs = exact_workflow_runs(repository, workflows.codeql, sha)?;
        let ci = match latest_ci_run(&ci_runs, workflows.ci, sha) {
            Some(run) => ci_run_state(run, &workflow_jobs(repository, run)?),
            None => AggregateState::Pending,
        };
        let codeql = match latest_codeql_run(&codeql_runs, workflows.codeql, sha) {
            Some(run) if run.status == "completed" => {
                codeql_run_state(run, &workflow_jobs(repository, run)?)
            }
            Some(_) | None => AggregateState::Pending,
        };
        if ci == AggregateState::Failed {
            bail!("commit {sha} has a failed latest trusted CI aggregate");
        }
        if codeql == AggregateState::Failed {
            bail!("commit {sha} has a failed latest trusted CodeQL run");
        }
        if ci == AggregateState::Passed && codeql == AggregateState::Passed {
            return Ok(());
        }
        if started.elapsed() >= RELEASE_CHECK_WAIT_TIMEOUT {
            bail!("timed out waiting for exact-head CI and CodeQL checks on commit {sha}");
        }
        println!("waiting for exact-head CI and CodeQL checks on ancestor {sha}");
        thread::sleep(RELEASE_CHECK_POLL_INTERVAL);
    }
}

fn release_workflows(repository: &str) -> Result<ReleaseWorkflows> {
    let endpoint = format!("repos/{repository}/actions/workflows?per_page=100");
    let pages: Vec<Workflows> = github_api_pages(&endpoint)?;
    let workflows = flatten_counted_pages(
        pages,
        |page| page.total_count,
        |page| page.workflows,
        "workflow catalog",
    )?;
    ensure_unique_by(&workflows, |workflow| workflow.id, "workflow catalog")?;
    Ok(ReleaseWorkflows {
        ci: required_active_workflow(&workflows, CI_WORKFLOW_PATH)?,
        codeql: required_active_workflow(&workflows, CODEQL_WORKFLOW_PATH)?,
    })
}

fn required_active_workflow(workflows: &[Workflow], path: &str) -> Result<u64> {
    let matching = workflows
        .iter()
        .filter(|workflow| workflow.path == path && workflow.state == "active")
        .collect::<Vec<_>>();
    ensure!(
        matching.len() == 1,
        "expected one active trusted workflow at {path}"
    );
    Ok(matching[0].id)
}

fn exact_workflow_runs(repository: &str, workflow_id: u64, sha: &str) -> Result<Vec<WorkflowRun>> {
    let endpoint = format!(
        "repos/{repository}/actions/workflows/{workflow_id}/runs?head_sha={sha}&per_page=100"
    );
    let pages: Vec<WorkflowRuns> = github_api_pages(&endpoint)?;
    let runs = flatten_counted_pages(
        pages,
        |page| page.total_count,
        |page| page.workflow_runs,
        "exact-SHA workflow runs",
    )?;
    ensure_unique_by(
        &runs,
        |run| (run.id, run.run_attempt),
        "exact-SHA workflow runs",
    )?;
    ensure!(
        runs.iter()
            .all(|run| run.workflow_id == workflow_id && run.head_sha == sha),
        "GitHub returned a mismatched exact-SHA workflow run"
    );
    Ok(runs)
}

fn workflow_jobs(repository: &str, run: &WorkflowRun) -> Result<Vec<WorkflowJob>> {
    let endpoint = format!(
        "repos/{repository}/actions/runs/{}/attempts/{}/jobs?per_page=100",
        run.id, run.run_attempt
    );
    let pages: Vec<WorkflowJobs> = github_api_pages(&endpoint)?;
    let jobs = flatten_counted_pages(
        pages,
        |page| page.total_count,
        |page| page.jobs,
        "workflow jobs",
    )?;
    ensure_unique_by(&jobs, |job| job.id, "workflow jobs")?;
    ensure!(
        jobs_belong_to_run(run, &jobs),
        "GitHub returned a job from another workflow run or attempt"
    );
    Ok(jobs)
}

fn github_api_pages<T: DeserializeOwned>(endpoint: &str) -> Result<Vec<T>> {
    let response = run(
        "gh",
        [
            "api",
            "--paginate",
            "--slurp",
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            endpoint,
        ],
    )?;
    serde_json::from_str(&response).context("GitHub returned invalid paginated JSON")
}

fn flatten_counted_pages<P, T, C, I>(
    pages: Vec<P>,
    count: C,
    into_items: I,
    description: &str,
) -> Result<Vec<T>>
where
    C: Fn(&P) -> usize,
    I: Fn(P) -> Vec<T>,
{
    ensure!(!pages.is_empty(), "GitHub returned no {description} page");
    let expected = count(&pages[0]);
    ensure!(
        pages.iter().all(|page| count(page) == expected),
        "GitHub changed the {description} count while paginating"
    );
    let items = pages.into_iter().flat_map(into_items).collect::<Vec<_>>();
    ensure!(
        items.len() == expected,
        "GitHub returned an incomplete {description} result"
    );
    Ok(items)
}

fn ensure_unique_by<T, K, F>(items: &[T], key: F, description: &str) -> Result<()>
where
    K: Eq + Hash,
    F: Fn(&T) -> K,
{
    let mut seen = HashSet::with_capacity(items.len());
    ensure!(
        items.iter().all(|item| seen.insert(key(item))),
        "GitHub returned duplicate {description} entries"
    );
    Ok(())
}

fn jobs_belong_to_run(run: &WorkflowRun, jobs: &[WorkflowJob]) -> bool {
    jobs.iter().all(|job| {
        job.run_id == run.id && job.run_attempt == run.run_attempt && job.head_sha == run.head_sha
    })
}

fn latest_ci_run<'a>(
    runs: &'a [WorkflowRun],
    workflow_id: u64,
    sha: &str,
) -> Option<&'a WorkflowRun> {
    runs.iter()
        .filter(|run| {
            run.workflow_id == workflow_id
                && run.path == CI_WORKFLOW_PATH
                && run.head_sha == sha
                && ((run.event == "push" && run.head_branch == "main")
                    || run.event == "workflow_dispatch")
        })
        .max_by_key(|run| (run.id, run.run_attempt))
}

fn latest_codeql_run<'a>(
    runs: &'a [WorkflowRun],
    workflow_id: u64,
    sha: &str,
) -> Option<&'a WorkflowRun> {
    runs.iter()
        .filter(|run| {
            run.workflow_id == workflow_id
                && run.path == CODEQL_WORKFLOW_PATH
                && run.head_sha == sha
                && run.head_branch == "main"
                && run.event == "dynamic"
        })
        .max_by_key(|run| (run.id, run.run_attempt))
}

fn ci_run_state(run: &WorkflowRun, jobs: &[WorkflowJob]) -> AggregateState {
    let aggregates = jobs
        .iter()
        .filter(|job| job.name == "CI aggregate")
        .collect::<Vec<_>>();
    match aggregates.as_slice() {
        [job] if job.status != "completed" => AggregateState::Pending,
        [job] if job.conclusion.as_deref() == Some("success") => AggregateState::Passed,
        [] if run.status != "completed" => AggregateState::Pending,
        _ => AggregateState::Failed,
    }
}

fn codeql_run_state(run: &WorkflowRun, jobs: &[WorkflowJob]) -> AggregateState {
    if run.status != "completed" {
        return AggregateState::Pending;
    }
    if run.conclusion.as_deref() != Some("success") {
        return AggregateState::Failed;
    }
    let mut pending = false;
    for name in REQUIRED_CODEQL_ANALYSES {
        let matching = jobs
            .iter()
            .filter(|job| job.name == name)
            .collect::<Vec<_>>();
        match matching.as_slice() {
            [job] if job.status != "completed" => pending = true,
            [job] if job.conclusion.as_deref() == Some("success") => {}
            _ => return AggregateState::Failed,
        }
    }
    if pending {
        AggregateState::Pending
    } else {
        AggregateState::Passed
    }
}

fn ci_passed(checks: &CheckRuns) -> bool {
    aggregate_state(checks) == AggregateState::Passed
}

fn aggregate_state(checks: &CheckRuns) -> AggregateState {
    let aggregates = checks
        .check_runs
        .iter()
        .filter(|check| check.name == "CI aggregate" && check.app.slug == "github-actions")
        .collect::<Vec<_>>();
    if aggregates
        .iter()
        .any(|check| check.status == "completed" && check.conclusion.as_deref() == Some("success"))
    {
        AggregateState::Passed
    } else if aggregates.is_empty() || aggregates.iter().any(|check| check.status != "completed") {
        AggregateState::Pending
    } else {
        AggregateState::Failed
    }
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
        println!("no Dependabot pull request matches {requested_sha}");
        return Ok(());
    };
    let (details, commits) = load_dependabot_commits(repository, &pull, requested_sha)?;
    let files = load_dependabot_files(repository, &pull, details.changed_files)?;
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
    if !dependabot_files_are_unprivileged(&files) {
        println!(
            "Dependabot pull request #{} changes files outside the unattended dependency-file allowlist and requires manual review",
            pull.number
        );
        return Ok(());
    }

    let merge_sha = if dependabot_already_merged(&details)? {
        println!(
            "Dependabot pull request #{} was already squash-merged",
            pull.number
        );
        details
            .merge_commit_sha
            .clone()
            .context("merged Dependabot pull request has no merge commit")?
    } else {
        if !dependabot_checks_passed(repository, requested_sha)? {
            println!(
                "Dependabot pull request #{} is waiting for successful CI and CodeQL with every check terminal",
                pull.number
            );
            return Ok(());
        }
        if !dependabot_base_is_current(repository, requested_sha)? {
            println!(
                "Dependabot pull request #{} is waiting for a rebase onto current main",
                pull.number
            );
            return Ok(());
        }
        let merge_sha = merge_dependabot_pull(repository, &pull, requested_sha)?;
        println!(
            "squash-merged checked Dependabot pull request #{}",
            pull.number
        );
        merge_sha
    };
    ensure!(
        main_contains_commit(repository, &merge_sha)?,
        "Dependabot squash commit is not reachable from current main"
    );
    ensure!(
        github_commit_is_verified(&merge_sha)?,
        "Dependabot squash commit is not a valid signed GitHub web-flow commit"
    );
    dispatch_exact_ci(repository, &merge_sha)?;
    println!("ensured CI exists for exact Dependabot squash {merge_sha}");
    Ok(())
}

fn dependabot_already_merged(details: &PullRequestDetails) -> Result<bool> {
    match (details.state.as_str(), details.merged) {
        ("open", false) => Ok(false),
        ("closed", true) if details.merge_commit_sha.is_some() => Ok(true),
        _ => bail!("Dependabot pull request has inconsistent merge state"),
    }
}

fn dependabot_checks_passed(repository: &str, requested_sha: &str) -> Result<bool> {
    let response = run(
        "gh",
        [
            "api",
            "-H",
            "Accept: application/vnd.github+json",
            &format!(
                "repos/{repository}/commits/{requested_sha}/check-runs?filter=latest&per_page=100"
            ),
        ],
    )?;
    let checks: CheckRuns = serde_json::from_str(&response)?;
    ensure!(
        checks.check_runs.len() < 100,
        "Dependabot check-run lookup reached its page limit and is ambiguous"
    );
    Ok(
        ci_passed(&checks)
            && codeql_passed(&checks)
            && all_checks_terminal_without_failure(&checks),
    )
}

fn codeql_passed(checks: &CheckRuns) -> bool {
    let mut summaries = checks
        .check_runs
        .iter()
        .filter(|check| check.name == "CodeQL" && check.app.slug == "github-advanced-security")
        .peekable();
    summaries.peek().is_some()
        && summaries.all(|check| {
            check.status == "completed" && check.conclusion.as_deref() == Some("success")
        })
}

fn all_checks_terminal_without_failure(checks: &CheckRuns) -> bool {
    !checks.check_runs.is_empty()
        && checks.check_runs.iter().all(|check| {
            check.status == "completed"
                && matches!(
                    check.conclusion.as_deref(),
                    Some("success" | "neutral" | "skipped")
                )
        })
}

fn dependabot_base_is_current(repository: &str, requested_sha: &str) -> Result<bool> {
    let main_sha = run(
        "gh",
        [
            "api",
            &format!("repos/{repository}/git/ref/heads/main"),
            "--jq",
            ".object.sha",
        ],
    )?;
    let response = run(
        "gh",
        [
            "api",
            &format!("repos/{repository}/compare/{main_sha}...{requested_sha}"),
        ],
    )?;
    let comparison: CompareResult = serde_json::from_str(&response)?;
    Ok(compare_contains_base(&comparison, &main_sha))
}

fn compare_contains_base(comparison: &CompareResult, base_sha: &str) -> bool {
    comparison.behind_by == 0
        && comparison.merge_base_commit.sha == base_sha
        && matches!(comparison.status.as_str(), "ahead" | "identical")
}

fn main_contains_commit(repository: &str, commit_sha: &str) -> Result<bool> {
    let main_sha = run(
        "gh",
        [
            "api",
            &format!("repos/{repository}/git/ref/heads/main"),
            "--jq",
            ".object.sha",
        ],
    )?;
    let response = run(
        "gh",
        [
            "api",
            &format!("repos/{repository}/compare/{commit_sha}...{main_sha}"),
        ],
    )?;
    let comparison: CompareResult = serde_json::from_str(&response)?;
    Ok(compare_contains_base(&comparison, commit_sha))
}

fn validate_dependabot_context(repository: &str, requested_sha: &str) -> Result<()> {
    ensure!(
        env::var("GITHUB_ACTIONS").as_deref() == Ok("true"),
        "Dependabot auto-merge may only run in GitHub Actions"
    );
    ensure!(
        env::var("GITHUB_REPOSITORY").as_deref() == Ok(repository),
        "Dependabot auto-merge repository must match GITHUB_REPOSITORY"
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
        is_complete_sha(requested_sha),
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
        (pull.state == "open" || pull.state == "closed")
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
) -> Result<(PullRequestDetails, Vec<PullCommit>)> {
    let details_json = run(
        "gh",
        ["api", &format!("repos/{repository}/pulls/{}", pull.number)],
    )?;
    let details: PullRequestDetails = serde_json::from_str(&details_json)?;
    ensure!(
        details.number == pull.number
            && details.node_id == pull.node_id
            && details.state == pull.state
            && details.base.name == pull.base.name
            && details.base.sha == pull.base.sha
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
    Ok((details, commits))
}

fn load_dependabot_files(
    repository: &str,
    pull: &PullRequest,
    expected_files: usize,
) -> Result<Vec<PullFile>> {
    ensure!(
        expected_files <= 250,
        "Dependabot pull request exceeds the verifiable changed-file limit"
    );
    let mut files = Vec::with_capacity(expected_files);
    for page in 1..=3 {
        let files_json = run(
            "gh",
            [
                "api",
                &format!(
                    "repos/{repository}/pulls/{}/files?per_page=100&page={page}",
                    pull.number
                ),
            ],
        )?;
        let mut page_files: Vec<PullFile> = serde_json::from_str(&files_json)?;
        let page_len = page_files.len();
        files.append(&mut page_files);
        if page_len < 100 {
            break;
        }
    }
    ensure!(
        files.len() == expected_files,
        "Dependabot changed-file pagination was incomplete"
    );
    Ok(files)
}

fn dependabot_files_are_unprivileged(files: &[PullFile]) -> bool {
    !files.is_empty()
        && files.iter().all(|file| {
            file.status == "modified"
                && file.previous_filename.is_none()
                && UNPRIVILEGED_DEPENDABOT_PATHS.contains(&file.filename.as_str())
        })
}

fn merge_dependabot_pull(
    repository: &str,
    pull: &PullRequest,
    requested_sha: &str,
) -> Result<String> {
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
    let details_json = run(
        "gh",
        ["api", &format!("repos/{repository}/pulls/{}", pull.number)],
    )?;
    let details: PullRequestDetails = serde_json::from_str(&details_json)?;
    let merge_sha = details
        .merge_commit_sha
        .clone()
        .context("merged Dependabot pull request has no merge commit")?;
    ensure!(
        details.number == pull.number
            && details.node_id == pull.node_id
            && details.state == "closed"
            && details.merged
            && details.base.name == "main"
            && details.head.sha == requested_sha
            && is_complete_sha(&merge_sha),
        "GitHub did not report the exact Dependabot pull request as merged"
    );
    Ok(merge_sha)
}

fn dispatch_exact_ci(repository: &str, merge_sha: &str) -> Result<()> {
    ensure!(
        is_complete_sha(merge_sha),
        "merge SHA must be a complete hexadecimal object ID"
    );
    let existing_runs = exact_ci_dispatches(repository, merge_sha)?;
    if !existing_runs.is_empty() {
        let run_ids = existing_runs
            .iter()
            .map(|run| run.id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        println!("exact-SHA CI was already dispatched in run(s) {run_ids}");
        return Ok(());
    }

    let ref_name = format!("pgshard-ci-{merge_sha}");
    if let Some(existing) = remote_tag_target(repository, &ref_name)? {
        ensure!(
            existing == merge_sha,
            "temporary CI ref points to another commit"
        );
    } else {
        run(
            "gh",
            [
                "api",
                "--method",
                "POST",
                &format!("repos/{repository}/git/refs"),
                "-f",
                &format!("ref=refs/tags/{ref_name}"),
                "-f",
                &format!("sha={merge_sha}"),
            ],
        )?;
        ensure!(
            remote_tag_target(repository, &ref_name)?.as_deref() == Some(merge_sha),
            "GitHub did not create the exact temporary CI ref"
        );
    }
    let response = run(
        "gh",
        [
            "api",
            "--method",
            "POST",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            &format!("repos/{repository}/actions/workflows/ci.yml/dispatches"),
            "-f",
            &format!("ref={ref_name}"),
        ],
    )?;
    let dispatch: WorkflowDispatch = serde_json::from_str(&response)?;
    let run_json = run(
        "gh",
        [
            "api",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            &format!(
                "repos/{repository}/actions/runs/{}",
                dispatch.workflow_run_id
            ),
        ],
    )?;
    let workflow_run: WorkflowRun = serde_json::from_str(&run_json)?;
    ensure!(
        is_exact_dispatch(
            &workflow_run,
            dispatch.workflow_run_id,
            merge_sha,
            &ref_name,
        ),
        "GitHub dispatched CI for a different commit or event"
    );
    Ok(())
}

fn is_exact_dispatch(
    run: &WorkflowRun,
    expected_id: u64,
    expected_sha: &str,
    expected_ref: &str,
) -> bool {
    run.id == expected_id
        && run.head_branch == expected_ref
        && run.head_sha == expected_sha
        && run.event == "workflow_dispatch"
}

fn exact_ci_dispatches(repository: &str, merge_sha: &str) -> Result<Vec<WorkflowRun>> {
    let workflow_id = release_workflows(repository)?.ci;
    Ok(exact_workflow_runs(repository, workflow_id, merge_sha)?
        .into_iter()
        .filter(|run| {
            run.path == CI_WORKFLOW_PATH
                && run.event == "workflow_dispatch"
                && run.head_sha == merge_sha
        })
        .collect())
}

fn remote_tag_target(repository: &str, ref_name: &str) -> Result<Option<String>> {
    let output = Command::new("gh")
        .args([
            "api",
            &format!("repos/{repository}/git/ref/tags/{ref_name}"),
        ])
        .output()
        .context("failed to inspect temporary GitHub ref")?;
    if output.status.success() {
        let reference: GitReference = serde_json::from_slice(&output.stdout)?;
        return Ok(Some(reference.object.sha));
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("HTTP 404") {
        return Ok(None);
    }
    bail!(
        "gh failed to inspect temporary GitHub ref: {}",
        stderr.trim()
    )
}

fn is_complete_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .chars()
            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
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
    Ok(String::from_utf8(git_bytes(args)?)?.trim().to_owned())
}

fn git_bytes(args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(args)
        .output()
        .with_context(|| format!("failed to run git {}", args.join(" ")))?;
    output_bytes("git", output)
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
    Ok(String::from_utf8(output_bytes(program, output)?)?
        .trim()
        .to_owned())
}

fn output_bytes(program: &str, output: Output) -> Result<Vec<u8>> {
    if !output.status.success() {
        bail!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_workflow_run(
        id: u64,
        workflow_id: u64,
        path: &str,
        head_branch: &str,
        event: &str,
        status: &str,
        conclusion: Option<&str>,
    ) -> WorkflowRun {
        WorkflowRun {
            id,
            workflow_id,
            head_branch: head_branch.to_owned(),
            head_sha: "a".repeat(40),
            path: path.to_owned(),
            event: event.to_owned(),
            status: status.to_owned(),
            conclusion: conclusion.map(str::to_owned),
            run_attempt: 1,
        }
    }

    fn test_workflow_job(
        run: &WorkflowRun,
        name: &str,
        status: &str,
        conclusion: Option<&str>,
    ) -> WorkflowJob {
        let name_id = REQUIRED_CODEQL_ANALYSES
            .iter()
            .position(|required| *required == name)
            .map_or(50, |index| index as u64 + 1);
        WorkflowJob {
            id: run.id * 100 + name_id,
            run_id: run.id,
            run_attempt: run.run_attempt,
            head_sha: run.head_sha.clone(),
            name: name.to_owned(),
            status: status.to_owned(),
            conclusion: conclusion.map(str::to_owned),
        }
    }

    fn successful_codeql_jobs(run: &WorkflowRun) -> Vec<WorkflowJob> {
        REQUIRED_CODEQL_ANALYSES
            .iter()
            .map(|name| test_workflow_job(run, name, "completed", Some("success")))
            .collect()
    }

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
    fn dependabot_merge_dispatches_the_single_ci_workflow() {
        let workflow = include_str!("../../../.github/workflows/dependabot-automerge.yml");
        assert!(workflow.contains("actions: write"));
        assert!(workflow.contains("checks: read"));
        assert!(workflow.contains("dependabot-automerge"));
        assert!(workflow.contains("workflows: [CI, CodeQL]"));
        assert!(!workflow.contains("github.event.workflow_run.name"));
        assert!(workflow.contains("group: pgshard-dependabot-automerge"));
        assert_eq!(workflow.matches("queue: max").count(), 1);

        let ci = include_str!("../../../.github/workflows/ci.yml");
        assert!(ci.contains("workflow_dispatch"));
        assert!(ci.contains("github.event_name == 'workflow_dispatch'"));
        assert!(ci.contains("refs/tags/pgshard-ci-"));
        assert!(ci.contains("cleanup-dependabot-ci-ref:"));
        assert!(ci.contains("group: pgshard-source-release"));
        assert_eq!(ci.matches("queue: max").count(), 1);
        assert!(ci.contains("always() &&"));
    }

    #[test]
    fn exact_ci_refs_require_full_object_ids() {
        assert!(is_complete_sha(&"a".repeat(40)));
        assert!(!is_complete_sha(&"A".repeat(40)));
        assert!(!is_complete_sha(&"a".repeat(39)));
        assert!(!is_complete_sha(&format!("{}g", "a".repeat(39))));

        let run = test_workflow_run(
            17,
            7,
            CI_WORKFLOW_PATH,
            &format!("pgshard-ci-{}", "a".repeat(40)),
            "workflow_dispatch",
            "queued",
            None,
        );
        let expected_ref = format!("pgshard-ci-{}", "a".repeat(40));
        assert!(is_exact_dispatch(&run, 17, &"a".repeat(40), &expected_ref));
        assert!(!is_exact_dispatch(&run, 18, &"a".repeat(40), &expected_ref));
    }

    #[test]
    fn current_base_requires_the_main_commit_as_merge_base() {
        let main = "a".repeat(40);
        let mut comparison: CompareResult = serde_json::from_value(serde_json::json!({
            "status": "ahead",
            "behind_by": 0,
            "merge_base_commit": {"sha": main}
        }))
        .expect("valid comparison");
        assert!(compare_contains_base(&comparison, &"a".repeat(40)));

        comparison.status = "diverged".to_owned();
        comparison.behind_by = 1;
        comparison.merge_base_commit.sha = "b".repeat(40);
        assert!(!compare_contains_base(&comparison, &"a".repeat(40)));
    }

    #[test]
    fn dependabot_merge_state_supports_retry_after_merge() {
        let mut details = PullRequestDetails {
            number: 7,
            node_id: "node".to_owned(),
            state: "open".to_owned(),
            merged: false,
            // GitHub may expose a test-merge SHA while the pull request is open.
            merge_commit_sha: Some("a".repeat(40)),
            base: PullRef {
                name: "main".to_owned(),
                sha: "c".repeat(40),
            },
            head: PullRef {
                name: "dependabot/example".to_owned(),
                sha: "b".repeat(40),
            },
            commits: 1,
            changed_files: 2,
        };
        assert!(!dependabot_already_merged(&details).expect("open state"));

        details.state = "closed".to_owned();
        details.merged = true;
        assert!(dependabot_already_merged(&details).expect("merged retry"));

        details.merge_commit_sha = None;
        assert!(dependabot_already_merged(&details).is_err());
        details.merged = false;
        assert!(dependabot_already_merged(&details).is_err());
    }

    #[test]
    fn dependabot_requires_successful_codeql_and_terminal_checks() {
        let mut checks: CheckRuns = serde_json::from_value(serde_json::json!({
            "check_runs": [
                {
                    "name": "CI aggregate",
                    "status": "completed",
                    "conclusion": "success",
                    "app": {"slug": "github-actions"}
                },
                {
                    "name": "CodeQL",
                    "status": "completed",
                    "conclusion": "neutral",
                    "app": {"slug": "github-advanced-security"}
                },
                {
                    "name": "Not applicable",
                    "status": "completed",
                    "conclusion": "skipped",
                    "app": {"slug": "github-actions"}
                }
            ]
        }))
        .expect("valid check runs");
        assert!(ci_passed(&checks));
        assert!(!codeql_passed(&checks));
        assert!(all_checks_terminal_without_failure(&checks));

        checks.check_runs[1].conclusion = Some("success".to_owned());
        assert!(codeql_passed(&checks));

        let duplicate_neutral: CheckRun = serde_json::from_value(serde_json::json!({
            "name": "CodeQL",
            "status": "completed",
            "conclusion": "neutral",
            "app": { "slug": "github-advanced-security" }
        }))
        .expect("valid duplicate CodeQL check");
        checks.check_runs.push(duplicate_neutral);
        assert!(!codeql_passed(&checks));
        checks.check_runs.pop();

        checks.check_runs[1].status = "in_progress".to_owned();
        checks.check_runs[1].conclusion = None;
        assert!(!codeql_passed(&checks));
        assert!(!all_checks_terminal_without_failure(&checks));

        checks.check_runs[1].status = "completed".to_owned();
        checks.check_runs[1].conclusion = Some("failure".to_owned());
        assert!(!codeql_passed(&checks));
        assert!(!all_checks_terminal_without_failure(&checks));

        checks.check_runs.remove(1);
        assert!(!codeql_passed(&checks));
    }

    #[test]
    fn dependabot_auto_merge_excludes_privileged_dependency_paths() {
        let file = |filename: &str| PullFile {
            filename: filename.to_owned(),
            status: "modified".to_owned(),
            previous_filename: None,
        };
        assert!(dependabot_files_are_unprivileged(&[
            file("operator/go.mod"),
            file("operator/go.sum"),
        ]));
        assert!(dependabot_files_are_unprivileged(&[
            file("crates/pgshard-pgwire/fuzz/Cargo.toml"),
            file("crates/pgshard-pgwire/fuzz/Cargo.lock"),
        ]));
        assert!(!dependabot_files_are_unprivileged(&[
            file("website/package.json"),
            file("website/package-lock.json"),
        ]));
        assert!(!dependabot_files_are_unprivileged(&[file(
            ".github/workflows/ci.yml"
        )]));
        assert!(!dependabot_files_are_unprivileged(&[file("Cargo.lock")]));
        assert!(!dependabot_files_are_unprivileged(&[file(
            "crates/pgshard-pgwire/Cargo.toml"
        )]));
        assert!(!dependabot_files_are_unprivileged(&[]));

        let renamed = PullFile {
            filename: "operator/go.mod".to_owned(),
            status: "renamed".to_owned(),
            previous_filename: Some(".github/workflows/ci.yml".to_owned()),
        };
        assert!(!dependabot_files_are_unprivileged(&[renamed]));
    }

    #[test]
    fn dependabot_version_updates_are_patch_only() {
        let configuration = include_str!("../../../.github/dependabot.yml");
        assert!(configuration.contains("directory: /crates/pgshard-pgwire/fuzz"));
        assert_eq!(
            configuration.matches("version-update:semver-minor").count(),
            5
        );
        assert_eq!(
            configuration.matches("version-update:semver-major").count(),
            5
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
        assert_eq!(aggregate_state(&successful), AggregateState::Passed);

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
        assert_eq!(aggregate_state(&failed), AggregateState::Failed);

        let pending: CheckRuns = serde_json::from_value(serde_json::json!({
            "check_runs": [{
                "name": "CI aggregate",
                "status": "in_progress",
                "conclusion": null,
                "app": {"slug": "github-actions"}
            }]
        }))
        .expect("valid checks response");
        assert_eq!(aggregate_state(&pending), AggregateState::Pending);
        assert_eq!(
            aggregate_state(&CheckRuns {
                check_runs: Vec::new()
            }),
            AggregateState::Pending
        );
    }

    #[test]
    fn release_uses_only_the_latest_trusted_ci_run() {
        let sha = "a".repeat(40);
        let old_success = test_workflow_run(
            10,
            7,
            CI_WORKFLOW_PATH,
            "main",
            "push",
            "completed",
            Some("success"),
        );
        let newer_failure = test_workflow_run(
            11,
            7,
            CI_WORKFLOW_PATH,
            "main",
            "push",
            "completed",
            Some("failure"),
        );
        let impostor = test_workflow_run(
            99,
            99,
            "dynamic/untrusted",
            "main",
            "push",
            "completed",
            Some("success"),
        );
        let runs = vec![old_success.clone(), newer_failure.clone(), impostor];
        let selected = latest_ci_run(&runs, 7, &sha).expect("trusted CI run");
        assert_eq!(selected.id, newer_failure.id);
        assert_eq!(
            ci_run_state(
                selected,
                &[test_workflow_job(
                    selected,
                    "CI aggregate",
                    "completed",
                    Some("failure")
                )]
            ),
            AggregateState::Failed
        );

        let newer_success =
            test_workflow_run(12, 7, CI_WORKFLOW_PATH, "main", "push", "in_progress", None);
        let mut runs = vec![old_success, newer_failure, newer_success.clone()];
        let selected = latest_ci_run(&runs, 7, &sha).expect("newer CI run");
        assert_eq!(selected.id, newer_success.id);
        assert_eq!(
            ci_run_state(
                selected,
                &[test_workflow_job(
                    selected,
                    "CI aggregate",
                    "completed",
                    Some("success")
                )]
            ),
            AggregateState::Passed
        );

        let newer_pending =
            test_workflow_run(13, 7, CI_WORKFLOW_PATH, "main", "push", "in_progress", None);
        runs.push(newer_pending.clone());
        let selected = latest_ci_run(&runs, 7, &sha).expect("pending CI run");
        assert_eq!(selected.id, newer_pending.id);
        assert_eq!(ci_run_state(selected, &[]), AggregateState::Pending);
    }

    #[test]
    fn release_requires_one_successful_codeql_job_set_from_the_latest_run() {
        let sha = "a".repeat(40);
        let old_failure = test_workflow_run(
            20,
            8,
            CODEQL_WORKFLOW_PATH,
            "main",
            "dynamic",
            "completed",
            Some("failure"),
        );
        let newer_success = test_workflow_run(
            21,
            8,
            CODEQL_WORKFLOW_PATH,
            "main",
            "dynamic",
            "completed",
            Some("success"),
        );
        let wrong_branch = test_workflow_run(
            99,
            8,
            CODEQL_WORKFLOW_PATH,
            "topic",
            "dynamic",
            "completed",
            Some("success"),
        );
        let wrong_path = test_workflow_run(
            100,
            8,
            "dynamic/untrusted/codeql",
            "main",
            "dynamic",
            "completed",
            Some("success"),
        );
        let runs = vec![old_failure, newer_success.clone(), wrong_branch, wrong_path];
        let selected = latest_codeql_run(&runs, 8, &sha).expect("trusted CodeQL run");
        assert_eq!(selected.id, newer_success.id);

        let mut jobs = successful_codeql_jobs(selected);
        assert_eq!(codeql_run_state(selected, &jobs), AggregateState::Passed);

        let missing = jobs.pop().expect("required analysis");
        assert_eq!(codeql_run_state(selected, &jobs), AggregateState::Failed);
        jobs.push(missing);
        jobs.push(test_workflow_job(
            selected,
            REQUIRED_CODEQL_ANALYSES[0],
            "completed",
            Some("success"),
        ));
        assert_eq!(codeql_run_state(selected, &jobs), AggregateState::Failed);

        let mut pending = newer_success;
        pending.id = 22;
        pending.status = "in_progress".to_owned();
        pending.conclusion = None;
        assert_eq!(
            codeql_run_state(&pending, &successful_codeql_jobs(&pending)),
            AggregateState::Pending
        );
    }

    #[test]
    fn release_does_not_combine_codeql_jobs_across_runs() {
        let old = test_workflow_run(
            30,
            8,
            CODEQL_WORKFLOW_PATH,
            "main",
            "dynamic",
            "completed",
            Some("success"),
        );
        let new = test_workflow_run(
            31,
            8,
            CODEQL_WORKFLOW_PATH,
            "main",
            "dynamic",
            "completed",
            Some("success"),
        );
        let mut new_jobs = successful_codeql_jobs(&new);
        new_jobs.pop();
        let old_jobs = successful_codeql_jobs(&old);
        assert_eq!(codeql_run_state(&new, &new_jobs), AggregateState::Failed);
        assert_eq!(codeql_run_state(&old, &old_jobs), AggregateState::Passed);
    }

    #[test]
    fn counted_github_pages_accept_boundaries_and_reject_incomplete_results() {
        let run = test_workflow_run(
            1,
            7,
            CI_WORKFLOW_PATH,
            "main",
            "push",
            "completed",
            Some("success"),
        );
        let page_99 = vec![run.clone(); 99];
        assert_eq!(
            flatten_counted_pages(
                vec![WorkflowRuns {
                    total_count: 99,
                    workflow_runs: page_99.clone(),
                }],
                |page| page.total_count,
                |page| page.workflow_runs,
                "test runs",
            )
            .expect("complete 99-item page")
            .len(),
            99
        );
        assert_eq!(
            flatten_counted_pages(
                vec![
                    WorkflowRuns {
                        total_count: 100,
                        workflow_runs: page_99.clone(),
                    },
                    WorkflowRuns {
                        total_count: 100,
                        workflow_runs: vec![run.clone()],
                    },
                ],
                |page| page.total_count,
                |page| page.workflow_runs,
                "test runs",
            )
            .expect("complete split result")
            .len(),
            100
        );
        assert!(
            flatten_counted_pages(
                vec![WorkflowRuns {
                    total_count: 100,
                    workflow_runs: page_99.clone(),
                }],
                |page| page.total_count,
                |page| page.workflow_runs,
                "test runs",
            )
            .is_err()
        );
        assert!(
            flatten_counted_pages(
                vec![
                    WorkflowRuns {
                        total_count: 100,
                        workflow_runs: page_99,
                    },
                    WorkflowRuns {
                        total_count: 101,
                        workflow_runs: vec![run],
                    },
                ],
                |page| page.total_count,
                |page| page.workflow_runs,
                "test runs",
            )
            .is_err()
        );
    }

    #[test]
    fn workflow_jobs_must_share_one_run_attempt_and_commit() {
        let run = test_workflow_run(40, 7, CI_WORKFLOW_PATH, "main", "push", "in_progress", None);
        let job = test_workflow_job(&run, "CI aggregate", "completed", Some("success"));
        assert!(jobs_belong_to_run(&run, std::slice::from_ref(&job)));

        let mut wrong_run = job.clone();
        wrong_run.run_id += 1;
        assert!(!jobs_belong_to_run(&run, &[wrong_run]));
        let mut wrong_attempt = job.clone();
        wrong_attempt.run_attempt += 1;
        assert!(!jobs_belong_to_run(&run, &[wrong_attempt]));
        let mut wrong_sha = job.clone();
        wrong_sha.head_sha = "b".repeat(40);
        assert!(!jobs_belong_to_run(&run, &[wrong_sha]));

        assert!(ensure_unique_by(std::slice::from_ref(&job), |item| item.id, "test jobs").is_ok());
        assert!(ensure_unique_by(&[job.clone(), job], |item| item.id, "test jobs").is_err());
    }

    #[test]
    fn trusted_workflow_catalog_requires_one_active_exact_path() {
        let workflows = vec![
            Workflow {
                id: 7,
                path: CI_WORKFLOW_PATH.to_owned(),
                state: "active".to_owned(),
            },
            Workflow {
                id: 8,
                path: "dynamic/untrusted".to_owned(),
                state: "active".to_owned(),
            },
            Workflow {
                id: 9,
                path: CI_WORKFLOW_PATH.to_owned(),
                state: "disabled_manually".to_owned(),
            },
        ];
        assert_eq!(
            required_active_workflow(&workflows, CI_WORKFLOW_PATH).unwrap(),
            7
        );
        let mut duplicated = workflows;
        duplicated.push(Workflow {
            id: 10,
            path: CI_WORKFLOW_PATH.to_owned(),
            state: "active".to_owned(),
        });
        assert!(required_active_workflow(&duplicated, CI_WORKFLOW_PATH).is_err());
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
            "crates/pgshard-agent/Cargo.toml",
            "crates/pgshard-planner/Cargo.toml",
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
        let image_trigger = workflow
            .lines()
            .find(|line| line.contains("emit_component images"))
            .expect("image CI trigger");
        for input in [
            "^\\.dockerignore$",
            "^Cargo\\.(toml|lock)$",
            "^rust-toolchain\\.toml$",
            "^rustfmt\\.toml$",
        ] {
            assert!(
                image_trigger.contains(input),
                "image CI trigger must include {input}"
            );
        }
        let postgres_agent_trigger = workflow
            .lines()
            .find(|line| line.contains("emit_component postgres_agent"))
            .expect("PostgreSQL agent lifecycle trigger");
        for input in [
            "^crates/(pgshard-agent|pgshard-types|pgshard-version)/",
            "images/rust\\.Dockerfile",
            "images/quarantine\\.pg_hba\\.conf",
        ] {
            assert!(
                postgres_agent_trigger.contains(input),
                "PostgreSQL agent trigger must include {input}"
            );
        }
        assert!(workflow.contains("if: needs.changes.outputs.postgres_agent == 'true'"));
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
        assert!(makefile.contains("actionlint@v1.7.12 -ignore"));
        assert!(makefile.contains("concurrency queue key"));
        assert!(workflow.contains("      - planner-postgres"));
        assert!(workflow.contains("planner-postgres=${{ needs.planner-postgres.result }}"));
    }
}
