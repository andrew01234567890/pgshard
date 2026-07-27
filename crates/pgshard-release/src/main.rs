//! Deterministic `SemVer` release and public-repository audit tooling.

use std::env;
use std::ffi::OsStr;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use semver::Version;
use serde::{Deserialize, Serialize};

const FIRST_VERSION: Version = Version::new(0, 1, 0);
const RELEASE_MARKER: &str = "crates/pgshard-release/RELEASE_START";
const RELEASE_HELPER_SOURCE: &str = "crates/pgshard-release/src/main.rs";
const CI_WAIT_TIMEOUT: Duration = Duration::from_mins(15);
const CI_POLL_INTERVAL: Duration = Duration::from_secs(10);
const UNPRIVILEGED_DEPENDABOT_FILE_PAIRS: [[&str; 2]; 2] = [
    ["operator/go.mod", "operator/go.sum"],
    [
        "crates/pgshard-pgwire/fuzz/Cargo.toml",
        "crates/pgshard-pgwire/fuzz/Cargo.lock",
    ],
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
    },
    /// Print the next aggregate version through the selected commit.
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
        /// Publish only the oldest contiguous CI-green prefix without waiting.
        #[arg(long)]
        ready_only: bool,
    },
    /// Safely squash-merge a verified patch or minor update after successful CI.
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlannedRelease {
    sha: String,
    messages: Vec<String>,
    version: Version,
    previous_tag: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReleaseCandidate {
    sha: String,
    messages: Vec<String>,
    state: AggregateState,
    existing_tag: Option<String>,
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
struct WorkflowRuns {
    total_count: usize,
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Debug, Deserialize)]
struct WorkflowRun {
    id: u64,
    head_branch: String,
    head_sha: String,
    event: String,
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
                ensure_release_exists(&tag, &sha)?;
                println!("{}", tag.trim_start_matches('v'));
            } else {
                let plan = release_plan(&sha)?;
                ensure_release_plan_baseline(&plan)?;
                let endpoint = plan
                    .last()
                    .context("selected commit is outside the release history")?;
                ensure!(
                    endpoint.sha == sha,
                    "selected commit is not first-parent releasable"
                );
                let current = plan
                    .first()
                    .and_then(|release| release.previous_tag.as_deref())
                    .map(|tag| Version::parse(tag.trim_start_matches('v')))
                    .transpose()?;
                let messages = plan
                    .iter()
                    .flat_map(|release| release.messages.iter().cloned())
                    .collect::<Vec<_>>();
                println!("{}", aggregate_next_version(current.as_ref(), &messages)?);
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
        ReleaseCommand::Publish { sha, ready_only } => publish(&sha, ready_only)?,
        ReleaseCommand::DependabotAutomerge { repository, sha } => {
            dependabot_automerge(&repository, &sha)?;
        }
    }
    Ok(())
}

fn audit(base: &str, head: &str) -> Result<()> {
    let merge_base = git(&["merge-base", base, head])?;
    let range = format!("{merge_base}..{head}");
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
            HISTORY_DIFF_FILTER,
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

/// Lowercase excludes, so this audits every status except deletion, which has
/// no child-tree content. An allowlist silently omits whatever git adds next;
/// this cannot.
const HISTORY_DIFF_FILTER: &str = "--diff-filter=d";

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

fn dependabot_squash_is_verified(sha: &str) -> Result<bool> {
    let repository = env::var("GITHUB_REPOSITORY")
        .context("GITHUB_REPOSITORY is required to verify a Dependabot squash commit")?;
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
    audit_plain_content(path, content)?;
    audit_encoded_content(path, content)
}

/// A Kubernetes Secret carries `tls.key` as base64 of the whole PEM, so the
/// delimiters never appear as text. Committing a rendered Secret is the most
/// likely way a private key reaches this repository, and it is exactly the form
/// a text scan cannot see.
fn audit_encoded_content(path: &str, content: &str) -> Result<()> {
    for_each_base64_run(content, |run| audit_encoded_run(path, run))
}

/// The encoded length of a twenty-byte access key id, which is the shortest
/// credential any forbidden pattern can match in full. Sixty-four sat above an
/// access key id and above a forty-byte access token alike, so the rendered
/// Secret this decoding exists for was skipped unread.
const MIN_ENCODED: usize = 28;

fn audit_encoded_run(path: &str, run: &str) -> Result<()> {
    if run.len() < MIN_ENCODED {
        return Ok(());
    }
    // A run can carry a character the encoding did not. A patch marks each line
    // with a plus, which is in the alphabet and so is absorbed instead of
    // separating anything, and a character picked up at either end leaves a
    // length that is not a whole number of quanta. Decoding is quantum-local, so
    // starting one, two or three characters in recovers everything past the
    // first boundary. Every alignment is decoded, because a run that decodes is
    // not thereby shown to be the alignment that was encoded.
    for offset in 0..4 {
        let tail = &run[offset..];
        let quanta = tail.len() - tail.len() % 4;
        if quanta < MIN_ENCODED {
            break;
        }
        let Ok(decoded) =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &tail[..quanta])
        else {
            continue;
        };
        // One level only: a decode of a decode is not a form anyone commits by
        // accident, and recursing would make the audit unbounded.
        audit_plain_content(
            &format!("{path} (base64-encoded)"),
            &String::from_utf8_lossy(&decoded),
        )?;
        // A run that is a whole number of quanta and decodes as it stands is an
        // encoding as it stands. The other alignments would decode the same
        // characters out of phase, which is only worth doing for a run that has
        // already shown it carries something the encoding did not.
        if offset == 0 && run.len().is_multiple_of(4) {
            break;
        }
    }
    Ok(())
}

const fn is_base64_character(character: char) -> bool {
    matches!(character, 'A'..='Z' | 'a'..='z' | '0'..='9' | '+' | '/' | '=')
}

/// Runs are offered four ways: as they fall on each line, rejoined from the
/// pieces one line was broken into, rejoined across lines that hold nothing but
/// encoded text, and rejoined across lines that hold one substantial run inside
/// decoration. Over-joining costs nothing here because a joined string is never
/// judged as text; it is decoded, and only the decoded bytes are matched.
fn for_each_base64_run(content: &str, mut visit: impl FnMut(&str) -> Result<()>) -> Result<()> {
    for line in content.lines() {
        for run in line.split(|character: char| !is_base64_character(character)) {
            if !run.is_empty() {
                visit(run)?;
            }
        }
        // What separates the pieces of a value collapsed onto one line is a
        // two-character escape whose letter is in the alphabet, so joining the
        // pieces without taking the escape out first would splice a stray
        // character into the middle of the value.
        let unescaped;
        let joinable = if line.contains('\\') {
            unescaped = line
                .replace(ESCAPED_NEWLINE, " ")
                .replace(ESCAPED_RETURN, " ")
                .replace(ESCAPED_TAB, " ");
            unescaped.as_str()
        } else {
            line
        };
        let pieces = || {
            joinable
                .split(|character: char| !is_base64_character(character))
                .filter(|run| !run.is_empty())
        };
        // Counted before anything is collected, so a line that cannot hold an
        // encoded credential costs nothing.
        if pieces().take(2).count() > 1 && pieces().map(str::len).sum::<usize>() >= MIN_ENCODED {
            visit_rejoined(&pieces().collect::<Vec<_>>(), &mut visit)?;
        }
    }
    rejoin_wrapped_lines(content, &mut visit)?;
    rejoin_decorated_lines(content, &mut visit)
}

/// Wrapping splits an encoded key across lines, and a line decoded on its own is
/// a window whose edges fall wherever the wrap fell, so whether a delimiter is
/// visible is decided by the length of whatever preceded the key. Contiguous
/// wrapped lines are rejoined into the run they were split from, together with
/// the trailing run of the line the wrap continues, which is what a value folded
/// after its own key name looks like and is misaligned without it.
fn rejoin_wrapped_lines(content: &str, visit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
    let mut parts: Vec<&str> = Vec::new();
    let mut continues_a_line = false;
    let mut trailing = "";

    for line in content.lines() {
        let body = line.trim();
        if !body.is_empty() && body.chars().all(is_base64_character) {
            if parts.is_empty() && !trailing.is_empty() {
                continues_a_line = true;
                parts.push(trailing);
            }
            parts.push(body);
        } else {
            visit_wrapped(&parts, continues_a_line, visit)?;
            parts.clear();
            continues_a_line = false;
        }
        trailing = trailing_base64_run(line);
    }
    visit_wrapped(&parts, continues_a_line, visit)
}

/// A single wrapped line that continues nothing was already offered on its own.
fn visit_wrapped(
    parts: &[&str],
    continues_a_line: bool,
    visit: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let lines_joined = parts.len() - usize::from(continues_a_line);
    if lines_joined > 1 || (lines_joined == 1 && continues_a_line) {
        visit_rejoined(parts, visit)?;
    }
    Ok(())
}

/// Wrapped base64 does not always arrive naked. A patch marks every line with a
/// plus, a C literal quotes every line, a shell continues every line with a
/// backslash and a sequence introduces every line with a dash. Each such line
/// still carries one substantial run, so those runs are rejoined as well.
fn rejoin_decorated_lines(content: &str, visit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
    /// Below any width a tool wraps at, and above the identifiers that make
    /// consecutive lines of ordinary code look like encoded text.
    const MIN_DECORATED: usize = 16;

    let mut parts: Vec<&str> = Vec::new();
    let mut naked = true;

    for line in content.lines() {
        let body = line.trim();
        let run = longest_base64_run(body.strip_prefix('+').unwrap_or(body));
        if run.len() >= MIN_DECORATED {
            naked &= run == body;
            parts.push(run);
        } else {
            visit_decorated(&parts, naked, visit)?;
            parts.clear();
            naked = true;
        }
    }
    visit_decorated(&parts, naked, visit)
}

/// Lines carrying nothing but encoded text are what the wrapped rejoin covers.
fn visit_decorated(
    parts: &[&str],
    naked: bool,
    visit: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    if parts.len() > 1 && !naked {
        visit_rejoined(parts, visit)?;
    }
    Ok(())
}

const ESCAPED_NEWLINE: &str = "\\n";
const ESCAPED_RETURN: &str = "\\r";
const ESCAPED_TAB: &str = "\\t";

/// The longest span the windowing guarantees a rule is shown whole. Every
/// pattern matched here is tens of bytes; what is long is a private key
/// collapsed onto one line, read from its opening delimiter to its closing one,
/// and an exported keyring of several large keys reaches tens of kilobytes.
///
/// This bounds the overlap and nothing else. The rule that reads the longest
/// span reads a span of any length, because refusing to judge a body for being
/// long turns a concern about window seams into a key that is never looked at:
/// a span longer than this is still read wherever it appears, and only its
/// detection across a seam inside a group large enough to be windowed is
/// unguaranteed.
const MAX_RULE_SPAN: usize = 64 << 10;

/// Derived rather than chosen, because an overlap shorter than the longest span
/// a rule needs is a rule that stops working at a seam, and that is how a
/// collapsed key came to be missed once already. Counted in characters, because
/// a window holds encoded text.
const WINDOW_OVERLAP: usize = MAX_RULE_SPAN.div_ceil(3) * 4;

/// The relationship the two constants above have to hold, checked where it
/// cannot be argued with: an overlap that decodes to less than the longest span
/// a rule reads is a rule that stops working at a seam, and lowering either
/// constant without the other stops this compiling.
const _: () = assert!(WINDOW_OVERLAP / 4 * 3 >= MAX_RULE_SPAN);

/// A group is offered in windows rather than as one string, because a blob that
/// is nothing but wrapped base64 would otherwise be copied whole into memory and
/// decoded whole beside it. Consecutive windows overlap by the longest span any
/// rule reads, and each one begins a whole number of quanta into the group, so a
/// seam hides nothing and shifts nothing.
fn visit_rejoined(parts: &[&str], visit: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
    const WINDOW: usize = 4 << 20;

    let total: usize = parts.iter().map(|part| part.len()).sum();
    let longest = parts
        .iter()
        .map(|part| part.len())
        .max()
        .unwrap_or_default();
    let mut window = String::with_capacity(total.min(WINDOW) + longest);
    for part in parts {
        window.push_str(part);
        if window.len() >= WINDOW {
            visit(&window)?;
            let dropped = (window.len() - WINDOW_OVERLAP) / 4 * 4;
            window.drain(..dropped);
        }
    }
    if !window.is_empty() {
        visit(&window)?;
    }
    Ok(())
}

fn longest_base64_run(text: &str) -> &str {
    text.split(|character: char| !is_base64_character(character))
        .max_by_key(|run| run.len())
        .unwrap_or_default()
}

fn trailing_base64_run(line: &str) -> &str {
    let line = line.trim_end();
    let start = line
        .char_indices()
        .rev()
        .take_while(|(_, character)| is_base64_character(*character))
        .last()
        .map_or(line.len(), |(at, _)| at);
    &line[start..]
}

fn audit_plain_content(path: &str, content: &str) -> Result<()> {
    let forbidden = forbidden_patterns();
    for line in content.lines() {
        ensure!(
            !is_private_key_delimiter(line),
            "content in {path} carries a private-key delimiter"
        );
        ensure!(
            !carries_collapsed_private_key(line),
            "content in {path} carries a collapsed private key"
        );
        ensure!(
            !carries_prefixed_credential(line),
            "content in {path} matched a forbidden sensitive-data pattern"
        );
        ensure!(
            !carries_password_bearing_dsn(line),
            "content in {path} carries a connection string with an inline password"
        );
        if let Some(pattern) = forbidden.iter().find(|pattern| line.contains(*pattern)) {
            ensure!(
                is_legacy_scanner_fixture(path, line, pattern),
                "content in {path} matched a forbidden sensitive-data pattern"
            );
        }
    }
    Ok(())
}

/// Each entry is assembled at run time so that this source never carries the
/// pattern it looks for, which is what lets the audit scan its own history.
fn forbidden_patterns() -> [String; 19] {
    [
        ["/", "home", "/"].concat(),
        ["BEGIN ", "OPENSSH PRIVATE KEY"].concat(),
        ["BEGIN ", "RSA PRIVATE KEY"].concat(),
        ["github", "_pat_"].concat(),
        ["gh", "p_"].concat(),
        ["gh", "s_"].concat(),
        ["gh", "o_"].concat(),
        ["gh", "u_"].concat(),
        ["gh", "r_"].concat(),
        ["AK", "IA"].concat(),
        ["gl", "pat-"].concat(),
        ["sk-", "ant-"].concat(),
        ["dckr", "_pat_"].concat(),
        ["xox", "b-"].concat(),
        ["xox", "p-"].concat(),
        ["xox", "a-"].concat(),
        ["xox", "s-"].concat(),
        ["xox", "r-"].concat(),
        ["xox", "e-"].concat(),
    ]
}

/// A credential prefix, the number of body characters its issuer emits after it,
/// and the alphabet that body is drawn from.
type PrefixedCredential = (String, usize, fn(char) -> bool);

/// A four-character prefix is not evidence on its own: one of these spells a
/// continent and another introduces every environment variable the node package
/// manager exports. They are a credential only when the body the issuer emits
/// follows the prefix in full.
fn carries_prefixed_credential(line: &str) -> bool {
    let prefixed: [PrefixedCredential; 3] = [
        (["AS", "IA"].concat(), 16, is_access_key_character),
        (["AI", "za"].concat(), 35, is_google_key_character),
        (["npm", "_"].concat(), 36, is_base62_character),
    ];
    prefixed.iter().any(|(prefix, body, permitted)| {
        line.match_indices(prefix.as_str()).any(|(at, _)| {
            line[at + prefix.len()..]
                .chars()
                .take_while(|character| permitted(*character))
                .count()
                >= *body
        })
    })
}

fn is_base62_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
}

fn is_access_key_character(character: char) -> bool {
    character.is_ascii_uppercase() || character.is_ascii_digit()
}

fn is_google_key_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
}

/// The credential this project will hold most of is a database password, and no
/// pattern describes one. Only the shape of the string that carries it does.
///
/// What this shape does not cover, so that the next reader does not have to
/// discover it: a password passed as a query parameter rather than as userinfo,
/// the key-and-value form `libpq` also accepts, a scheme belonging to another
/// database, and a password containing one of the characters that ends the
/// string being read. Each is a separate shape rather than a wider pattern.
fn carries_password_bearing_dsn(line: &str) -> bool {
    if !line.contains("://") {
        return false;
    }
    // Lowercased only to find the scheme, because lowercasing what is then
    // compared against the exemption would let a password differing only in
    // case reach it. Lowercasing ASCII does not move any byte, so the offsets
    // hold in the line itself.
    let lowered = line.to_ascii_lowercase();
    let schemes = [["postgres", "://"].concat(), ["postgresql", "://"].concat()];
    schemes.iter().any(|scheme| {
        lowered.match_indices(scheme.as_str()).any(|(at, _)| {
            let dsn = connection_string_at(&line[at..]);
            dsn_carries_a_password(dsn, scheme.len()) && !PERMITTED_TEST_DSNS.contains(&dsn)
        })
    })
}

fn connection_string_at(text: &str) -> &str {
    let end = text
        .find(|character: char| {
            character.is_whitespace() || matches!(character, '"' | '\'' | '`' | '\\' | '<' | '>')
        })
        .unwrap_or(text.len());
    &text[..end]
}

fn dsn_carries_a_password(dsn: &str, scheme: usize) -> bool {
    let Some(authority) = dsn.get(scheme..) else {
        return false;
    };
    let authority = &authority[..authority.find(['/', '?', '#']).unwrap_or(authority.len())];
    authority
        .rsplit_once('@')
        .and_then(|(userinfo, _)| userinfo.split_once(':'))
        .is_some_and(|(_, password)| !password.is_empty())
}

/// Exact strings, because an exemption that recognised a shape could be shaped
/// to. These are the loopback connection strings this repository's own workflow
/// and README have carried throughout its history; reaching this exemption
/// requires a leaked password to be one of these two, on this loopback address,
/// at one of these ports, against one of these databases.
const PERMITTED_TEST_DSNS: [&str; 5] = [
    "postgresql://postgres:password@127.0.0.1:5432/shardschema",
    "postgresql://postgres:pgshard-ci-only@127.0.0.1:5432/postgres",
    "postgresql://postgres:pgshard-ci-only@127.0.0.1:5432/shardschema",
    "postgresql://postgres:pgshard-ci-only@127.0.0.1:5433/shardschema",
    "postgresql://postgres:pgshard-ci-only@127.0.0.1:5434/shardschema",
];

/// Code that validates PEM has to name the delimiter, so matching it anywhere in
/// a line rejects the agent's own key-file validator. A leaked key carries the
/// delimiter as a whole line; a validator embeds it in an expression.
fn is_private_key_delimiter(line: &str) -> bool {
    let line = line.trim_start();
    // A key written into a multi-line literal closes with the quoting of
    // whatever embedded it rather than with a newline, so the anchor has to look
    // past that quoting.
    let line = line.trim_end_matches(|character: char| {
        character.is_whitespace()
            || matches!(
                character,
                '"' | '\'' | '`' | ',' | ';' | ')' | '}' | ']' | '\\'
            )
    });
    is_whole_line_private_key_delimiter(line) || is_putty_private_key_header(line)
}

/// The whole of the line and nothing else: a line that opens with a delimiter
/// and closes with one has something in between, and what that something is
/// decides, which is the collapsed rule's job and not this one's. Every format's
/// delimiter is recognised here, including the four-dash spaced form that
/// `ssh-keygen -e` writes for RFC 4716.
fn is_whole_line_private_key_delimiter(line: &str) -> bool {
    private_key_delimiter_forms()
        .into_iter()
        .any(|(opens, closes, label_end)| {
            [opens, closes]
                .iter()
                .any(|opening| delimiter_span(line, opening, &label_end) == Some((0, line.len())))
        })
}

/// A `PuTTY` key file carries no delimiter of any kind: a version header and the
/// line introducing the private body are its only markers. Both are followed by
/// a value the format fixes, and requiring that value is what keeps prose that
/// merely opens with the same words from being read as a key.
fn is_putty_private_key_header(line: &str) -> bool {
    if let Some(version) = line.strip_prefix(&["PuTTY-User-", "Key-File-"].concat()) {
        let after_version =
            version.trim_start_matches(|character: char| character.is_ascii_digit());
        return after_version.len() < version.len() && after_version.starts_with(':');
    }
    if let Some(count) = line.strip_prefix(&["Private-", "Lines:"].concat()) {
        let count = count.trim();
        return !count.is_empty() && count.chars().all(|character| character.is_ascii_digit());
    }
    false
}

/// A key collapsed onto one line carries both delimiters with its body between
/// them, and so does source that merely names them. No measurement of what lies
/// between separates the two: the longest run, the total, and the total with an
/// average were each tried, and each let a body through at some wrap width or
/// refused ordinary source at some identifier length. What separates them is not
/// a length at all. A key body is base64 that decodes to a private key, and a
/// private key has a determinate structure that source code does not have, so
/// the body is decoded and the structure decides.
fn carries_collapsed_private_key(line: &str) -> bool {
    let Some(body) = collapsed_private_key_body(line) else {
        return false;
    };
    if carries_encrypted_pem_headers(body) {
        return true;
    }
    // The escapes that put the body on one line are not part of it, and the
    // letter in each is in the alphabet, so they come out before anything is
    // joined.
    let body = body
        .replace(ESCAPED_NEWLINE, " ")
        .replace(ESCAPED_RETURN, " ")
        .replace(ESCAPED_TAB, " ");
    // The body put back together, which is what a key wrapped, quoted or
    // space-separated becomes; and each run on its own, which is what reaches
    // the key material when a header precedes it inside the block, as an
    // encrypted PEM and an armoured PGP block both have.
    let rejoined = std::iter::once(body.as_str());
    let alone = body.split(|character: char| !is_base64_character(character));
    rejoined
        .chain(alone)
        .filter_map(decoded_key_candidate)
        .any(|candidate| is_private_key_material(&candidate))
}

/// Whatever of the candidate is a whole number of quanta, decoded. Padding is
/// dropped wherever it appears rather than only where it belongs, so that a body
/// reassembled from pieces still decodes: nothing is skipped for being
/// unreadable, because a key cut short or split apart is still a key.
fn decoded_key_candidate(candidate: &str) -> Option<Vec<u8>> {
    /// The shortest of the structures below is a sequence tag, a length and a
    /// version, which is five bytes. Each structure bounds itself beyond that,
    /// and a floor higher than the shortest one is a key cut short that nothing
    /// looks at: measured against real `openssl` output, a floor of thirty-two
    /// let a truncated Ed25519 and a truncated SEC1 key through.
    const MIN_DECODED: usize = 5;

    let encoded: String = candidate
        .chars()
        .filter(|character| is_base64_character(*character) && *character != '=')
        .collect();
    let quanta = encoded.len() - encoded.len() % 4;
    let decoded = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &encoded[..quanta],
    )
    .ok()?;
    (decoded.len() >= MIN_DECODED).then_some(decoded)
}

/// A traditional encrypted key has no structure to decode: the encryption
/// replaced its body with ciphertext, and ciphertext is indistinguishable from
/// anything else. What identifies it is the header RFC 1421 requires the
/// encryption to write into the block, and both halves of that header together
/// appear between these delimiters in no other circumstance. Reading the body is
/// therefore the wrong question for this one form, which is why asking it let
/// every label but the two with separate literal cover through.
fn carries_encrypted_pem_headers(body: &str) -> bool {
    body.contains("Proc-Type:") && body.contains("DEK-Info:")
}

/// Every form these delimiters wrap that has a body worth decoding is one of
/// four things: a DER object, an `OpenSSH` key, an `OpenPGP` secret-key packet,
/// or an RFC 4716 key. A prefix of any of them counts, because a key cut short by
/// a quote or a wrap is still a key.
fn is_private_key_material(bytes: &[u8]) -> bool {
    is_der_private_key(bytes)
        || is_openssh_private_key(bytes)
        || is_pgp_secret_key_packet(bytes)
        || is_rfc4716_private_key(bytes)
}

/// The ssh.com format opens with a magic number rather than a structure.
fn is_rfc4716_private_key(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0x3f, 0x6f, 0xf9, 0xeb])
}

/// PKCS#8, PKCS#1, SEC1 and the DSA and encrypted forms are all a SEQUENCE large
/// enough to hold a key, opening either with the version integer of an unencrypted
/// key or with the algorithm identifier of an encrypted one. The size is the one
/// the SEQUENCE declares where it declares one, and what is present where it does
/// not, since a BER indefinite length declares nothing.
fn is_der_private_key(bytes: &[u8]) -> bool {
    /// An Ed25519 key is forty-eight bytes of DER all told, the smallest anyone
    /// issues. What follows the object is not measured, because a candidate
    /// joined from the rest of a line carries whatever else that line held.
    const MIN_DER_KEY: usize = 48;
    const INTEGER: u8 = 0x02;
    const OBJECT_IDENTIFIER: u8 = 0x06;

    let Some((declared, header)) = der_sequence(bytes) else {
        return false;
    };
    if header + declared < MIN_DER_KEY {
        return false;
    }
    let contents = &bytes[header..];
    matches!(contents, [INTEGER, 0x01, 0x00 | 0x01, ..])
        || der_sequence(contents)
            .is_some_and(|(_, algorithm)| contents.get(algorithm) == Some(&OBJECT_IDENTIFIER))
}

/// The declared length of a DER SEQUENCE and the width of its header.
fn der_sequence(bytes: &[u8]) -> Option<(usize, usize)> {
    const SEQUENCE: u8 = 0x30;
    /// A length byte of `0x80` opens a BER indefinite length: the contents run to
    /// an end-of-contents pair instead of declaring a size. DER forbids it, but
    /// `OpenSSL` reads a key encoded that way, so refusing to recognise the SEQUENCE
    /// at all would let one past. Measure what is present instead — this rule
    /// already accepts a prefix, so an unterminated candidate is judged on the
    /// same footing as a truncated definite-length one.
    const INDEFINITE: u8 = 0x80;

    if bytes.first() != Some(&SEQUENCE) {
        return None;
    }
    let first = *bytes.get(1)?;
    if first < 0x80 {
        return Some((usize::from(first), 2));
    }
    if first == INDEFINITE {
        return Some((bytes.len().saturating_sub(2), 2));
    }
    let width = usize::from(first & 0x7f);
    if width == 0 || width > 4 {
        return None;
    }
    let mut declared = 0usize;
    for index in 0..width {
        declared = (declared << 8) | usize::from(*bytes.get(2 + index)?);
    }
    Some((declared, 2 + width))
}

fn is_openssh_private_key(bytes: &[u8]) -> bool {
    bytes.starts_with(b"openssh-key-v1\0")
}

/// An armoured secret key opens with a packet tag: the top bit set, then the tag
/// number, five for a secret key and seven for a secret subkey, in either the
/// framing that predates RFC 4880 or the one it introduced. The packet body
/// opens with its own version, three or four.
fn is_pgp_secret_key_packet(bytes: &[u8]) -> bool {
    const SECRET_KEY: u8 = 5;
    const SECRET_SUBKEY: u8 = 7;
    /// A tag and a version are three bytes of agreement, which is little enough
    /// that the packet has to be long enough to be one as well. The smallest
    /// secret key packet anyone writes is hundreds of bytes.
    const MIN_PACKET: usize = 32;

    if bytes.len() < MIN_PACKET {
        return false;
    }
    let Some(&tag) = bytes.first() else {
        return false;
    };
    let (number, header) = if tag & 0xc0 == 0xc0 {
        let Some(&first) = bytes.get(1) else {
            return false;
        };
        // Two octets for a length in the middle range, five for the marker
        // that introduces a four-octet one, and one for everything else.
        let length = match first {
            192..=223 => 2,
            255 => 5,
            _ => 1,
        };
        (tag & 0x3f, 1 + length)
    } else if tag & 0xc0 == 0x80 {
        let length = match tag & 0x03 {
            0 => 1,
            1 => 2,
            2 => 4,
            _ => 0,
        };
        ((tag & 0x3c) >> 2, 1 + length)
    } else {
        return false;
    };
    matches!(number, SECRET_KEY | SECRET_SUBKEY) && matches!(bytes.get(header), Some(3 | 4))
}

/// Everything after the opening delimiter, up to the closing one where there is
/// one. A key cut short by a quote, a wrap or a truncated paste has no closing
/// delimiter and is still a key, so its absence bounds the body at the end of
/// the line rather than abandoning it.
fn collapsed_private_key_body(line: &str) -> Option<&str> {
    private_key_delimiter_forms()
        .into_iter()
        .find_map(|(opens, closes, label_end)| {
            let opened = delimiter_span(line, &opens, &label_end)?.1;
            let body = &line[opened..];
            let closed = delimiter_span(body, &closes, &label_end).map_or(body.len(), |at| at.0);
            Some(&body[..closed])
        })
}

/// The opening of a delimiter, the opening of the delimiter that closes the
/// block, and the text that ends either. Split so this source carries none of
/// them assembled.
fn private_key_delimiter_forms() -> [(String, String, String); 3] {
    let pem_open = ["-----", "BEGIN"].concat();
    let pem_close = ["-----", "END"].concat();
    [
        (
            pem_open.clone(),
            pem_close.clone(),
            [" PRIVATE ", "KEY-----"].concat(),
        ),
        (
            pem_open,
            pem_close,
            [" PRIVATE ", "KEY BLOCK-----"].concat(),
        ),
        (
            ["----", " BEGIN"].concat(),
            ["----", " END"].concat(),
            [" PRIVATE ", "KEY ----"].concat(),
        ),
    ]
}

/// Longer than `SSH2 ENCRYPTED`, which is the longest label any of these
/// formats gives a private key.
const MAX_LABEL: usize = 24;

/// A delimiter is its opening, a label no longer than any format gives one, and
/// the text that ends it. The cap is what stops a certificate delimiter earlier
/// in the line from pairing with a key delimiter much later.
fn delimiter_span(haystack: &str, opening: &str, label_end: &str) -> Option<(usize, usize)> {
    haystack.match_indices(opening).find_map(|(at, _)| {
        let labelled = haystack.get(at + opening.len()..)?;
        let ends = labelled.find(label_end)?;
        (ends <= MAX_LABEL).then_some((at, at + opening.len() + ends + label_end.len()))
    })
}

fn audit_content_bytes(path: &str, content: &[u8]) -> Result<()> {
    ensure!(
        !is_compressed(content),
        "content in {path} is compressed, and the audit refuses what it cannot read"
    );
    let text = String::from_utf8_lossy(content);
    audit_content(path, &text)?;
    // UTF-16 and NUL-padded text keep every byte of a secret but never place two
    // of its characters next to each other, so the same bytes are read again
    // with the padding taken out. Decoding UTF-16 properly would add only the
    // non-ASCII range, which no credential format uses.
    if text.contains('\0') {
        let mut stripped = String::with_capacity(text.len());
        stripped.extend(text.chars().filter(|character| *character != '\0'));
        audit_content(&format!("{path} (NUL-stripped)"), &stripped)?;
    }
    Ok(())
}

/// Reading inside a container means a decompressor for each of them, every one
/// with its own bomb, and handling one of them is worse than handling none
/// because it implies the rest are covered. The audit refuses what it cannot
/// read instead of passing it, which turns a silent miss into a stop. A
/// compressed asset added deliberately has to be exempted in the commit that
/// adds it, because afterwards it is history and history is what is scanned.
fn is_compressed(content: &[u8]) -> bool {
    const MAGIC: [&[u8]; 9] = [
        &[0x1f, 0x8b, 0x08],
        b"PK\x03\x04",
        b"PK\x07\x08",
        &[0xfd, b'7', b'z', b'X', b'Z', 0x00],
        &[0x28, 0xb5, 0x2f, 0xfd],
        &[0x04, 0x22, 0x4d, 0x18],
        b"7z\xbc\xaf\x27\x1c",
        b"Rar!\x1a\x07",
        // A zlib stream, whose header is a deflate method and one of the four
        // levels a standard compressor writes. Bare deflate and brotli carry no
        // header at all and cannot be recognised by any magic; those two remain
        // unread, which is written here so the next reader is not surprised.
        &[0x78, 0x9c],
    ];
    MAGIC.iter().any(|magic| content.starts_with(magic))
        || (content.starts_with(b"BZh") && content.get(3).is_some_and(u8::is_ascii_digit))
        || (content.starts_with(&[0x78]) && matches!(content.get(1), Some(0x01 | 0x5e | 0xda)))
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

fn publish(requested_sha: &str, ready_only: bool) -> Result<()> {
    ensure!(
        env::var("GITHUB_ACTIONS").as_deref() == Ok("true"),
        "publish may only run in GitHub Actions"
    );
    let sha = git(&["rev-parse", &format!("{requested_sha}^{{commit}}")])?;
    if let Ok(expected) = env::var("PGSHARD_RELEASE_SHA").or_else(|_| env::var("GITHUB_SHA")) {
        ensure!(
            sha == expected,
            "requested SHA does not match workflow event SHA"
        );
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
    ensure_release_plan_baseline(&plan)?;

    let current = plan
        .first()
        .and_then(|release| release.previous_tag.as_deref())
        .map(|tag| Version::parse(tag.trim_start_matches('v')))
        .transpose()?;
    let previous_tag = plan
        .first()
        .and_then(|release| release.previous_tag.clone());
    let mut candidates = Vec::with_capacity(plan.len());
    for release in plan {
        let state = exact_aggregate_state(&repository, &release.sha)?;
        let existing_tag = semver_tag_at(&release.sha)?;
        candidates.push(ReleaseCandidate {
            sha: release.sha,
            messages: release.messages,
            state,
            existing_tag,
        });
    }

    if !ready_only {
        let endpoint = candidates
            .last_mut()
            .context("no releasable first-parent commits found")?;
        endpoint.state = wait_for_aggregate_terminal(&repository, &endpoint.sha)?;
        let endpoint = candidates
            .last()
            .context("no releasable first-parent commits found")?;
        ensure!(
            endpoint.state == AggregateState::Passed,
            "release endpoint {} does not have a successful exact-head CI aggregate",
            endpoint.sha
        );
    }

    // Leading successful commits retain one release each. After the first
    // non-passing aggregate, only the requested endpoint may close the gap:
    // its widened CI range covers the full untagged history, while an earlier
    // successful aggregate may have used the old one-commit detector. Apply
    // the strongest bump across that complete recovery range only once.
    let releases = aggregate_release_plan(current, previous_tag, &candidates)?;
    for release in releases {
        publish_one(&repository, &release)?;
    }

    let recovery_start = release_recovery_start(&candidates);
    if recovery_start < candidates.len()
        && candidates.last().map(|candidate| candidate.state) != Some(AggregateState::Passed)
    {
        println!(
            "release deferred from {} until a later exact CI aggregate succeeds",
            candidates[recovery_start].sha
        );
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

    let notes = release_notes(
        repository,
        &release.sha,
        &release.messages,
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
    match wait_for_aggregate_terminal(repository, sha)? {
        AggregateState::Passed => Ok(()),
        AggregateState::Failed => {
            bail!("commit {sha} has a failed exact-head CI aggregate check")
        }
        AggregateState::Pending => unreachable!("wait returns only terminal aggregate states"),
    }
}

fn wait_for_aggregate_terminal(repository: &str, sha: &str) -> Result<AggregateState> {
    let started = Instant::now();
    loop {
        match exact_aggregate_state(repository, sha)? {
            state @ (AggregateState::Passed | AggregateState::Failed) => return Ok(state),
            AggregateState::Pending if started.elapsed() >= CI_WAIT_TIMEOUT => {
                bail!("timed out waiting for exact-head CI aggregate on commit {sha}")
            }
            AggregateState::Pending => {
                println!("waiting for exact-head CI aggregate on ancestor {sha}");
                thread::sleep(CI_POLL_INTERVAL);
            }
        }
    }
}

fn exact_aggregate_state(repository: &str, sha: &str) -> Result<AggregateState> {
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
    Ok(aggregate_state(&checks))
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
    // Exactly one: a second job named `CI aggregate` publishes a second check
    // run under the same name, and accepting any success would let that decoy
    // answer for the real gate's failure.
    if aggregates.len() > 1 {
        return AggregateState::Failed;
    }
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
            messages: vec![message],
            version: version.clone(),
            previous_tag: previous_tag.clone(),
        });
        previous_tag = Some(format!("v{version}"));
        current = Some(version);
    }
    Ok(plan)
}

fn aggregate_release_plan(
    mut current: Option<Version>,
    mut previous_tag: Option<String>,
    candidates: &[ReleaseCandidate],
) -> Result<Vec<PlannedRelease>> {
    let mut releases = Vec::new();

    for candidate in candidates {
        ensure!(
            candidate.existing_tag.is_none(),
            "release history contains a tagged gap at {}",
            candidate.sha
        );
    }

    let recovery_start = release_recovery_start(candidates);

    for candidate in &candidates[..recovery_start] {
        let version = aggregate_next_version(current.as_ref(), &candidate.messages)?;
        releases.push(PlannedRelease {
            sha: candidate.sha.clone(),
            messages: candidate.messages.clone(),
            version: version.clone(),
            previous_tag: previous_tag.clone(),
        });
        previous_tag = Some(format!("v{version}"));
        current = Some(version);
    }

    let recovery = &candidates[recovery_start..];
    let Some(endpoint) = recovery.last() else {
        return Ok(releases);
    };
    if endpoint.state != AggregateState::Passed {
        return Ok(releases);
    }

    let messages = recovery
        .iter()
        .flat_map(|candidate| candidate.messages.iter().cloned())
        .collect::<Vec<_>>();
    let version = aggregate_next_version(current.as_ref(), &messages)?;
    releases.push(PlannedRelease {
        sha: endpoint.sha.clone(),
        messages,
        version,
        previous_tag,
    });

    Ok(releases)
}

fn ensure_release_plan_baseline(plan: &[PlannedRelease]) -> Result<()> {
    let Some(tag) = plan
        .first()
        .and_then(|release| release.previous_tag.as_deref())
    else {
        return Ok(());
    };
    let sha = tag_target(tag)?.context("release baseline tag disappeared")?;
    ensure_release_exists(tag, &sha)
}

fn release_recovery_start(candidates: &[ReleaseCandidate]) -> usize {
    candidates
        .iter()
        .position(|candidate| candidate.state != AggregateState::Passed)
        .unwrap_or(candidates.len())
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
    aggregate_next_version(current, &[message.to_owned()])
}

fn aggregate_next_version(current: Option<&Version>, messages: &[String]) -> Result<Version> {
    ensure!(
        !messages.is_empty(),
        "a release must contain at least one commit"
    );
    let bump = messages
        .iter()
        .map(|message| parse_bump(message))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max_by_key(|bump| bump_precedence(*bump))
        .context("a release must contain at least one commit")?;
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

fn bump_precedence(bump: Bump) -> u8 {
    match bump {
        Bump::Patch => 0,
        Bump::Minor => 1,
        Bump::Major => 2,
    }
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
    let version = tag
        .strip_prefix('v')
        .and_then(|value| Version::parse(value).ok())?;
    if version.pre.is_empty() && version.build.is_empty() {
        Some(version)
    } else {
        None
    }
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

fn release_notes(
    repository: &str,
    sha: &str,
    messages: &[String],
    previous_tag: Option<&str>,
) -> String {
    let short_sha = &sha[..sha.len().min(12)];
    let compare = previous_tag.map_or_else(
        || format!("https://github.com/{repository}/commit/{sha}"),
        |tag| format!("https://github.com/{repository}/compare/{tag}...{sha}"),
    );
    let changes = messages
        .iter()
        .map(|message| format!("- {}", message.lines().next().unwrap_or_default()))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "## Changes\n\n{changes}\n\nRelease commit: [`{short_sha}`](https://github.com/{repository}/commit/{sha})\n\n[Compare changes]({compare})\n\nThis prerelease contains source code only. No container images, binaries, charts, or packages are published."
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
    if !dependabot_patch_or_minor(commits.iter().map(|commit| commit.commit.message.as_str())) {
        println!(
            "Dependabot pull request #{} is not a verified patch-or-minor update",
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
        dependabot_squash_is_verified(&merge_sha)?,
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
    files.len() == 2
        && files
            .iter()
            .all(|file| file.status == "modified" && file.previous_filename.is_none())
        && UNPRIVILEGED_DEPENDABOT_FILE_PAIRS.iter().any(|pair| {
            pair.iter()
                .all(|expected| files.iter().any(|file| file.filename.as_str() == *expected))
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
    let response = run(
        "gh",
        [
            "api",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            &format!(
                "repos/{repository}/actions/workflows/ci.yml/runs?event=workflow_dispatch&head_sha={merge_sha}&per_page=100"
            ),
        ],
    )?;
    let runs: WorkflowRuns = serde_json::from_str(&response)?;
    ensure!(
        runs.total_count == runs.workflow_runs.len() && runs.total_count < 100,
        "exact-SHA workflow-run lookup reached its page limit and is ambiguous"
    );
    ensure!(
        runs.workflow_runs
            .iter()
            .all(|run| run.head_sha == merge_sha && run.event == "workflow_dispatch"),
        "GitHub returned a mismatched exact-SHA workflow run"
    );
    Ok(runs.workflow_runs)
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

fn dependabot_patch_or_minor<'a>(messages: impl IntoIterator<Item = &'a str>) -> bool {
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
        && update_types.iter().all(|update_type| {
            matches!(
                *update_type,
                "version-update:semver-patch" | "version-update:semver-minor"
            )
        })
}

fn ensure_release_exists(tag: &str, sha: &str) -> Result<()> {
    let tagged_sha = tag_target(tag)?.context("release tag disappeared")?;
    ensure!(
        tagged_sha == sha,
        "existing release tag points to another commit"
    );
    let release_sha = run(
        "gh",
        [
            "release",
            "view",
            tag,
            "--json",
            "targetCommitish",
            "--jq",
            ".targetCommitish",
        ],
    )?;
    ensure!(
        release_sha == sha,
        "GitHub Release {tag} does not target exact commit {sha}"
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

    fn release_candidate(sha: &str, message: &str, state: AggregateState) -> ReleaseCandidate {
        ReleaseCandidate {
            sha: sha.to_owned(),
            messages: vec![message.to_owned()],
            state,
            existing_tag: None,
        }
    }

    #[test]
    fn first_release_is_fixed() {
        assert_eq!(
            next_version(None, "docs: start documentation").unwrap(),
            FIRST_VERSION
        );
    }

    #[test]
    fn release_tags_are_plain_canonical_semver_only() {
        assert_eq!(release_tag_version("v0.75.0"), Some(Version::new(0, 75, 0)));
        for rejected in [
            "0.75.0",
            "v01.75.0",
            "v0.75",
            "v0.75.0-rc.1",
            "v0.75.0+build.1",
            "v18446744073709551616.0.0",
        ] {
            assert_eq!(release_tag_version(rejected), None, "accepted {rejected}");
        }
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
    fn failed_feature_folds_into_green_fix_as_one_minor_release() {
        let candidates = vec![
            release_candidate(
                "failed-feature",
                "feat(operator): require durable bootstrap generation",
                AggregateState::Failed,
            ),
            release_candidate(
                "green-fix",
                "test(operator): retry identity conflict",
                AggregateState::Passed,
            ),
        ];

        let releases = aggregate_release_plan(
            Some(Version::new(0, 74, 0)),
            Some("v0.74.0".to_owned()),
            &candidates,
        )
        .unwrap();

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].sha, "green-fix");
        assert_eq!(releases[0].version, Version::new(0, 75, 0));
        assert_eq!(releases[0].previous_tag.as_deref(), Some("v0.74.0"));
        assert_eq!(releases[0].messages.len(), 2);
    }

    #[test]
    fn old_narrow_green_commit_cannot_close_a_recovery_gap() {
        let candidates = vec![
            release_candidate(
                "failed-feature",
                "feat(operator): require durable bootstrap generation",
                AggregateState::Failed,
            ),
            release_candidate(
                "old-narrow-green",
                "test(operator): retry identity conflict",
                AggregateState::Passed,
            ),
            release_candidate(
                "new-full-gap-green",
                "fix(release): widen catch-up validation",
                AggregateState::Passed,
            ),
        ];

        let releases = aggregate_release_plan(
            Some(Version::new(0, 74, 0)),
            Some("v0.74.0".to_owned()),
            &candidates,
        )
        .unwrap();

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].sha, "new-full-gap-green");
        assert_eq!(releases[0].version, Version::new(0, 75, 0));
        assert_eq!(releases[0].messages.len(), 3);
    }

    #[test]
    fn recovery_gap_folds_through_old_green_run_to_the_new_endpoint() {
        let candidates = vec![
            release_candidate(
                "missing-aggregate",
                "fix(operator): preserve state",
                AggregateState::Pending,
            ),
            release_candidate(
                "old-narrow-green",
                "test(operator): cover state preservation",
                AggregateState::Passed,
            ),
            release_candidate(
                "full-gap-green",
                "docs: describe state preservation",
                AggregateState::Passed,
            ),
        ];

        let releases = aggregate_release_plan(
            Some(Version::new(0, 75, 0)),
            Some("v0.75.0".to_owned()),
            &candidates,
        )
        .unwrap();

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].sha, "full-gap-green");
        assert_eq!(releases[0].version, Version::new(0, 75, 1));
        assert_eq!(releases[0].messages.len(), 3);
    }

    #[test]
    fn trailing_nonpassed_gap_is_not_skipped() {
        let candidates = vec![
            release_candidate(
                "failed",
                "feat(router): add routing mode",
                AggregateState::Failed,
            ),
            release_candidate(
                "old-narrow-green",
                "test(router): cover routing mode",
                AggregateState::Passed,
            ),
            release_candidate(
                "pending",
                "fix(router): finish routing mode",
                AggregateState::Pending,
            ),
        ];

        assert_eq!(release_recovery_start(&candidates), 0);
        assert!(
            aggregate_release_plan(
                Some(Version::new(0, 75, 0)),
                Some("v0.75.0".to_owned()),
                &candidates,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn all_green_backlog_keeps_one_release_per_commit() {
        let candidates = vec![
            release_candidate(
                "feature",
                "feat(router): add routing mode",
                AggregateState::Passed,
            ),
            release_candidate(
                "tests",
                "test(router): cover routing mode",
                AggregateState::Passed,
            ),
        ];

        let releases = aggregate_release_plan(
            Some(Version::new(0, 74, 0)),
            Some("v0.74.0".to_owned()),
            &candidates,
        )
        .unwrap();

        assert_eq!(releases.len(), 2);
        assert_eq!(releases[0].sha, "feature");
        assert_eq!(releases[0].version, Version::new(0, 75, 0));
        assert_eq!(releases[1].sha, "tests");
        assert_eq!(releases[1].version, Version::new(0, 75, 1));
        assert_eq!(releases[1].previous_tag.as_deref(), Some("v0.75.0"));
    }

    #[test]
    fn aggregate_release_applies_the_strongest_bump_once() {
        let candidates = vec![
            release_candidate(
                "breaking",
                "refactor!: replace protocol",
                AggregateState::Failed,
            ),
            release_candidate(
                "feature",
                "feat(router): add routing mode",
                AggregateState::Pending,
            ),
            release_candidate(
                "patch-endpoint",
                "fix(router): finish routing mode",
                AggregateState::Passed,
            ),
        ];

        let releases = aggregate_release_plan(
            Some(Version::new(1, 7, 4)),
            Some("v1.7.4".to_owned()),
            &candidates,
        )
        .unwrap();

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].version, Version::new(2, 0, 0));
        assert_eq!(releases[0].messages.len(), 3);
    }

    #[test]
    fn aggregate_release_rejects_a_tagged_gap() {
        let mut candidate = release_candidate(
            "unexpected-tag",
            "fix(router): finish routing mode",
            AggregateState::Passed,
        );
        candidate.existing_tag = Some("v0.76.0".to_owned());

        let error = aggregate_release_plan(
            Some(Version::new(0, 75, 0)),
            Some("v0.75.0".to_owned()),
            &[candidate],
        )
        .unwrap_err();
        assert!(error.to_string().contains("tagged gap"));
    }

    #[test]
    fn folded_release_notes_retain_every_subject() {
        let notes = release_notes(
            "owner/repository",
            &"a".repeat(40),
            &[
                "feat(operator): add generation\n\nbody".to_owned(),
                "test(operator): retry identity conflict".to_owned(),
            ],
            Some("v0.74.0"),
        );

        assert!(notes.contains("- feat(operator): add generation"));
        assert!(notes.contains("- test(operator): retry identity conflict"));
        assert!(notes.contains("compare/v0.74.0..."));
        assert!(notes.contains("source code only"));
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

    fn encode(content: &str) -> String {
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            content.as_bytes(),
        )
    }

    fn leaked_private_key() -> String {
        leaked_private_key_of(4)
    }

    /// A key whose body is `lines` lines of the sixty-four characters `openssl`
    /// wraps at.
    fn leaked_private_key_of(lines: usize) -> String {
        let mut body = String::new();
        for line in 0..lines {
            use std::fmt::Write;
            writeln!(
                body,
                "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDB{line:012}"
            )
            .expect("writing to a string cannot fail");
        }
        [
            "-----BEGIN ",
            "PRIVATE ",
            "KEY-----\n",
            &body,
            "-----END ",
            "PRIVATE ",
            "KEY-----\n",
        ]
        .concat()
    }

    /// The same key with its body rewrapped at `width`, which is what defeats
    /// any rule that asks how long the longest unbroken run of it is.
    fn rewrapped_private_key(lines: usize, width: usize) -> String {
        let key = leaked_private_key_of(lines);
        let mut parts = key.lines();
        let begin = parts.next().expect("the opening delimiter");
        let rest: Vec<&str> = parts.collect();
        let (end, body) = rest.split_last().expect("the closing delimiter");
        let body = body.concat();
        let rewrapped = body
            .as_bytes()
            .chunks(width)
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        format!("{begin}\n{rewrapped}\n{end}\n")
    }

    /// A key with `preamble` bytes ahead of it, encoded. A bundle that puts a
    /// certificate first, or a `Bag Attributes` header, is a preamble of some
    /// length and nothing more, and the length is what decides where the edges
    /// of every decoded window fall.
    fn encoded_key(preamble: usize) -> String {
        let ahead = if preamble == 0 {
            String::new()
        } else {
            format!("{}\n", "#".repeat(preamble - 1))
        };
        assert_eq!(ahead.len(), preamble);
        encode(&format!("{ahead}{}", leaked_private_key()))
    }

    /// That encoding wrapped at `width` into the block scalar of a Secret.
    fn wrapped_key_manifest(preamble: usize, width: usize) -> String {
        let wrapped = encoded_key(preamble)
            .as_bytes()
            .chunks(width)
            .map(|chunk| format!("    {}", String::from_utf8_lossy(chunk)))
            .collect::<Vec<_>>()
            .join("\n");
        format!("apiVersion: v1\nkind: Secret\ndata:\n  tls.key: |\n{wrapped}\n")
    }

    /// The forbidden list exists to catch credentials of the length their
    /// issuers actually emit, and a rendered Secret carries them encoded, so the
    /// length below which nothing is decoded has to sit under the shortest of
    /// them rather than above it.
    #[test]
    fn an_encoded_credential_is_refused_at_the_length_its_issuer_emits() {
        let secret = |value: &str| {
            format!(
                "apiVersion: v1\nkind: Secret\ndata:\n  value: {}\n",
                encode(value)
            )
        };

        let access_key = ["AK", "IA", "IOSFODNN7EXAMPLE"].concat();
        assert_eq!(
            access_key.len(),
            20,
            "an access key id is twenty characters"
        );
        assert_eq!(encode(&access_key).len(), 28);
        assert!(
            audit_content("secret.yaml", &secret(&access_key)).is_err(),
            "an encoded access key id must be refused"
        );

        let token = ["gh", "s_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat();
        assert_eq!(token.len(), 40, "an access token is forty characters");
        assert_eq!(encode(&token).len(), 56);
        assert!(
            audit_content("secret.yaml", &secret(&token)).is_err(),
            "an encoded access token must be refused"
        );

        let key = leaked_private_key();
        assert!(
            !encode(&key).contains("PRIVATE"),
            "the encoded form must not carry the delimiter as text, or this proves nothing"
        );
        assert!(audit_content("secret.yaml", &secret(&key)).is_err());

        // Ordinary long hashes decode to noise and must not be rejected.
        assert!(
            audit_content(
                "ok.lock",
                &format!("checksum = \"{}\"", "a1b2c3d4".repeat(8))
            )
            .is_ok()
        );
        assert!(
            audit_content(
                "ok.lock",
                &format!("integrity = \"{}\"", encode("harmless"))
            )
            .is_ok()
        );
    }

    /// `base64 file` wraps at seventy-six characters and a block scalar wraps
    /// wherever its writer chose, so an encoded key arrives as many short lines.
    /// Decoded a line at a time, each is a window whose edges fall where the
    /// wrap fell, and a delimiter straddling two of them is invisible. Whatever
    /// precedes the key decides where those edges land, so the sweep varies it:
    /// a certificate ahead of the key in a bundle, or a `Bag Attributes`
    /// preamble, is nothing more than a preamble of some length.
    #[test]
    fn a_wrapped_key_is_refused_at_every_alignment() {
        let mut missed = Vec::new();
        let mut cases = 0usize;
        for width in [8, 13, 32, 64, 76] {
            for length in 0..48usize {
                cases += 1;
                if audit_content("secret.yaml", &wrapped_key_manifest(length, width)).is_ok() {
                    missed.push((width, length));
                }
            }
        }
        assert_eq!(cases, 240);
        assert!(
            missed.is_empty(),
            "a wrapped key escaped {} of {cases} times, at (width, preamble) {missed:?}",
            missed.len()
        );
    }

    /// Two lines is the first count that has to be rejoined at all, and a
    /// rejoin that starts at three passes every other test in this file. What
    /// exposes it is a credential that straddles the wrap: each line decodes to
    /// a window that holds part of it and no window holds all of it, so nothing
    /// short of putting the lines back together sees it.
    #[test]
    fn a_credential_wrapped_onto_each_small_number_of_lines_is_refused() {
        let token = ["gh", "p_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat();
        let mut missed = Vec::new();
        let mut cases = 0usize;
        for lines in 2..=6usize {
            for preamble in 9..48usize {
                cases += 1;
                let encoded = encode(&format!("{}{token}", "#".repeat(preamble)));
                let width = encoded.len().div_ceil(lines);
                let wrapped = encoded
                    .as_bytes()
                    .chunks(width)
                    .map(|chunk| format!("    {}", String::from_utf8_lossy(chunk)))
                    .collect::<Vec<_>>();
                assert_eq!(
                    wrapped.len(),
                    lines,
                    "the fixture must wrap onto {lines} lines"
                );
                let manifest = format!("data:\n  token: |\n{}\n", wrapped.join("\n"));
                if audit_content("secret.yaml", &manifest).is_ok() {
                    missed.push((lines, preamble));
                }
            }
        }
        assert_eq!(cases, 195);
        assert!(
            missed.is_empty(),
            "a wrapped credential escaped {} of {cases} times, at (lines, preamble) {missed:?}",
            missed.len()
        );
    }

    /// A group longer than one window is offered in overlapping pieces. The
    /// overlap is what keeps a credential lying across a seam from falling
    /// between two of them, and every piece has to begin a whole number of
    /// quanta into the group or it would decode out of phase.
    #[test]
    fn rejoined_windows_overlap_and_stay_in_phase() {
        const SEAM: usize = (4 << 20) / 4 * 3;

        let alphabet: Vec<char> = ('a'..='z').chain('A'..='Z').chain('0'..='9').collect();
        let mut state = 1u64;
        let whole: String = (0..6 * 1024 * 1024)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                alphabet[(state >> 33) as usize % alphabet.len()]
            })
            .collect();
        let parts: Vec<&str> = whole
            .as_bytes()
            // Deliberately not a multiple of four, so that a window ending
            // mid quantum is what the phase assertion below reads.
            .chunks(1023)
            .map(|chunk| std::str::from_utf8(chunk).expect("the alphabet is ASCII"))
            .collect();

        let mut windows: Vec<String> = Vec::new();
        visit_rejoined(&parts, &mut |window| {
            windows.push(window.to_owned());
            Ok(())
        })
        .expect("the visitor accepts every window");
        assert!(windows.len() > 1, "the group must exceed one window");

        let mut covered = 0usize;
        let mut previous: Option<(usize, usize)> = None;
        for (index, window) in windows.iter().enumerate() {
            let at = whole
                .find(window.as_str())
                .expect("every window is a slice of the group");
            assert_eq!(at % 4, 0, "window {index} begins out of phase");
            if let Some((before, length)) = previous {
                let overlap = before + length - at;
                assert!(
                    overlap >= WINDOW_OVERLAP,
                    "window {index} overlaps the one before by only {overlap} characters"
                );
            }
            covered = at + window.len();
            previous = Some((at, window.len()));
        }
        assert_eq!(
            covered,
            whole.len(),
            "the windows must cover the whole group"
        );

        // The same shape end to end, with a whole collapsed key lying across
        // the seam. A key is the longest thing any rule reads at once, and an
        // overlap shorter than one is how a key came to be missed before.
        let collapsed = rewrapped_private_key(25, 64)
            .trim_end()
            .replace('\n', "\\n");
        assert!(
            collapsed.len() > 1600,
            "the span must be a realistic key rather than a token"
        );
        let across = format!("{{\"tls.key\": \"{collapsed}\"}}");
        let encoded = encode(&format!("{}{across}", "#".repeat(SEAM - across.len() / 2)));
        let wrapped = encoded
            .as_bytes()
            .chunks(76)
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            audit_content("secret.b64", &format!("{wrapped}\n")).is_err(),
            "a credential lying across the seam must be refused"
        );
    }

    /// A rendered Secret folded to a column carries the start of the value on
    /// the line that names it, and what follows is the rest of that value. A
    /// rejoin that starts at the line below decodes a value it has already lost
    /// the front of, and a short credential lives entirely in what was lost.
    #[test]
    fn a_value_folded_after_its_key_name_is_refused() {
        let access_key = ["AK", "IA", "IOSFODNN7EXAMPLE"].concat();
        let encoded = encode(&access_key);
        assert_eq!(encoded.len(), 28);
        for fold in 1..encoded.len() {
            let (carried, rest) = encoded.split_at(fold);
            let folded = format!("data:\n  key: {carried}\n{rest}\n");
            assert!(
                audit_content("secret.yaml", &folded).is_err(),
                "a value folded after {fold} characters must be refused"
            );
        }
    }

    /// A named way of decorating each line of a wrapped encoding.
    type Decoration = (&'static str, fn(&str) -> String);

    /// Wrapped encoding does not always arrive naked: a committed patch marks
    /// every line, a C literal quotes every line, a shell continues every line
    /// and a sequence introduces every line. The preamble is swept for the same
    /// reason as everywhere else, because a single length is a single alignment.
    #[test]
    fn a_wrapped_key_inside_line_decoration_is_refused() {
        let decorations: [Decoration; 4] = [
            ("a patch", |line| format!("+{line}")),
            ("a C literal", |line| format!("  \"{line}\"")),
            ("a continuation", |line| format!("{line}\\")),
            ("a sequence", |line| format!("  - {line}")),
        ];
        let mut missed = Vec::new();
        for (shape, decorate) in decorations {
            for preamble in 0..48usize {
                let body = encoded_key(preamble)
                    .as_bytes()
                    .chunks(20)
                    .map(|chunk| decorate(&String::from_utf8_lossy(chunk)))
                    .collect::<Vec<_>>()
                    .join("\n");
                if audit_content("bad.txt", &format!("{body}\n")).is_ok() {
                    missed.push((shape, preamble));
                }
            }
        }
        assert!(
            missed.is_empty(),
            "a decorated wrapped key escaped {} of 192 times: {missed:?}",
            missed.len()
        );

        // A marker absorbed into the run leaves a length that is not a whole
        // number of quanta, and a decoder that will not realign sees nothing.
        let marked = format!("  tls.key: +{}", encoded_key(0));
        assert!(
            marked.rsplit(' ').next().unwrap_or_default().len() % 4 != 0,
            "the fixture proves nothing unless the run is misaligned"
        );
        assert!(
            audit_content("secret.yaml", &marked).is_err(),
            "a marked single-line value must be refused"
        );
    }

    /// Collapsed onto one line, a key reaches JSON, dotenv, Terraform, a chart
    /// value and an inline literal with no delimiter standing alone anywhere,
    /// whatever escaped its newlines and whatever else shares the line.
    #[test]
    fn a_key_collapsed_onto_one_line_is_refused() {
        let key = leaked_private_key();
        let collapsed = |escape: &str| key.trim_end().replace('\n', escape);
        let mut holders = Vec::new();
        for escape in ["\\n", "\\r\\n", "\\r", ""] {
            let key = collapsed(escape);
            holders.extend([
                format!("{{\"tls.key\": \"{key}\"}}"),
                format!("TLS_KEY=\"{key}\""),
                format!("  private_key = \"{key}\""),
                format!("const KEY: &str = \"{key}\";"),
                format!("helm upgrade app . --set tls.key='{key}'"),
                // The delimiter is not the last thing on the line in any of
                // these, which is every position an anchor cannot reach.
                format!("{{\"tls.key\": \"{key}\", \"other\": 1}}"),
                format!("key = \"{key}\"  # rotate me"),
                format!("INSERT INTO secrets VALUES ('{key}', 1);"),
                format!("name,key,note\nleak,\"{key}\",rotate"),
                format!("[\"{key}\", \"second\"]"),
                format!("<secret key=\"{key}\" id=\"1\"/>"),
            ]);
        }
        for holder in holders {
            assert!(
                audit_content("bad.txt", &holder).is_err(),
                "a collapsed private key must be refused: {holder}"
            );
        }

        // Every label a private key is issued under, collapsed. Without a label
        // allowance nothing but the unlabelled form is recognised.
        for label in [
            "RSA ",
            "EC ",
            "DSA ",
            "ENCRYPTED ",
            "OPENSSH ",
            "X25519 ",
            "SSH2 ENCRYPTED ",
        ] {
            let labelled = leaked_private_key()
                .replace("BEGIN ", &["BEGIN ", label].concat())
                .replace("END ", &["END ", label].concat());
            let collapsed = labelled.trim_end().replace('\n', "\\n");
            assert!(
                audit_content("bad.json", &format!("{{\"k\": \"{collapsed}\"}}")).is_err(),
                "a collapsed {label}key must be refused"
            );
        }

        // A multi-line literal closes with the quoting of whatever embedded it,
        // so the last delimiter never reaches the end of its line.
        let literal = format!("const KEY: &str = \"{}\";", key.trim_end());
        assert!(audit_content("bad.rs", &literal).is_err());
    }

    /// A rule that asks how long the longest unbroken run of the body is falls
    /// to rewrapping it, and the body of a real key can be rewrapped at any
    /// Encrypting a traditional key replaces its body with ciphertext, so there is
    /// no structure left to read. Every label but the two carrying separate literal
    /// cover would otherwise walk through, which is what asking the body rather
    /// than the header costs.
    #[test]
    fn an_encrypted_traditional_key_is_refused_whatever_its_label() {
        for label in ["EC", "DSA", "ANY-FUTURE-LABEL"] {
            let key = [
                "-----BEGIN ",
                label,
                " PRIVATE ",
                "KEY-----\n",
                "Proc-Type: 4,ENCRYPTED\n",
                "DEK-Info: AES-256-CBC,8E4A1B2C3D4E5F60718293A4B5C6D7E8\n\n",
                // Ciphertext decodes to nothing in particular, which is the point.
                "9k3Hs0mQxV2pLbY7cRtN4wZaEjFgUiOpAsDfGhJkLzXcVbNm1234567890abcdef\n",
                "-----END ",
                label,
                " PRIVATE ",
                "KEY-----\n",
            ]
            .concat();

            assert!(
                audit_content("bad.pem", &key).is_err(),
                "an encrypted {label} key must be refused"
            );
            let collapsed = key.trim_end().replace('\n', "\\n");
            assert!(
                audit_content("bad.json", &format!("{{\"tls.key\": \"{collapsed}\"}}")).is_err(),
                "an encrypted {label} key must be refused collapsed onto one line"
            );
        }
    }

    /// The ssh.com format opens with a magic number where the others open with a
    /// structure, so it needs naming or it is simply not looked for.
    #[test]
    fn an_rfc4716_key_is_refused() {
        let mut material = vec![0x3f, 0x6f, 0xf9, 0xeb];
        material.extend(std::iter::repeat_n(0xcd, 60));
        let body = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &material);
        let key = [
            "-----BEGIN ",
            "PRIVATE ",
            "KEY-----\n",
            &body,
            "\n-----END ",
            "PRIVATE ",
            "KEY-----\n",
        ]
        .concat();
        let collapsed = key.trim_end().replace('\n', "\\n");

        assert!(
            audit_content("bad.json", &format!("{{\"tls.key\": \"{collapsed}\"}}")).is_err(),
            "an RFC 4716 key must be refused"
        );
    }

    /// A SEQUENCE may declare no length at all. DER forbids it and OpenSSL reads
    /// it anyway, so a key encoded that way is a key, and a rule that recognises
    /// only declared lengths would hand it straight through.
    #[test]
    fn a_key_whose_sequence_declares_no_length_is_refused() {
        // SEQUENCE, indefinite length, then the version integer of an
        // unencrypted key, then enough body to be one.
        let mut material = vec![0x30, 0x80, 0x02, 0x01, 0x00];
        material.extend(std::iter::repeat_n(0xab, 45));
        let body = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &material);
        let key = [
            "-----BEGIN ",
            "PRIVATE ",
            "KEY-----\n",
            &body,
            "\n-----END ",
            "PRIVATE ",
            "KEY-----\n",
        ]
        .concat();

        assert!(
            audit_content("bad.pem", &key).is_err(),
            "an indefinite-length key must be refused"
        );
        let collapsed = key.trim_end().replace('\n', "\\n");
        assert!(
            audit_content("bad.json", &format!("{{\"tls.key\": \"{collapsed}\"}}")).is_err(),
            "an indefinite-length key must be refused collapsed onto one line too"
        );
    }

    /// width at all without ceasing to be one.
    #[test]
    fn a_collapsed_key_is_refused_at_every_wrap_width() {
        // Whatever put the body on one line left something between its lines,
        // and both an escape and a plain space are used in the wild.
        let mut missed = Vec::new();
        for separator in ["\\n", " ", "\\r\\n", "\\t"] {
            for width in 1..=76usize {
                let collapsed = rewrapped_private_key(4, width)
                    .trim_end()
                    .replace('\n', separator);
                if audit_content("bad.json", &format!("{{\"tls.key\": \"{collapsed}\"}}")).is_ok() {
                    missed.push((separator, width));
                }
            }
        }
        assert!(
            missed.is_empty(),
            "a rewrapped collapsed key escaped at {} (separator, width) pairs: {missed:?}",
            missed.len()
        );

        // A body long enough to be an exported keyring is judged, not skipped:
        // being too long to fit a window is a reason to bound what the audit
        // copies, never a reason to stop reading what it has.
        let keyring = leaked_private_key_of(255);
        let collapsed = keyring.trim_end().replace('\n', "\\n");
        assert!(
            collapsed.len() > 16_300,
            "the body must be keyring-sized, not key-sized: {}",
            collapsed.len()
        );
        for carrier in [
            collapsed.clone(),
            format!("TLS_KEY=\"{collapsed}\""),
            format!("{{\"tls.key\": \"{collapsed}\"}}"),
            format!(
                "apiVersion: v1\nkind: Secret\ndata:\n  tls.key: {}\n",
                encode(&collapsed)
            ),
        ] {
            assert!(
                audit_content("leak.txt", &carrier).is_err(),
                "a keyring-sized collapsed key must be refused, carrier of {} characters",
                carrier.len()
            );
        }

        // The smallest key anyone issues is the floor, and a redaction shorter
        // than one is not a key however much it looks like encoded text.
        let smallest = leaked_private_key_of(1);
        assert_eq!(
            smallest.lines().nth(1).expect("a body line").len(),
            64,
            "the floor is the body of an Ed25519 key"
        );
        assert!(
            audit_content(
                "bad.json",
                &format!(
                    "{{\"k\": \"{}\"}}",
                    smallest.trim_end().replace('\n', "\\n")
                )
            )
            .is_err()
        );
        let begin = ["-----BEGIN ", "PRIVATE ", "KEY-----"].concat();
        let end = ["-----END ", "PRIVATE ", "KEY-----"].concat();
        let redacted = "REDACTED".repeat(4);
        assert_eq!(redacted.len(), 32);
        assert!(
            audit_content("ok.md", &format!("{begin}\\n{redacted}\\n{end}")).is_ok(),
            "a redaction shorter than the smallest key body is not a key"
        );
    }

    /// What a body is cannot be measured, only decoded. These are the forms the
    /// delimiters wrap, each recognised by the structure it actually has, and a
    /// prefix of each, because a key cut short is still a key.
    #[test]
    fn a_collapsed_body_is_judged_by_what_it_decodes_to() {
        // A PKCS#8 header: SEQUENCE, version zero, then the algorithm.
        let pkcs8 = [
            vec![0x30, 0x82, 0x04, 0xbd, 0x02, 0x01, 0x00, 0x30, 0x0d],
            vec![0x2a; 200],
        ]
        .concat();
        // SEC1 names version one, and an encrypted key opens with its algorithm.
        let sec1 = [
            vec![0x30, 0x77, 0x02, 0x01, 0x01, 0x04, 0x20],
            vec![0x11; 114],
        ]
        .concat();
        let encrypted = [
            vec![0x30, 0x82, 0x01, 0x1e, 0x30, 0x58, 0x06, 0x09],
            vec![0x33; 200],
        ]
        .concat();
        let openssh = [b"openssh-key-v1\0".to_vec(), vec![0x44; 200]].concat();
        let pgp = [
            vec![0x95, 0x01, 0xa2, 0x04, 0x64, 0x00, 0x00, 0x00, 0x01],
            vec![0x55; 200],
        ]
        .concat();
        for (name, material) in [
            ("PKCS#8", pkcs8),
            ("SEC1", sec1),
            ("an encrypted key", encrypted),
            ("OpenSSH", openssh),
            ("PGP", pgp),
        ] {
            assert!(
                is_private_key_material(&material),
                "{name} must be recognised by its structure"
            );
            assert!(
                is_private_key_material(&material[..48]),
                "a prefix of {name} must be recognised too"
            );
            let encoded =
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &material);
            let begin = ["-----BEGIN ", "PRIVATE ", "KEY-----"].concat();
            let end = ["-----END ", "PRIVATE ", "KEY-----"].concat();
            assert!(
                audit_content("bad.json", &format!("{begin}\\n{encoded}\\n{end}")).is_err(),
                "{name} collapsed onto one line must be refused"
            );
        }

        // Structure is not something arbitrary bytes have.
        for (name, bytes) in [
            (
                "text",
                b"the quick brown fox jumps over the lazy dog again".to_vec(),
            ),
            (
                "a sequence that is too short",
                vec![0x30, 0x20, 0x02, 0x01, 0x00],
            ),
            (
                "a sequence declaring less than a key",
                [vec![0x30, 0x20, 0x02, 0x01, 0x00], vec![0x66; 200]].concat(),
            ),
            (
                "a sequence with no version and no algorithm",
                [
                    vec![0x30, 0x82, 0x04, 0xbd, 0x13, 0x01, 0x00],
                    vec![0x77; 200],
                ]
                .concat(),
            ),
        ] {
            assert!(
                !is_private_key_material(&bytes),
                "{name} is not private key material"
            );
        }
    }

    /// Two shapes a length cannot judge: a body whose runs are too short to
    /// average anything, and source whose identifiers are long enough to look
    /// like one. Both were wrong under the measurement this replaced.
    #[test]
    fn what_no_length_could_separate_is_separated_by_decoding() {
        let key = leaked_private_key_of(4);
        let body = key
            .lines()
            .skip(1)
            .take_while(|line| !line.contains("END"))
            .collect::<String>();
        let begin = ["-----BEGIN ", "PRIVATE ", "KEY-----"].concat();
        let end = ["-----END ", "PRIVATE ", "KEY-----"].concat();

        // Runs of fifteen average below sixteen; a run of sixty-four followed by
        // single characters averages below one and a half.
        let narrow = body
            .as_bytes()
            .chunks(15)
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect::<Vec<_>>()
            .join(" ");
        let (head, tail) = body.split_at(64);
        let singles = tail
            .chars()
            .map(|character| character.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let mixed = format!("{head} {singles}");
        for (shape, collapsed) in [("runs of fifteen", narrow), ("one run then singles", mixed)] {
            assert!(
                audit_content("bad.json", &format!("{begin} {collapsed} {end}")).is_err(),
                "a key body as {shape} must be refused"
            );
        }

        // Three long identifiers between the two names are still source, in
        // the shape that puts nothing but a comma between them.
        let identifiers = [
            "encodedPrivateKeyMaterial",
            "reconstructedDelimiters",
            "serialisedBodyBetween",
        ];
        assert!(
            identifiers.iter().map(|name| name.len()).sum::<usize>() >= 64,
            "the shape only bites when the identifiers are long"
        );
        for source in [
            format!(
                "let pem = [{begin}, {}, {end}].concat();",
                identifiers.join(", ")
            ),
            format!("let pem = concat({begin},{},{end});", identifiers.join(",")),
        ] {
            assert!(
                audit_content("ok.rs", &source).is_ok(),
                "long identifiers between the two names are not a key: {source}"
            );
        }
    }

    /// A key cut short has no closing delimiter, and a key pasted into the
    /// middle of a line has the rest of the line after it. Neither stops it
    /// being a key, and neither is something a length could have known.
    #[test]
    fn a_key_without_a_closing_delimiter_is_still_refused() {
        let collapsed = leaked_private_key_of(4).trim_end().replace('\n', "\\n");
        let begin = ["-----BEGIN ", "PRIVATE ", "KEY-----"].concat();
        let opening = collapsed
            .split_once("KEY-----")
            .expect("the opening delimiter")
            .1;
        for shape in [
            format!("{begin}{opening}"),
            format!("{begin}{}", &opening[..opening.len() / 3]),
            format!("let pem = \"{begin}{opening}\" + footer + trailerGoesHereToo;"),
        ] {
            assert!(
                audit_content("bad.rs", &shape).is_err(),
                "a key without its closing delimiter must be refused"
            );
        }

        // Six decoded bytes is all a key cut this short leaves, and it is still
        // the head of a key.
        let head = format!("{begin}\\nMIGuAgEA");
        assert!(audit_content("bad.rs", &head).is_err());
    }

    /// A credential encoded and then broken up inside one line is that
    /// credential once the pieces are put back together, and joining them is
    /// safe because only the decoded bytes are ever matched.
    #[test]
    fn a_credential_split_inside_one_line_is_refused() {
        let token = ["gh", "p_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat();
        let encoded = encode(&token);
        for width in [8, 16, 24, 32] {
            let pieces = encoded
                .as_bytes()
                .chunks(width)
                .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
                .collect::<Vec<_>>();
            for line in [
                format!("  token: {}", pieces.join(" ")),
                format!("  token: \"{}\"", pieces.join("\\n")),
                format!(
                    "[{}]",
                    pieces
                        .iter()
                        .map(|piece| format!("\"{piece}\""))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ] {
                assert!(
                    audit_content("secret.yaml", &line).is_err(),
                    "a credential split into {width}-character pieces must be refused: {line}"
                );
            }
        }
    }

    /// Source that names both delimiters is how PEM gets built, parsed and
    /// documented, and this gate reads history: one such line, once committed,
    /// would refuse every release that ever follows it.
    #[test]
    fn source_that_names_the_delimiters_is_not_a_key() {
        let begin = ["-----BEGIN ", "PRIVATE ", "KEY-----"].concat();
        let end = ["-----END ", "PRIVATE ", "KEY-----"].concat();
        let ordinary = [
            format!("    if !contents.starts_with(b\"{begin}\\n\") {{"),
            format!("let pem = format!(\"{begin}\\n{{body}}\\n{end}\\n\");"),
            format!("// keys start with {begin} and end with {end}"),
            format!("const FOOTER: &str = \"\\n{end}\\n\";"),
            format!("let pem = \"{begin}\" + body + \"{end}\";"),
            format!("assert_eq!(lines.first(), Some(&\"{begin}\"));"),
            format!("expected the header {begin} before the body"),
            format!("{begin} is written by every implementation"),
            // Long enough that summing every encoded character between the two
            // names reaches the floor for a key body; what it is not is a body.
            format!(
                "// the file opens with {begin}, then the base64 encoded DER of \
                 the private key itself, and finally the trailer {end}"
            ),
        ];
        for line in ordinary {
            assert!(
                audit_content("ok.rs", &line).is_ok(),
                "ordinary source must not be read as a key: {line}"
            );
        }

        // The body is what separates them, so a real one is still refused in
        // exactly the shape those lines take.
        let body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDBJ0000000000";
        assert!(
            audit_content(
                "bad.rs",
                &format!("let pem = \"{begin}\\n{body}\\n{end}\";")
            )
            .is_err()
        );
    }

    /// `ssh-keygen -e` and `PuTTY` both write private keys that carry no PEM
    /// delimiter for any anchor to match.
    #[test]
    fn key_formats_without_a_pem_delimiter_are_refused() {
        let ssh2 = [
            "---- BEGIN ",
            "SSH2 ENCRYPTED PRIVATE ",
            "KEY ----\nComment: \"key\"\n---- END ",
            "SSH2 ENCRYPTED PRIVATE ",
            "KEY ----\n",
        ]
        .concat();
        assert!(audit_content("bad.txt", &ssh2).is_err());
        let ssh2_plain = ["---- BEGIN ", "SSH2 PRIVATE ", "KEY ----"].concat();
        assert!(audit_content("bad.txt", &ssh2_plain).is_err());

        let putty = [
            "PuTTY-User-",
            "Key-File-3: ssh-ed25519\nEncryption: none\n",
            "Private-",
            "Lines: 1\n",
        ]
        .concat();
        assert!(audit_content("bad.ppk", &putty).is_err());
        for header in [
            ["PuTTY-User-", "Key-File-2: ssh-rsa"].concat(),
            ["Private-", "Lines: 14"].concat(),
        ] {
            assert!(audit_content("bad.ppk", &header).is_err());
        }

        // Naming either format in prose or in an expression is not a key. Both
        // markers are followed by a value the format fixes, and prose that opens
        // with the same words is not.
        assert!(audit_content("ok.md", "generated with ssh-keygen -e (RFC 4716)").is_ok());
        for prose in [
            ["let header = \"PuTTY-User-", "Key-File-3:\";"].concat(),
            [
                "PuTTY-User-",
                "Key-File-3 is the version this release writes",
            ]
            .concat(),
            ["PuTTY-User-", "Key-File- headers name the format version"].concat(),
            [
                "Private-",
                "Lines: how many lines the private body occupies",
            ]
            .concat(),
            ["Private-", "Lines is the field that introduces the body"].concat(),
        ] {
            assert!(
                audit_content("ok.md", &prose).is_ok(),
                "prose that opens with a marker is not a key: {prose}"
            );
        }
    }

    /// A connection string with the password inline is the credential this
    /// project will hold most of, and the loopback ones its own workflow has
    /// always carried are exempt by their exact text, so nothing about the
    /// exemption can be adopted by a leak.
    #[test]
    fn a_connection_string_carrying_a_password_is_refused() {
        let password = "s3cr3t";
        for leak in [
            ["postgres://app:", password, "@db.example.com:5432/app"].concat(),
            [
                "postgresql://app:",
                password,
                "@10.0.0.4/app?sslmode=require",
            ]
            .concat(),
            [
                "PGSHARD_URL=\"postgresql://app:",
                password,
                "@db.internal/app\"",
            ]
            .concat(),
            [
                "  url: postgres://postgres:",
                password,
                "@127.0.0.1:5432/shardschema",
            ]
            .concat(),
        ] {
            assert!(
                audit_content("bad.yaml", &leak).is_err(),
                "an inline password must be refused: {leak}"
            );
        }

        for permitted in PERMITTED_TEST_DSNS {
            assert!(
                audit_content("ci.yml", &format!("  URL: {permitted}")).is_ok(),
                "the workflow's own loopback string must stay auditable: {permitted}"
            );
            let extended = permitted.replace("shardschema", "elsewhere");
            assert!(
                extended == permitted || audit_content("ci.yml", &extended).is_err(),
                "the exemption must not extend past its exact text: {extended}"
            );
            let repassworded = permitted.replace("pgshard-ci-only", "hunter2");
            assert!(
                repassworded == permitted || audit_content("ci.yml", &repassworded).is_err(),
                "the exemption must not survive a different password: {repassworded}"
            );
        }

        for permitted in PERMITTED_TEST_DSNS {
            assert!(
                audit_content("ci.yml", &format!("  URL: {permitted}_extra")).is_err(),
                "the exemption must not extend to anything appended to it"
            );
        }
        assert!(
            audit_content(
                "bad.yaml",
                &["  url: POSTGRESQL://app:", password, "@db.example.com/app"].concat()
            )
            .is_err(),
            "a scheme in capitals is the same scheme"
        );

        // A connection string with no password is how every other one in this
        // repository is written.
        for safe in [
            "postgresql://unused",
            "postgresql://postgres@127.0.0.1/shardschema?sslmode=disable",
            "postgresql://pgshard_pooler@127.0.0.1:5432/shardschema",
        ] {
            assert!(
                audit_content("ok.rs", safe).is_ok(),
                "not a password: {safe}"
            );
        }
    }

    /// Reading inside a container is a decompressor per format; refusing one is
    /// a stop the maintainer can see.
    #[test]
    fn compressed_content_is_refused() {
        for magic in [
            vec![0x1f, 0x8b, 0x08, 0x00],
            b"PK\x03\x04ordinary zip".to_vec(),
            b"PK\x07\x08spanned zip".to_vec(),
            vec![0xfd, b'7', b'z', b'X', b'Z', 0x00, 0x00],
            vec![0x28, 0xb5, 0x2f, 0xfd, 0x00],
            b"BZh9ordinary bzip".to_vec(),
            vec![0x04, 0x22, 0x4d, 0x18, 0x00],
            b"7z\xbc\xaf\x27\x1c".to_vec(),
            b"Rar!\x1a\x07\x00".to_vec(),
            vec![0x78, 0x9c, 0x00],
            vec![0x78, 0x01, 0x00],
            vec![0x78, 0xda, 0x00],
            vec![0x78, 0x5e, 0x00],
        ] {
            assert!(
                audit_content_bytes("asset.bin", &magic).is_err(),
                "compressed content must be refused: {:?}",
                &magic[..4.min(magic.len())]
            );
        }
        assert!(audit_content_bytes("ok.bin", b"PKZIP is a program").is_ok());
        assert!(audit_content_bytes("ok.md", b"BZh is the bzip2 magic").is_ok());
        assert!(audit_content_bytes("ok.bin", &[0x1f, 0x8b, 0x09]).is_ok());
    }

    /// This audit is the only gate between a mistake and a permanently public
    /// credential, and it holds for every issuer the project's own tooling can
    /// reach, not only for the one whose forge hosts the repository.
    #[test]
    fn credentials_from_other_issuers_are_refused() {
        let leaks = [
            ["AS", "IA", "IOSFODNN7EXAMPLE"].concat(),
            ["AI", "za", "SyA0123456789abcdefghijklmnopqrstuvw"].concat(),
            ["npm", "_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat(),
            ["gl", "pat-", "0123456789abcdefghij"].concat(),
            ["sk-", "ant-", "api03-0123456789"].concat(),
            ["dckr", "_pat_", "0123456789abcdefghij"].concat(),
            ["xox", "b-", "0123456789-0123456789"].concat(),
            ["xox", "p-", "0123456789-0123456789"].concat(),
            ["xox", "a-", "0123456789-0123456789"].concat(),
            ["xox", "s-", "0123456789-0123456789"].concat(),
            ["xox", "r-", "0123456789-0123456789"].concat(),
            ["xox", "e-", "0123456789-0123456789"].concat(),
        ];
        for leak in &leaks {
            assert!(
                audit_content("bad.env", &format!("TOKEN={leak}")).is_err(),
                "a leaked credential must be refused: {leak}"
            );
            assert!(
                audit_content(
                    "secret.yaml",
                    &format!("  token: {}\n", encode(leak.as_str()))
                )
                .is_err(),
                "an encoded leaked credential must be refused: {leak}"
            );
        }

        // A prefix that is also ordinary text is not a credential on its own.
        for innocent in [
            ["AS", "IA", "_PACIFIC_REGION"].concat(),
            ["npm", "_", "config_registry"].concat(),
            ["npm", "_", "package_json"].concat(),
            ["AI", "za", "rd support"].concat(),
        ] {
            assert!(
                audit_content("ok.md", &innocent).is_ok(),
                "ordinary text must not be read as a credential: {innocent}"
            );
        }
    }

    /// UTF-16 and NUL padding keep every byte of a credential but never place
    /// two of its characters next to each other.
    #[test]
    fn a_credential_padded_with_nul_bytes_is_refused() {
        let token = ["gh", "p_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat();
        let utf16: Vec<u8> = token.encode_utf16().flat_map(u16::to_le_bytes).collect();
        assert!(
            audit_content_bytes("leak.bin", &utf16).is_err(),
            "a UTF-16 credential must be refused"
        );
        let big_endian: Vec<u8> = token.encode_utf16().flat_map(u16::to_be_bytes).collect();
        assert!(audit_content_bytes("leak.bin", &big_endian).is_err());
        assert!(audit_content_bytes("ok.bin", &[0, 0xff, 0xfe, b's', b'a', b'f', b'e', 0]).is_ok());
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

        for label in ["", "EC ", "DSA ", "RSA ", "OPENSSH "] {
            for delimiter in ["-----BEGIN ", "-----END "] {
                let pem = [delimiter, label, "PRIVATE ", "KEY-----"].concat();
                assert!(
                    audit_content("bad.md", &pem).is_err(),
                    "a {label}private key PEM delimiter must be refused"
                );
                let indented = ["   ", &pem].concat();
                assert!(
                    audit_content("bad.md", &indented).is_err(),
                    "indentation must not smuggle a {label}private key past the audit"
                );
            }
        }
        let pgp = ["-----BEGIN ", "PGP PRIVATE ", "KEY BLOCK", "-----"].concat();
        assert!(audit_content("bad.md", &pgp).is_err());

        // The agent must be able to validate that a key file has the expected
        // PEM header, and the operator to name the PEM type.
        let validator = [
            "if !contents.starts_with(b\"-----BEGIN ",
            "PRIVATE ",
            "KEY-----",
            "\\n\") {",
        ]
        .concat();
        assert!(audit_content("ok.rs", &validator).is_ok());

        for prefix in ["p", "s", "o", "u", "r"] {
            let token = ["gh", prefix, "_", "0123456789abcdef"].concat();
            assert!(
                audit_content("bad.md", &token).is_err(),
                "a gh{prefix}_ token must be refused"
            );
        }

        // The operator legitimately names the PEM type without delimiters, and
        // refusing that would make the gate unusable rather than safe.
        assert!(audit_content("ok.go", "pem.Block{Type: \"PRIVATE KEY\"}").is_ok());
        assert!(audit_content("ok.go", "expected exactly one PRIVATE KEY PEM block").is_ok());
    }

    #[test]
    fn dependabot_metadata_must_cover_only_patch_or_minor_updates() {
        let patch = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-patch\n...";
        let minor = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-minor\n...";
        let mixed = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-patch\n- dependency-name: tokio\n  update-type: version-update:semver-minor\n...";
        let major = "---\nupdated-dependencies:\n- dependency-name: serde\n  update-type: version-update:semver-major\n...";
        let incomplete = "---\nupdated-dependencies:\n- dependency-name: serde\n...";
        assert!(dependabot_patch_or_minor([patch]));
        assert!(dependabot_patch_or_minor([minor]));
        assert!(dependabot_patch_or_minor([mixed]));
        assert!(!dependabot_patch_or_minor([major]));
        assert!(!dependabot_patch_or_minor([incomplete]));
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
        assert!(workflow.contains("github.event.workflow_run.actor.login == 'dependabot[bot]'"));
        assert!(workflow.contains(
            "group: pgshard-dependabot-automerge-${{ github.event.workflow_run.head_sha }}"
        ));
        assert!(workflow.contains("cancel-in-progress: false"));
        assert!(!workflow.contains("queue: max"));

        let ci = include_str!("../../../.github/workflows/ci.yml");
        let release = include_str!("../../../.github/workflows/release.yml");
        assert!(ci.contains("workflow_dispatch"));
        assert!(ci.contains(
            "group: pgshard-ci-${{ github.event_name == 'pull_request' && github.run_id || 'main' }}"
        ));
        assert_eq!(ci.matches("queue: max").count(), 1);
        assert!(ci.contains("aggregate:"));
        assert_eq!(ci.matches(".github/scripts/ci-diff-base.sh").count(), 3);
        assert!(ci.contains("latest released first-parent commit"));
        assert!(ci.contains("^\\.github/scripts/ci-diff-base\\.sh$"));
        assert_eq!(ci.matches("ci-diff-base.sh --audit").count(), 2);
        assert!(release.contains("workflow_run:"));
        assert!(release.contains("workflows: [CI]"));
        assert!(release.contains("github.event.workflow_run.conclusion == 'success'"));
        assert!(release.contains("github.event.workflow_run.head_repository.full_name"));
        assert!(release.contains("github.event.workflow_run.event == 'workflow_dispatch'"));
        assert!(
            release.contains("startsWith(github.event.workflow_run.head_branch, 'pgshard-ci-')")
        );
        assert!(release.contains("pgshard-source-release-${{"));
        assert!(release.contains("'eligible' || github.run_id"));
        assert_eq!(release.matches("queue: max").count(), 1);
        assert!(release.contains("PGSHARD_RELEASE_SHA"));
        assert!(release.contains("--ready-only"));
        assert!(release.contains("git/ref/heads/main"));
        assert!(release.contains("ref: ${{ github.sha }}"));
        assert!(release.contains("Delete temporary ref after release"));
        assert!(release.contains("Deploy released main documentation"));
        assert!(release.contains("actions: read"));
        assert!(release.contains("actions/workflows/ci.yml/runs?head_sha=$live_sha"));
        assert!(release.contains("git tag --points-at \"$live_sha\""));
        assert!(ci.contains("github.event_name != 'pull_request'"));
        assert!(ci.contains("needs.changes.outputs.website_exists == 'true'"));
        assert!(release.contains("run_ids=\"$("));
        assert!(release.contains("done <<< \"$run_ids\""));
        assert!(!release.contains("done < <("));
        assert!(release.contains("run-id: ${{ steps.candidate.outputs.run_id }}"));
        assert!(!ci.contains("Deploy documentation to GitHub Pages"));
    }

    /// The shipped script, not a copy of it: a copy drifts, and the gate this
    /// asserts on is the one that authorizes a release.
    fn aggregate_gate_script() -> String {
        // Taken from the step the assertions validate, not found by splitting
        // the file: a text search can land on a decoy `run:` while the step
        // that actually decides the release is something else entirely.
        aggregate_gate_step(&parsed_workflow())["run"]
            .as_str()
            .expect("the aggregate gate step runs a script")
            .to_owned()
    }

    fn run_aggregate_gate(expectations: &str) -> (bool, String) {
        run_aggregate_gate_with("rust=true\n", expectations)
    }

    fn run_aggregate_gate_with(components: &str, expectations: &str) -> (bool, String) {
        let output = Command::new("bash")
            .arg("-c")
            .arg(aggregate_gate_script())
            .env("COMPONENT_OUTPUTS", components)
            .env("JOB_EXPECTATIONS", expectations)
            .output()
            .expect("run the aggregate gate");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    /// A release is authorized by this gate, so a component's tests not having
    /// run may only pass when the detector said that component was untouched.
    #[test]
    fn the_aggregate_refuses_a_skip_the_detector_did_not_authorize() {
        let (passed, stderr) =
            run_aggregate_gate("rust-test=success=true go-operator=skipped=false");
        assert!(
            passed,
            "a skip the detector authorized was rejected: {stderr}"
        );

        let (passed, stderr) =
            run_aggregate_gate("rust-test=skipped=true go-operator=success=true");
        assert!(!passed, "a gate skipped while its component changed passed");
        assert!(stderr.contains("rust-test=skipped"), "{stderr}");

        // An empty expectation is what a failed or skipped detector produces.
        let (passed, stderr) = run_aggregate_gate("rust-test=skipped=");
        assert!(!passed, "a skip with no expectation behind it passed");
        assert!(stderr.contains("No expectation for rust-test"), "{stderr}");

        for state in ["failure", "cancelled"] {
            let (passed, _) = run_aggregate_gate(&format!("rust-test={state}=false"));
            assert!(!passed, "an untouched component still shipped on {state}");
        }
    }

    /// An expectation is a boolean expression, so a detector output that was
    /// never emitted is falsy and indistinguishable from a legitimate `false`.
    /// The detector's own reporting therefore has to be checked as a literal.
    #[test]
    fn a_detector_that_does_not_report_a_component_fails_closed() {
        let (passed, stderr) =
            run_aggregate_gate_with("rust=true\ngo=true\n", "rust-static=success=true");
        assert!(passed, "a fully reported detector was rejected: {stderr}");

        // `go=true ` is not `'true'` to the condition that gates the job, so
        // the job is skipped -- and a check that let the value be trimmed
        // before comparing would see a clean `true` and excuse that skip.
        for unreported in [
            "go=",
            "go=maybe",
            "go=TRUE",
            "go=true ",
            "go=true\t",
            "go= true",
        ] {
            let (passed, stderr) = run_aggregate_gate_with(
                &format!("rust=true\n{unreported}\n"),
                "go-operator=skipped=false",
            );
            assert!(!passed, "a skip authorized by '{unreported}' was accepted");
            assert!(stderr.contains("Detector did not report go"), "{stderr}");
        }
    }

    /// Every detector output an expectation consumes has to be one the gate
    /// validates, that validation has to derive the named component's own
    /// validity, and the detector has to declare and emit it. Parsed as YAML:
    /// a line-based reading of a YAML file can be fooled by continuation
    /// lines that fold into the previous scalar, which is exactly how a wrong
    /// mapping can hide behind a correct-looking one.
    #[test]
    fn every_consumed_detector_output_is_validated() {
        let workflow = parsed_workflow();
        let environment = aggregate_gate_step(&workflow)["env"]
            .as_mapping()
            .expect("the aggregate step declares an environment");

        let validated: std::collections::BTreeSet<String> = environment["COMPONENT_OUTPUTS"]
            .as_str()
            .expect("COMPONENT_OUTPUTS is a scalar")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let (name, derivation) = line
                    .trim()
                    .split_once('=')
                    .expect("an entry names a component");
                assert_eq!(
                    derivation,
                    format!(
                        "${{{{ needs.changes.outputs.{name} == 'true' \
                         || needs.changes.outputs.{name} == 'false' }}}}"
                    ),
                    "the {name} entry does not derive {name}'s validity from {name}"
                );
                name.to_owned()
            })
            .collect();
        assert!(!validated.is_empty(), "no detector outputs are validated");

        let expectations = environment["JOB_EXPECTATIONS"]
            .as_str()
            .expect("JOB_EXPECTATIONS is a scalar");
        for consumed in expectations.split("needs.changes.outputs.").skip(1) {
            let name: &str = consumed
                .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .next()
                .expect("an output reference names an output");
            assert!(
                validated.contains(name),
                "the gate consumes {name} without validating the detector reported it"
            );
        }

        // The detector's own mapping, read as YAML rather than as lines: a
        // declaration folded into a previous scalar is not a declaration, and
        // must not be able to stand in for the real one.
        let detector = workflow_job(&workflow, "changes");
        let declared = detector["outputs"]
            .as_mapping()
            .expect("the detector declares its outputs");
        let detect = serde_norway::to_string(&detector["steps"]).expect("the detect step renders");
        for name in &validated {
            let source = declared
                .get(serde_norway::Value::from(name.as_str()))
                .unwrap_or_else(|| {
                    panic!("the gate validates {name}, which the detector does not declare")
                })
                .as_str()
                .unwrap_or_else(|| panic!("the {name} declaration is not a scalar"));
            // A declaration exposing another component's detection is a
            // valid-but-wrong value: it skips the jobs it gates while every
            // other check still agrees.
            assert_eq!(
                source.trim(),
                format!("${{{{ steps.detect.outputs.{name} }}}}"),
                "the {name} output does not expose {name}'s detection"
            );
            // A declared output the detect step never writes renders empty, so
            // the gate fails closed on every run. That is loud rather than
            // dangerous, but it is cheaper to catch here.
            assert!(
                detect.contains(&format!("emit_component {name} "))
                    || detect.contains(&format!("{name}=")),
                "the detector declares {name} but never emits it"
            );
        }
    }

    /// The gate step, selected by identity: `steps[0]` would let a prepended
    /// decoy satisfy every structural assertion while the real gate differed.
    /// Its lack of an `if:` is part of the assertion — a gate that can be
    /// conditioned out is not a gate.
    fn aggregate_gate_step(workflow: &serde_norway::Value) -> &serde_norway::Value {
        let steps = workflow_job(workflow, "aggregate")["steps"]
            .as_sequence()
            .expect("the aggregate declares steps");
        let named: Vec<&serde_norway::Value> = steps
            .iter()
            .filter(|step| step["name"].as_str() == Some("Require every applicable job"))
            .collect();
        assert_eq!(named.len(), 1, "the aggregate gate step is not unique");
        let step = named[0];
        assert!(
            step.get("if").is_none(),
            "the aggregate gate step is conditional"
        );
        assert!(
            step.get("uses").is_none() && step.get("run").is_some(),
            "the aggregate gate step does not run a script of its own"
        );
        assert_step_environment(
            step,
            &["COMPONENT_OUTPUTS", "JOB_EXPECTATIONS"],
            "the aggregate gate step",
        );
        step
    }

    /// What each aggregated job is gated on, restated here so the workflow is
    /// not its own only authority.
    ///
    /// A shape check alone accepts a job gated on the wrong component -- the
    /// Go jobs gated on `rust`, say -- because that is still a component
    /// detection, still mirrored faithfully into the expectation, and still
    /// skips the jobs it gates on an operator-only change. Changing what a job
    /// is gated on therefore has to be done twice: once in the workflow and
    /// once here, where it is reviewed as code.
    const ALWAYS: &str = "";
    const GATED_ON: [(&str, &str); 21] = [
        ("changes", ALWAYS),
        ("repository-policy", ALWAYS),
        ("rust-static", "needs.changes.outputs.rust == 'true'"),
        ("rust-test", "needs.changes.outputs.rust == 'true'"),
        (
            "catalog-postgres",
            "needs.changes.outputs.catalog == 'true'",
        ),
        (
            "orch-catalog-postgres",
            "needs.changes.outputs.orch_catalog == 'true'",
        ),
        (
            "pgwire-postgres",
            "needs.changes.outputs.pgwire == 'true' || needs.changes.outputs.pooler_postgres == 'true'",
        ),
        ("pgwire-fuzz", "needs.changes.outputs.pgwire == 'true'"),
        (
            "planner-postgres",
            "needs.changes.outputs.planner == 'true'",
        ),
        ("protobuf", "needs.changes.outputs.proto == 'true'"),
        ("go-operator", "needs.changes.outputs.go == 'true'"),
        ("operator-kind", "needs.changes.outputs.go == 'true'"),
        (
            "operator-kind-manager",
            "needs.changes.outputs.go == 'true' || needs.changes.outputs.images == 'true'",
        ),
        (
            "operator-kind-quarantine",
            "needs.changes.outputs.go == 'true' || needs.changes.outputs.images == 'true'",
        ),
        (
            "website",
            "needs.changes.outputs.website == 'true' || (github.event_name != 'pull_request' && needs.changes.outputs.website_exists == 'true')",
        ),
        ("ui", "needs.changes.outputs.ui == 'true'"),
        ("integration", "needs.changes.outputs.integration == 'true'"),
        ("images", "needs.changes.outputs.images == 'true'"),
        (
            "agent-postgres",
            "needs.changes.outputs.postgres_agent == 'true'",
        ),
        ("kind", "needs.changes.outputs.kind == 'true'"),
        ("performance", "needs.changes.outputs.performance == 'true'"),
    ];

    /// Whether a job's condition is built only from component detections.
    ///
    /// The expectations mirror each job's condition, so a condition weakened
    /// with anything else -- `&& false`, an actor check, a branch check --
    /// would be mirrored faithfully and the resulting skip accepted.
    fn condition_is_component_shaped(
        condition: &str,
        components: &std::collections::BTreeSet<String>,
    ) -> bool {
        let mut residue = condition.to_owned();
        // Longest first: replacing `website` before `website_exists` would
        // leave `_exists == 'true'` behind and read as a foreign term.
        let mut names: Vec<&String> = components.iter().collect();
        names.sort_by_key(|name| std::cmp::Reverse(name.len()));
        for name in names {
            residue = residue.replace(&format!("needs.changes.outputs.{name} == 'true'"), " ");
        }
        residue = residue.replace("github.event_name != 'pull_request'", " ");
        residue.chars().all(|character| {
            character.is_whitespace() || matches!(character, '(' | ')' | '|' | '&')
        })
    }

    /// The tests in this file are what stop a gate being retired, so they
    /// cannot run from a job that a gate governs: the edit that retires a gate
    /// would also stop the test that catches it. They run from the
    /// unconditional policy job, which `GATED_ON` pins as unconditional.
    #[test]
    fn the_gate_tests_run_from_an_unconditional_job() {
        let workflow = parsed_workflow();
        let policy = workflow_job(&workflow, "repository-policy");
        assert!(
            policy.get("if").is_none(),
            "the job that proves the gates is itself gated"
        );
        let steps = steps_of(policy, "the job that proves the gates");
        // The aggregate is the one externally required check, and GitHub counts
        // a conditionally skipped job as passing. `if: github.event_name ==
        // '\''issues'\''` on it is therefore a one-line disarmament that leaves
        // every named step untouched.
        let aggregate = workflow_job(&workflow, "aggregate");
        assert_eq!(
            aggregate.get("if").and_then(serde_norway::Value::as_str),
            Some("always()"),
            "the aggregate is conditioned on something other than always()"
        );
        steps_of(aggregate, "the aggregate");
        // Identity, not existence: any other step carrying that text would
        // otherwise satisfy this while the named step ran something else.
        let enforcing: Vec<&serde_norway::Value> = steps
            .iter()
            .filter(|step| step["name"].as_str() == Some("Prove the gates cannot be retired"))
            .collect();
        assert_eq!(enforcing.len(), 1, "the enforcement step is not unique");
        let enforcing = enforcing[0];
        assert!(
            enforcing.get("uses").is_none(),
            "the enforcement step defers to an action instead of running the tests"
        );
        assert_eq!(
            enforcing["run"]
                .as_str()
                .expect("the enforcement step runs a command")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" "),
            "cargo test --locked -p pgshard-release",
            "the enforcement step does not run this crate's tests"
        );
        assert_step_environment(enforcing, &[], "the enforcement step");
        assert_eq!(
            GATED_ON
                .iter()
                .find(|(job, _)| *job == "repository-policy")
                .map(|(_, condition)| *condition),
            Some(ALWAYS),
            "the policy job is not pinned as unconditional"
        );
    }

    /// A job's steps, having proved none of them — nor the job — can be
    /// conditioned out or allowed to fail. A step carries its own `if:`, and
    /// one keyed on an event the workflow never receives runs never while the
    /// job still reports success; `continue-on-error` is the same hole spelled
    /// differently.
    fn steps_of<'a>(job: &'a serde_norway::Value, described: &str) -> &'a [serde_norway::Value] {
        assert!(
            job.get("continue-on-error").is_none(),
            "{described} may fail without failing"
        );
        assert!(
            job.get("defaults").is_none(),
            "{described} sets step defaults, which can replace the shell"
        );
        // A job-level `env:` reaches every step of the job, so it is the same
        // hole as a step-level one with a wider blast radius.
        assert!(
            job.get("env").is_none(),
            "{described} sets an environment for every one of its steps"
        );
        let steps = job["steps"].as_sequence().expect("the job declares steps");
        for step in steps {
            let keys: std::collections::BTreeSet<&str> = step
                .as_mapping()
                .expect("a step is a mapping")
                .keys()
                .filter_map(serde_norway::Value::as_str)
                .collect();
            // An allowlist rather than a list of known-bad keys: the ways to
            // stop a step doing its job are not enumerable in advance, and a
            // key nobody considered is exactly the one that gets used.
            for key in &keys {
                assert!(
                    matches!(
                        *key,
                        "name" | "id" | "run" | "shell" | "uses" | "with" | "env"
                    ),
                    "a step of {described} carries `{key}`, which nothing here constrains"
                );
            }
            // Allowlisting `shell` as a key says nothing about its value:
            // `shell: /bin/true {0}` hands the script to a program that never
            // reads it, so the step succeeds having executed nothing.
            if let Some(shell) = step.get("shell") {
                assert_eq!(
                    shell.as_str(),
                    Some("bash"),
                    "a step of {described} runs under a shell that need not execute it"
                );
            }
        }
        steps
    }

    /// A job outside the aggregate's `needs` is a job outside the gate.
    ///
    /// The exempt jobs are the ones that run after it, so they are also the
    /// ones that could take its display name: branch protection requires a
    /// check called `CI aggregate`, and a second job publishing a check run
    /// under that name lets a decoy answer for the real gate.
    fn assert_every_job_is_aggregated(workflow: &serde_norway::Value, waited: &[&str]) {
        let claiming: Vec<&str> = workflow["jobs"]
            .as_mapping()
            .expect("the workflow declares jobs")
            .iter()
            .filter(|(_, job)| job["name"].as_str() == Some("CI aggregate"))
            .filter_map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(
            claiming,
            ["aggregate"],
            "the required check name is published by something other than the aggregate alone"
        );
        // A literal comparison only sees literal names. `name: ${{ ... }}` is
        // evaluated at run time and can render as anything, including the
        // required check name, so a name carrying an expression is constrained
        // to a shape whose fixed prefix cannot produce it.
        for (job, definition) in workflow["jobs"]
            .as_mapping()
            .expect("the workflow declares jobs")
            .iter()
            .filter_map(|(job, definition)| Some((job.as_str()?, definition)))
        {
            let Some(name) = definition["name"].as_str() else {
                continue;
            };
            if !name.contains("${{") {
                continue;
            }
            let (literal, expression) = name
                .split_once(" / ")
                .unwrap_or_else(|| panic!("{job}'s computed name has no fixed prefix: {name}"));
            assert!(
                !literal.contains("${{") && literal != "CI aggregate",
                "{job}'s computed name can render as the required check name: {name}"
            );
            assert!(
                expression.starts_with("${{ matrix.") && expression.ends_with("}}"),
                "{job}'s computed name is not a matrix leg suffix: {name}"
            );
        }
        for job in workflow["jobs"]
            .as_mapping()
            .expect("the workflow declares jobs")
            .keys()
            .filter_map(serde_norway::Value::as_str)
        {
            assert!(
                waited.contains(&job)
                    || matches!(
                        job,
                        "aggregate" | "release" | "pages" | "cleanup-dependabot-ci-ref"
                    ),
                "{job} is neither aggregated nor one of the jobs that follow the gate"
            );
        }
    }

    /// A job whose `success` the aggregate consumes must not be able to
    /// report it while a step of it failed.
    fn assert_nothing_swallows_failure(job: &serde_norway::Value, described: &str) {
        assert!(
            job.get("continue-on-error").is_none(),
            "{described} may fail without failing"
        );
        assert!(
            job.get("defaults").is_none(),
            "{described} sets step defaults, which can replace the shell"
        );
        assert!(
            job.get("env").is_none(),
            "{described} sets an environment for every one of its steps"
        );
        for step in job["steps"].as_sequence().expect("the job declares steps") {
            assert!(
                step.get("continue-on-error").is_none(),
                "a step of {described} may fail without failing it"
            );
        }
    }

    /// The environment a gate step may declare, as an exact set.
    ///
    /// `env:` is a master key: `SHELLOPTS: noexec` makes bash parse the script
    /// and exit zero without running a line of it, and `BASH_ENV` pointed at a
    /// file containing `exit 0` does the same — with `run:` still stating
    /// exactly what the step was supposed to do.
    fn assert_step_environment(step: &serde_norway::Value, allowed: &[&str], described: &str) {
        let declared: std::collections::BTreeSet<&str> = step
            .get("env")
            .and_then(serde_norway::Value::as_mapping)
            .map(|env| env.keys().filter_map(serde_norway::Value::as_str).collect())
            .unwrap_or_default();
        let allowed: std::collections::BTreeSet<&str> = allowed.iter().copied().collect();
        assert_eq!(
            declared, allowed,
            "{described} does not declare exactly the environment it is allowed"
        );
    }

    fn parsed_workflow() -> serde_norway::Value {
        let workflow: serde_norway::Value =
            serde_norway::from_str(include_str!("../../../.github/workflows/ci.yml"))
                .expect("the workflow is valid YAML");
        assert!(
            workflow.get("defaults").is_none(),
            "the workflow sets step defaults, which can replace every shell"
        );
        // Workflow-level `env:` reaches every step of every job at once.
        let environment: std::collections::BTreeSet<&str> = workflow
            .get("env")
            .and_then(serde_norway::Value::as_mapping)
            .map(|env| env.keys().filter_map(serde_norway::Value::as_str).collect())
            .unwrap_or_default();
        assert_eq!(
            environment,
            ["CARGO_TERM_COLOR", "RUST_BACKTRACE"].into_iter().collect(),
            "the workflow declares an environment for every step of every job"
        );
        workflow
    }

    fn workflow_job<'a>(workflow: &'a serde_norway::Value, job: &str) -> &'a serde_norway::Value {
        let job = &workflow["jobs"][job];
        assert!(!job.is_null(), "the workflow declares no job named it");
        job
    }

    /// Every job the aggregate waits on has to carry an expectation, that
    /// expectation has to read that job's own result, and it has to repeat the
    /// job's own `if:` exactly. Any of the three being wrong retires the gate
    /// while the aggregate still reports success.
    #[test]
    fn every_aggregated_job_expects_its_own_condition() {
        let workflow = parsed_workflow();
        let waited: Vec<&str> = workflow_job(&workflow, "aggregate")["needs"]
            .as_sequence()
            .expect("the aggregate declares its needs")
            .iter()
            .map(|job| job.as_str().expect("a needed job is named"))
            .collect();

        let environment = &aggregate_gate_step(&workflow)["env"];
        let components: std::collections::BTreeSet<String> = environment["COMPONENT_OUTPUTS"]
            .as_str()
            .expect("COMPONENT_OUTPUTS is a scalar")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                line.trim()
                    .split_once('=')
                    .expect("an entry names a component")
                    .0
                    .to_owned()
            })
            .collect();
        let expectations = environment["JOB_EXPECTATIONS"]
            .as_str()
            .expect("the aggregate declares its expectations");
        let declared: Vec<(&str, &str, &str)> = expectations
            .lines()
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let (job, rest) = entry.split_once('=').expect("an entry names a job");
                let (result, expected) = rest
                    .split_once("}}=")
                    .map_or((rest, ""), |(result, expected)| {
                        (result.trim_end(), expected)
                    });
                (job, result, expected)
            })
            .collect();
        assert_eq!(
            waited.len(),
            declared.len(),
            "the aggregate's needs and expectations are not the same set"
        );
        // Lengths alone survive substitution: needing one job twice in place of
        // another keeps every count identical while the replaced job stops
        // being aggregated at all.
        let unique: std::collections::BTreeSet<&&str> = waited.iter().collect();
        assert_eq!(
            unique.len(),
            waited.len(),
            "the aggregate needs a job twice"
        );
        assert_every_job_is_aggregated(&workflow, &waited);
        for (job, _) in &GATED_ON {
            assert!(
                waited.contains(job),
                "{job} has a restated condition but is not aggregated"
            );
            // The jobs the aggregate consumes are what `success` is claimed
            // about. They may legitimately carry conditional steps, so the
            // guarantee asserted here is narrower than for a gate job: nothing
            // may let a step fail without failing the job, because that is what
            // turns `success` into a claim about work that did not pass.
            assert_nothing_swallows_failure(workflow_job(&workflow, job), job);
        }
        assert_eq!(
            waited.len(),
            GATED_ON.len(),
            "the restated conditions do not cover exactly the aggregated jobs"
        );
        for job in &waited {
            let (_, result, expected) = declared
                .iter()
                .find(|(name, ..)| name == job)
                .unwrap_or_else(|| panic!("the aggregate waits on {job} without requiring it"));
            // The result must be this job's, not a neighbour's: reading another
            // job's result would let this one vanish unnoticed.
            assert_eq!(
                *result,
                format!("${{{{ needs.{job}.result"),
                "{job}'s expectation does not read {job}'s own result"
            );
            let condition = workflow_job(&workflow, job)
                .get("if")
                .and_then(serde_norway::Value::as_str)
                .map(|condition| condition.split_whitespace().collect::<Vec<_>>().join(" "));
            assert_eq!(
                *expected,
                condition.clone().map_or_else(
                    || "true".to_owned(),
                    |condition| format!("${{{{ {condition} }}}}")
                ),
                "{job}'s expectation does not repeat {job}'s own condition"
            );
            // Mirroring is only a gate while the thing mirrored is a component
            // detection. A condition weakened by any other term would be
            // mirrored just as faithfully, and the skip it caused accepted.
            if let Some(condition) = &condition {
                assert!(
                    condition_is_component_shaped(condition, &components),
                    "{job} is gated on something other than its components: {condition}"
                );
            }
            let (_, gated_on) = GATED_ON
                .iter()
                .find(|(name, _)| name == job)
                .unwrap_or_else(|| panic!("{job} has no restated condition"));
            assert_eq!(
                condition.unwrap_or_default(),
                *gated_on,
                "{job} is not gated on what it is supposed to be gated on"
            );
        }
    }

    #[test]
    fn exact_ci_refs_require_full_object_ids() {
        assert!(is_complete_sha(&"a".repeat(40)));
        assert!(!is_complete_sha(&"A".repeat(40)));
        assert!(!is_complete_sha(&"a".repeat(39)));
        assert!(!is_complete_sha(&format!("{}g", "a".repeat(39))));

        let run = WorkflowRun {
            id: 17,
            head_branch: format!("pgshard-ci-{}", "a".repeat(40)),
            head_sha: "a".repeat(40),
            event: "workflow_dispatch".to_owned(),
        };
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
        assert!(!dependabot_files_are_unprivileged(&[file(
            "operator/go.mod"
        )]));
        assert!(!dependabot_files_are_unprivileged(&[
            file("operator/go.mod"),
            file("crates/pgshard-pgwire/fuzz/Cargo.lock"),
        ]));
        assert!(!dependabot_files_are_unprivileged(&[
            file("operator/go.mod"),
            file("operator/go.mod"),
        ]));
        assert!(!dependabot_files_are_unprivileged(&[
            file("operator/go.mod"),
            file("operator/go.sum"),
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
    fn dependabot_covers_supported_dependency_ecosystems() {
        let configuration = include_str!("../../../.github/dependabot.yml");
        let entries = [
            ("cargo", "/"),
            ("cargo", "/crates/pgshard-pgwire/fuzz"),
            ("npm", "/website"),
            ("gomod", "/operator"),
            ("docker", "/deploy/images"),
            ("github-actions", "/"),
        ];
        assert_eq!(
            configuration.matches("  - package-ecosystem:").count(),
            entries.len()
        );
        for (ecosystem, directory) in entries {
            let entry = format!(
                "  - package-ecosystem: {ecosystem}\n    directory: {directory}\n    schedule:"
            );
            assert!(
                configuration.contains(&entry),
                "missing Dependabot entry: {entry}"
            );
        }
        let patch_group = "    groups:\n      patch-updates:\n        patterns:\n          - \"*\"\n        update-types:\n          - patch\n";
        assert_eq!(configuration.matches(patch_group).count(), entries.len());
        assert!(!configuration.contains("    ignore:"));
        assert!(!configuration.contains("version-update:semver-minor"));
        assert!(!configuration.contains("version-update:semver-major"));
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
    fn dependabot_squash_requires_verified_web_flow_commit() {
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
            "crates/pgshard-integration-tests/Cargo.toml",
            "deploy/docker-bake.hcl",
            "crates/pgshard-e2e/Cargo.toml",
            "benchmarks/Cargo.toml",
        ] {
            assert!(
                workflow.contains(&format!("exists_at_head_or_base {manifest}")),
                "CI must check {manifest} at both head and base"
            );
        }
        // What each trigger has to cover is not asserted from its text. A
        // pattern that mentions a path still skips it when the mention sits
        // inside a narrower alternative, and shell word concatenation can append
        // to a quoted pattern without the quoted part changing at all. Those
        // inputs are put to the detector itself in
        // `the_detector_fires_for_build_inputs_outside_the_crates`.
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
        assert!(workflow.contains("bufbuild/buf-action@8c6a16e16f12ba20b6470afa9c2ba9b5ba8c97c3"));
        assert!(!workflow.contains("bufbuild/buf-setup-action"));
        assert!(workflow.contains("run: make go-check"));
        assert!(makefile.contains("actionlint@v1.7.12 -ignore"));
        assert!(makefile.contains("concurrency queue key"));
        assert!(workflow.contains("      - planner-postgres"));
        assert!(workflow.contains("planner-postgres=${{ needs.planner-postgres.result }}"));
    }

    /// The repository root. CI reports changed files relative to it, so every
    /// path handed to the detector below is stated the same way.
    fn workspace_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("the release crate lives two levels below the workspace root")
            .to_owned()
    }

    fn git_stdout(arguments: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(workspace_root())
            .output()
            .unwrap_or_else(|error| panic!("git {arguments:?} runs: {error}"));
        assert!(
            output.status.success(),
            "git {arguments:?} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        String::from_utf8(output.stdout).expect("git reports paths as UTF-8")
    }

    /// Every path git tracks, which is exactly the universe CI can report as
    /// changed.
    ///
    /// `git diff --name-only` C-quotes any name outside printable ASCII, so a
    /// raw path holding a space or a newline would stand for something the
    /// detector never sees, and the scratch index below is tab-delimited for the
    /// same reason. The assumption is pinned here rather than relied upon.
    fn tracked_files() -> std::collections::BTreeSet<String> {
        let tracked: std::collections::BTreeSet<String> = git_stdout(&["ls-files", "-z"])
            .split('\0')
            .filter(|path| !path.is_empty())
            .map(str::to_owned)
            .collect();
        assert!(
            !tracked.is_empty(),
            "git tracks no file, so every requirement below would be vacuous"
        );
        for path in &tracked {
            assert!(
                path.chars().all(|character| character.is_ascii_graphic()),
                "{path} is not printable ASCII, so CI would quote it and this raw path no \
                 longer represents what the detector matches"
            );
        }
        tracked
    }

    /// A repository-relative, lexically normalized form of `path`, or `None`
    /// when it resolves outside the repository — where CI can never report a
    /// change to it, and where the toolchain and the registry live.
    fn repository_path(root: &std::path::Path, path: &str) -> Option<String> {
        let candidate = std::path::Path::new(path);
        let relative = if candidate.is_absolute() {
            candidate.strip_prefix(root).ok()?
        } else {
            candidate
        };
        let mut normalized = std::path::PathBuf::new();
        for component in relative.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if !normalized.pop() {
                        return None;
                    }
                }
                std::path::Component::Normal(part) => normalized.push(part),
                std::path::Component::Prefix(_) | std::path::Component::RootDir => return None,
            }
        }
        normalized.to_str().map(str::to_owned)
    }

    struct WorkspaceCrate {
        directory: String,
        manifest: String,
        dependencies: Vec<String>,
    }

    /// The workspace as cargo resolves it: inheritance, renames, every
    /// dependency table and every valid path spelling. Reading one of those
    /// spellings out of the manifest text agreed with cargo today and would
    /// silently shrink the closure the first time someone used another.
    fn workspace_crates() -> std::collections::BTreeMap<String, WorkspaceCrate> {
        let root = workspace_root();
        let output = std::process::Command::new(env!("CARGO"))
            .args([
                "metadata",
                "--format-version",
                "1",
                "--no-deps",
                "--locked",
                "--manifest-path",
            ])
            .arg(root.join("Cargo.toml"))
            .output()
            .expect("cargo metadata runs");
        assert!(
            output.status.success(),
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        let metadata: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("cargo metadata emits JSON");
        let packages = metadata["packages"]
            .as_array()
            .expect("cargo metadata lists the workspace packages");
        assert!(!packages.is_empty(), "cargo metadata listed no package");
        let members: std::collections::BTreeSet<&str> = packages
            .iter()
            .map(|package| package["name"].as_str().expect("a package is named"))
            .collect();
        let mut crates = std::collections::BTreeMap::new();
        for package in packages {
            let name = package["name"].as_str().expect("a package is named");
            let manifest = package["manifest_path"]
                .as_str()
                .expect("a package has a manifest");
            let manifest = repository_path(&root, manifest)
                .unwrap_or_else(|| panic!("{name} is manifested outside the repository"));
            let directory = manifest
                .strip_suffix("/Cargo.toml")
                .unwrap_or_else(|| panic!("{name} is not manifested in a directory of its own"))
                .to_owned();
            let mut dependencies: Vec<String> = package["dependencies"]
                .as_array()
                .expect("a package lists its dependencies")
                .iter()
                .map(|dependency| {
                    dependency["name"]
                        .as_str()
                        .expect("a dependency is named")
                        .to_owned()
                })
                .filter(|dependency| members.contains(dependency.as_str()))
                .collect();
            dependencies.sort();
            dependencies.dedup();
            let previous = crates.insert(
                name.to_owned(),
                WorkspaceCrate {
                    directory,
                    manifest,
                    dependencies,
                },
            );
            assert!(previous.is_none(), "cargo metadata listed {name} twice");
        }
        crates
    }

    fn transitive_closure(
        root: &str,
        crates: &std::collections::BTreeMap<String, WorkspaceCrate>,
    ) -> std::collections::BTreeSet<String> {
        let mut reached = std::collections::BTreeSet::new();
        let mut pending = vec![root.to_owned()];
        while let Some(name) = pending.pop() {
            let entry = crates
                .get(&name)
                .unwrap_or_else(|| panic!("{name} is not a workspace crate"));
            for dependency in &entry.dependencies {
                if reached.insert(dependency.clone()) {
                    pending.push(dependency.clone());
                }
            }
        }
        reached
    }

    /// The scratch build whose dep-info the coverage requirement is read from.
    /// It is kept apart from the workspace target directory so that only this
    /// build's own records are there to be read.
    ///
    /// CI carries this directory between commits in its cache, so a renamed or
    /// deleted target can leave dep-info behind. That is fail-closed — the
    /// record names a source cargo no longer reports, and it is dropped — but a
    /// record that still names live files can require a path that no longer
    /// needs covering. Deleting this directory is the cure for a failure that
    /// names a file the tree no longer has.
    fn component_detector_target() -> std::path::PathBuf {
        workspace_root().join("target/component-detector")
    }

    /// Every dep-info record the scratch build wrote.
    ///
    /// A build script is a compilation like any other and reads whatever it is
    /// given, but cargo files its record under `build/` rather than `deps/`.
    /// Reading only `deps/` left a whole class of compiled-in input — the class
    /// this check exists to catch — unseen.
    fn dep_info_files(build: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut records = Vec::new();
        let mut directories = vec![build.join("deps")];
        let scripts = build.join("build");
        if scripts.is_dir() {
            for entry in std::fs::read_dir(&scripts)
                .unwrap_or_else(|error| panic!("{} is readable: {error}", scripts.display()))
            {
                let path = entry.expect("a build directory entry").path();
                if path.is_dir() {
                    directories.push(path);
                }
            }
        }
        for directory in directories {
            for entry in std::fs::read_dir(&directory)
                .unwrap_or_else(|error| panic!("{} is readable: {error}", directory.display()))
            {
                let path = entry.expect("a dep-info directory entry").path();
                if path.extension() == Some("d".as_ref()) {
                    records.push(path);
                }
            }
        }
        assert!(
            !records.is_empty(),
            "{} holds no dep-info, so nothing could be required of the detector",
            build.display()
        );
        records
    }

    /// Compiles the test targets of each root and returns, for every source
    /// cargo built, the workspace crate it belongs to.
    ///
    /// Which crate a compilation belongs to comes from cargo, not from the
    /// artifact's file name: a test target is named after its own source, so a
    /// name would attribute it to no crate at all.
    fn compiled_target_owners(
        crates: &std::collections::BTreeMap<String, WorkspaceCrate>,
    ) -> std::collections::BTreeMap<String, String> {
        let root = workspace_root();
        let mut command = std::process::Command::new(env!("CARGO"));
        // `--workspace --all-targets`, because `rust-test` runs
        // `cargo test --workspace --all-features` and its planner-gated step
        // compiles bench targets. Taking a superset of what any live job builds
        // is the direction that fails closed.
        command.args([
            "check",
            "--locked",
            "--workspace",
            "--all-features",
            "--all-targets",
            "--message-format",
            "json",
        ]);
        let output = command
            .current_dir(&root)
            .env("CARGO_TARGET_DIR", component_detector_target())
            // Neither incremental state nor debug information changes what rustc
            // records that it read, and both are most of what this scratch build
            // would otherwise leave in the cache CI carries between runs.
            .env("CARGO_INCREMENTAL", "0")
            .env("CARGO_PROFILE_DEV_DEBUG", "none")
            .output()
            .expect("cargo check runs");
        assert!(
            output.status.success(),
            "cargo check --all-targets failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );

        let owners: std::collections::BTreeMap<&str, &str> = crates
            .iter()
            .map(|(name, entry)| (entry.manifest.as_str(), name.as_str()))
            .collect();
        let mut compiled_by: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for line in String::from_utf8(output.stdout)
            .expect("cargo emits UTF-8")
            .lines()
        {
            let message: serde_json::Value =
                serde_json::from_str(line).expect("cargo emits one JSON object a line");
            if message["reason"].as_str() != Some("compiler-artifact") {
                continue;
            }
            let manifest = message["manifest_path"]
                .as_str()
                .expect("an artifact names the manifest it was built from");
            let Some(owner) = repository_path(&root, manifest)
                .and_then(|manifest| owners.get(manifest.as_str()).copied())
            else {
                continue;
            };
            let source = message["target"]["src_path"]
                .as_str()
                .expect("an artifact names the source it was built from");
            let source = repository_path(&root, source)
                .unwrap_or_else(|| panic!("{owner} compiles {source}, outside the repository"));
            compiled_by.insert(source, owner.to_owned());
        }
        assert!(
            !compiled_by.is_empty(),
            "cargo check reported no workspace artifact, so no input could be attributed"
        );
        compiled_by
    }

    /// Which repository files each workspace crate's compilation actually reads,
    /// taken from the dep-info rustc writes beside every artifact.
    ///
    /// Reading `include_str!` out of the source missed a contract that a line
    /// break separated from its macro, and would go on missing raw strings,
    /// `concat!`, `include!`, `#[path]` modules and any name a macro builds.
    /// rustc records what it opened, whatever the spelling. `cargo check` is
    /// enough, because dep-info is written after expansion, and `--all-targets`
    /// is what puts the `cfg(test)` and bench inputs into it.
    fn compiled_inputs_by_crate(
        crates: &std::collections::BTreeMap<String, WorkspaceCrate>,
        tracked: &std::collections::BTreeSet<String>,
    ) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
        let root = workspace_root();
        let compiled_by = compiled_target_owners(crates);
        let mut inputs: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
            std::collections::BTreeMap::new();
        for path in dep_info_files(&component_detector_target().join("debug")) {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
            // Every input rustc records gets a rule of its own, which is the one
            // reading that survives a name holding a space. What git does not
            // track cannot reach a changed-file list, and is not in the checkout
            // CI builds from either, so it is dropped rather than required.
            let read: std::collections::BTreeSet<String> = text
                .lines()
                .filter_map(|line| line.strip_suffix(':'))
                .filter_map(|input| repository_path(&root, input))
                .filter(|input| tracked.contains(input))
                .collect();
            let compiled: std::collections::BTreeSet<&str> = read
                .iter()
                .filter_map(|input| compiled_by.get(input.as_str()).map(String::as_str))
                .collect();
            // A cached target directory keeps the dep-info of targets that have
            // since been renamed or deleted, and those name no source cargo just
            // reported. The per-crate assertion below is what stops this from
            // swallowing a live one.
            if compiled.is_empty() {
                continue;
            }
            assert_eq!(
                compiled.len(),
                1,
                "{} reads {read:?}, which cargo attributes to {compiled:?} rather than to one \
                 workspace crate",
                path.display()
            );
            let owner = compiled.into_iter().next().expect("exactly one owner");
            inputs.entry(owner.to_owned()).or_default().extend(read);
        }

        for (name, entry) in crates {
            let prefix = format!("{}/", entry.directory);
            assert!(
                inputs.get(name).is_some_and(|files| {
                    files.iter().any(|file| {
                        file.starts_with(&prefix)
                            && std::path::Path::new(file).extension() == Some("rs".as_ref())
                    })
                }),
                "no dep-info records a Rust source under {prefix}, so {name} was never compiled \
                 and whatever it reads would go unchecked"
            );
        }
        inputs
    }

    /// The workflow's own detector step, lifted out by identity. Taking it by
    /// position would let a step prepended to the job answer for the one CI
    /// runs.
    fn detector_step_script() -> String {
        let workflow = parsed_workflow();
        let steps = workflow["jobs"]["changes"]["steps"]
            .as_sequence()
            .expect("the detector job lists its steps");
        let detector: Vec<&serde_norway::Value> = steps
            .iter()
            .filter(|step| step["id"].as_str() == Some("detect"))
            .collect();
        assert_eq!(
            detector.len(),
            1,
            "the detector job declares exactly one step identified as detect"
        );
        let step = detector[0];
        assert_eq!(
            step["shell"].as_str(),
            Some("bash"),
            "the detector step names bash, so running it under bash is faithful"
        );
        step["run"]
            .as_str()
            .expect("the detector step runs a script")
            .to_owned()
    }

    /// Refuses to answer for CI unless the `grep` the detector will resolve is
    /// the one the runner resolves.
    ///
    /// Running the step is only faithful because the runner and this check
    /// execute the same matcher. Extended regular expressions are not one
    /// language: a drop-in such as ugrep accepts `\p{L}`, which GNU grep does
    /// not, so a developer machine could quietly approve a trigger the runner
    /// never matches. Resolved through bash, exactly as the step's own call is.
    fn assert_grep_is_the_one_ci_runs() {
        let output = std::process::Command::new("bash")
            .args(["-c", "grep --version"])
            .output()
            .expect("bash resolves grep");
        assert!(output.status.success(), "grep --version failed");
        let reported = String::from_utf8_lossy(&output.stdout);
        let first = reported.lines().next().unwrap_or_default();
        assert!(
            first.contains("GNU grep"),
            "the detector would match with {first}, not the GNU grep the runner has, so this run \
             does not say what CI would decide"
        );
    }

    /// Asks the detector what CI would decide about each candidate path.
    ///
    /// Nothing here reproduces the detector. The script is the workflow's own
    /// `run:`, run by bash, so shell word concatenation cannot append to a
    /// trigger behind this check's back; the trigger is never read, so no
    /// second regular-expression engine can disagree with the `grep -E` the
    /// script calls; and the answer is read from the `GITHUB_OUTPUT` the step
    /// writes rather than inferred.
    ///
    /// The candidate reaches the script down the path a pull request takes: two
    /// commits are written, and the step's own `git diff --name-only` between
    /// them reports that one path. The candidate is the file the base commit
    /// holds and the head commit does not, so it arrives as a deletion —
    /// `--name-only` prints the same line for a deletion as for an edit, and
    /// driving the deletion is what proves a removed input is covered too. The
    /// commits are written to a scratch object directory, so the repository's
    /// own object store is not touched.
    fn detect_components(
        candidates: &std::collections::BTreeSet<String>,
    ) -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, bool>> {
        assert!(
            !candidates.is_empty(),
            "no candidate path, so the check would be vacuous"
        );
        for candidate in candidates {
            assert!(
                candidate
                    .chars()
                    .all(|character| character.is_ascii_graphic()),
                "{candidate} is not printable ASCII, so it does not represent a path CI reports"
            );
        }
        assert_grep_is_the_one_ci_runs();
        let root = workspace_root();
        let scratch = tempfile::tempdir().expect("a scratch directory");
        let script = scratch.path().join("detect.sh");
        std::fs::write(&script, detector_step_script()).expect("the detector script is written");
        let objects = scratch.path().join("objects");
        std::fs::create_dir(&objects).expect("a scratch object directory");
        let commits = ScratchCommits {
            objects,
            alternates: format!(
                "{}/objects",
                git_stdout(&["rev-parse", "--path-format=absolute", "--git-common-dir"]).trim()
            ),
            // A committed path needs a blob and the detector only ever reads
            // names, so every candidate is written as the content of a file
            // certainly present.
            filler: git_stdout(&["rev-parse", "HEAD:Cargo.toml"])
                .trim()
                .to_owned(),
            // Git's empty tree is known to every version of it, so the head
            // commit holding nothing needs nothing written for it.
            empty_tree: "4b825dc642cb6eb9a060e54bf8d69288fbee4904".to_owned(),
        };

        // A path no trigger mentions. A detector short-circuited to test
        // everything — which is what a scheduled or dispatched run does — would
        // answer true for it, and would then answer true for every requirement
        // below without matching a thing.
        let control = "component-detector-negative-control";
        for (output, fired) in run_detector(&root, &script, &commits, control) {
            assert!(
                !fired || output == "website_exists",
                "the detector reports {output} for {control}, which no trigger mentions, so this \
                 run cannot tell a covered path from an uncovered one"
            );
        }

        let ordered: Vec<&String> = candidates.iter().collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let detected = std::sync::Mutex::new(std::collections::BTreeMap::new());
        let workers = std::thread::available_parallelism()
            .map_or(4, std::num::NonZeroUsize::get)
            .min(ordered.len());
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(candidate) = ordered.get(index) else {
                            return;
                        };
                        let outputs = run_detector(&root, &script, &commits, candidate);
                        detected
                            .lock()
                            .expect("the detector results survive every candidate")
                            .insert((*candidate).clone(), outputs);
                    }
                });
            }
        });
        let detected = detected
            .into_inner()
            .expect("the detector results survive every candidate");
        assert_eq!(
            detected.len(),
            candidates.len(),
            "every candidate was put to the detector exactly once"
        );
        detected
    }

    /// Where the two commits a detector run diffs are written, and what they are
    /// written from.
    struct ScratchCommits {
        objects: std::path::PathBuf,
        alternates: String,
        filler: String,
        empty_tree: String,
    }

    impl ScratchCommits {
        fn git(&self, root: &std::path::Path, arguments: &[&str]) -> std::process::Command {
            let mut command = std::process::Command::new("git");
            command
                .args(arguments)
                .current_dir(root)
                .env("GIT_OBJECT_DIRECTORY", &self.objects)
                .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", &self.alternates);
            // A commit needs an identity and a runner checkout has none: the
            // workflows that commit configure one first, which they would not
            // need to if git could find it. Supplying it here rather than
            // reading `user.name` keeps this working on a bare checkout, which
            // is exactly what the job running these tests has.
            for (name, value) in [
                ("GIT_AUTHOR_NAME", "pgshard component detector"),
                ("GIT_AUTHOR_EMAIL", "component-detector@invalid"),
                ("GIT_COMMITTER_NAME", "pgshard component detector"),
                ("GIT_COMMITTER_EMAIL", "component-detector@invalid"),
            ] {
                command.env(name, value);
            }
            command
        }

        fn run(&self, root: &std::path::Path, arguments: &[&str], described: &str) -> String {
            let output = self
                .git(root, arguments)
                .output()
                .unwrap_or_else(|error| panic!("git {arguments:?} runs: {error}"));
            assert!(
                output.status.success(),
                "{described} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
            String::from_utf8(output.stdout)
                .expect("git reports object ids as UTF-8")
                .trim()
                .to_owned()
        }

        /// A commit whose tree holds `candidate` and nothing else.
        fn holding(
            &self,
            root: &std::path::Path,
            candidate: &str,
            index: &std::path::Path,
        ) -> String {
            let mut stage = self
                .git(root, &["update-index", "--index-info"])
                .env("GIT_INDEX_FILE", index)
                .stdin(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("git update-index runs");
            {
                use std::io::Write as _;
                let mut input = stage.stdin.take().expect("git update-index reads an index");
                writeln!(input, "100644 {} 0\t{candidate}", self.filler)
                    .expect("the candidate is staged");
            }
            let staged = stage.wait_with_output().expect("git update-index finishes");
            assert!(
                staged.status.success(),
                "staging {candidate} failed: {}",
                String::from_utf8_lossy(&staged.stderr).trim()
            );
            let tree = self
                .git(root, &["write-tree"])
                .env("GIT_INDEX_FILE", index)
                .output()
                .expect("git write-tree runs");
            assert!(
                tree.status.success(),
                "writing a tree holding {candidate} failed: {}",
                String::from_utf8_lossy(&tree.stderr).trim()
            );
            let tree = String::from_utf8(tree.stdout)
                .expect("git reports object ids as UTF-8")
                .trim()
                .to_owned();
            self.run(
                root,
                &["commit-tree", &tree, "-m", "component detector base"],
                "committing the base tree",
            )
        }
    }

    fn run_detector(
        root: &std::path::Path,
        script: &std::path::Path,
        commits: &ScratchCommits,
        candidate: &str,
    ) -> std::collections::BTreeMap<String, bool> {
        let scratch = tempfile::tempdir().expect("a scratch directory");
        let index = scratch.path().join("index");
        let runner_temp = scratch.path().join("runner-temp");
        std::fs::create_dir(&runner_temp).expect("a runner temporary directory");
        let published = scratch.path().join("github-output");
        std::fs::write(&published, "").expect("an empty step output file");

        let base = commits.holding(root, candidate, &index);
        let head = commits.run(
            root,
            &[
                "commit-tree",
                &commits.empty_tree,
                "-p",
                &base,
                "-m",
                "component detector head",
            ],
            "committing the head tree",
        );

        let run = std::process::Command::new("bash")
            .arg(script)
            .current_dir(root)
            .env("GIT_OBJECT_DIRECTORY", &commits.objects)
            .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", &commits.alternates)
            .env("EVENT_NAME", "pull_request")
            .env("PR_BASE_SHA", &base)
            .env("PUSH_BEFORE_SHA", "")
            .env("GH_TOKEN", "")
            .env("GITHUB_SHA", &head)
            .env("GITHUB_OUTPUT", &published)
            .env("RUNNER_TEMP", &runner_temp)
            .output()
            .expect("the detector step runs");
        assert!(
            run.status.success(),
            "the detector failed on {candidate}: {}",
            String::from_utf8_lossy(&run.stderr).trim()
        );

        let mut outputs = std::collections::BTreeMap::new();
        let emitted = std::fs::read_to_string(&published).expect("the detector output is UTF-8");
        for line in emitted.lines() {
            let (output, value) = line
                .split_once('=')
                .unwrap_or_else(|| panic!("the detector published {line}, which names no output"));
            let value = match value {
                "true" => true,
                "false" => false,
                other => panic!("the detector published {output}={other}, which is not a boolean"),
            };
            let previous = outputs.insert(output.to_owned(), value);
            assert!(previous.is_none(), "the detector published {output} twice");
        }
        assert!(
            !outputs.is_empty(),
            "the detector published nothing for {candidate}"
        );
        outputs
    }

    /// The components whose job is built out of one workspace crate, and the
    /// crate it is built out of.
    ///
    /// Stated here rather than derived from the workflow. Deriving it let the
    /// file under test authorize itself: quoting a manifest path in an
    /// `exists_at_head_or_base` line is a semantic no-op that silently dropped a
    /// component while every count still agreed. What stops an entry being
    /// dropped instead is `every_gated_component_is_classified`.
    fn component_root_crates() -> std::collections::BTreeMap<&'static str, &'static str> {
        [
            ("postgres_agent", "pgshard-agent"),
            ("catalog", "pgshard-catalog"),
            ("orch_catalog", "pgshard-orch"),
            ("pgwire", "pgshard-pgwire"),
            ("pooler_postgres", "pgshard-pooler"),
            ("planner", "pgshard-planner"),
        ]
        .into_iter()
        .collect()
    }

    /// The components whose job is not built out of one workspace crate, and
    /// what each is built out of instead.
    const UNROOTED_COMPONENTS: [(&str, &str); 10] = [
        ("rust", "compiles and tests the whole workspace at once"),
        ("proto", "builds protobuf definitions, not a Rust crate"),
        ("go", "builds and tests the Go operator"),
        ("website", "builds the documentation site"),
        ("website_exists", "reports availability rather than change"),
        ("ui", "builds the admin interface"),
        ("integration", "runs suites drawn from the whole workspace"),
        (
            "images",
            "builds container images out of the whole repository",
        ),
        ("kind", "runs a Kubernetes cluster against the built images"),
        (
            "performance",
            "runs benchmarks that have no crate in the workspace",
        ),
    ];

    /// The components a skipped job costs the most: each one gates work that no
    /// other job repeats.
    const LIVE_COMPONENTS: &[&str] = &[
        "postgres_agent",
        "catalog",
        "orch_catalog",
        "pgwire",
        "pooler_postgres",
        "planner",
        "rust",
        "images",
    ];

    /// The components the workflow gates a job on, read off the conditions
    /// `every_aggregated_job_expects_its_own_condition` pins to the workflow.
    fn gated_components() -> std::collections::BTreeSet<&'static str> {
        let mut gated: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for (_, condition) in GATED_ON {
            for term in condition.split("needs.changes.outputs.").skip(1) {
                let name = term
                    .split_once(' ')
                    .unwrap_or_else(|| panic!("{condition} names a component it does not compare"))
                    .0;
                gated.insert(name);
            }
        }
        assert!(
            !gated.is_empty(),
            "no job is gated on a component, so this check would be vacuous"
        );
        gated
    }

    /// The components no change can make the detector report, and the manifest
    /// whose absence is why.
    ///
    /// `website_exists` is not a trigger at all — it reports availability. The
    /// other two are declared against a manifest the repository does not have,
    /// so their outputs are permanently false and requiring them would prove
    /// nothing. That also makes their share of a widened trigger a dead path:
    /// reverting `^\.github/scripts/` for either is invisible, and stays so
    /// until the manifest exists. The manifests are asserted absent, so adding
    /// one puts the component back into the requirement rather than leaving it
    /// excused.
    const UNDETECTABLE_COMPONENTS: [(&str, Option<&str>); 3] = [
        ("website_exists", None),
        ("ui", Some("ui/package.json")),
        ("performance", Some("benchmarks/Cargo.toml")),
    ];

    /// Every component the workflow gates a job on is either built out of a
    /// workspace crate or explicitly is not.
    ///
    /// Without this, dropping a component from `component_root_crates` retires
    /// its coverage in silence, and a component added to the workflow never
    /// enters the coverage requirement at all. `GATED_ON` is what the workflow's
    /// own conditions are pinned to, so every list here is answerable to it.
    #[test]
    fn every_gated_component_is_classified() {
        let gated = gated_components();
        let rooted: std::collections::BTreeSet<&str> =
            component_root_crates().keys().copied().collect();
        let unrooted: std::collections::BTreeSet<&str> =
            UNROOTED_COMPONENTS.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            unrooted.len(),
            UNROOTED_COMPONENTS.len(),
            "a component is declared unrooted twice"
        );
        assert!(
            rooted.is_disjoint(&unrooted),
            "a component is declared both built out of a crate and not"
        );
        let classified: std::collections::BTreeSet<&str> =
            rooted.union(&unrooted).copied().collect();
        assert_eq!(
            gated, classified,
            "every component the workflow gates a job on has to be classified here, or its \
             coverage is never required of anything"
        );
        let live: std::collections::BTreeSet<&str> = LIVE_COMPONENTS.iter().copied().collect();
        assert_eq!(
            live.len(),
            LIVE_COMPONENTS.len(),
            "a component is declared live twice"
        );
        assert!(
            live.is_subset(&gated),
            "a component declared live gates no job, so requiring the detector to report it \
             proves nothing"
        );
        assert!(
            rooted.is_subset(&live),
            "a component whose job is built out of a crate of its own is live by construction, \
             and dropping it here drops what the loose build inputs are required against"
        );

        // Derived rather than restated: a hand-kept second copy of the
        // workflow's component set loses a name without anything objecting, and
        // the directory it is required for is the only probe covering that tree.
        let tracked = tracked_files();
        let mut detectable = gated.clone();
        for (component, manifest) in UNDETECTABLE_COMPONENTS {
            assert!(
                detectable.remove(component),
                "{component} gates no job, so excusing it from detection proves nothing"
            );
            if let Some(manifest) = manifest {
                assert!(
                    !tracked.contains(manifest),
                    "{manifest} is present, so {component} can be detected and has to be required \
                     rather than excused"
                );
            }
        }
        let available: std::collections::BTreeSet<&str> =
            EVERY_AVAILABLE_COMPONENT.iter().copied().collect();
        assert_eq!(
            available.len(),
            EVERY_AVAILABLE_COMPONENT.len(),
            "a component is declared available twice"
        );
        assert_eq!(
            available, detectable,
            "the components a change can be detected against are the gated ones the repository \
             can report, and this list has to be exactly those"
        );
    }

    /// A live `PostgreSQL` job skipped because something it compiles changed is
    /// a silently unverified merge: the aggregate counts a skipped job as a
    /// pass. So every file that reaches a component's binary has to make the
    /// detector fire, and every part of that is taken from the world rather than
    /// described — git says which files exist, cargo says which crates a
    /// component is built from, rustc says which files it read, and the
    /// workflow's own step says what CI would do about each of them.
    ///
    /// Whole-directory triggering is deliberate: a `README` change runs a job
    /// that did not need to run, which costs time, while a missed source change
    /// merges something no job verified.
    #[test]
    fn every_component_detects_every_file_its_job_compiles() {
        let expected = component_root_crates();
        let crates = workspace_crates();
        let script = detector_step_script();
        for (component, root) in &expected {
            let entry = crates
                .get(*root)
                .unwrap_or_else(|| panic!("the workspace no longer contains {root}"));
            let declaration = format!(
                "exists_at_head_or_base {} && {component}_exists=true",
                entry.manifest
            );
            assert!(
                script.contains(&declaration),
                "the detector no longer declares {component} against {root}"
            );
        }

        let tracked = tracked_files();
        let compiled = compiled_inputs_by_crate(&crates, &tracked);

        // `rust-test` runs `cargo test --workspace`, so its component is built
        // out of every crate there is. Stating that as a closure rather than
        // leaving it out is what makes a contract compiled into any crate the
        // detector's problem, not only one a live job happens to name.
        let mut behind: std::collections::BTreeMap<
            &'static str,
            std::collections::BTreeSet<String>,
        > = std::collections::BTreeMap::new();
        for (component, root) in &expected {
            let mut closure = transitive_closure(root, &crates);
            closure.insert((*root).to_owned());
            behind.insert(component, closure);
        }
        behind.insert("rust", crates.keys().cloned().collect());

        let mut required: std::collections::BTreeMap<&str, std::collections::BTreeSet<String>> =
            std::collections::BTreeMap::new();
        for (component, closure) in behind {
            let mut paths = std::collections::BTreeSet::new();
            for name in &closure {
                let entry = crates
                    .get(name)
                    .unwrap_or_else(|| panic!("{name} is not a workspace crate"));
                let prefix = format!("{}/", entry.directory);
                let owned: Vec<&String> = tracked
                    .iter()
                    .filter(|path| path.starts_with(&prefix))
                    .collect();
                assert!(
                    !owned.is_empty(),
                    "git tracks no file under {prefix}, so requiring it would be vacuous"
                );
                paths.extend(owned.into_iter().cloned());
                paths.extend(compiled.get(name).into_iter().flatten().cloned());
            }
            required.insert(component, paths);
        }

        let candidates: std::collections::BTreeSet<String> =
            required.values().flatten().cloned().collect();
        let detected = detect_components(&candidates);
        for (component, paths) in &required {
            for path in paths {
                let outputs = detected
                    .get(path)
                    .unwrap_or_else(|| panic!("{path} was not put to the detector"));
                let fired = outputs
                    .get(*component)
                    .unwrap_or_else(|| panic!("the detector publishes no {component} output"));
                assert!(
                    *fired,
                    "{component} is built from {path}, but the detector leaves {component} false, \
                     so that change would merge with its job skipped"
                );
            }
        }
    }

    /// A build input no compilation records, named either exactly or as the
    /// directory it is one of.
    enum Probe {
        File(&'static str),
        Directory(&'static str),
    }

    impl Probe {
        /// The names a probe uses that are deliberately absent from the tree.
        ///
        /// A direct child is what separates a trigger that watches the directory
        /// from one narrowed to the files in it today. The nested name is there
        /// because a trigger narrowed by one level — `^contracts/[^/]+$` —
        /// matches every current file and the direct child alike while it
        /// silently stops covering `contracts/v2/anything`.
        ///
        /// Two blind spots, both inherent to probing with a finite set of names
        /// rather than an oversight, and both in the shape "a trigger nobody
        /// anticipated".
        ///
        /// DEPTH. These names refute a narrowing shallower than the deepest one
        /// probed and nothing beyond it. `^contracts/([^/]+/)?[^/]+$` passes
        /// both of these, and an `^operator/([^/]+/){0,4}[^/]+$` passes anything
        /// short of five levels. Only a trigger that is a plain prefix is
        /// actually safe, and reading one to find out is what this whole file
        /// refuses to do. Add depth here if a narrowing of that shape ever
        /// looks reachable.
        ///
        /// SIBLINGS. Broadening escapes them from the other side:
        /// `^extensions/pgshard_fence/` widened to `^extensions/pgshard_` picks
        /// up a second extension while matching neither name generated here.
        /// That direction is the one this file's policy calls the safe one — it
        /// runs a job that did not need to run — and the dangerous direction,
        /// narrowing, is what the fire assertions catch. Left alone
        /// deliberately.
        fn absent_names(&self) -> Vec<String> {
            match self {
                Self::File(_) => Vec::new(),
                Self::Directory(directory) => vec![
                    format!("{directory}/added-after-this-test"),
                    format!("{directory}/added-after-this-test/nested-added-after-this-test"),
                ],
            }
        }

        /// The paths a probe puts to the detector: the absent names, and every
        /// file that is there.
        fn candidates(&self, tracked: &std::collections::BTreeSet<String>) -> Vec<String> {
            match self {
                Self::File(path) => vec![(*path).to_owned()],
                Self::Directory(directory) => {
                    let prefix = format!("{directory}/");
                    let mut paths = self.absent_names();
                    paths.extend(
                        tracked
                            .iter()
                            .filter(|path| path.starts_with(&prefix))
                            .cloned(),
                    );
                    paths
                }
            }
        }

        fn named(&self) -> &'static str {
            match self {
                Self::File(path) | Self::Directory(path) => path,
            }
        }

        /// The tracked files a probe accounts for.
        fn accounts_for<'a>(
            &self,
            tracked: &'a std::collections::BTreeSet<String>,
        ) -> Vec<&'a String> {
            match self {
                Self::File(path) => tracked.get(*path).into_iter().collect(),
                Self::Directory(directory) => {
                    let prefix = format!("{directory}/");
                    tracked
                        .iter()
                        .filter(|path| path.starts_with(&prefix))
                        .collect()
                }
            }
        }
    }

    /// Build inputs a live job reads from inside a crate directory, which the
    /// crate's own compilation does not record.
    ///
    /// The agent compiles whole catalog directories in without depending on the
    /// catalog crate, so nothing in the Cargo graph puts them behind its
    /// component.
    const PROBED_INPUTS: [(&str, &[Probe]); 2] = [
        ("rust", &[Probe::Directory(".cargo")]),
        (
            "postgres_agent",
            &[
                Probe::Directory("crates/pgshard-catalog/migrations"),
                Probe::Directory("crates/pgshard-catalog/inventory"),
                Probe::Directory("crates/pgshard-catalog/testdata"),
            ],
        ),
    ];

    /// Every component `.github/scripts` decides for. The helper there computes
    /// the diff base every one of them is detected against, so a second script
    /// beside it is theirs too. `ui` and `performance` are absent because
    /// neither exists to be detected.
    const EVERY_AVAILABLE_COMPONENT: &[&str] = &[
        "postgres_agent",
        "catalog",
        "orch_catalog",
        "pgwire",
        "pooler_postgres",
        "planner",
        "rust",
        "images",
        "proto",
        "go",
        "website",
        "integration",
        "kind",
    ];

    /// Every tracked path outside the workspace crates: the components the
    /// detector has to report for it, and the components it has to leave alone.
    ///
    /// What a job reads that no compilation records — the bake file, the ignore
    /// file that defines its context, the format policy the image copies in, the
    /// shell helpers the steps invoke, the contracts the operator reads at run
    /// time — cannot be derived. Deriving it would mean running docker buildx
    /// and make, which is what the jobs themselves are for. So it is stated, and
    /// stated exhaustively: this has to account for the whole repository outside
    /// the crates, which is the only way a tree that no probe reaches becomes
    /// impossible rather than merely unlucky. Three inputs of the agent job were
    /// missing while the list was a list of files someone thought of.
    ///
    /// The refusals are what pin a distinction rather than describe it. Over
    /// triggering is deliberate almost everywhere — a `README` change running a
    /// job that did not need to run costs time, and a missed source change
    /// merges something no job verified — so a refusal is stated only where the
    /// boundary is the point: the agent bakes `deploy/images`, not all of
    /// `deploy`, and runs the fence out of `extensions/pgshard_fence`, not all
    /// of `extensions`. A refusal is checked against the absent names only.
    /// Applied to the files that are there it would contradict the narrower
    /// entry that follows it.
    ///
    /// LIMIT, and it is the important one: a `Directory` entry accounts for
    /// every future file beneath it, so this suite proves each path is CLAIMED,
    /// never that the claim is CORRECT. `extensions` is claimed for `images`
    /// alone, so a second extension — `extensions/pgshard_other/foo.c` —
    /// arrives already classified while `postgres_agent` skips it, and nothing
    /// here will say so. Split the entry when that happens.
    ///
    /// A refusal does not stand in the way of fixing that. It forbids the
    /// blanket form only — widening the agent to `^extensions/` or `^deploy/`,
    /// which erases the distinction the narrower entry beside it exists to
    /// record. Covering the new tree specifically is what a refusal leaves
    /// alone: adding `^extensions/pgshard_other/`, or a
    /// `^deploy/agent-entrypoint\.sh$`, passes with nothing here touched. Only
    /// the agent genuinely consuming all of `extensions` forces an edit, and it
    /// is the one-line removal of `postgres_agent` from the refusal, with the
    /// reasoning to weigh three lines above it.
    ///
    /// The crates themselves are not here: `every_component_detects_every_file_its_job_compiles`
    /// requires every one of their files against the components built from them.
    const OUTSIDE_THE_CRATES: [(Probe, &[&str], &[&str]); 32] = [
        (
            Probe::File(".dockerignore"),
            &["images", "postgres_agent"],
            &[],
        ),
        (Probe::File(".editorconfig"), &[], &[]),
        (Probe::File(".gitignore"), &[], &[]),
        (Probe::File("CODE_OF_CONDUCT.md"), &[], &[]),
        (Probe::File("CONTRIBUTING.md"), &[], &[]),
        (Probe::File("Cargo.lock"), LIVE_COMPONENTS, &[]),
        (Probe::File("Cargo.toml"), LIVE_COMPONENTS, &[]),
        (Probe::File("LICENSE"), &[], &[]),
        (Probe::File("Makefile"), LIVE_COMPONENTS, &[]),
        (Probe::File("README.md"), &[], &[]),
        (Probe::File("SECURITY.md"), &[], &[]),
        (Probe::File("buf.yaml"), &["proto"], &[]),
        (Probe::File("deny.toml"), &["rust"], &[]),
        (Probe::File("rust-toolchain.toml"), LIVE_COMPONENTS, &[]),
        (
            Probe::File("rustfmt.toml"),
            &["rust", "images", "postgres_agent"],
            &[],
        ),
        (
            Probe::Directory(".github/scripts"),
            EVERY_AVAILABLE_COMPONENT,
            &[],
        ),
        (
            Probe::File(".github/workflows/ci.yml"),
            LIVE_COMPONENTS,
            &[],
        ),
        (Probe::File(".github/dependabot.yml"), &["rust"], &[]),
        (Probe::File(".github/pull_request_template.md"), &[], &[]),
        (
            Probe::File(".github/workflows/dependabot-automerge.yml"),
            &["rust"],
            &[],
        ),
        // A schedule, so it gates nothing this detector decides. It is here for
        // the one component that does compile it: the release crate reads it to
        // prove the absolute audit still runs somewhere that fails.
        (
            Probe::File(".github/workflows/dependency-advisories.yml"),
            &["rust"],
            &[],
        ),
        (Probe::File(".github/workflows/release.yml"), &["rust"], &[]),
        // The Go operator reads the shared contracts at run time, so no
        // compilation records them for it. Its own tests are what fail when a
        // contract and the operator disagree.
        (
            Probe::Directory("contracts"),
            &["rust", "go", "orch_catalog"],
            &[],
        ),
        (
            Probe::Directory("deploy"),
            &["images", "kind"],
            &["postgres_agent"],
        ),
        // Only this subtree reaches the agent image: its bake target builds
        // rust.Dockerfile, which copies out of here.
        (
            Probe::Directory("deploy/images"),
            &["images", "kind", "postgres_agent"],
            &[],
        ),
        (
            Probe::File("deploy/docker-bake.hcl"),
            &["images", "kind", "postgres_agent"],
            &[],
        ),
        (
            Probe::Directory("extensions"),
            &["images"],
            &["postgres_agent"],
        ),
        (
            Probe::Directory("extensions/pgshard_fence"),
            &["images", "postgres_agent"],
            &[],
        ),
        (
            Probe::Directory("operator"),
            &["go"],
            // The orchestrator watches one subtree of the operator, below, and
            // this is what keeps that from quietly becoming all of it.
            &[
                "postgres_agent",
                "catalog",
                "orch_catalog",
                "pgwire",
                "pooler_postgres",
                "planner",
                "rust",
            ],
        ),
        // Nothing in the orchestrator crate reads this package, so it is an
        // over-trigger rather than a compiled input — kept because narrowing a
        // trigger on a reading of intent is the direction that merges unverified
        // changes, and pinned here so it cannot be dropped by accident.
        (
            Probe::Directory("operator/internal/tuning"),
            &["go", "orch_catalog"],
            &[],
        ),
        (
            Probe::Directory("proto"),
            &["rust", "go", "proto"],
            &["postgres_agent", "catalog", "pooler_postgres"],
        ),
        (Probe::Directory("website"), &["website"], LIVE_COMPONENTS),
    ];

    fn assert_detector_fires(
        detected: &std::collections::BTreeMap<String, std::collections::BTreeMap<String, bool>>,
        component: &str,
        path: &str,
    ) {
        let outputs = detected
            .get(path)
            .unwrap_or_else(|| panic!("{path} was not put to the detector"));
        let fired = outputs
            .get(component)
            .unwrap_or_else(|| panic!("the detector publishes no {component} output"));
        assert!(
            *fired,
            "{path} is a build input of {component}, but the detector leaves {component} false, \
             so that change would merge with its job skipped"
        );
    }

    /// The other direction: a component the path is deliberately not an input of
    /// must be left alone, or the boundary it was drawn at has moved.
    fn assert_detector_refuses(
        detected: &std::collections::BTreeMap<String, std::collections::BTreeMap<String, bool>>,
        component: &str,
        path: &str,
    ) {
        let outputs = detected
            .get(path)
            .unwrap_or_else(|| panic!("{path} was not put to the detector"));
        let fired = outputs
            .get(component)
            .unwrap_or_else(|| panic!("the detector publishes no {component} output"));
        assert!(
            !*fired,
            "{path} is not a build input of {component}, but the detector reports it, so the \
             boundary a narrower entry draws no longer holds"
        );
    }

    fn assert_every_probe_fires(required: &[(&str, String)]) {
        assert!(
            !required.is_empty(),
            "no probe, so the check would be vacuous"
        );
        let candidates: std::collections::BTreeSet<String> =
            required.iter().map(|(_, path)| path.clone()).collect();
        let detected = detect_components(&candidates);
        for (component, path) in required {
            assert_detector_fires(&detected, component, path);
        }
    }

    /// Everything outside the workspace crates is accounted for, and everything
    /// claimed to be a build input fires the job that consumes it.
    ///
    /// The classification has to name the same paths the repository does, in
    /// both directions. A tree nobody classified — a `charts/`, a second script
    /// beside the one the detector special-cases by name — is then a failure
    /// rather than a silence.
    #[test]
    fn every_path_outside_the_crates_is_classified() {
        let crates = workspace_crates();
        let tracked = tracked_files();
        let inside: Vec<String> = crates
            .values()
            .map(|entry| format!("{}/", entry.directory))
            .collect();
        let outside: std::collections::BTreeSet<&String> = tracked
            .iter()
            .filter(|path| !inside.iter().any(|prefix| path.starts_with(prefix)))
            .collect();
        assert!(
            !outside.is_empty(),
            "every tracked path is inside a crate, so this check would be vacuous"
        );

        let named: std::collections::BTreeSet<&str> = OUTSIDE_THE_CRATES
            .iter()
            .map(|(probe, _, _)| probe.named())
            .collect();
        assert_eq!(
            named.len(),
            OUTSIDE_THE_CRATES.len(),
            "a path outside the crates is classified twice"
        );

        let mut accounted: std::collections::BTreeSet<&String> = std::collections::BTreeSet::new();
        let mut required: Vec<(&str, String)> = Vec::new();
        let mut refused: Vec<(&str, String)> = Vec::new();
        for (probe, fires, refuses) in &OUTSIDE_THE_CRATES {
            let covered = probe.accounts_for(&tracked);
            assert!(
                !covered.is_empty(),
                "{} is classified but names no tracked path",
                probe.named()
            );
            // A refusal on its own is a claim that a path is nobody's input,
            // which is how a tree ends up classified and built by no job at all.
            assert!(
                refuses.is_empty() || !fires.is_empty(),
                "{} refuses a component while requiring none, so nothing would notice it \
                 becoming an input of no job whatsoever",
                probe.named()
            );
            accounted.extend(covered);
            for path in probe.candidates(&tracked) {
                for component in *fires {
                    required.push((component, path.clone()));
                }
            }
            for path in probe.absent_names() {
                for component in *refuses {
                    refused.push((component, path.clone()));
                }
            }
        }
        assert_eq!(
            accounted, outside,
            "this classification and the repository outside the crates have to name the same paths"
        );
        assert!(
            !refused.is_empty(),
            "no boundary is pinned, so over-triggering would be invisible"
        );

        let candidates: std::collections::BTreeSet<String> = required
            .iter()
            .chain(refused.iter())
            .map(|(_, path)| path.clone())
            .collect();
        let detected = detect_components(&candidates);
        for (component, path) in &required {
            assert_detector_fires(&detected, component, path);
        }
        for (component, path) in &refused {
            assert_detector_refuses(&detected, component, path);
        }
    }

    /// The build inputs a crate directory holds but its own compilation does not
    /// record.
    ///
    /// A trigger that mentions one of these still skips it when the mention sits
    /// inside a narrower alternative, and shell word concatenation can append to
    /// a quoted trigger without the quoted part changing, so each is put to the
    /// detector rather than looked for in the trigger's text.
    #[test]
    fn the_detector_fires_for_build_inputs_no_compilation_records() {
        let tracked = tracked_files();
        let mut required: Vec<(&str, String)> = Vec::new();
        for (component, probes) in PROBED_INPUTS {
            for probe in probes {
                for path in probe.candidates(&tracked) {
                    required.push((component, path));
                }
            }
        }
        assert_every_probe_fires(&required);
    }

    fn repository_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn git_in(repository: &std::path::Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(repository)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {arguments:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("git wrote UTF-8")
            .trim()
            .to_owned()
    }

    /// The recipe as make will run it, which is the only form that shows a
    /// disabled step: a commented recipe line keeps every substring a text
    /// search looks for while make hands the shell a comment.
    fn make_recipe(target: &str, overrides: &[&str]) -> Vec<String> {
        let output = std::process::Command::new("make")
            .arg("-n")
            .arg(target)
            .args(overrides)
            .current_dir(repository_root())
            .output()
            .expect("run make");
        assert!(
            output.status.success(),
            "make -n {target} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("make wrote UTF-8")
            .lines()
            .map(|line| line.trim().to_owned())
            .filter(|line| !line.is_empty())
            .collect()
    }

    fn audit_command(recipe: &[String]) -> &str {
        let commands: Vec<&String> = recipe
            .iter()
            .filter(|line| line.starts_with("node .github/scripts/npm-audit-gate.mjs"))
            .collect();
        assert_eq!(
            commands.len(),
            1,
            "the recipe does not run the advisory gate exactly once: {recipe:?}"
        );
        commands[0]
    }

    /// The audit is a supply-chain gate whose severity floor and dependency
    /// coverage are the entire signal. Comparing against a base is what stops
    /// somebody else's publication blocking unrelated work; it must never
    /// become the excuse for auditing less than before.
    #[test]
    fn the_documentation_audit_keeps_its_severity_floor_and_coverage() {
        let gate = include_str!("../../../.github/scripts/npm-audit-gate.mjs");
        assert!(gate.contains(r#"const BLOCKING_SEVERITIES = ["high", "critical"];"#));
        assert!(!gate.contains("--omit"));
        assert!(!gate.contains("--audit-level"));
        assert!(!include_str!("../../../Makefile").contains("--omit"));

        // What the job summary must not carry unescaped is a property of
        // Markdown, so the requirement is stated here rather than copied from
        // the gate, and the gate's own class is read out of its source and
        // required to cover it. The class is pinned as source text as well:
        // covering the requirement is satisfied by weakening both at once,
        // and a pin is not. Exactly once, because a second copy satisfies the
        // pin while the declaration that runs says something else.
        assert_eq!(
            gate.matches(r"const MARKDOWN_ACTIVE = /[\\`*_[\]<>&!~|]/g;")
                .count(),
            1
        );
        let escaped = gate_escape_class();
        for starter in MARKDOWN_INLINE_STARTERS.chars() {
            assert!(
                escaped.contains(starter),
                "the gate leaves {starter:?} active in the job summary"
            );
        }

        assert_eq!(
            audit_command(&make_recipe("docs-check", &[])),
            "node .github/scripts/npm-audit-gate.mjs --directory website --base origin/main"
        );
        // No base is the absent-predecessor case CI hands it, and the answer
        // to a comparison that cannot be made is the whole audit, never none.
        assert_eq!(
            audit_command(&make_recipe("docs-check", &["PGSHARD_DOCS_AUDIT_BASE="])),
            "node .github/scripts/npm-audit-gate.mjs --directory website --report"
        );
        assert_eq!(
            audit_command(&make_recipe("docs-audit", &[])),
            "node .github/scripts/npm-audit-gate.mjs --directory website --report"
        );
    }

    fn documentation_gate_script() -> String {
        let workflow = parsed_workflow();
        let job = workflow_job(&workflow, "website");
        let step = job["steps"]
            .as_sequence()
            .expect("the website job declares steps")
            .iter()
            .find(|step| step["name"].as_str() == Some("Run documentation checks"))
            .expect("the website job runs the documentation checks");
        assert_eq!(
            step["shell"].as_str(),
            Some("bash"),
            "the documentation gate step does not pin its shell"
        );
        assert_step_environment(
            step,
            &["EVENT_NAME", "PR_BASE_SHA", "PUSH_BEFORE_SHA"],
            "the documentation gate step",
        );
        // The comparison reads the base commit's manifests out of the object
        // store, which a shallow checkout does not contain.
        let checkout = job["steps"]
            .as_sequence()
            .expect("the website job declares steps")
            .iter()
            .find(|step| {
                step["uses"]
                    .as_str()
                    .is_some_and(|uses| uses.starts_with("actions/checkout@"))
            })
            .expect("the website job checks out the source");
        assert_eq!(checkout["with"]["fetch-depth"].as_u64(), Some(0));
        // Release reconciliation publishes the `pages-site` artifact of a CI
        // run. Uploading it from anywhere but the job that audits the tree
        // would let the published documentation come from a tree this gate
        // never looked at.
        let upload = job["steps"]
            .as_sequence()
            .expect("the website job declares steps")
            .iter()
            .find(|step| {
                step["uses"]
                    .as_str()
                    .is_some_and(|uses| uses.starts_with("actions/upload-artifact@"))
            })
            .expect("the website job stores the Pages candidate");
        assert_eq!(upload["with"]["name"].as_str(), Some("pages-site"));
        assert_eq!(upload["with"]["if-no-files-found"].as_str(), Some("error"));
        // The name alone says nothing about what is inside it: repointing the
        // path publishes a different tree under the name release
        // reconciliation looks for.
        assert_eq!(upload["with"]["path"].as_str(), Some("website/build"));
        step["run"]
            .as_str()
            .expect("the documentation gate step runs a script")
            .to_owned()
    }

    /// A baseline this commit did not come from is not evidence about this
    /// commit. Comparing a tree against itself excuses every advisory in it,
    /// and a release-authorizing dispatch that does so publishes a tree whose
    /// push run failed the same audit.
    #[test]
    fn the_documentation_gate_baselines_only_a_verifiable_predecessor() {
        use std::os::unix::fs::PermissionsExt;

        let script = documentation_gate_script();
        let repository = tempfile::TempDir::new().expect("temporary repository");
        let root = repository.path();
        git_in(root, &["init", "--quiet", "--initial-branch=main"]);
        git_in(root, &["config", "user.email", "gate@example.invalid"]);
        git_in(root, &["config", "user.name", "gate"]);
        let commit = |message: &str| {
            std::fs::write(root.join("tree"), message).expect("write tree");
            git_in(root, &["add", "--all"]);
            git_in(root, &["commit", "--quiet", "-m", message]);
            git_in(root, &["rev-parse", "HEAD"])
        };
        let base = commit("base");
        git_in(root, &["switch", "--quiet", "-c", "rewritten"]);
        let abandoned = commit("abandoned");
        git_in(root, &["switch", "--quiet", "main"]);
        let head = commit("head");

        let stub = root.join("stub");
        std::fs::create_dir(&stub).expect("stub directory");
        std::fs::write(stub.join("make"), "#!/bin/sh\nprintf '%s\\n' \"$*\"\n")
            .expect("write stub");
        std::fs::set_permissions(stub.join("make"), std::fs::Permissions::from_mode(0o755))
            .expect("stub is executable");
        let path = format!("{}:{}", stub.display(), env::var("PATH").expect("PATH"));

        let selected = |event: &str, pull_base: &str, push_before: &str| -> String {
            let output = Command::new("bash")
                .arg("-c")
                .arg(&script)
                .current_dir(root)
                .env("PATH", &path)
                .env("EVENT_NAME", event)
                .env("PR_BASE_SHA", pull_base)
                .env("PUSH_BEFORE_SHA", push_before)
                .env("GITHUB_SHA", &head)
                .output()
                .expect("run the documentation gate step");
            assert!(
                output.status.success(),
                "the step failed for {event}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).expect("the step wrote UTF-8");
            let invocation = stdout
                .lines()
                .last()
                .expect("the step invokes make")
                .to_owned();
            invocation
                .strip_prefix("docs-check PGSHARD_DOCS_AUDIT_BASE=")
                .unwrap_or_else(|| panic!("the step invoked make as {invocation}"))
                .to_owned()
        };

        assert_eq!(selected("pull_request", &base, ""), base);
        assert_eq!(selected("push", "", &base), base);
        // Every remaining case has to fall back to the whole audit. A commit
        // this history no longer contains, one it never contained, one from a
        // discarded line of development, and the absent predecessor of a
        // scheduled or dispatched run are all baselines that cannot say what
        // this change did.
        assert_eq!(selected("push", "", &abandoned), "");
        assert_eq!(selected("push", "", &"0".repeat(40)), "");
        assert_eq!(selected("push", "", &"b".repeat(40)), "");
        assert_eq!(selected("push", "", ""), "");
        assert_eq!(selected("pull_request", &abandoned, ""), "");
        assert_eq!(selected("schedule", "", ""), "");
        assert_eq!(selected("workflow_dispatch", "", ""), "");
        // A dispatch is release-authorizing, so its fallback is the one that
        // decides whether a failed push audit can be laundered into a green
        // run. It must never resolve to the commit being audited.
        assert_ne!(selected("workflow_dispatch", &head, &head), head);
    }

    /// Nothing is fixed on the strength of an advisory nobody sees. The audit
    /// that no longer blocks a pull request still has to run somewhere that
    /// fails, and a `run:` a workflow never reaches is text, not a gate.
    #[test]
    fn the_scheduled_advisory_workflow_audits_main_outright() {
        let workflow: serde_norway::Value = serde_norway::from_str(include_str!(
            "../../../.github/workflows/dependency-advisories.yml"
        ))
        .expect("the workflow is valid YAML");
        // An unquoted `on` key is a YAML 1.1 boolean.
        let triggers = workflow
            .get(serde_norway::Value::Bool(true))
            .or_else(|| workflow.get("on"))
            .expect("the workflow declares triggers");
        let schedule = triggers["schedule"]
            .as_sequence()
            .expect("the workflow is scheduled");
        assert!(!schedule.is_empty());
        for entry in schedule {
            assert!(
                entry["cron"].as_str().is_some_and(|cron| !cron.is_empty()),
                "a schedule entry carries no cron expression"
            );
        }
        assert!(
            workflow.get("defaults").is_none(),
            "the workflow sets step defaults, which can replace every shell"
        );

        let jobs = workflow["jobs"]
            .as_mapping()
            .expect("the workflow declares jobs");
        let auditing: Vec<&serde_norway::Value> = jobs
            .iter()
            .filter(|(_, job)| {
                job["steps"]
                    .as_sequence()
                    .is_some_and(|steps| steps.iter().any(runs_the_audit))
            })
            .map(|(_, job)| job)
            .collect();
        assert_eq!(
            auditing.len(),
            1,
            "the workflow does not run the absolute audit in exactly one job"
        );
        assert_nothing_swallows_failure(auditing[0], "the scheduled advisory job");
        assert!(
            auditing[0].get("if").is_none(),
            "the scheduled advisory job is conditional and can decline to run"
        );
    }

    fn runs_the_audit(step: &serde_norway::Value) -> bool {
        step["run"]
            .as_str()
            .is_some_and(|run| run.trim() == "make docs-audit")
    }

    /// `make -n` prints a recipe line with any `-`, `@` or `+` prefix stripped,
    /// so the expansion cannot show that make was told to ignore the gate's
    /// exit status. Only running the recipe against a failing gate can.
    #[test]
    fn the_documentation_recipe_fails_when_the_advisory_gate_fails() {
        use std::os::unix::fs::PermissionsExt;

        let support = tempfile::TempDir::new().expect("temporary support directory");
        let stub = support.path().join("stub");
        std::fs::create_dir(&stub).expect("stub directory");
        let invocations = support.path().join("invocations");
        std::fs::write(stub.join("npm"), "#!/bin/sh\nexit 0\n").expect("write the npm stub");
        std::fs::write(
            stub.join("node"),
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nexit 1\n",
                invocations.display()
            ),
        )
        .expect("write the node stub");
        for stubbed in ["npm", "node"] {
            std::fs::set_permissions(stub.join(stubbed), std::fs::Permissions::from_mode(0o755))
                .expect("the stub is executable");
        }

        let output = Command::new("make")
            .arg("docs-check")
            .current_dir(repository_root())
            .env(
                "PATH",
                format!("{}:{}", stub.display(), env::var("PATH").expect("PATH")),
            )
            .output()
            .expect("run make");
        let invoked = std::fs::read_to_string(&invocations).unwrap_or_default();
        assert!(
            invoked.contains(".github/scripts/npm-audit-gate.mjs"),
            "the recipe never ran the advisory gate: {invoked:?}"
        );
        assert!(
            !output.status.success(),
            "the recipe reported success while the advisory gate failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    /// `npm audit --json` as npm emits it, reduced to the fields the gate
    /// reads and keeping the shapes it has to survive: a package whose `via`
    /// names another package by string rather than by advisory, and a severe
    /// count that exceeds the advisories any parser can extract. Auditing for
    /// real would ask the advisory database what is severe today, which is the
    /// answer this gate exists to stop deciding whether a change passes.
    fn advisory_report(title: &str, direct: bool, nodes: &[&str]) -> String {
        // npm reports every package a vulnerable one is installed inside as
        // vulnerable too, naming it by string rather than by advisory. Which
        // packages those are is not free to choose: they are the enclosing
        // installs the resolved paths name, all the way out, because npm nests
        // deeper than one level.
        let mut enclosing: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for node in nodes {
            let mut path = *node;
            while let Some((parent, _)) = path.rsplit_once("/node_modules/") {
                enclosing
                    .entry(package_at(parent).to_owned())
                    .or_default()
                    .push(parent.to_owned());
                path = parent;
            }
        }

        let mut vulnerabilities = serde_json::Map::new();
        vulnerabilities.insert(
            "lodash".to_owned(),
            serde_json::json!({
                "name": "lodash",
                "severity": "high",
                "isDirect": direct,
                "nodes": nodes,
                "effects": enclosing.keys().collect::<Vec<_>>(),
                "range": "<4.17.21",
                "fixAvailable": true,
                "via": [{
                    "source": 1_106_913,
                    "name": "lodash",
                    "dependency": "lodash",
                    "title": title,
                    "url": "https://github.com/advisories/GHSA-35jh-r3h4-6jhm",
                    "severity": "high",
                    "range": "<4.17.21"
                }]
            }),
        );
        for (dependent, paths) in &enclosing {
            // Only an install the root reaches without going through another
            // one is a direct dependency.
            let root = paths.iter().any(|path| !path.contains("/node_modules/"));
            vulnerabilities.insert(
                dependent.clone(),
                serde_json::json!({
                    "name": dependent,
                    "severity": "high",
                    "isDirect": root,
                    "nodes": paths,
                    "effects": [],
                    "range": "*",
                    "fixAvailable": false,
                    "via": ["lodash"]
                }),
            );
        }

        let severe = vulnerabilities.len();
        serde_json::json!({
            "auditReportVersion": 2,
            "vulnerabilities": serde_json::Value::Object(vulnerabilities),
            "metadata": {"vulnerabilities": {
                "info": 0, "low": 0, "moderate": 0, "high": severe, "critical": 0, "total": severe
            }}
        })
        .to_string()
    }

    /// npm reaching the registry and being told something, rather than being
    /// handed a report. What it was told is written by whoever answered.
    fn registry_failure(said: &str) -> String {
        format!("#stderr\n{said}")
    }

    fn clean_report() -> String {
        serde_json::json!({
            "auditReportVersion": 2,
            "vulnerabilities": {},
            "metadata": {"vulnerabilities": {
                "info": 0, "low": 0, "moderate": 0, "high": 0, "critical": 0, "total": 0
            }}
        })
        .to_string()
    }

    /// One side of the comparison: the manifests a commit carries and what the
    /// advisory database says about them. The two agree by construction --
    /// what the report resolves an advisory at is what the lockfile installs,
    /// and a package the report calls direct is one the manifest declares --
    /// because a fixture npm would never have produced proves nothing about a
    /// gate that reads npm.
    struct Tree<'a> {
        name: &'a str,
        production: &'a [&'a str],
        development: &'a [&'a str],
        report: String,
    }

    impl Tree<'_> {
        /// Deliberately not the name the fixture is keyed by. A stub that
        /// selected its report from here would find nothing, which is what
        /// makes the selection provably the lockfile's.
        fn manifest(&self) -> String {
            let (production, development) = self.declared();
            serde_json::json!({
                "name": format!("{}-manifest", self.name),
                "version": "0.0.0",
                "private": true,
                "dependencies": serde_json::Value::Object(production),
                "devDependencies": serde_json::Value::Object(development)
            })
            .to_string()
        }

        /// What the root declares, which npm is the one deciding: a package
        /// the report calls direct is one the root declares, and the report
        /// never says which section, so an advisory the scenario does not
        /// place is a production dependency.
        fn declared(
            &self,
        ) -> (
            serde_json::Map<String, serde_json::Value>,
            serde_json::Map<String, serde_json::Value>,
        ) {
            let section = |packages: &[&str]| -> serde_json::Map<String, serde_json::Value> {
                packages
                    .iter()
                    .map(|package| {
                        (
                            (*package).to_owned(),
                            serde_json::json!(installed_version(package)),
                        )
                    })
                    .collect()
            };
            let mut production = section(self.production);
            let development = section(self.development);
            let mut declare = |name: &str| {
                if !production.contains_key(name) && !development.contains_key(name) {
                    production.insert(name.to_owned(), serde_json::json!(installed_version(name)));
                }
            };
            for (name, vulnerability) in self.vulnerabilities() {
                if vulnerability["isDirect"].as_bool() == Some(true) {
                    declare(&name);
                }
            }
            // An install nothing else encloses is one the root reached, said
            // or not: npm has no other way to have put it there.
            for path in self.installed().keys() {
                if !path.contains("/node_modules/") {
                    declare(package_at(path));
                }
            }
            (production, development)
        }

        /// Every path this tree installs a package at, keyed to the package
        /// installed there: the paths the report resolves advisories at, and
        /// every install enclosing them, because npm nests deeper than one
        /// level and a path nothing installs is not a tree npm produced.
        fn installed(&self) -> std::collections::BTreeMap<String, String> {
            let mut paths = std::collections::BTreeMap::new();
            for (name, vulnerability) in self.vulnerabilities() {
                for node in vulnerability["nodes"].as_array().into_iter().flatten() {
                    let mut path = node.as_str().expect("a node path");
                    paths.insert(path.to_owned(), name.clone());
                    while let Some((parent, _)) = path.rsplit_once("/node_modules/") {
                        paths.insert(parent.to_owned(), package_at(parent).to_owned());
                        path = parent;
                    }
                }
            }
            paths
        }

        fn vulnerabilities(&self) -> Vec<(String, serde_json::Value)> {
            // A registry failure is what npm was told, not a tree it resolved.
            let Ok(report) = serde_json::from_str::<serde_json::Value>(&self.report) else {
                assert!(
                    self.report.starts_with("#stderr"),
                    "the report is neither JSON nor a registry failure"
                );
                return Vec::new();
            };
            report["vulnerabilities"]
                .as_object()
                .into_iter()
                .flatten()
                .filter(|(_, vulnerability)| vulnerability["nodes"].is_array())
                .map(|(name, vulnerability)| (name.clone(), vulnerability.clone()))
                .collect()
        }

        /// npm resolves what it audits from the lockfile, so the lockfile is
        /// what tells the two sides of a comparison apart. It is derived from
        /// the report it is paired with rather than fixed: a clean tree
        /// installs nothing, every path an advisory is resolved at exists, and
        /// a nested install is reachable through the package it sits inside.
        fn lockfile(&self) -> String {
            let (production, development) = self.declared();
            let installed = self.installed();
            let mut packages = serde_json::Map::new();

            for (path, name) in &installed {
                let version = installed_version(name);
                let mut entry = serde_json::json!({
                    "version": version,
                    "resolved": format!("https://registry.npmjs.org/{name}/-/{name}-{version}.tgz")
                });
                let inside: serde_json::Map<String, serde_json::Value> = installed
                    .iter()
                    .filter(|(nested, _)| {
                        nested
                            .rsplit_once("/node_modules/")
                            .map(|(parent, _)| parent)
                            == Some(path.as_str())
                    })
                    .map(|(_, child)| (child.clone(), serde_json::json!(installed_version(child))))
                    .collect();
                if !inside.is_empty() {
                    entry["dependencies"] = serde_json::Value::Object(inside);
                }
                packages.insert(path.clone(), entry);
            }
            packages.insert(
                String::new(),
                serde_json::json!({
                    "name": self.name,
                    "version": "0.0.0",
                    "dependencies": serde_json::Value::Object(production),
                    "devDependencies": serde_json::Value::Object(development)
                }),
            );

            serde_json::json!({
                "name": self.name,
                "version": "0.0.0",
                "lockfileVersion": 3,
                "requires": true,
                "packages": serde_json::Value::Object(packages)
            })
            .to_string()
        }
    }

    /// The package installed at a resolved path, which is the last segment
    /// after the `node_modules` it is installed into.
    fn package_at(path: &str) -> &str {
        path.rsplit("/node_modules/")
            .next()
            .unwrap_or(path)
            .trim_start_matches("node_modules/")
    }

    fn installed_version(package: &str) -> &'static str {
        if package == "lodash" {
            "4.17.15"
        } else {
            "1.0.0"
        }
    }

    /// An `npm` that answers `npm audit --json` from the fixture named by the
    /// lockfile it is asked about, and refuses anything else it is asked, so a
    /// gate that quietly narrows what it audits cannot be served a report as
    /// though it had asked the question the fixture answers. The vector is
    /// checked and recorded argument by argument: joined, a single argument
    /// `audit --json` is indistinguishable from the two the gate must pass. A
    /// fixture marked `#stderr` is npm failing to reach the registry, which
    /// reports what the far end said and no report at all.
    const NPM_AUDIT_STUB: &str = r##"#!/bin/sh
{ printf '%s\n' "$#"; [ "$#" -eq 0 ] || printf '%s\n' "$@"; } >> "$PGSHARD_AUDIT_ARGV"
if [ "$#" -ne 2 ] || [ "$1" != audit ] || [ "$2" != --json ]; then
  echo "the gate asked npm for $# arguments, not 'audit' '--json'" >&2
  exit 3
fi
name=$(sed -n 's/.*"name": *"\([^"]*\)".*/\1/p' package-lock.json | head -n 1)
report="$PGSHARD_AUDIT_FIXTURES/$name.json"
if [ ! -f "$report" ]; then
  echo "no audit fixture for '$name'" >&2
  exit 3
fi
if [ "$(head -n 1 "$report")" = "#stderr" ]; then
  tail -n +2 "$report" >&2
  exit 1
fi
cat "$report"
grep -q '"total": *0' "$report" && exit 0
exit 1
"##;

    struct AuditScenario {
        repository: tempfile::TempDir,
        support: tempfile::TempDir,
        path: String,
        base: String,
    }

    impl AuditScenario {
        fn new(base: &Tree<'_>, head: &Tree<'_>) -> Self {
            use std::os::unix::fs::PermissionsExt;

            let repository = tempfile::TempDir::new().expect("temporary repository");
            let support = tempfile::TempDir::new().expect("temporary support directory");
            let root = repository.path();
            let write = |tree: &Tree<'_>| {
                std::fs::write(root.join("website/package.json"), tree.manifest())
                    .expect("write the manifest");
                std::fs::write(root.join("website/package-lock.json"), tree.lockfile())
                    .expect("write the lockfile");
            };
            std::fs::create_dir(root.join("website")).expect("website directory");
            write(base);
            git_in(root, &["init", "--quiet", "--initial-branch=main"]);
            git_in(root, &["config", "user.email", "gate@example.invalid"]);
            git_in(root, &["config", "user.name", "gate"]);
            git_in(root, &["add", "--all"]);
            git_in(root, &["commit", "--quiet", "-m", "base"]);
            let commit = git_in(root, &["rev-parse", "HEAD"]);
            write(head);

            let fixtures = support.path().join("fixtures");
            std::fs::create_dir(&fixtures).expect("fixtures directory");
            for tree in [base, head] {
                std::fs::write(fixtures.join(format!("{}.json", tree.name)), &tree.report)
                    .expect("write a fixture");
            }
            let stub = support.path().join("stub");
            std::fs::create_dir(&stub).expect("stub directory");
            std::fs::write(stub.join("npm"), NPM_AUDIT_STUB).expect("write the npm stub");
            std::fs::set_permissions(stub.join("npm"), std::fs::Permissions::from_mode(0o755))
                .expect("the stub is executable");
            let path = format!("{}:{}", stub.display(), env::var("PATH").expect("PATH"));

            Self {
                repository,
                support,
                path,
                base: commit,
            }
        }

        /// The gate as CI runs it: annotating, and writing a job summary.
        fn run(&self, arguments: &[&str]) -> Output {
            self.invoke(arguments, true)
        }

        /// The gate as a maintainer runs it from a terminal, where an
        /// annotation is noise rather than a record.
        fn run_outside_actions(&self, arguments: &[&str]) -> Output {
            self.invoke(arguments, false)
        }

        fn invoke(&self, arguments: &[&str], actions: bool) -> Output {
            let mut gate = Command::new("node");
            gate.arg(repository_root().join(".github/scripts/npm-audit-gate.mjs"))
                .args(arguments)
                .current_dir(self.repository.path())
                .env("PATH", &self.path)
                .env(
                    "PGSHARD_AUDIT_FIXTURES",
                    self.support.path().join("fixtures"),
                )
                .env("PGSHARD_AUDIT_ARGV", self.support.path().join("argv"))
                .env("GITHUB_STEP_SUMMARY", self.support.path().join("summary"));
            if actions {
                gate.env("GITHUB_ACTIONS", "true");
            } else {
                gate.env_remove("GITHUB_ACTIONS");
            }
            gate.output().expect("run the advisory gate")
        }

        fn summary(&self) -> String {
            std::fs::read_to_string(self.support.path().join("summary")).unwrap_or_default()
        }

        /// Every argument vector the gate handed npm. The stub records an
        /// argument count and then the arguments, so a vector cannot be read
        /// back as a different one that happens to join to the same line.
        fn npm_invocations(&self) -> Vec<Vec<String>> {
            let recorded =
                std::fs::read_to_string(self.support.path().join("argv")).unwrap_or_default();
            let mut lines = recorded.lines();
            let mut invocations = Vec::new();
            while let Some(count) = lines.next() {
                let count: usize = count.parse().expect("the stub recorded an argument count");
                invocations.push(lines.by_ref().take(count).map(ToOwned::to_owned).collect());
            }
            invocations
        }
    }

    fn audited_the_whole_tree(scenario: &AuditScenario, times: usize) {
        assert_eq!(
            scenario.npm_invocations(),
            vec![vec!["audit".to_owned(), "--json".to_owned()]; times],
            "the gate asked npm something other than for the whole tree"
        );
    }

    /// Every character an inline Markdown construct is built from. This is a
    /// statement about Markdown, not a copy of what the gate escapes: a copy
    /// would agree with the gate however the gate changed, and an escape
    /// dropped from both halves of a mirror is a coherent-looking refactor
    /// that retires the escape. The backslash is one of them, because escaping
    /// by prefixing one hands whoever supplies their own the ability to
    /// re-activate the character after it.
    ///
    /// Deliberately wider than the minimum: `]`, `>` and `!` begin nothing on
    /// their own, and escaping `[` and `<` already forecloses what they finish.
    /// Requiring them costs a backslash and removes the argument.
    const MARKDOWN_INLINE_STARTERS: &str = "\\`*_[]<>&!~|";

    /// How the gate declares its escape class, up to the members themselves.
    const ESCAPE_CLASS_DECLARATION: &str = "const MARKDOWN_ACTIVE = /[";

    /// The character class the gate escapes with, read out of its source. The
    /// class is written as a regular expression, where a member that would
    /// otherwise be syntax carries a backslash.
    ///
    /// A second declaration anywhere in the file -- commented out, in a
    /// string, in a branch nothing reaches -- is what a reader taking the
    /// first match reads, and it satisfies a containment check while the
    /// declaration that runs says something else. There has to be one.
    fn gate_escape_class() -> String {
        let gate = include_str!("../../../.github/scripts/npm-audit-gate.mjs");
        assert_eq!(
            gate.matches(ESCAPE_CLASS_DECLARATION).count(),
            1,
            "the gate declares its escape class more than once"
        );
        let regex = gate
            .split_once(ESCAPE_CLASS_DECLARATION)
            .expect("the gate declares an escape class")
            .1
            .split_once("]/g;")
            .expect("the escape class is a regular expression character class")
            .0;
        let mut members = String::new();
        let mut characters = regex.chars();
        while let Some(character) = characters.next() {
            members.push(if character == '\\' {
                characters.next().expect("the class ends inside an escape")
            } else {
                character
            });
        }
        members
    }

    /// Markdown left active in the job summary, once every backslash escape is
    /// accounted for. An image or a link is the forgery this rendering allows.
    fn unescaped_markdown(summary: &str) -> String {
        let mut remaining = summary.chars();
        let mut plain = String::new();
        while let Some(character) = remaining.next() {
            if character == '\\' {
                remaining.next();
            } else {
                plain.push(character);
            }
        }
        plain.retain(|character| MARKDOWN_INLINE_STARTERS.contains(character));
        plain
    }

    fn streams(output: &Output) -> (String, String) {
        (
            String::from_utf8(output.stdout.clone()).expect("the gate wrote UTF-8"),
            String::from_utf8(output.stderr.clone()).expect("the gate wrote UTF-8"),
        )
    }

    fn tree<'a>(name: &'a str, production: &'a [&'a str], report: String) -> Tree<'a> {
        Tree {
            name,
            production,
            development: &[],
            report,
        }
    }

    fn development_tree<'a>(name: &'a str, development: &'a [&'a str], report: String) -> Tree<'a> {
        Tree {
            name,
            production: &[],
            development,
            report,
        }
    }

    const ADVISORY_TITLE: &str = "Command Injection in lodash";
    const ADVISORY_LINE: &str = "high lodash: Command Injection in lodash (https://github.com/advisories/GHSA-35jh-r3h4-6jhm)";

    /// The gate's whole job is to fail a change that adds a severe advisory,
    /// to say which one, and to have asked npm about the whole tree. Every way
    /// of retiring it that leaves the file looking untouched — an early exit,
    /// a filter inverted around its own pinned constant, a quietly narrowed
    /// audit — is invisible to anything that only reads the source.
    #[test]
    fn the_advisory_gate_fails_a_change_that_introduces_an_advisory() {
        let scenario = AuditScenario::new(
            &tree("base", &[], clean_report()),
            &tree(
                "head",
                &["lodash"],
                advisory_report(ADVISORY_TITLE, true, &["node_modules/lodash"]),
            ),
        );
        let output = scenario.run(&["--directory", "website", "--base", &scenario.base]);
        let (stdout, stderr) = streams(&output);
        assert!(
            !output.status.success(),
            "the gate passed a change that introduced a high advisory"
        );
        assert!(
            stderr.contains("1 high or critical npm advisory is introduced into website"),
            "the gate did not report the advisory it failed on: {stderr}"
        );
        assert!(
            stderr.contains(ADVISORY_LINE),
            "the gate did not name the advisory: {stderr}"
        );
        assert!(
            stdout.contains(&format!("::error::{ADVISORY_LINE}")),
            "the gate did not annotate the advisory: {stdout}"
        );
        // What the gate asks npm is the entire scope of the audit, and it is
        // not visible in anything the gate writes.
        audited_the_whole_tree(&scenario, 2);
    }

    /// The absolute audit is the whole gate on two paths that never compare
    /// anything: the scheduled run that is meant to reach a maintainer, and
    /// the release-authorizing dispatch, which has no predecessor to be
    /// measured against and so audits the tree outright. Neither has a
    /// comparison to notice going quiet.
    #[test]
    fn the_absolute_audit_fails_on_any_advisory_and_passes_a_clean_tree() {
        let severe = AuditScenario::new(
            &tree("base", &[], clean_report()),
            &tree(
                "head",
                &["lodash"],
                advisory_report(ADVISORY_TITLE, true, &["node_modules/lodash"]),
            ),
        );
        let output = severe.run(&["--directory", "website", "--report"]);
        let (stdout, stderr) = streams(&output);
        assert!(
            !output.status.success(),
            "the absolute audit passed a tree with a high advisory"
        );
        assert!(
            stderr.contains("1 high or critical npm advisory affects website"),
            "the absolute audit did not report the advisory: {stderr}"
        );
        assert!(
            stderr.contains(ADVISORY_LINE),
            "the absolute audit did not name the advisory: {stderr}"
        );
        assert!(
            stdout.contains(&format!("::error::{ADVISORY_LINE}")),
            "the absolute audit did not annotate the advisory: {stdout}"
        );
        assert!(
            severe.summary().contains(ADVISORY_LINE),
            "the absolute audit wrote no job summary: {}",
            severe.summary()
        );
        // The comparison is what the base-relative path skips; the audit
        // itself is not, and a tree audited only at the head is audited once.
        audited_the_whole_tree(&severe, 1);

        // Outside Actions an annotation is noise, and suppressing it must not
        // suppress the finding.
        let output = severe.run_outside_actions(&["--directory", "website", "--report"]);
        let (stdout, stderr) = streams(&output);
        assert!(!output.status.success(), "the absolute audit passed");
        assert!(
            stderr.contains(ADVISORY_LINE),
            "the finding went missing outside Actions: {stderr}"
        );
        assert_eq!(
            stdout, "",
            "the gate annotated a terminal that reads no commands: {stdout}"
        );

        let clean = AuditScenario::new(
            &tree("base", &[], clean_report()),
            &tree("head", &[], clean_report()),
        );
        let output = clean.run(&["--directory", "website", "--report"]);
        let (stdout, stderr) = streams(&output);
        assert!(
            output.status.success(),
            "the absolute audit failed a clean tree: {stderr}"
        );
        assert_eq!(
            stdout, "No high or critical npm advisory affects website.\n",
            "the absolute audit did not report a clean tree"
        );
        audited_the_whole_tree(&clean, 1);
    }

    /// Somebody else publishing an advisory against a tree that is already
    /// merged is not this change's regression, and the whole point of the
    /// comparison is that it does not block. Each of the three ways a change
    /// widens its own exposure to that same advisory is.
    #[test]
    fn the_advisory_gate_blocks_only_the_exposure_this_change_adds() {
        let reached = |direct: bool, nodes: &[&str]| advisory_report(ADVISORY_TITLE, direct, nodes);
        let transitive = ["node_modules/widget/node_modules/lodash"];

        let unchanged = AuditScenario::new(
            &tree("base", &[], reached(false, &transitive)),
            &tree("head", &[], reached(false, &transitive)),
        );
        let output = unchanged.run(&["--directory", "website", "--base", &unchanged.base]);
        let (stdout, stderr) = streams(&output);
        assert!(
            output.status.success(),
            "the gate failed a change on an advisory the base already had: {stderr}"
        );
        assert!(
            stdout.contains("already affected website"),
            "the gate did not report the pre-existing advisory: {stdout}"
        );

        let widening = |base: &Tree<'_>, head: &Tree<'_>, reason: &str| {
            let scenario = AuditScenario::new(base, head);
            let output = scenario.run(&["--directory", "website", "--base", &scenario.base]);
            let (_, stderr) = streams(&output);
            assert!(
                !output.status.success(),
                "the gate passed a change that widened its exposure: {reason}"
            );
            assert!(
                stderr.contains(reason),
                "the gate did not say what this change widened: {stderr}"
            );
        };
        // Installed by something else this change added.
        widening(
            &tree("base", &[], reached(false, &transitive)),
            &tree(
                "head",
                &[],
                reached(false, &["node_modules/other/node_modules/lodash"]),
            ),
            "this change reaches it at node_modules/other/node_modules/lodash",
        );
        // Declared by the root, which is also what hoists a copy to the top of
        // the tree beside the one already nested there.
        widening(
            &tree("base", &[], reached(false, &transitive)),
            &tree(
                "head",
                &[],
                reached(true, &["node_modules/lodash", transitive[0]]),
            ),
            "this change depends on it directly",
        );
        // Already declared on both sides, so npm calls it direct on both, and
        // what moved is the section it is declared in.
        widening(
            &development_tree("base", &["lodash"], reached(true, &["node_modules/lodash"])),
            &tree("head", &["lodash"], reached(true, &["node_modules/lodash"])),
            "this change promotes it into production dependencies",
        );
    }

    /// npm reports "the audit found nothing" and "the audit never ran" on the
    /// same nonzero exit, and both parse as JSON. A tree nobody managed to
    /// audit is not a tree that passed.
    #[test]
    fn the_advisory_gate_fails_a_tree_it_could_not_audit() {
        let scenario = AuditScenario::new(
            &tree("base", &[], clean_report()),
            &tree(
                "head",
                &[],
                serde_json::json!({
                    "error": {
                        "code": "ENETUNREACH",
                        "summary": "request to https://registry.npmjs.org failed",
                        "detail": ""
                    }
                })
                .to_string(),
            ),
        );
        let output = scenario.run(&["--directory", "website", "--base", &scenario.base]);
        let (_, stderr) = streams(&output);
        assert!(
            !output.status.success(),
            "the gate passed a tree the registry never answered for"
        );
        assert!(
            stderr.contains("Cannot audit website"),
            "the gate did not say the audit failed: {stderr}"
        );

        // What the far end said is quoted into that message, and the far end
        // is the one part of this the repository does not write.
        let registry = AuditScenario::new(
            &tree("base", &[], clean_report()),
            &tree(
                "head",
                &[],
                registry_failure(
                    "connection reset\n::error::forged by the registry\r::stop-commands::TOKEN of 100%",
                ),
            ),
        );
        let output = registry.run(&["--directory", "website", "--base", &registry.base]);
        let (stdout, stderr) = streams(&output);
        assert!(
            !output.status.success(),
            "the gate passed a tree the registry refused"
        );
        assert_eq!(
            stderr.lines().count(),
            1,
            "the registry wrote lines of its own to standard error: {stderr:?}"
        );
        assert!(
            stderr.starts_with("Cannot audit website: npm audit wrote no report:"),
            "the gate did not say the audit failed: {stderr}"
        );
        assert!(
            stderr.contains("connection reset%0A%3A%3Aerror%3A%3Aforged by the registry"),
            "the gate dropped what the registry said instead of escaping it: {stderr}"
        );
        assert!(
            stderr.contains("%0D%3A%3Astop-commands%3A%3ATOKEN of 100%25"),
            "the gate left what the registry said active: {stderr}"
        );
        assert_eq!(
            stdout
                .lines()
                .filter(|line| line.trim_start().starts_with("::"))
                .count(),
            0,
            "the registry forged a command on standard output: {stdout:?}"
        );
    }

    /// Every stream this gate writes an advisory title to is parsed by
    /// something, and the title is written by somebody else. The ordinary
    /// pull-request path is the one that prints a title to standard output as
    /// prose, so a test that proves the annotation alone is inert proves it on
    /// the one path where nothing could have gone wrong.
    ///
    /// What is asserted is the bytes the gate emits, against the workflow
    /// command and Markdown grammars. No runner renders them here, so the
    /// step reading them back as text is reasoned from those contracts rather
    /// than executed.
    #[test]
    fn no_sink_lets_an_advisory_title_forge_a_workflow_command() {
        // A title already carrying backslashes is the case the escaping has to
        // survive: escaping by prefixing one re-activates whatever the gate
        // just escaped unless the backslash is escaped too.
        let title = "Injection\n::error::forged from the log line\r::stop-commands::TOKEN\n## Injected heading\n<img src=x onerror=alert(1)> ![pixel](https://attacker.example/p.png) [click](https://attacker.example) \\!\\[re\\](https://attacker.example/q.png) `code` *em* _em_ ~~strike~~ R&D a|b at 100%";
        // A character the title never carries is one no mutation can be caught
        // removing from the escape.
        for starter in MARKDOWN_INLINE_STARTERS.chars() {
            assert!(
                title.contains(starter),
                "the hostile title exercises no {starter:?}"
            );
        }
        let hostile = |direct: bool, nodes: &[&str]| advisory_report(title, direct, nodes);
        let transitive = ["node_modules/widget/node_modules/lodash"];

        let commands = |stream: &str| {
            stream
                .lines()
                .filter(|line| line.trim_start().starts_with("::"))
                .count()
        };
        let assert_escaped = |text: &str, described: &str| {
            assert!(text.contains("Injection%0A"), "{described}: {text}");
            assert!(text.contains("the log line%0D"), "{described}: {text}");
            assert!(text.contains("at 100%25"), "{described}: {text}");
            assert!(
                text.contains("%3A%3Aerror%3A%3Aforged from the log line"),
                "the title was dropped instead of escaped in {described}: {text}"
            );
            assert!(
                text.contains("%3A%3Astop-commands%3A%3ATOKEN"),
                "{described}: {text}"
            );
        };

        // An advisory the base already had: the gate warns, and prints the
        // title to standard output as prose next to its own annotation.
        let existing = AuditScenario::new(
            &tree("base", &[], hostile(false, &transitive)),
            &tree("head", &[], hostile(false, &transitive)),
        );
        let output = existing.run(&["--directory", "website", "--base", &existing.base]);
        let (stdout, stderr) = streams(&output);
        assert!(output.status.success(), "the gate failed: {stderr}");
        assert_eq!(
            stdout.lines().count(),
            5,
            "the title produced lines of its own on standard output: {stdout:?}"
        );
        assert_eq!(
            commands(&stdout),
            1,
            "forged commands on stdout: {stdout:?}"
        );
        assert_eq!(stderr, "", "the gate wrote to stderr: {stderr:?}");
        assert_escaped(&stdout, "the pre-existing standard output");
        assert_escaped(&existing.summary(), "the pre-existing job summary");

        // An advisory this change introduced: the gate annotates and lists it
        // on standard error, and writes a second summary section.
        let introduced = AuditScenario::new(
            &tree("base", &[], clean_report()),
            &tree("head", &[], hostile(true, &["node_modules/lodash"])),
        );
        let output = introduced.run(&["--directory", "website", "--base", &introduced.base]);
        let (stdout, stderr) = streams(&output);
        assert!(!output.status.success(), "the gate passed: {stdout}");
        assert_eq!(
            stdout.lines().count(),
            1,
            "the title produced lines of its own on standard output: {stdout:?}"
        );
        assert_eq!(
            commands(&stdout),
            1,
            "forged commands on stdout: {stdout:?}"
        );
        assert_eq!(
            stderr.lines().count(),
            2,
            "the title produced lines of its own on standard error: {stderr:?}"
        );
        assert_eq!(
            commands(&stderr),
            0,
            "forged commands on stderr: {stderr:?}"
        );
        assert_escaped(&stdout, "the introduced standard output");
        assert_escaped(&stderr, "the introduced standard error");
        assert_escaped(&introduced.summary(), "the introduced job summary");

        // The job summary is rendered Markdown: a heading the gate did not
        // write, an element, an image GitHub's proxy fetches, or a link whose
        // text disagrees with its target is the same forgery in another
        // syntax.
        for (summary, described) in [
            (existing.summary(), "the pre-existing job summary"),
            (introduced.summary(), "the introduced job summary"),
        ] {
            assert_eq!(
                summary.lines().filter(|line| line.starts_with('#')).count(),
                2,
                "{described} carries a heading the gate did not write: {summary}"
            );
            assert_eq!(
                unescaped_markdown(&summary),
                "",
                "{described} leaves Markdown active: {summary}"
            );
        }
    }

    /// These tests run the advisory gate itself, so the job that runs them
    /// needs the Node the gate runs under. A runner image that happens to
    /// carry one is not a pin, and a gate proved on a different Node from the
    /// one CI audits with is proved about something else.
    #[test]
    fn the_gate_tests_run_on_the_node_the_gate_runs_on() {
        let workflow = parsed_workflow();
        let node_version = |job: &str| -> String {
            workflow_job(&workflow, job)["steps"]
                .as_sequence()
                .expect("the job declares steps")
                .iter()
                .find(|step| {
                    step["uses"]
                        .as_str()
                        .is_some_and(|uses| uses.starts_with("actions/setup-node@"))
                })
                .unwrap_or_else(|| panic!("{job} selects no Node toolchain"))["with"]
                ["node-version"]
                .as_str()
                .unwrap_or_else(|| panic!("{job} pins no Node version"))
                .to_owned()
        };
        assert_eq!(
            node_version("repository-policy"),
            node_version("website"),
            "the gate is proved on a different Node from the one it is run on"
        );
    }
}
