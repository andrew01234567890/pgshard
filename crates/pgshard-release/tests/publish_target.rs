//! Which commit a release run is allowed to target.
//!
//! A GitHub Release whose target does not share the default branch's
//! `.github/workflows/` content is refused outright, so the reference the run
//! compares against decides whether every call it makes can succeed. These
//! drive the shipped binary against a real repository and a recorded `gh`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

const WORKFLOW: &str = "name: CI\non: push\n";
const EDITED_WORKFLOW: &str = "name: CI\non: [push, workflow_dispatch]\n";

#[test]
fn a_history_without_a_workflow_edit_still_releases_every_commit() {
    let history = History::new();
    let first = history.commit(WORKFLOW, "ci: start releases");
    let second = history.commit(WORKFLOW, "fix(agent): fence the postmaster");
    let head = history.commit(WORKFLOW, "feat(agent): reach TLS by reload");

    let run = history.publish(&head, &head, true);

    run.expect_success();
    assert_eq!(
        run.targets(),
        vec![first, second, head],
        "an unobstructed history keeps one release per commit"
    );
}

/// The backlog this exists for: the leading commits cannot be targeted, so the
/// whole gap is closed by the single release at the reachable endpoint rather
/// than by calls the token cannot make.
#[test]
fn commits_below_a_workflow_edit_are_released_by_the_endpoint() {
    let history = History::new();
    history.commit(WORKFLOW, "ci: start releases");
    history.commit(WORKFLOW, "fix(agent): fence the postmaster");
    history.commit(EDITED_WORKFLOW, "ci: retune the aggregate");
    let head = history.commit(EDITED_WORKFLOW, "feat(agent): reach TLS by reload");

    let run = history.publish(&head, &head, true);

    run.expect_success();
    assert_eq!(
        run.targets(),
        vec![head],
        "one aggregate release closes the gap the refusal opened"
    );
}

/// The reference is the live default branch, not the commit the run was handed:
/// an exact-SHA run is handed an arbitrary commit below the head, and comparing
/// that commit against itself calls every one of its predecessors targetable.
#[test]
fn an_exact_sha_run_below_a_workflow_edit_defers() {
    let history = History::new();
    let marker = history.commit(WORKFLOW, "ci: start releases");
    let requested = history.commit(WORKFLOW, "fix(agent): fence the postmaster");
    history.commit(EDITED_WORKFLOW, "ci: retune the aggregate");
    let head = history.commit(EDITED_WORKFLOW, "feat(agent): reach TLS by reload");

    let run = history.publish(&requested, &head, false);

    run.expect_success();
    assert!(
        run.targets().is_empty(),
        "no release may be attempted for a commit the token cannot tag"
    );
    assert!(
        run.stdout().contains(&format!(
            "release deferred from {marker} until an endpoint shares the default branch's \
             workflow files"
        )),
        "the run reports what it deferred and why: {}",
        run.stdout()
    );
}

/// The live head is read from the API and can name a commit pushed after this
/// job fetched, so the object it has to diff against may not be in the checkout
/// at all.
#[test]
fn a_default_branch_head_outside_the_checkout_is_fetched() {
    let history = History::new();
    let marker = history.commit(WORKFLOW, "ci: start releases");
    let requested = history.commit(WORKFLOW, "fix(agent): fence the postmaster");
    history.commit(EDITED_WORKFLOW, "ci: retune the aggregate");
    let head = history.commit(EDITED_WORKFLOW, "feat(agent): reach TLS by reload");
    let checkout = history.checkout_stopping_at(&requested);
    assert!(
        !checkout.holds(&head),
        "the fixture must start without the head the run has to fetch"
    );

    let run = checkout.publish(&requested, &head, false);

    run.expect_success();
    assert!(checkout.holds(&head), "the run fetched the head it diffs");
    assert!(run.targets().is_empty());
    assert!(
        run.stdout()
            .contains(&format!("release deferred from {marker}"))
    );
}

#[test]
fn an_unobtainable_default_branch_head_fails_the_run() {
    let history = History::new();
    history.commit(WORKFLOW, "ci: start releases");
    let requested = history.commit(WORKFLOW, "fix(agent): fence the postmaster");
    history.commit(EDITED_WORKFLOW, "ci: retune the aggregate");
    let head = history.commit(EDITED_WORKFLOW, "feat(agent): reach TLS by reload");
    let checkout = history.checkout_stopping_at(&requested);
    checkout.detach_origin();

    let run = checkout.publish(&requested, &head, false);

    assert!(
        !run.output.status.success(),
        "an unreadable reference is a failure, not a licence to target anything"
    );
    assert!(run.targets().is_empty());
}

struct History {
    repository: TempDir,
}

struct Checkout {
    _origin: History,
    directory: TempDir,
}

struct PublishRun {
    output: Output,
    log: String,
}

impl History {
    fn new() -> Self {
        let repository = TempDir::new().expect("temporary repository");
        let history = Self { repository };
        git(
            history.root(),
            &["init", "--quiet", "--initial-branch=main"],
        );
        git(history.root(), &["config", "user.name", "pgshard release"]);
        git(
            history.root(),
            &["config", "user.email", "release@example.invalid"],
        );
        history
    }

    fn root(&self) -> &Path {
        self.repository.path()
    }

    fn commit(&self, workflow: &str, message: &str) -> String {
        write(self.root(), ".github/workflows/ci.yml", workflow);
        write(
            self.root(),
            "crates/pgshard-release/RELEASE_START",
            "release history starts here\n",
        );
        write(self.root(), "source", message);
        git(self.root(), &["add", "--all"]);
        git(self.root(), &["commit", "--quiet", "-m", message]);
        git(self.root(), &["rev-parse", "HEAD"])
    }

    /// A checkout of this history as it stood at `sha`, holding no object the
    /// commits above it introduced.
    fn checkout_stopping_at(self, sha: &str) -> Checkout {
        let directory = TempDir::new().expect("temporary checkout");
        let work = directory.path().join("work");
        git(
            directory.path(),
            &[
                "clone",
                "--quiet",
                &self.root().display().to_string(),
                &work.display().to_string(),
            ],
        );
        git(&work, &["reset", "--quiet", "--hard", sha]);
        git(&work, &["update-ref", "-d", "refs/remotes/origin/main"]);
        git(&work, &["reflog", "expire", "--expire=now", "--all"]);
        git(&work, &["gc", "--prune=now", "--quiet"]);
        Checkout {
            _origin: self,
            directory,
        }
    }

    fn publish(&self, requested: &str, default_head: &str, ready_only: bool) -> PublishRun {
        publish_in(self.root(), requested, default_head, ready_only)
    }
}

impl Checkout {
    fn work(&self) -> PathBuf {
        self.directory.path().join("work")
    }

    fn holds(&self, sha: &str) -> bool {
        Command::new("git")
            .args(["cat-file", "-e", &format!("{sha}^{{commit}}")])
            .current_dir(self.work())
            .output()
            .expect("run git")
            .status
            .success()
    }

    fn detach_origin(&self) {
        git(
            &self.work(),
            &[
                "remote",
                "set-url",
                "origin",
                &self.directory.path().join("absent").display().to_string(),
            ],
        );
    }

    fn publish(&self, requested: &str, default_head: &str, ready_only: bool) -> PublishRun {
        publish_in(&self.work(), requested, default_head, ready_only)
    }
}

impl PublishRun {
    fn expect_success(&self) {
        assert!(
            self.output.status.success(),
            "publish failed: {}",
            String::from_utf8_lossy(&self.output.stderr)
        );
    }

    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).into_owned()
    }

    /// Every commit a GitHub Release was actually asked for, in order.
    fn targets(&self) -> Vec<String> {
        self.log
            .lines()
            .filter_map(|call| call.strip_prefix("release create "))
            .map(|call| {
                let target = call
                    .split(" --target ")
                    .nth(1)
                    .expect("a release names its target");
                target
                    .split_whitespace()
                    .next()
                    .expect("a target is a commit")
                    .to_owned()
            })
            .collect()
    }
}

fn publish_in(
    repository: &Path,
    requested: &str,
    default_head: &str,
    ready_only: bool,
) -> PublishRun {
    let log = repository.join("gh-calls");
    let path = format!(
        "{}:{}",
        recording_gh(repository).display(),
        std::env::var("PATH").expect("PATH")
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pgshard-release"));
    command.args(["publish", "--sha", requested]);
    if ready_only {
        command.arg("--ready-only");
    }
    let output = command
        .current_dir(repository)
        .env("PATH", path)
        .env("GITHUB_ACTIONS", "true")
        .env("GITHUB_REPOSITORY", "owner/repository")
        .env("PGSHARD_RELEASE_SHA", requested)
        .env("PGSHARD_TEST_DEFAULT_HEAD", default_head)
        .env("PGSHARD_TEST_GH_LOG", &log)
        .output()
        .expect("run the release helper");
    PublishRun {
        log: fs::read_to_string(&log).unwrap_or_default(),
        output,
    }
}

/// A `gh` that answers exactly the calls a publish makes and records every one
/// of them, so a release the run never had permission to create is visible as a
/// call that was made.
fn recording_gh(repository: &Path) -> PathBuf {
    let directory = repository.join("recorded-bin");
    fs::create_dir_all(&directory).expect("recording directory");
    let executable = directory.join("gh");
    fs::write(
        &executable,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$PGSHARD_TEST_GH_LOG"
if [[ "${1:-}" == "release" && "${2:-}" == "create" ]]; then
  exit 0
fi
if [[ "${1:-}" == "api" ]]; then
  for argument in "$@"; do
    case "$argument" in
      */git/ref/heads/main)
        printf '%s\n' "$PGSHARD_TEST_DEFAULT_HEAD"
        exit 0
        ;;
      */compare/*)
        base="${argument##*/compare/}"
        printf '{"status":"ahead","behind_by":0,"merge_base_commit":{"sha":"%s"}}\n' \
          "${base%%...*}"
        exit 0
        ;;
      */check-runs*)
        printf '%s\n' '{"check_runs":[{"name":"CI aggregate","status":"completed","conclusion":"success","app":{"slug":"github-actions"}}]}'
        exit 0
        ;;
    esac
  done
fi
printf 'unexpected gh call: %s\n' "$*" >&2
exit 1
"#,
    )
    .expect("write the recording gh");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
        .expect("the recording gh is executable");
    directory
}

fn write(repository: &Path, path: &str, contents: &str) {
    let file = repository.join(path);
    fs::create_dir_all(file.parent().expect("a file has a parent")).expect("create parent");
    fs::write(file, contents).expect("write fixture file");
}

fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("UTF-8 git output")
        .trim()
        .to_owned()
}
