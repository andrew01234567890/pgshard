//! Regression tests for widening CI across an untagged release gap.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

#[test]
fn released_push_parent_is_the_detection_base() {
    let (repository, released) = repository_with_commit("released");
    git(repository.path(), &["tag", "v0.74.0", &released]);
    let head = commit(repository.path(), "head");

    assert_eq!(
        detection_base(
            repository.path(),
            &head,
            &released,
            false,
            &[("v0.74.0", &released)],
        ),
        released
    );
    let path = format!(
        "{}:{}",
        repository.path().join("fake-bin").display(),
        std::env::var("PATH").expect("PATH")
    );
    let output = Command::new(env!("CARGO_BIN_EXE_pgshard-release"))
        .args(["next", "--sha", &head])
        .env("PATH", path)
        .env("PGSHARD_TEST_RELEASES", format!("v0.74.0 {released}"))
        .current_dir(repository.path())
        .output()
        .expect("run next-version helper");
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "0.74.1");
}

#[test]
fn untagged_rapid_pushes_widen_to_the_latest_release() {
    let (repository, released) = repository_with_commit("released");
    git(repository.path(), &["tag", "v0.74.0", &released]);
    let failed = commit(repository.path(), "failed feature");
    let narrow_green = commit(repository.path(), "old narrow green fix");
    let full_gap_green = commit(repository.path(), "release catch-up fix");

    assert_eq!(
        detection_base(
            repository.path(),
            &narrow_green,
            &failed,
            false,
            &[("v0.74.0", &released)],
        ),
        released
    );
    assert_eq!(
        detection_base(
            repository.path(),
            &full_gap_green,
            &narrow_green,
            false,
            &[("v0.74.0", &released)],
        ),
        released
    );
}

#[test]
fn repository_without_a_release_tag_forces_full_detection() {
    let (repository, first) = repository_with_commit("bootstrap");
    let head = commit(repository.path(), "head");

    assert_eq!(
        detection_base(repository.path(), &head, &first, false, &[]),
        ""
    );
}

#[test]
fn side_branch_before_sha_cannot_narrow_main_detection() {
    let (repository, released) = repository_with_commit("released");
    git(repository.path(), &["tag", "v0.74.0", &released]);
    let main_head = commit(repository.path(), "main head");
    git(
        repository.path(),
        &["switch", "--quiet", "-c", "side", &released],
    );
    let side_head = commit(repository.path(), "side head");
    git(repository.path(), &["tag", "v0.99.0", &side_head]);

    assert_eq!(
        detection_base(
            repository.path(),
            &main_head,
            &side_head,
            false,
            &[("v0.74.0", &released), ("v0.99.0", &side_head)],
        ),
        released
    );
}

#[test]
fn malformed_version_tag_does_not_authorize_a_narrow_diff() {
    let (repository, first) = repository_with_commit("bootstrap");
    git(repository.path(), &["tag", "v01.2.3", &first]);
    git(
        repository.path(),
        &["tag", "v18446744073709551616.0.0", &first],
    );
    let head = commit(repository.path(), "head");

    assert_eq!(
        detection_base(
            repository.path(),
            &head,
            &first,
            false,
            &[("v01.2.3", &first), ("v18446744073709551616.0.0", &first),],
        ),
        ""
    );
}

#[test]
fn orphan_semver_tag_is_not_a_release_detection_base() {
    let (repository, released) = repository_with_commit("released");
    git(repository.path(), &["tag", "v0.74.0", &released]);
    let orphan = commit(repository.path(), "orphan tag");
    git(repository.path(), &["tag", "v9.9.9", &orphan]);
    let head = commit(repository.path(), "head");

    assert_eq!(
        detection_base(
            repository.path(),
            &head,
            &orphan,
            false,
            &[("v0.74.0", &released)],
        ),
        released
    );
    assert_eq!(
        detection_base(
            repository.path(),
            &head,
            &orphan,
            false,
            &[("v0.74.0", &released), ("v9.9.9", &released)],
        ),
        released
    );

    let path = format!(
        "{}:{}",
        repository.path().join("fake-bin").display(),
        std::env::var("PATH").expect("PATH")
    );
    let release_map = format!("v0.74.0 {released}\nv9.9.9 {released}");
    for selected in [&head, &orphan] {
        let output = Command::new(env!("CARGO_BIN_EXE_pgshard-release"))
            .args(["next", "--sha", selected])
            .env("PATH", &path)
            .env("PGSHARD_TEST_RELEASES", &release_map)
            .current_dir(repository.path())
            .output()
            .expect("run next-version helper");
        assert!(
            !output.status.success(),
            "orphan SemVer tag must not become a release baseline"
        );
    }
}

#[test]
fn no_tag_audit_starts_before_marker_and_catches_deleted_content() {
    let (repository, bootstrap) = repository_with_commit("bootstrap");
    let marker = repository
        .path()
        .join("crates/pgshard-release/RELEASE_START");
    fs::create_dir_all(marker.parent().expect("marker parent")).expect("create marker parent");
    fs::write(&marker, "release history starts here\n").expect("write marker");
    git(repository.path(), &["add", "."]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "ci: start releases"],
    );
    let leak = repository.path().join("transient.txt");
    fs::write(&leak, ["/", "home", "/private"].concat()).expect("write transient content");
    git(repository.path(), &["add", "transient.txt"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add transient content"],
    );
    fs::remove_file(leak).expect("delete transient content");
    git(repository.path(), &["add", "--update"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: remove transient content"],
    );
    let head = git(repository.path(), &["rev-parse", "HEAD"]);

    let audit_base = detection_base(repository.path(), &head, "", true, &[]);
    assert_eq!(audit_base, bootstrap);
    let output = Command::new(env!("CARGO_BIN_EXE_pgshard-release"))
        .args(["audit", "--base", &audit_base, "--head", &head])
        .current_dir(repository.path())
        .output()
        .expect("run public-history audit");
    assert!(
        !output.status.success(),
        "full release history audit must reject deleted sensitive content"
    );
}

/// Widening the base may only add components, never remove one. An endpoint
/// diff breaks that: a change reverted inside the wider window is invisible to
/// it, so a run that starts before its predecessor is released would skip the
/// component the narrower run tested, and the aggregate would authorize a tree
/// pairing an untested file with a tested one.
#[test]
fn a_reverted_change_still_detects_its_component_when_the_base_widens() {
    let (repository, released, unreleased, head) = reverted_change_repository();
    let serialized_releases = [
        ("v0.74.0", released.as_str()),
        ("v0.74.1", unreleased.as_str()),
    ];
    let concurrent_releases = [("v0.74.0", released.as_str())];

    assert_eq!(
        detection_base(
            repository.path(),
            &head,
            &unreleased,
            false,
            &serialized_releases,
        ),
        unreleased,
        "a released predecessor must stay the narrow base"
    );
    assert_eq!(
        detection_base(
            repository.path(),
            &head,
            &unreleased,
            false,
            &concurrent_releases,
        ),
        released,
        "an unreleased predecessor must widen to the last release"
    );

    let serialized = detect_components(repository.path(), &head, &unreleased, &serialized_releases);
    let concurrent = detect_components(repository.path(), &head, &unreleased, &concurrent_releases);

    assert_eq!(
        serialized.get("pgwire").map(String::as_str),
        Some("true"),
        "the narrow base did not detect the reverting commit's own component"
    );
    assert_eq!(
        concurrent.get("pgwire").map(String::as_str),
        Some("true"),
        "widening the base dropped a component the narrow base detected"
    );
    for (component, value) in &serialized {
        if value == "true" {
            assert_eq!(
                concurrent.get(component).map(String::as_str),
                Some("true"),
                "widening the base dropped {component}"
            );
        }
    }
}

/// A merge commit carries its whole branch as one first-parent step. Reading
/// no diff for it, which is what `git log` does for merges by default, would
/// report every component of that branch as untouched.
#[test]
fn a_merge_commit_reports_the_components_it_brings_to_the_branch() {
    let (repository, released) = component_repository();
    git(repository.path(), &["tag", "v0.74.0", &released]);
    git(repository.path(), &["switch", "--quiet", "-c", "feature"]);
    write_file(
        repository.path(),
        "crates/pgshard-pgwire/src/proto.rs",
        "v1",
    );
    commit_all(repository.path(), "feat(pgwire): branch change");
    write_file(repository.path(), "crates/pgshard-types/src/lib.rs", "t1");
    commit_all(repository.path(), "feat(types): branch change");
    write_file(repository.path(), "crates/pgshard-types/src/lib.rs", "t0");
    commit_all(repository.path(), "revert(types): branch change");
    git(repository.path(), &["switch", "--quiet", "-"]);
    git(
        repository.path(),
        &[
            "merge",
            "--quiet",
            "--no-ff",
            "-m",
            "merge feature",
            "feature",
        ],
    );
    let head = git(repository.path(), &["rev-parse", "HEAD"]);

    let components = detect_components(
        repository.path(),
        &head,
        &released,
        &[("v0.74.0", released.as_str())],
    );
    assert_eq!(
        components.get("pgwire").map(String::as_str),
        Some("true"),
        "a merge commit's own change was invisible to the detector"
    );
}

/// A rename reports one path when git pairs the delete with the add, and the
/// pairing depends on what else the window contains. The component losing the
/// file would then go untested, so both paths have to be reported.
#[test]
fn a_rename_reports_the_component_the_file_left() {
    let (repository, released) = component_repository();
    git(repository.path(), &["tag", "v0.74.0", &released]);
    git(
        repository.path(),
        &[
            "mv",
            "crates/pgshard-pgwire/src/proto.rs",
            "crates/pgshard-types/src/proto.rs",
        ],
    );
    let head = commit_all(repository.path(), "refactor: move the wire protocol");

    let components = detect_components(
        repository.path(),
        &head,
        &released,
        &[("v0.74.0", released.as_str())],
    );
    assert_eq!(
        components.get("pgwire").map(String::as_str),
        Some("true"),
        "the component the renamed file left was not detected"
    );
}

/// A base equal to the head is an empty range, not an error: the detector has
/// to report every component untouched rather than fail the run.
#[test]
fn a_base_equal_to_the_head_detects_nothing() {
    let (repository, released) = component_repository();
    git(repository.path(), &["tag", "v0.74.0", &released]);

    let components = detect_components(
        repository.path(),
        &released,
        &released,
        &[("v0.74.0", released.as_str())],
    );
    assert_eq!(components.get("pgwire").map(String::as_str), Some("false"));
    assert_eq!(components.get("rust").map(String::as_str), Some("false"));
}

/// The shipped detection step, not a copy of it, and selected by name rather
/// than by position: a prepended decoy step must not be able to stand in for
/// the step that decides which components a release is authorized on.
fn component_detection_script() -> String {
    let workflow: serde_norway::Value =
        serde_norway::from_str(include_str!("../../../.github/workflows/ci.yml"))
            .expect("the workflow is valid YAML");
    let steps = workflow["jobs"]["changes"]["steps"]
        .as_sequence()
        .expect("the detector declares steps");
    let named: Vec<&serde_norway::Value> = steps
        .iter()
        .filter(|step| step["name"].as_str() == Some("Detect changed and available components"))
        .collect();
    assert_eq!(named.len(), 1, "the component detection step is not unique");
    named[0]["run"]
        .as_str()
        .expect("the detection step runs a script")
        .to_owned()
}

fn detect_components(
    repository: &Path,
    head: &str,
    before: &str,
    releases: &[(&str, &str)],
) -> BTreeMap<String, String> {
    let scripts = repository.join(".github/scripts");
    fs::create_dir_all(&scripts).expect("create workflow scripts directory");
    let shipped =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.github/scripts/ci-diff-base.sh");
    let installed = scripts.join("ci-diff-base.sh");
    fs::copy(shipped, &installed).expect("install the shipped detection-base helper");
    fs::set_permissions(&installed, fs::Permissions::from_mode(0o755))
        .expect("make the detection-base helper executable");

    let runner_temp = repository.join("runner-temp");
    fs::create_dir_all(&runner_temp).expect("create runner temporary directory");
    let outputs = runner_temp.join("github-output");
    fs::write(&outputs, "").expect("create the step output file");

    let output = Command::new("bash")
        .arg("-c")
        .arg(component_detection_script())
        .env("EVENT_NAME", "push")
        .env("GH_TOKEN", "fixture")
        .env("GITHUB_OUTPUT", &outputs)
        .env("GITHUB_REPOSITORY", "owner/repository")
        .env("GITHUB_SHA", head)
        .env("PATH", fake_gh_path(repository))
        .env("PGSHARD_TEST_RELEASES", release_map(releases))
        .env("PR_BASE_SHA", "")
        .env("PUSH_BEFORE_SHA", before)
        .env("RUNNER_TEMP", &runner_temp)
        .current_dir(repository)
        .output()
        .expect("run the component detector");
    assert!(
        output.status.success(),
        "detector failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    fs::read_to_string(&outputs)
        .expect("read the step outputs")
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

/// The reverted-change gap: an untagged commit changes two components, and its
/// successor reverts one of them.
fn reverted_change_repository() -> (TempDir, String, String, String) {
    let (repository, released) = component_repository();
    git(repository.path(), &["tag", "v0.74.0", &released]);
    write_file(
        repository.path(),
        "crates/pgshard-pgwire/src/proto.rs",
        "v1",
    );
    write_file(repository.path(), "crates/pgshard-types/src/lib.rs", "t1");
    let unreleased = commit_all(repository.path(), "feat: change two components");
    git(repository.path(), &["tag", "v0.74.1", &unreleased]);
    write_file(
        repository.path(),
        "crates/pgshard-pgwire/src/proto.rs",
        "v0",
    );
    let head = commit_all(repository.path(), "revert: restore the wire protocol");
    (repository, released, unreleased, head)
}

fn component_repository() -> (TempDir, String) {
    let repository = tempfile::tempdir().expect("temporary repository");
    git(repository.path(), &["init", "--quiet"]);
    git(repository.path(), &["config", "user.name", "pgshard test"]);
    git(
        repository.path(),
        &["config", "user.email", "noreply@github.com"],
    );
    write_file(repository.path(), "Cargo.toml", "[workspace]");
    write_file(
        repository.path(),
        "crates/pgshard-pgwire/Cargo.toml",
        "[package]",
    );
    write_file(
        repository.path(),
        "crates/pgshard-pgwire/src/proto.rs",
        "v0",
    );
    write_file(repository.path(), "crates/pgshard-types/src/lib.rs", "t0");
    let sha = commit_all(repository.path(), "feat: released baseline");
    (repository, sha)
}

fn write_file(repository: &Path, path: &str, contents: &str) {
    let path = repository.join(path);
    fs::create_dir_all(path.parent().expect("fixture parent")).expect("create fixture parent");
    fs::write(path, format!("{contents}\n")).expect("write fixture");
}

fn commit_all(repository: &Path, subject: &str) -> String {
    git(repository, &["add", "--all"]);
    git(repository, &["commit", "--quiet", "-m", subject]);
    git(repository, &["rev-parse", "HEAD"])
}

fn repository_with_commit(contents: &str) -> (TempDir, String) {
    let repository = tempfile::tempdir().expect("temporary repository");
    git(repository.path(), &["init", "--quiet"]);
    git(repository.path(), &["config", "user.name", "pgshard test"]);
    git(
        repository.path(),
        &["config", "user.email", "noreply@github.com"],
    );
    let sha = commit(repository.path(), contents);
    (repository, sha)
}

fn commit(repository: &Path, contents: &str) -> String {
    fs::write(repository.join("state"), contents).expect("write fixture");
    git(repository, &["add", "state"]);
    git(repository, &["commit", "--quiet", "-m", "test: fixture"]);
    git(repository, &["rev-parse", "HEAD"])
}

fn detection_base(
    repository: &Path,
    head: &str,
    before: &str,
    audit: bool,
    releases: &[(&str, &str)],
) -> String {
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.github/scripts/ci-diff-base.sh");
    let mut command = Command::new("bash");
    command.arg(script);
    if audit {
        command.arg("--audit");
    }
    let output = command
        .arg(head)
        .arg(before)
        .env("GH_TOKEN", "fixture")
        .env("GITHUB_REPOSITORY", "owner/repository")
        .env("PATH", fake_gh_path(repository))
        .env("PGSHARD_TEST_RELEASES", release_map(releases))
        .current_dir(repository)
        .output()
        .expect("run detection-base helper");
    assert!(
        output.status.success(),
        "helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("UTF-8 helper output")
        .trim()
        .to_owned()
}

fn fake_gh_path(repository: &Path) -> String {
    let fake_bin = repository.join("fake-bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin");
    let fake_gh = fake_bin.join("gh");
    fs::write(
        &fake_gh,
        "#!/usr/bin/env bash\nset -euo pipefail\ntag=\"${3:?}\"\nwhile read -r expected_tag expected_sha; do\n  if [[ \"$tag\" == \"$expected_tag\" ]]; then\n    printf '%s\\n' \"$expected_sha\"\n    exit 0\n  fi\ndone <<< \"${PGSHARD_TEST_RELEASES:-}\"\nexit 1\n",
    )
    .expect("write fake gh");
    fs::set_permissions(&fake_gh, fs::Permissions::from_mode(0o755))
        .expect("make fake gh executable");
    format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").expect("PATH")
    )
}

fn release_map(releases: &[(&str, &str)]) -> String {
    releases
        .iter()
        .map(|(tag, sha)| format!("{tag} {sha}"))
        .collect::<Vec<_>>()
        .join("\n")
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
