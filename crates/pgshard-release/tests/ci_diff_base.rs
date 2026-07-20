//! Regression tests for widening CI across an untagged release gap.

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
    let release_map = releases
        .iter()
        .map(|(tag, sha)| format!("{tag} {sha}"))
        .collect::<Vec<_>>()
        .join("\n");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").expect("PATH")
    );
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
        .env("PATH", path)
        .env("PGSHARD_TEST_RELEASES", release_map)
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
