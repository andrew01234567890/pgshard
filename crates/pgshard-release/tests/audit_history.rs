//! Regression tests for public-history scanning across intermediate commits.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

#[test]
fn rejects_sensitive_content_added_then_deleted() {
    let repository = TempDir::new().expect("temporary repository");
    git(
        repository.path(),
        &["init", "--quiet", "--initial-branch=main"],
    );
    git(repository.path(), &["config", "user.name", "pgshard test"]);
    git(
        repository.path(),
        &["config", "user.email", "noreply@github.com"],
    );

    fs::write(repository.path().join("README.md"), "safe\n").expect("write base");
    git(repository.path(), &["add", "README.md"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: base"],
    );
    let base = git(repository.path(), &["rev-parse", "HEAD"]);

    let sensitive = ["github", "_pat_", "transient"].concat();
    fs::write(repository.path().join("transient.txt"), sensitive).expect("write transient");
    git(repository.path(), &["add", "transient.txt"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add transient"],
    );
    fs::remove_file(repository.path().join("transient.txt")).expect("remove transient");
    git(
        repository.path(),
        &["commit", "--quiet", "-am", "test: remove transient"],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_pgshard-release"))
        .current_dir(repository.path())
        .args(["audit", "--base", base.trim(), "--head", "HEAD"])
        .output()
        .expect("run audit");
    assert!(!output.status.success(), "transient secret escaped audit");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("forbidden sensitive-data pattern"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repository)
        .args(args)
        .output()
        .expect("run git");
    assert_success(&output, args);
    String::from_utf8(output.stdout)
        .expect("UTF-8 git output")
        .trim()
        .to_owned()
}

fn assert_success(output: &Output, args: &[&str]) {
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}
