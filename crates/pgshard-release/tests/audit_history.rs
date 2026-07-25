//! Regression tests for public-history scanning across intermediate commits.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

#[test]
fn rejects_sensitive_content_added_then_deleted() {
    let (repository, base) = repository_with_base();

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

    let output = audit(repository.path(), &base);
    assert!(!output.status.success(), "transient secret escaped audit");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("forbidden sensitive-data pattern"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn accepts_safe_non_utf8_binary_history() {
    let (repository, base) = repository_with_base();
    fs::write(
        repository.path().join("asset.bin"),
        [0, 0xff, 0xfe, b's', b'a', b'f', b'e', 0],
    )
    .expect("write binary asset");
    git(repository.path(), &["add", "asset.bin"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add binary asset"],
    );

    let output = audit(repository.path(), &base);
    assert_success(&output, &["pgshard-release", "audit"]);
}

#[test]
fn accepts_safe_history_with_non_noreply_commit_email() {
    let (repository, base) = repository_with_base();
    fs::write(repository.path().join("safe.txt"), "safe public content\n").expect("write safe");
    git(repository.path(), &["add", "safe.txt"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add safe content"],
    );

    let output = audit(repository.path(), &base);
    assert_success(&output, &["pgshard-release", "audit"]);
}

#[test]
fn rejects_sensitive_ascii_inside_non_utf8_binary_history() {
    let (repository, base) = repository_with_base();
    let mut content = vec![0, 0xff, 0xfe];
    content.extend_from_slice(["github", "_pat_", "binary"].concat().as_bytes());
    content.push(0);
    fs::write(repository.path().join("asset.bin"), content).expect("write binary asset");
    git(repository.path(), &["add", "asset.bin"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add unsafe binary asset"],
    );

    let output = audit(repository.path(), &base);
    assert!(!output.status.success(), "binary secret escaped audit");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("forbidden sensitive-data pattern"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A rendered Kubernetes Secret carries `tls.key` as base64 of the whole PEM, so
/// no delimiter appears as text anywhere in the committed file. This has to be
/// driven through the binary: a unit test on the content scanner cannot tell
/// whether the walk over committed files still reaches the decoder.
#[test]
fn rejects_a_private_key_hidden_by_base64_in_a_committed_file() {
    let (repository, base) = repository_with_base();
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        leaked_private_key().as_bytes(),
    );
    assert!(
        !encoded.contains("PRIVATE"),
        "the encoded form must not carry the delimiter as text, or this proves nothing"
    );
    fs::write(
        repository.path().join("secret.yaml"),
        format!("apiVersion: v1\nkind: Secret\ndata:\n  tls.key: {encoded}\n"),
    )
    .expect("write secret");
    git(repository.path(), &["add", "secret.yaml"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add rendered secret"],
    );

    let output = audit(repository.path(), &base);
    assert!(
        !output.status.success(),
        "encoded private key escaped audit"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("private-key delimiter"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Wrapping the same encoding across lines is what `base64 file` and a YAML
/// block scalar both produce.
#[test]
fn rejects_a_private_key_wrapped_across_lines_in_a_committed_file() {
    let (repository, base) = repository_with_base();
    let bundle = format!(
        "Bag Attributes: friendlyName=leak\n{}",
        leaked_private_key()
    );
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        bundle.as_bytes(),
    );
    let wrapped = encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(repository.path().join("key.b64"), format!("{wrapped}\n")).expect("write wrapped");
    git(repository.path(), &["add", "key.b64"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add wrapped key"],
    );

    let output = audit(repository.path(), &base);
    assert!(
        !output.status.success(),
        "wrapped private key escaped audit"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("private-key delimiter"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Replacing a regular file with a symlink is a type change, and an allowlist of
/// diff statuses omits it. Only the binary exercises the walk that chooses them.
#[test]
fn rejects_a_secret_introduced_by_a_type_change() {
    let (repository, base) = repository_with_base();
    fs::write(repository.path().join("secret"), "placeholder\n").expect("write placeholder");
    git(repository.path(), &["add", "secret"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add regular file"],
    );

    fs::remove_file(repository.path().join("secret")).expect("remove placeholder");
    let target = ["gh", "p_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat();
    std::os::unix::fs::symlink(&target, repository.path().join("secret")).expect("create symlink");
    git(repository.path(), &["add", "secret"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: replace with symlink"],
    );

    let allowlisted = git(
        repository.path(),
        &[
            "diff-tree",
            "--root",
            "-m",
            "--no-commit-id",
            "--name-only",
            "--diff-filter=ACMR",
            "-r",
            "HEAD",
            "--",
        ],
    );
    assert!(
        !allowlisted.contains("secret"),
        "this test proves nothing unless an allowlist really does omit the type change"
    );

    let output = audit(repository.path(), &base);
    assert!(!output.status.success(), "type change escaped audit");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("forbidden sensitive-data pattern"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// UTF-16 keeps every byte of a credential and separates none of them by more
/// than a NUL.
#[test]
fn rejects_a_credential_stored_as_utf16_in_committed_history() {
    let (repository, base) = repository_with_base();
    let token = ["gh", "p_", "0123456789abcdefghijklmnopqrstuvwxyz"].concat();
    let utf16: Vec<u8> = token.encode_utf16().flat_map(u16::to_le_bytes).collect();
    fs::write(repository.path().join("asset.bin"), utf16).expect("write utf16 asset");
    git(repository.path(), &["add", "asset.bin"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add utf16 asset"],
    );

    let output = audit(repository.path(), &base);
    assert!(!output.status.success(), "utf16 credential escaped audit");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("forbidden sensitive-data pattern"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A container the audit cannot read is refused rather than passed, so the miss
/// is a stop the maintainer sees rather than silence.
#[test]
fn refuses_compressed_content_in_committed_history() {
    let (repository, base) = repository_with_base();
    let mut archive = vec![0x1f, 0x8b, 0x08, 0x00];
    archive.extend_from_slice(b"whatever the container holds");
    fs::write(repository.path().join("asset.gz"), archive).expect("write archive");
    git(repository.path(), &["add", "asset.gz"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: add an archive"],
    );

    let output = audit(repository.path(), &base);
    assert!(!output.status.success(), "compressed content was audited");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("refuses what it cannot read"),
        "unexpected failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn leaked_private_key() -> String {
    [
        "-----BEGIN ",
        "PRIVATE ",
        "KEY-----\n",
        "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDBJ0000000000\n",
        "-----END ",
        "PRIVATE ",
        "KEY-----\n",
    ]
    .concat()
}

fn repository_with_base() -> (TempDir, String) {
    let repository = TempDir::new().expect("temporary repository");
    git(
        repository.path(),
        &["init", "--quiet", "--initial-branch=main"],
    );
    git(repository.path(), &["config", "user.name", "pgshard test"]);
    git(
        repository.path(),
        &["config", "user.email", "contributor@example.com"],
    );

    fs::write(repository.path().join("README.md"), "safe\n").expect("write base");
    git(repository.path(), &["add", "README.md"]);
    git(
        repository.path(),
        &["commit", "--quiet", "-m", "test: base"],
    );
    let base = git(repository.path(), &["rev-parse", "HEAD"]);
    (repository, base)
}

fn audit(repository: &Path, base: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pgshard-release"))
        .current_dir(repository)
        .args(["audit", "--base", base.trim(), "--head", "HEAD"])
        .output()
        .expect("run audit")
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
