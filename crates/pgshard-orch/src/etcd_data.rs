//! Crash-safe migration of the transitional etcd data directory.

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::Path;

const DATA_DIRECTORY: &str = "data";
const LEGACY_MEMBER_DIRECTORY: &str = "member";
const STAGING_DIRECTORY: &str = ".pgshard-etcd-data-v1";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

/// Moves a legacy etcd `member` directory below an owner-private `data`
/// directory without copying or discarding any durable membership state.
///
/// The two renames occur within the same PVC. A fixed staging directory makes
/// every crash point replayable: the legacy member is either still in place,
/// already staged, or published below `data`. Conflicting layouts fail closed.
///
/// # Errors
///
/// Returns an error for I/O failures, symlinks, non-directory paths, or an
/// ambiguous layout containing both legacy and migrated state.
pub fn prepare_etcd_data_dir(root: &Path) -> io::Result<()> {
    require_directory(root, "etcd volume root")?;
    let data = root.join(DATA_DIRECTORY);
    let legacy = root.join(LEGACY_MEMBER_DIRECTORY);
    let staging = root.join(STAGING_DIRECTORY);

    if directory_exists(&data, "migrated etcd data directory")? {
        if directory_exists(&legacy, "legacy etcd member directory")?
            || directory_exists(&staging, "etcd migration staging directory")?
        {
            return Err(invalid_layout(
                "migrated etcd data conflicts with legacy or staged state",
            ));
        }
        make_private(&data)?;
        sync_directory(&data)?;
        sync_directory(root)?;
        return Ok(());
    }

    if !directory_exists(&staging, "etcd migration staging directory")? {
        create_private_directory(&staging)?;
        sync_directory(root)?;
    }

    let legacy_exists = directory_exists(&legacy, "legacy etcd member directory")?;
    let staged_member = staging.join(LEGACY_MEMBER_DIRECTORY);
    let staged_member_exists = directory_exists(&staged_member, "staged etcd member directory")?;
    ensure_only_staged_member(&staging, staged_member_exists)?;

    match (legacy_exists, staged_member_exists) {
        (true, false) => {
            fs::rename(&legacy, &staged_member)?;
            sync_directory(&staging)?;
            sync_directory(root)?;
        }
        (false, true | false) => {}
        (true, true) => {
            return Err(invalid_layout(
                "legacy and staged etcd member directories both exist",
            ));
        }
    }

    make_private(&staging)?;
    sync_directory(&staging)?;
    fs::rename(&staging, &data)?;
    sync_directory(root)?;
    Ok(())
}

fn directory_exists(path: &Path, description: &str) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(invalid_layout(&format!("{description} is not a directory"))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn require_directory(path: &Path, description: &str) -> io::Result<()> {
    if directory_exists(path, description)? {
        Ok(())
    } else {
        Err(invalid_layout(&format!("{description} does not exist")))
    }
}

fn ensure_only_staged_member(staging: &Path, member_exists: bool) -> io::Result<()> {
    let mut entries = fs::read_dir(staging)?;
    let first = entries.next().transpose()?;
    let second = entries.next().transpose()?;
    match (first, second, member_exists) {
        (None, None, false) => Ok(()),
        (Some(entry), None, true) if entry.file_name() == LEGACY_MEMBER_DIRECTORY => Ok(()),
        _ => Err(invalid_layout(
            "etcd migration staging directory contains unexpected state",
        )),
    }
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .mode(PRIVATE_DIRECTORY_MODE)
        .create(path)?;
    make_private(path)
}

fn make_private(path: &Path) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE))
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn invalid_layout(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    fn mode(path: &Path) -> u32 {
        fs::metadata(path)
            .expect("path metadata")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn creates_a_private_data_directory_for_a_fresh_volume() {
        let root = TempDir::new().expect("temporary etcd volume");
        prepare_etcd_data_dir(root.path()).expect("prepare fresh volume");
        assert_eq!(mode(&root.path().join(DATA_DIRECTORY)), 0o700);
        assert!(!root.path().join(STAGING_DIRECTORY).exists());
    }

    #[test]
    fn preserves_legacy_membership_and_replays_after_publication() {
        let root = TempDir::new().expect("temporary etcd volume");
        let legacy = root.path().join(LEGACY_MEMBER_DIRECTORY);
        fs::create_dir(&legacy).expect("legacy member");
        fs::write(legacy.join("sentinel"), b"membership").expect("legacy membership");

        prepare_etcd_data_dir(root.path()).expect("migrate legacy volume");
        prepare_etcd_data_dir(root.path()).expect("replay migrated volume");

        let data = root.path().join(DATA_DIRECTORY);
        assert_eq!(mode(&data), 0o700);
        assert_eq!(
            fs::read(data.join(LEGACY_MEMBER_DIRECTORY).join("sentinel"))
                .expect("migrated membership"),
            b"membership"
        );
        assert!(!legacy.exists());
    }

    #[test]
    fn resumes_after_the_legacy_member_was_staged() {
        let root = TempDir::new().expect("temporary etcd volume");
        let staging = root.path().join(STAGING_DIRECTORY);
        fs::create_dir(&staging).expect("staging directory");
        let member = staging.join(LEGACY_MEMBER_DIRECTORY);
        fs::create_dir(&member).expect("staged member");
        fs::write(member.join("sentinel"), b"staged").expect("staged membership");

        prepare_etcd_data_dir(root.path()).expect("resume staged migration");

        assert_eq!(
            fs::read(
                root.path()
                    .join(DATA_DIRECTORY)
                    .join(LEGACY_MEMBER_DIRECTORY)
                    .join("sentinel")
            )
            .expect("published membership"),
            b"staged"
        );
    }

    #[test]
    fn refuses_ambiguous_or_symlinked_layouts() {
        let root = TempDir::new().expect("temporary etcd volume");
        fs::create_dir(root.path().join(DATA_DIRECTORY)).expect("migrated data");
        fs::create_dir(root.path().join(LEGACY_MEMBER_DIRECTORY)).expect("legacy member");
        assert_eq!(
            prepare_etcd_data_dir(root.path())
                .expect_err("ambiguous layout must fail")
                .kind(),
            io::ErrorKind::InvalidData
        );

        let linked_root = TempDir::new().expect("temporary linked volume");
        let target = linked_root.path().join("target");
        fs::create_dir(&target).expect("symlink target");
        symlink(&target, linked_root.path().join(DATA_DIRECTORY)).expect("data symlink");
        assert_eq!(
            prepare_etcd_data_dir(linked_root.path())
                .expect_err("symlinked data must fail")
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
