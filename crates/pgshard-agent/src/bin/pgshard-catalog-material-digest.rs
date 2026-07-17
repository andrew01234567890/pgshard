//! Internal catalog-material fingerprint helper for `PostgreSQL` bootstrap.

use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

use pgshard_types::catalog_material::{
    CATALOG_CLIENT_DIGEST_DOMAIN, CATALOG_SERVER_DIGEST_DOMAIN, catalog_material_sha256,
};

const MAXIMUM_KEY_BYTES: u64 = 64 * 1024;
const MAXIMUM_VALUE_BYTES: u64 = 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let profile = required_argument(&mut arguments, "profile")?;
    let key_path = required_argument(&mut arguments, "key file")?;
    let value_path = required_argument(&mut arguments, "value file")?;
    if arguments.next().is_some() {
        return Err(
            "usage: pgshard-catalog-material-digest <client|server> <key-file> <value-file>".into(),
        );
    }

    let profile = profile
        .into_string()
        .map_err(|_| "catalog material profile must be UTF-8")?;
    let (domain, key_description, value_description) = match profile.as_str() {
        "client" => (
            CATALOG_CLIENT_DIGEST_DOMAIN,
            "catalog password",
            "catalog CA certificate",
        ),
        "server" => (
            CATALOG_SERVER_DIGEST_DOMAIN,
            "catalog server private key",
            "catalog server certificate",
        ),
        _ => return Err("catalog material profile must be client or server".into()),
    };

    let key = read_bounded_file(&key_path, key_description, MAXIMUM_KEY_BYTES)?;
    let value = read_bounded_file(&value_path, value_description, MAXIMUM_VALUE_BYTES)?;
    let fingerprint = catalog_material_sha256(domain, &key, [&value[..]]);
    let mut stdout = io::stdout().lock();
    stdout.write_all(fingerprint.as_bytes())?;
    stdout.write_all(b"\n")?;
    Ok(())
}

fn required_argument(
    arguments: &mut impl Iterator<Item = OsString>,
    description: &str,
) -> Result<OsString, Box<dyn std::error::Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("missing required {description}").into())
}

fn read_bounded_file(
    path: impl AsRef<Path>,
    description: &'static str,
    maximum: u64,
) -> Result<Vec<u8>, io::Error> {
    let file = File::open(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not open {description}: {error}"),
        )
    })?;
    let metadata = file.metadata().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not inspect {description}: {error}"),
        )
    })?;
    if !metadata.is_file() {
        return Err(io::Error::other(format!(
            "{description} must resolve to a regular file"
        )));
    }
    if metadata.len() == 0 || metadata.len() > maximum {
        return Err(io::Error::other(format!(
            "{description} must contain between 1 and {maximum} bytes"
        )));
    }

    let capacity = usize::try_from(metadata.len())
        .map_err(|_| io::Error::other(format!("{description} length does not fit in memory")))?;
    let mut contents = Vec::with_capacity(capacity);
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut contents)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("could not read {description}: {error}"),
            )
        })?;
    if contents.is_empty() || u64::try_from(contents.len()).unwrap_or(u64::MAX) > maximum {
        return Err(io::Error::other(format!(
            "{description} must contain between 1 and {maximum} bytes"
        )));
    }
    Ok(contents)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_reader_rejects_empty_and_oversized_material() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let empty = directory.path().join("empty");
        std::fs::write(&empty, []).expect("write empty fixture");
        assert!(read_bounded_file(&empty, "fixture", 4).is_err());

        let oversized = directory.path().join("oversized");
        std::fs::write(&oversized, b"12345").expect("write oversized fixture");
        assert!(read_bounded_file(&oversized, "fixture", 4).is_err());
    }
}
