//! Versioned backup-manifest wire types shared with the Go operator.

use thiserror::Error;

const MANIFEST_V1_DOMAIN: &[u8] = b"pgshard.restore-manifest.v1\0";
const MAXIMUM_SHARDS: usize = 128;

/// One ordered half-open routing range in a version-one backup manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestoreShardRangeV1<'a> {
    /// Zero-based range position.
    pub ordinal: i32,
    /// Canonical decimal inclusive start.
    pub start: &'a str,
    /// Canonical decimal exclusive end.
    pub end: &'a str,
}

/// Complete logical topology stored in a version-one backup manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestoreTopologyV1<'a> {
    /// `PostgreSQL` major version.
    pub postgresql_major: &'a str,
    /// Routing-hash contract version.
    pub hash_version: i32,
    /// Canonical unsigned-decimal hash seed.
    pub hash_seed: &'a str,
    /// Ordered routing ranges.
    pub shards: &'a [RestoreShardRangeV1<'a>],
}

/// Signed version-one backup-manifest fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestoreManifestV1<'a> {
    /// Repository-unique backup-set identity.
    pub backup_set_id: &'a str,
    /// Logical source database name.
    pub source_database: &'a str,
    /// Topology that must be recreated by restore.
    pub topology: RestoreTopologyV1<'a>,
}

/// A manifest cannot be encoded within the bounded version-one wire contract.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RestoreManifestEncodingError {
    /// A string field exceeds its byte bound.
    #[error("restore manifest field {field} exceeds {maximum} bytes")]
    FieldTooLong {
        /// Stable field name.
        field: &'static str,
        /// Maximum accepted UTF-8 byte length.
        maximum: usize,
    },
    /// More than 128 ranges were supplied.
    #[error("restore manifest contains {actual} shards; maximum is 128")]
    TooManyShards {
        /// Supplied range count.
        actual: usize,
    },
}

/// Encodes the language-neutral bytes signed by backup publication and checked
/// by the Go operator. Fields use fixed order, big-endian signed 32-bit integer
/// bit patterns, and big-endian u32-length-prefixed string bytes.
///
/// # Errors
///
/// Returns [`RestoreManifestEncodingError`] before allocation can grow beyond
/// the version-one field and shard-count bounds.
pub fn encode_restore_manifest_v1(
    manifest: RestoreManifestV1<'_>,
) -> Result<Vec<u8>, RestoreManifestEncodingError> {
    if manifest.topology.shards.len() > MAXIMUM_SHARDS {
        return Err(RestoreManifestEncodingError::TooManyShards {
            actual: manifest.topology.shards.len(),
        });
    }
    let shard_count = u32::try_from(manifest.topology.shards.len()).map_err(|_| {
        RestoreManifestEncodingError::TooManyShards {
            actual: manifest.topology.shards.len(),
        }
    })?;
    let mut payload = Vec::with_capacity(256 + manifest.topology.shards.len() * 64);
    payload.extend_from_slice(MANIFEST_V1_DOMAIN);
    write_i32(&mut payload, 1);
    write_string(&mut payload, "backupSetID", manifest.backup_set_id, 128)?;
    write_string(&mut payload, "sourceDatabase", manifest.source_database, 63)?;
    write_string(
        &mut payload,
        "topology.postgresqlMajor",
        manifest.topology.postgresql_major,
        8,
    )?;
    write_i32(&mut payload, manifest.topology.hash_version);
    write_string(
        &mut payload,
        "topology.hashSeed",
        manifest.topology.hash_seed,
        20,
    )?;
    write_i32(
        &mut payload,
        i32::try_from(shard_count).map_err(|_| RestoreManifestEncodingError::TooManyShards {
            actual: manifest.topology.shards.len(),
        })?,
    );
    write_u32(&mut payload, shard_count);
    for shard in manifest.topology.shards {
        write_i32(&mut payload, shard.ordinal);
        write_string(&mut payload, "topology.shards.start", shard.start, 20)?;
        write_string(&mut payload, "topology.shards.end", shard.end, 20)?;
    }
    Ok(payload)
}

fn write_string(
    payload: &mut Vec<u8>,
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), RestoreManifestEncodingError> {
    if value.len() > maximum {
        return Err(RestoreManifestEncodingError::FieldTooLong { field, maximum });
    }
    let length = u32::try_from(value.len())
        .map_err(|_| RestoreManifestEncodingError::FieldTooLong { field, maximum })?;
    write_u32(payload, length);
    payload.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_i32(payload: &mut Vec<u8>, value: i32) {
    payload.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(payload: &mut Vec<u8>, value: u32) {
    payload.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_PAYLOAD_HEX: &str = "706773686172642e726573746f72652d6d616e69666573742e76310000000001000000136261636b75702d612d323032362d30372d313700000001410000000231380000000100000001370000000100000001000000000000000130000000143138343436373434303733373039353531363136";

    #[test]
    fn matches_go_operator_golden_payload() {
        let shards = [RestoreShardRangeV1 {
            ordinal: 0,
            start: "0",
            end: "18446744073709551616",
        }];
        let payload = encode_restore_manifest_v1(RestoreManifestV1 {
            backup_set_id: "backup-a-2026-07-17",
            source_database: "A",
            topology: RestoreTopologyV1 {
                postgresql_major: "18",
                hash_version: 1,
                hash_seed: "7",
                shards: &shards,
            },
        })
        .expect("golden manifest is bounded");
        let encoded = lower_hex(&payload);
        assert_eq!(encoded, EXPECTED_PAYLOAD_HEX);
    }

    #[test]
    fn bounds_strings_and_shards_before_encoding() {
        let oversized = "x".repeat(129);
        let error = encode_restore_manifest_v1(RestoreManifestV1 {
            backup_set_id: &oversized,
            source_database: "A",
            topology: RestoreTopologyV1 {
                postgresql_major: "18",
                hash_version: 1,
                hash_seed: "7",
                shards: &[],
            },
        })
        .expect_err("oversized identity must fail");
        assert!(matches!(
            error,
            RestoreManifestEncodingError::FieldTooLong {
                field: "backupSetID",
                maximum: 128
            }
        ));

        let ranges = vec![
            RestoreShardRangeV1 {
                ordinal: 0,
                start: "0",
                end: "1",
            };
            129
        ];
        let error = encode_restore_manifest_v1(RestoreManifestV1 {
            backup_set_id: "backup",
            source_database: "A",
            topology: RestoreTopologyV1 {
                postgresql_major: "18",
                hash_version: 1,
                hash_seed: "7",
                shards: &ranges,
            },
        })
        .expect_err("oversized shard list must fail");
        assert_eq!(
            error,
            RestoreManifestEncodingError::TooManyShards { actual: 129 }
        );
    }

    fn lower_hex(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
            encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}
