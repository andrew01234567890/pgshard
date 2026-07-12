//! Core identifiers and deterministic routing primitives shared by pgshard.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// The exclusive upper bound of the 64-bit hash keyspace.
pub const KEYSPACE_END: u128 = 1_u128 << 64;

/// Stable identifier for a shard.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ShardId(pub u32);

/// A monotonically increasing catalog revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CatalogEpoch(pub u64);

/// A monotonically increasing routing revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RoutingEpoch(pub u64);

/// A `PostgreSQL` WAL location encoded as a monotonically increasing integer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgLsn(pub u64);

/// A half-open range in the unsigned 64-bit hash keyspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyRange {
    /// Inclusive range start.
    pub start: u128,
    /// Exclusive range end. `2^64` represents the end of the keyspace.
    pub end: u128,
}

impl KeyRange {
    /// Creates a validated key range.
    ///
    /// # Errors
    ///
    /// Returns [`KeyRangeError`] when the bounds are empty, reversed, or
    /// outside the unsigned 64-bit keyspace.
    pub fn new(start: u128, end: u128) -> Result<Self, KeyRangeError> {
        if start >= end {
            return Err(KeyRangeError::EmptyOrReversed { start, end });
        }
        if end > KEYSPACE_END {
            return Err(KeyRangeError::OutsideKeyspace { end });
        }
        Ok(Self { start, end })
    }

    /// Returns whether a hash belongs to this range.
    #[must_use]
    pub fn contains(self, hash: u64) -> bool {
        let hash = u128::from(hash);
        self.start <= hash && hash < self.end
    }
}

/// Validation failure for a key range.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KeyRangeError {
    /// The range has no values or its bounds are reversed.
    #[error("key range start {start} must be less than end {end}")]
    EmptyOrReversed {
        /// Inclusive range start.
        start: u128,
        /// Exclusive range end.
        end: u128,
    },
    /// The end exceeds the unsigned 64-bit keyspace.
    #[error("key range end {end} exceeds 2^64")]
    OutsideKeyspace {
        /// Invalid exclusive range end.
        end: u128,
    },
}

/// Canonical shard-key value accepted by the Milestone 1 hash function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardKey<'a> {
    /// Signed 64-bit integer, encoded big-endian.
    Integer(i64),
    /// UUID bytes in network order.
    Uuid(&'a [u8; 16]),
    /// UTF-8 text bytes.
    Text(&'a str),
    /// Arbitrary byte string.
    Bytes(&'a [u8]),
}

impl ShardKey<'_> {
    /// Computes the version-one stable hash with a catalog-provided seed.
    #[must_use]
    pub fn hash_v1(self, seed: u64) -> u64 {
        let (tag, bytes): (u8, &[u8]) = match self {
            Self::Integer(value) => return hash_tagged(1, &value.to_be_bytes(), seed),
            Self::Uuid(value) => (2, value),
            Self::Text(value) => (3, value.as_bytes()),
            Self::Bytes(value) => (4, value),
        };
        hash_tagged(tag, bytes, seed)
    }
}

fn hash_tagged(tag: u8, bytes: &[u8], seed: u64) -> u64 {
    let mut canonical = Vec::with_capacity(bytes.len() + 1);
    canonical.push(tag);
    canonical.extend_from_slice(bytes);
    xxh3_64_with_seed(&canonical, seed)
}

/// One shard position in a vector checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorPosition {
    /// Shard that produced the WAL.
    pub shard_id: ShardId,
    /// `PostgreSQL` timeline containing the WAL location.
    pub timeline: u32,
    /// Acknowledged WAL location.
    pub lsn: PgLsn,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_range_accepts_last_hash() {
        let range = KeyRange::new(0, KEYSPACE_END).expect("full range is valid");
        assert!(range.contains(0));
        assert!(range.contains(u64::MAX));
    }

    #[test]
    fn range_is_half_open() {
        let range = KeyRange::new(10, 20).expect("range is valid");
        assert!(range.contains(10));
        assert!(range.contains(19));
        assert!(!range.contains(20));
    }

    #[test]
    fn range_validation_rejects_invalid_bounds() {
        assert!(matches!(
            KeyRange::new(2, 2),
            Err(KeyRangeError::EmptyOrReversed { .. })
        ));
        assert!(matches!(
            KeyRange::new(0, KEYSPACE_END + 1),
            Err(KeyRangeError::OutsideKeyspace { .. })
        ));
    }

    #[test]
    fn hash_is_stable_and_type_separated() {
        let seed = 42;
        assert_eq!(
            ShardKey::Text("42").hash_v1(seed),
            ShardKey::Text("42").hash_v1(seed)
        );
        assert_ne!(
            ShardKey::Text("42").hash_v1(seed),
            ShardKey::Bytes(b"42").hash_v1(seed)
        );
    }
}
