//! Core identifiers and deterministic routing primitives shared by pgshard.

pub mod catalog_material;

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use xxhash_rust::xxh3::Xxh3;

/// The exclusive upper bound of the 64-bit hash keyspace.
pub const KEYSPACE_END: u128 = 1_u128 << 64;

/// Stable identifier for a shard.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ShardId(pub u32);

/// A monotonically increasing catalog revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogEpoch(pub u64);

/// A monotonically increasing routing revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RoutingEpoch(pub u64);

/// A `PostgreSQL` WAL location encoded as an integer.
///
/// It is monotonic only within one timeline. Bare LSN values deliberately do
/// not implement ordering because ordering across a failover fork is invalid.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PgLsn(pub u64);

/// A half-open range in the unsigned 64-bit hash keyspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyRange {
    /// Inclusive range start.
    start: u128,
    /// Exclusive range end. `2^64` represents the end of the keyspace.
    end: u128,
}

impl KeyRange {
    /// Creates a validated key range.
    ///
    /// # Errors
    ///
    /// Returns [`KeyRangeError`] when the bounds are empty, reversed, or
    /// outside the unsigned 64-bit keyspace.
    pub const fn new(start: u128, end: u128) -> Result<Self, KeyRangeError> {
        if start >= end {
            return Err(KeyRangeError::EmptyOrReversed { start, end });
        }
        if end > KEYSPACE_END {
            return Err(KeyRangeError::OutsideKeyspace { end });
        }
        Ok(Self { start, end })
    }

    /// Returns the inclusive range start.
    #[must_use]
    pub const fn start(self) -> u128 {
        self.start
    }

    /// Returns the exclusive range end.
    #[must_use]
    pub const fn end(self) -> u128 {
        self.end
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
    /// UTF-8 text bytes from a `PostgreSQL` database using the deterministic,
    /// byte-distinguishing built-in `C` collation for this shard-key column.
    /// Catalog registration must enforce that precondition.
    Text(&'a str),
    /// Arbitrary byte string.
    Bytes(&'a [u8]),
}

impl<'a> ShardKey<'a> {
    fn canonical_parts(self) -> (u8, CanonicalBytes<'a>) {
        match self {
            Self::Integer(value) => (1, CanonicalBytes::Integer(value.to_be_bytes())),
            Self::Uuid(value) => (2, CanonicalBytes::Borrowed(value)),
            Self::Text(value) => (3, CanonicalBytes::Borrowed(value.as_bytes())),
            Self::Bytes(value) => (4, CanonicalBytes::Borrowed(value)),
        }
    }
}

enum CanonicalBytes<'a> {
    Integer([u8; 8]),
    Borrowed(&'a [u8]),
}

impl CanonicalBytes<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Integer(bytes) => bytes,
            Self::Borrowed(bytes) => bytes,
        }
    }
}

/// Immutable version-one routing-hash configuration stored in `shardschema`.
///
/// A cluster creates this value once. Changing either its algorithm version or
/// seed requires an explicit online reshard; ordinary catalog updates cannot
/// mutate it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingHashV1 {
    seed: u64,
}

impl RoutingHashV1 {
    /// The catalog algorithm identifier for this encoding and XXH3 contract.
    pub const VERSION: u16 = 1;

    /// Creates the immutable version-one configuration for a new cluster.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Returns the catalog seed.
    #[must_use]
    pub const fn seed(self) -> u64 {
        self.seed
    }

    /// Hashes a typed key without allocating.
    #[must_use]
    pub fn hash(self, key: ShardKey<'_>) -> u64 {
        let (tag, bytes) = key.canonical_parts();
        let mut hasher = Xxh3::with_seed(self.seed);
        hasher.update(&[tag]);
        hasher.update(bytes.as_slice());
        hasher.digest()
    }
}

/// One shard position in a vector checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorPosition {
    /// Shard that produced the WAL.
    pub shard_id: ShardId,
    /// `PostgreSQL` timeline containing the WAL location.
    pub timeline: u32,
    /// Acknowledged WAL location.
    pub lsn: PgLsn,
}

impl VectorPosition {
    /// Compares positions only when they describe the same shard and timeline.
    ///
    /// # Errors
    ///
    /// Returns an error rather than inventing an ordering across shards or WAL
    /// histories.
    pub fn checked_cmp(&self, other: &Self) -> Result<Ordering, PositionComparisonError> {
        if self.shard_id != other.shard_id {
            return Err(PositionComparisonError::DifferentShard {
                left: self.shard_id,
                right: other.shard_id,
            });
        }
        if self.timeline != other.timeline {
            return Err(PositionComparisonError::DifferentTimeline {
                left: self.timeline,
                right: other.timeline,
            });
        }
        Ok(self.lsn.0.cmp(&other.lsn.0))
    }
}

/// Invalid attempt to order unrelated WAL positions.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PositionComparisonError {
    /// Positions came from different shards.
    #[error("cannot compare WAL positions from shards {left:?} and {right:?}")]
    DifferentShard {
        /// Left shard.
        left: ShardId,
        /// Right shard.
        right: ShardId,
    },
    /// Positions came from different `PostgreSQL` timelines.
    #[error("cannot compare WAL positions from timelines {left} and {right}")]
    DifferentTimeline {
        /// Left timeline.
        left: u32,
        /// Right timeline.
        right: u32,
    },
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
        assert_eq!(range.start(), 10);
        assert_eq!(range.end(), 20);
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
    fn hash_is_type_separated() {
        let hash = RoutingHashV1::new(42);
        assert_ne!(
            hash.hash(ShardKey::Text("42")),
            hash.hash(ShardKey::Bytes(b"42"))
        );
        assert_ne!(
            hash.hash(ShardKey::Text("A")),
            hash.hash(ShardKey::Text("a")),
            "byte-distinguishing C collation is a routing precondition"
        );
    }

    #[test]
    fn routing_hash_v1_matches_golden_vectors() {
        let uuid = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        let uuid_zero = [0; 16];
        let uuid_max = [u8::MAX; 16];
        let bytes_16: Vec<u8> = (0..16).collect();
        let bytes_17: Vec<u8> = (0..17).collect();
        let bytes_128: Vec<u8> = (0..128).collect();
        let bytes_129: Vec<u8> = (0..129).collect();
        let bytes_240: Vec<u8> = (0..240).collect();
        let bytes_241: Vec<u8> = (0..241).collect();
        let cases = [
            RoutingHashV1::new(0).hash(ShardKey::Integer(i64::MIN)),
            RoutingHashV1::new(0).hash(ShardKey::Integer(0)),
            RoutingHashV1::new(u64::MAX).hash(ShardKey::Integer(i64::MAX)),
            RoutingHashV1::new(0).hash(ShardKey::Uuid(&uuid_zero)),
            RoutingHashV1::new(42).hash(ShardKey::Uuid(&uuid)),
            RoutingHashV1::new(u64::MAX).hash(ShardKey::Uuid(&uuid_max)),
            RoutingHashV1::new(42).hash(ShardKey::Text("")),
            RoutingHashV1::new(42).hash(ShardKey::Text("pgshard")),
            RoutingHashV1::new(42).hash(ShardKey::Text("分片")),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&[])),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&[0, 255])),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&bytes_16)),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&bytes_17)),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&bytes_128)),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&bytes_129)),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&bytes_240)),
            RoutingHashV1::new(42).hash(ShardKey::Bytes(&bytes_241)),
        ];
        assert_eq!(
            cases,
            [
                6_834_552_262_684_119_129,
                3_547_760_990_396_968_576,
                17_209_831_906_688_329_482,
                14_419_510_823_407_099_226,
                5_906_330_825_808_846_518,
                2_325_824_285_543_005_152,
                15_044_340_851_791_431_563,
                1_118_524_738_512_168_610,
                5_884_530_194_252_116_679,
                13_545_918_211_138_518_346,
                7_324_023_272_056_575_253,
                10_932_846_924_442_393_542,
                8_629_757_135_075_089_821,
                14_868_452_095_708_363_663,
                17_538_407_021_030_774_196,
                14_368_470_560_858_418_990,
                4_209_940_504_094_720_787,
            ]
        );
    }

    #[test]
    fn wal_ordering_rejects_forked_timelines_and_shards() {
        let base = VectorPosition {
            shard_id: ShardId(1),
            timeline: 7,
            lsn: PgLsn(100),
        };
        let later = VectorPosition {
            lsn: PgLsn(200),
            ..base.clone()
        };
        assert_eq!(base.checked_cmp(&later), Ok(Ordering::Less));

        let fork = VectorPosition {
            timeline: 8,
            ..later.clone()
        };
        assert!(matches!(
            later.checked_cmp(&fork),
            Err(PositionComparisonError::DifferentTimeline { .. })
        ));

        let other_shard = VectorPosition {
            shard_id: ShardId(2),
            ..later.clone()
        };
        assert!(matches!(
            later.checked_cmp(&other_shard),
            Err(PositionComparisonError::DifferentShard { .. })
        ));
    }
}
