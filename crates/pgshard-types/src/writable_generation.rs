//! Canonical writable-generation records shared by coordination and storage.

use crate::ShardId;
use thiserror::Error;

/// Exact cell and holder generation authorized for one writable attempt.
///
/// The value is durable evidence, not authority. Callers must independently
/// prove that the attempt-private Lease authority is still current before
/// acting on it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableWritableGeneration {
    cluster_name: String,
    cluster_uid: String,
    shard_id: ShardId,
    lease_namespace: String,
    lease_name: String,
    lease_uid: String,
    holder: String,
    term: u64,
}

impl DurableWritableGeneration {
    /// Creates a validated generation from its exact coordination identity.
    ///
    /// # Errors
    ///
    /// Returns an error when a field is empty, overlong, or contains bytes
    /// outside its canonical alphabet, or when `term` is zero.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_name: String,
        cluster_uid: String,
        shard_id: ShardId,
        lease_namespace: String,
        lease_name: String,
        lease_uid: String,
        holder: String,
        term: u64,
    ) -> Result<Self, WritableGenerationValidationError> {
        validate_field("cluster_name", &cluster_name, 63, false)?;
        validate_field("cluster_uid", &cluster_uid, 128, false)?;
        validate_field("lease_namespace", &lease_namespace, 63, false)?;
        validate_field("lease_name", &lease_name, 63, false)?;
        validate_field("lease_uid", &lease_uid, 128, false)?;
        validate_field("holder", &holder, 128, true)?;
        if term == 0 {
            return Err(WritableGenerationValidationError::ZeroTerm);
        }
        Ok(Self {
            cluster_name,
            cluster_uid,
            shard_id,
            lease_namespace,
            lease_name,
            lease_uid,
            holder,
            term,
        })
    }

    /// Returns the monotonic fencing term.
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.term
    }

    /// Returns the exact Lease holder identity.
    #[must_use]
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Returns whether two records belong to the same coordination universe.
    #[must_use]
    pub fn same_cell(&self, other: &Self) -> bool {
        self.cluster_name == other.cluster_name
            && self.cluster_uid == other.cluster_uid
            && self.shard_id == other.shard_id
            && self.lease_namespace == other.lease_namespace
            && self.lease_name == other.lease_name
            && self.lease_uid == other.lease_uid
    }

    /// Encodes the one canonical, bounded on-disk and WAL-row representation.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        format!(
            "format=1\ncluster_name={}\ncluster_uid={}\nshard={}\nlease_namespace={}\nlease_name={}\nlease_uid={}\nholder={}\nterm={}\n",
            self.cluster_name,
            self.cluster_uid,
            self.shard_id.0,
            self.lease_namespace,
            self.lease_name,
            self.lease_uid,
            self.holder,
            self.term,
        )
        .into_bytes()
    }

    /// Encodes the immutable PGDATA bootstrap identity for comparison.
    #[must_use]
    pub fn bootstrap_identity_bytes(&self) -> Vec<u8> {
        format!(
            "cluster_uid={}\nshard={:04}\n",
            self.cluster_uid, self.shard_id.0
        )
        .into_bytes()
    }

    /// Parses only the canonical generation representation.
    #[must_use]
    pub fn parse_canonical(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.split_terminator('\n');
        if lines.next()? != "format=1" {
            return None;
        }
        let cluster_name = parse_field(lines.next()?, "cluster_name", 63, false)?;
        let cluster_uid = parse_field(lines.next()?, "cluster_uid", 128, false)?;
        let shard = parse_decimal::<u32>(lines.next()?, "shard")?;
        let lease_namespace = parse_field(lines.next()?, "lease_namespace", 63, false)?;
        let lease_name = parse_field(lines.next()?, "lease_name", 63, false)?;
        let lease_uid = parse_field(lines.next()?, "lease_uid", 128, false)?;
        let holder = parse_field(lines.next()?, "holder", 128, true)?;
        let term = parse_decimal::<u64>(lines.next()?, "term").filter(|term| *term > 0)?;
        if lines.next().is_some() || !text.ends_with('\n') {
            return None;
        }
        let parsed = Self::new(
            cluster_name,
            cluster_uid,
            ShardId(shard),
            lease_namespace,
            lease_name,
            lease_uid,
            holder,
            term,
        )
        .ok()?;
        (parsed.canonical_bytes() == bytes).then_some(parsed)
    }
}

/// Invalid input at the canonical writable-generation type boundary.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WritableGenerationValidationError {
    /// One identity field violated its fixed canonical alphabet or length.
    #[error("writable-generation field {field} is empty, overlong, or noncanonical")]
    InvalidField {
        /// Rejected field name.
        field: &'static str,
    },
    /// Fencing terms start at one.
    #[error("writable-generation term must be greater than zero")]
    ZeroTerm,
}

/// Safe state transition for one durable writable-generation location.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WritableGenerationTransition {
    /// No generation exists and the requested value may initialize the record.
    Initialize,
    /// The exact requested generation is already durable.
    Replay,
    /// A higher term in the same coordination universe may replace the record.
    Advance,
}

/// Rejected transition between durable writable generations.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WritableGenerationTransitionError {
    /// The existing value belongs to another cell or Lease incarnation.
    #[error("durable writable generation belongs to another coordination universe")]
    ForeignUniverse,
    /// The requested term is below the durable fencing floor.
    #[error("writable-term regression: requested {requested}, durable {durable}")]
    Regression {
        /// Existing durable term.
        durable: u64,
        /// Requested stale term.
        requested: u64,
    },
    /// Two holders claimed the same fencing term.
    #[error("writable term {term} has conflicting holders")]
    ConflictingHolder {
        /// Conflicting term.
        term: u64,
    },
}

/// Classifies a requested durable generation without performing I/O.
///
/// Initialization, exact replay, and a strictly higher term in the same
/// coordination universe are the only permitted transitions.
///
/// # Errors
///
/// Returns an error for a foreign coordination universe, a term regression,
/// or different holders claiming the same term.
pub fn classify_writable_generation_transition(
    existing: Option<&DurableWritableGeneration>,
    requested: &DurableWritableGeneration,
) -> Result<WritableGenerationTransition, WritableGenerationTransitionError> {
    let Some(existing) = existing else {
        return Ok(WritableGenerationTransition::Initialize);
    };
    if !existing.same_cell(requested) {
        return Err(WritableGenerationTransitionError::ForeignUniverse);
    }
    if existing.term() > requested.term() {
        return Err(WritableGenerationTransitionError::Regression {
            durable: existing.term(),
            requested: requested.term(),
        });
    }
    if existing.term() < requested.term() {
        return Ok(WritableGenerationTransition::Advance);
    }
    if existing.holder() != requested.holder() {
        return Err(WritableGenerationTransitionError::ConflictingHolder {
            term: requested.term(),
        });
    }
    Ok(WritableGenerationTransition::Replay)
}

fn parse_field(
    line: &str,
    name: &'static str,
    maximum: usize,
    allow_slash: bool,
) -> Option<String> {
    let value = line.strip_prefix(name)?.strip_prefix('=')?;
    validate_field(name, value, maximum, allow_slash)
        .ok()
        .map(|()| value.to_owned())
}

fn validate_field(
    name: &'static str,
    value: &str,
    maximum: usize,
    allow_slash: bool,
) -> Result<(), WritableGenerationValidationError> {
    if value.is_empty()
        || value.len() > maximum
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b'-')
                || (allow_slash && byte == b'/')
        })
    {
        return Err(WritableGenerationValidationError::InvalidField { field: name });
    }
    Ok(())
}

fn parse_decimal<T>(line: &str, name: &str) -> Option<T>
where
    T: std::str::FromStr + ToString,
{
    let value = line.strip_prefix(name)?.strip_prefix('=')?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let parsed = value.parse::<T>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation(term: u64, holder: &str) -> DurableWritableGeneration {
        DurableWritableGeneration::new(
            "cluster-1".to_owned(),
            "11111111-2222-3333-4444-555555555555".to_owned(),
            ShardId(0),
            "database".to_owned(),
            "cluster-1-cell-0000-writable".to_owned(),
            "99999999-8888-7777-6666-555555555555".to_owned(),
            holder.to_owned(),
            term,
        )
        .expect("valid generation fixture")
    }

    #[test]
    fn canonical_bytes_remain_compatible_and_round_trip() {
        let value = generation(42, "cluster-1-shard-0-0/pod/attempt");
        let expected = b"format=1\ncluster_name=cluster-1\ncluster_uid=11111111-2222-3333-4444-555555555555\nshard=0\nlease_namespace=database\nlease_name=cluster-1-cell-0000-writable\nlease_uid=99999999-8888-7777-6666-555555555555\nholder=cluster-1-shard-0-0/pod/attempt\nterm=42\n";
        assert_eq!(value.canonical_bytes(), expected);
        assert_eq!(
            DurableWritableGeneration::parse_canonical(expected),
            Some(value)
        );
    }

    #[test]
    fn parser_rejects_noncanonical_and_unbounded_values() {
        let canonical = generation(42, "holder").canonical_bytes();
        let noncanonical_term = String::from_utf8(canonical.clone())
            .expect("generation is UTF-8")
            .replace("term=42\n", "term=042\n");
        for invalid in [
            canonical.strip_suffix(b"\n").expect("canonical newline"),
            &canonical[..canonical.len() - b"term=42\n".len()],
            noncanonical_term.as_bytes(),
        ] {
            assert!(DurableWritableGeneration::parse_canonical(invalid).is_none());
        }
        let overlong = format!(
            "format=1\ncluster_name={}\ncluster_uid=u\nshard=0\nlease_namespace=n\nlease_name=l\nlease_uid=u\nholder=h\nterm=1\n",
            "a".repeat(64)
        );
        assert!(DurableWritableGeneration::parse_canonical(overlong.as_bytes()).is_none());
    }

    #[test]
    fn transition_classifier_exhausts_safe_and_rejected_cases() {
        let requested = generation(2, "holder-b");
        assert_eq!(
            classify_writable_generation_transition(None, &requested),
            Ok(WritableGenerationTransition::Initialize)
        );
        assert_eq!(
            classify_writable_generation_transition(Some(&requested), &requested),
            Ok(WritableGenerationTransition::Replay)
        );
        assert_eq!(
            classify_writable_generation_transition(Some(&generation(1, "holder-a")), &requested),
            Ok(WritableGenerationTransition::Advance)
        );
        assert_eq!(
            classify_writable_generation_transition(Some(&generation(3, "holder-c")), &requested),
            Err(WritableGenerationTransitionError::Regression {
                durable: 3,
                requested: 2,
            })
        );
        assert_eq!(
            classify_writable_generation_transition(Some(&generation(2, "holder-a")), &requested),
            Err(WritableGenerationTransitionError::ConflictingHolder { term: 2 })
        );

        let foreign = DurableWritableGeneration::new(
            "cluster-2".to_owned(),
            "uid-2".to_owned(),
            ShardId(0),
            "database".to_owned(),
            "lease".to_owned(),
            "lease-uid".to_owned(),
            "holder".to_owned(),
            1,
        )
        .expect("valid foreign generation");
        assert_eq!(
            classify_writable_generation_transition(Some(&foreign), &requested),
            Err(WritableGenerationTransitionError::ForeignUniverse)
        );
    }

    #[test]
    fn constructor_rejects_every_noncanonical_boundary() {
        let valid = generation(1, "holder/attempt");
        assert_eq!(
            DurableWritableGeneration::parse_canonical(&valid.canonical_bytes()),
            Some(valid)
        );
        let base = [
            "cluster",
            "uid",
            "namespace",
            "lease",
            "lease-uid",
            "holder",
        ];
        for (index, (field, maximum)) in [
            ("cluster_name", 63),
            ("cluster_uid", 128),
            ("lease_namespace", 63),
            ("lease_name", 63),
            ("lease_uid", 128),
            ("holder", 128),
        ]
        .into_iter()
        .enumerate()
        {
            for invalid in [
                String::new(),
                "bad\nvalue".to_owned(),
                "x".repeat(maximum + 1),
            ] {
                let mut values = base.map(str::to_owned);
                values[index] = invalid;
                assert_eq!(
                    DurableWritableGeneration::new(
                        values[0].clone(),
                        values[1].clone(),
                        ShardId(0),
                        values[2].clone(),
                        values[3].clone(),
                        values[4].clone(),
                        values[5].clone(),
                        1,
                    ),
                    Err(WritableGenerationValidationError::InvalidField { field })
                );
            }
        }
        assert_eq!(
            DurableWritableGeneration::new(
                "cluster".to_owned(),
                "uid".to_owned(),
                ShardId(0),
                "namespace".to_owned(),
                "lease".to_owned(),
                "lease-uid".to_owned(),
                "holder".to_owned(),
                0,
            ),
            Err(WritableGenerationValidationError::ZeroTerm)
        );
    }
}
