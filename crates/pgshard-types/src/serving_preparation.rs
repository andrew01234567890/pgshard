//! Pure contract binding one `ServingPrimary` preparation to the exact state
//! it was prepared against.
//!
//! Serving activation replaces the physical Pod incarnation of member zero and
//! only then publishes routing state. Everything the replacement is judged
//! against has to be decided before the old Pod is deleted, because afterwards
//! there is nothing left to compare with: the process that held the evidence is
//! gone. This module is that decision, and nothing else — no I/O, no runtime
//! selection, no policy about when a replacement may proceed.
//!
//! The digest is the whole point. A preparation names a materialized catalog,
//! a source Pod incarnation, the configuration and access policy the
//! replacement must come up under, the generation it is bound to, and the
//! system and timeline identity it must still have afterwards. Binding those
//! together means a replacement cannot be judged against a preparation made for
//! a different catalog, a different Pod, a different policy, or a different
//! timeline.

use crate::writable_generation::DurableWritableGeneration;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Selects the supported canonical encoding.
pub const SERVING_PREPARATION_VERSION: &str = "pgshard.serving-preparation.v1";

/// Domain separator, so a preparation digest can never collide with another
/// contract's digest over the same bytes.
pub const SERVING_PREPARATION_DIGEST_DOMAIN: &str = "pgshard-serving-preparation-v1";

/// Longest accepted bounded text field.
const MAXIMUM_TEXT: usize = 253;

/// The source incarnation a preparation was made against.
///
/// The replacement is required to have a *different* Pod UID and the *same*
/// member identity: that is what distinguishes a real replacement from the
/// original process still running.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServingPreparationSource {
    /// Physical shard ordinal. Serving activation supports only zero.
    pub shard: u32,
    /// Physical member ordinal. Serving activation supports only zero.
    pub member: u32,
    /// Stable member identity, which the replacement must retain.
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    /// Pod name, which the replacement must retain.
    pub pod_name: String,
    /// API-assigned Pod UID, which the replacement must NOT retain.
    #[serde(rename = "podUID")]
    pub pod_uid: String,
    /// Owning `StatefulSet` UID, which pins the workload the replacement comes
    /// from.
    #[serde(rename = "statefulSetUID")]
    pub stateful_set_uid: String,
    /// Revision the replacement is required to be created at.
    pub update_revision: String,
}

/// The exact configuration and access policy a replacement must come up under.
///
/// Held as digests rather than content: this contract decides *which* policy,
/// and the stage that installs it proves the bytes match.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServingPreparationPolicy {
    /// Digest of the `PostgreSQL` configuration the replacement must load.
    #[serde(rename = "configurationSHA256")]
    pub configuration_sha256: String,
    /// Digest of the replication-only policy every incarnation starts under.
    #[serde(rename = "nonServingHBASHA256")]
    pub non_serving_hba_sha256: String,
    /// Digest of the sealed policy that admits application traffic, installed
    /// only after every proof has passed.
    #[serde(rename = "servingHBASHA256")]
    pub serving_hba_sha256: String,
    /// Digest of the workload template the replacement is rendered from.
    #[serde(rename = "templateSHA256")]
    pub template_sha256: String,
}

/// The identity a replacement must still have to be the same database.
///
/// A timeline change is a failure, not evidence of a successful activation:
/// member zero is already the writable primary, so a new timeline means
/// something was promoted or recovered underneath this preparation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServingPreparationIdentity {
    /// `pg_control` system identifier, which must be unchanged.
    pub system_identifier: u64,
    /// Timeline the preparation was made on, which must be unchanged.
    pub timeline: u32,
}

/// One `ServingPrimary` preparation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServingPreparation {
    /// Selects the canonical encoding.
    pub schema_version: String,
    /// Digest of the activation request whose catalog was materialized. Ties
    /// this preparation to one materialization rather than to "a catalog".
    #[serde(rename = "requestSHA256")]
    pub request_sha256: String,
    /// Canonical writable generation the preparation is bound to, exactly as
    /// `DurableWritableGeneration::canonical_bytes` renders it. Multi-line by
    /// construction, so it is parsed rather than treated as bounded text.
    pub generation: String,
    /// Incarnation being replaced.
    pub source: ServingPreparationSource,
    /// Configuration and access policy the replacement must satisfy.
    pub policy: ServingPreparationPolicy,
    /// Database identity that must survive the replacement.
    pub identity: ServingPreparationIdentity,
}

/// Why a preparation is not canonical.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ServingPreparationError {
    /// The schema version does not select the supported encoding.
    #[error("unsupported serving preparation schema version")]
    UnsupportedVersion,
    /// A bounded text field is empty, too long, or contains unsafe bytes.
    #[error("serving preparation contains an invalid bounded text field")]
    InvalidText,
    /// A SHA-256 field is not canonical lowercase hexadecimal.
    #[error("serving preparation contains an invalid SHA-256 digest")]
    InvalidDigest,
    /// The generation is not a canonical writable generation. Accepting
    /// arbitrary text here would bind the preparation to something no other
    /// stage can reconstruct or compare against.
    #[error("serving preparation does not carry a canonical writable generation")]
    InvalidGeneration,
    /// Serving activation is defined only for shard zero, member zero.
    #[error("serving preparation names a shard or member other than zero")]
    UnsupportedTarget,
    /// A system identifier of zero is not a real `pg_control` identity.
    #[error("serving preparation names a zero system identifier")]
    InvalidIdentity,
    /// The non-serving and serving policies are the same bytes, so activation
    /// would not be a transition at all.
    #[error("serving preparation reuses one policy for both serving states")]
    IndistinctPolicy,
}

impl ServingPreparation {
    /// Proves the contract is canonical.
    ///
    /// # Errors
    ///
    /// Returns the first violated rule.
    pub fn validate(&self) -> Result<(), ServingPreparationError> {
        // Destructured exhaustively: a field added later is a compile error
        // here rather than a value that is silently neither validated nor
        // signed.
        let Self {
            schema_version,
            request_sha256,
            generation,
            source:
                ServingPreparationSource {
                    shard,
                    member,
                    instance_id,
                    pod_name,
                    pod_uid,
                    stateful_set_uid,
                    update_revision,
                },
            policy:
                ServingPreparationPolicy {
                    configuration_sha256,
                    non_serving_hba_sha256,
                    serving_hba_sha256,
                    template_sha256,
                },
            identity:
                ServingPreparationIdentity {
                    system_identifier,
                    timeline,
                },
        } = self;

        if schema_version != SERVING_PREPARATION_VERSION {
            return Err(ServingPreparationError::UnsupportedVersion);
        }
        if *shard != 0 || *member != 0 {
            return Err(ServingPreparationError::UnsupportedTarget);
        }
        for text in [
            instance_id,
            pod_name,
            pod_uid,
            stateful_set_uid,
            update_revision,
        ] {
            validate_text(text)?;
        }
        // Reconstructed rather than pattern-matched: the only definition of
        // canonical is what `DurableWritableGeneration` itself round-trips, and
        // every other stage compares against exactly that.
        let parsed = DurableWritableGeneration::parse_canonical(generation.as_bytes())
            .ok_or(ServingPreparationError::InvalidGeneration)?;
        if parsed.canonical_bytes() != generation.as_bytes() {
            return Err(ServingPreparationError::InvalidGeneration);
        }
        // Round-tripping proves the generation is canonical. It does NOT prove
        // it is the generation for the shard being acted on, so the two are
        // compared: a preparation naming shard zero and carrying another
        // shard's generation is not a preparation for either of them.
        if parsed.shard_id().0 != *shard {
            return Err(ServingPreparationError::InvalidGeneration);
        }
        for digest in [
            request_sha256,
            configuration_sha256,
            non_serving_hba_sha256,
            serving_hba_sha256,
            template_sha256,
        ] {
            validate_digest(digest)?;
        }
        if non_serving_hba_sha256 == serving_hba_sha256 {
            return Err(ServingPreparationError::IndistinctPolicy);
        }
        if *system_identifier == 0 {
            return Err(ServingPreparationError::InvalidIdentity);
        }
        // A timeline of zero is not a value PostgreSQL assigns.
        if *timeline == 0 {
            return Err(ServingPreparationError::InvalidIdentity);
        }
        Ok(())
    }

    /// Returns the lowercase SHA-256 digest of the validated, fixed-order,
    /// length-framed contract.
    ///
    /// # Errors
    ///
    /// Returns a validation error rather than hashing a non-canonical
    /// preparation.
    pub fn sha256(&self) -> Result<String, ServingPreparationError> {
        self.validate()?;
        let mut hash = Sha256::new();
        frame(&mut hash, SERVING_PREPARATION_DIGEST_DOMAIN);
        self.for_each_component(|component| frame(&mut hash, component));
        Ok(lower_hex(&hash.finalize()))
    }

    /// Visits every signed component in fixed order.
    ///
    /// Exhaustively destructured for the same reason as `validate`: a field
    /// that is added but not signed is a field an attacker may vary freely.
    fn for_each_component(&self, mut visit: impl FnMut(&str)) {
        let Self {
            schema_version,
            request_sha256,
            generation,
            source:
                ServingPreparationSource {
                    shard,
                    member,
                    instance_id,
                    pod_name,
                    pod_uid,
                    stateful_set_uid,
                    update_revision,
                },
            policy:
                ServingPreparationPolicy {
                    configuration_sha256,
                    non_serving_hba_sha256,
                    serving_hba_sha256,
                    template_sha256,
                },
            identity:
                ServingPreparationIdentity {
                    system_identifier,
                    timeline,
                },
        } = self;
        visit(schema_version);
        visit(request_sha256);
        visit(generation);
        visit(&shard.to_string());
        visit(&member.to_string());
        visit(instance_id);
        visit(pod_name);
        visit(pod_uid);
        visit(stateful_set_uid);
        visit(update_revision);
        visit(configuration_sha256);
        visit(non_serving_hba_sha256);
        visit(serving_hba_sha256);
        visit(template_sha256);
        visit(&system_identifier.to_string());
        visit(&timeline.to_string());
    }
}

fn validate_text(value: &str) -> Result<(), ServingPreparationError> {
    if value.is_empty()
        || value.len() > MAXIMUM_TEXT
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(ServingPreparationError::InvalidText);
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), ServingPreparationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ServingPreparationError::InvalidDigest);
    }
    Ok(())
}

fn frame(hash: &mut Sha256, value: &str) {
    hash.update(
        u64::try_from(value.len())
            .expect("bounded preparation component length fits u64")
            .to_be_bytes(),
    );
    hash.update(value.as_bytes());
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
        text
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(seed: u8) -> String {
        (0..32).fold(String::new(), |mut text, index| {
            use std::fmt::Write as _;
            let _ = write!(text, "{:02x}", seed.wrapping_add(index));
            text
        })
    }

    /// A real canonical generation, rendered by the type that defines what
    /// canonical means. Inventing the text here is what the first version of
    /// this fixture did, and it hid a validator that rejected every real value.
    fn generation() -> String {
        let generation = crate::writable_generation::DurableWritableGeneration::new(
            "demo".to_owned(),
            "cccccccc-1111-2222-3333-444444444444".to_owned(),
            crate::ShardId(0),
            "database".to_owned(),
            "demo-shard-0000-term".to_owned(),
            "dddddddd-1111-2222-3333-444444444444".to_owned(),
            "demo-shard-0000-0".to_owned(),
            7,
        )
        .expect("the fixture generation is valid");
        String::from_utf8(generation.canonical_bytes()).expect("canonical bytes are UTF-8")
    }

    fn preparation() -> ServingPreparation {
        ServingPreparation {
            schema_version: SERVING_PREPARATION_VERSION.to_owned(),
            request_sha256: digest(1),
            generation: generation(),
            source: ServingPreparationSource {
                shard: 0,
                member: 0,
                instance_id: "demo-shard-0000-0".to_owned(),
                pod_name: "demo-shard-0000-0".to_owned(),
                pod_uid: "11111111-2222-3333-4444-555555555555".to_owned(),
                stateful_set_uid: "66666666-7777-8888-9999-aaaaaaaaaaaa".to_owned(),
                update_revision: "demo-shard-0000-7c9f4b8d5".to_owned(),
            },
            policy: ServingPreparationPolicy {
                configuration_sha256: digest(2),
                non_serving_hba_sha256: digest(3),
                serving_hba_sha256: digest(4),
                template_sha256: digest(5),
            },
            identity: ServingPreparationIdentity {
                system_identifier: 7_248_119_402_113_558_016,
                timeline: 1,
            },
        }
    }

    /// A field that is bound but not signed is a field that may be varied
    /// freely, so every one of them has to move the digest.
    #[test]
    fn the_digest_is_sensitive_to_every_binding() {
        let baseline = preparation().sha256().expect("the fixture is canonical");
        let mut mutations: Vec<(&str, ServingPreparation)> = Vec::new();

        let mut it = preparation();
        it.request_sha256 = digest(9);
        mutations.push(("request", it));
        let mut it = preparation();
        it.generation = {
            let other = crate::writable_generation::DurableWritableGeneration::new(
                "demo".to_owned(),
                "cccccccc-1111-2222-3333-444444444444".to_owned(),
                crate::ShardId(0),
                "database".to_owned(),
                "demo-shard-0000-term".to_owned(),
                "dddddddd-1111-2222-3333-444444444444".to_owned(),
                "demo-shard-0000-0".to_owned(),
                8,
            )
            .expect("valid");
            String::from_utf8(other.canonical_bytes()).expect("utf8")
        };
        mutations.push(("generation", it));
        let mut it = preparation();
        it.source.instance_id = "demo-shard-0000-1".to_owned();
        mutations.push(("instance", it));
        let mut it = preparation();
        it.source.pod_name = "other".to_owned();
        mutations.push(("pod name", it));
        let mut it = preparation();
        it.source.pod_uid = "99999999-2222-3333-4444-555555555555".to_owned();
        mutations.push(("pod uid", it));
        let mut it = preparation();
        it.source.stateful_set_uid = "99999999-7777-8888-9999-aaaaaaaaaaaa".to_owned();
        mutations.push(("statefulset uid", it));
        let mut it = preparation();
        it.source.update_revision = "demo-shard-0000-000000000".to_owned();
        mutations.push(("revision", it));
        let mut it = preparation();
        it.policy.configuration_sha256 = digest(9);
        mutations.push(("configuration", it));
        let mut it = preparation();
        it.policy.non_serving_hba_sha256 = digest(9);
        mutations.push(("non-serving policy", it));
        let mut it = preparation();
        it.policy.serving_hba_sha256 = digest(9);
        mutations.push(("serving policy", it));
        let mut it = preparation();
        it.policy.template_sha256 = digest(9);
        mutations.push(("template", it));
        let mut it = preparation();
        it.identity.system_identifier = 1;
        mutations.push(("system identifier", it));
        let mut it = preparation();
        it.identity.timeline = 2;
        mutations.push(("timeline", it));

        for (binding, mutated) in mutations {
            let moved = mutated.sha256().expect("the mutation stays canonical");
            assert_ne!(baseline, moved, "the digest ignores the {binding} binding");
        }
    }

    /// Length framing, so two adjacent fields cannot be re-split between them
    /// to produce the same bytes.
    #[test]
    fn adjacent_bindings_cannot_be_reflowed_into_each_other() {
        let mut left = preparation();
        left.source.pod_name = "ab".to_owned();
        left.source.pod_uid = "cdef0000-2222-3333-4444-555555555555".to_owned();
        let mut right = preparation();
        right.source.pod_name = "abc".to_owned();
        right.source.pod_uid = "def0000-2222-3333-4444-555555555555".to_owned();
        assert_ne!(
            left.sha256().expect("canonical"),
            right.sha256().expect("canonical"),
            "moving a byte across a field boundary produced the same digest"
        );
    }

    #[test]
    fn a_non_canonical_preparation_is_never_hashed() {
        for (expected, mutate) in [
            (
                ServingPreparationError::UnsupportedVersion,
                Box::new(|it: &mut ServingPreparation| it.schema_version = "v2".to_owned())
                    as Box<dyn Fn(&mut ServingPreparation)>,
            ),
            (
                ServingPreparationError::UnsupportedTarget,
                Box::new(|it: &mut ServingPreparation| it.source.shard = 1),
            ),
            (
                ServingPreparationError::UnsupportedTarget,
                Box::new(|it: &mut ServingPreparation| it.source.member = 1),
            ),
            (
                ServingPreparationError::InvalidText,
                Box::new(|it: &mut ServingPreparation| it.source.pod_name = String::new()),
            ),
            (
                ServingPreparationError::InvalidDigest,
                Box::new(|it: &mut ServingPreparation| {
                    it.policy.template_sha256 = "ABCD".to_owned();
                }),
            ),
            (
                ServingPreparationError::InvalidIdentity,
                Box::new(|it: &mut ServingPreparation| it.identity.system_identifier = 0),
            ),
            (
                ServingPreparationError::InvalidIdentity,
                Box::new(|it: &mut ServingPreparation| it.identity.timeline = 0),
            ),
            (
                ServingPreparationError::InvalidGeneration,
                Box::new(|it: &mut ServingPreparation| {
                    it.generation = "cluster-1:holder-a:7".to_owned();
                }),
            ),
            (
                ServingPreparationError::IndistinctPolicy,
                Box::new(|it: &mut ServingPreparation| {
                    it.policy.serving_hba_sha256 = it.policy.non_serving_hba_sha256.clone();
                }),
            ),
        ] {
            let mut mutated = preparation();
            mutate(&mut mutated);
            assert_eq!(mutated.validate(), Err(expected));
            assert_eq!(
                mutated.sha256(),
                Err(expected),
                "a non-canonical preparation was hashed anyway"
            );
        }
    }

    /// A canonical generation for a different shard is still canonical, so
    /// round-tripping it proves nothing about whether it belongs to the shard
    /// being acted on. This is the case that argument missed.
    #[test]
    fn a_generation_for_another_shard_is_rejected() {
        let other_shard = crate::writable_generation::DurableWritableGeneration::new(
            "demo".to_owned(),
            "cccccccc-1111-2222-3333-444444444444".to_owned(),
            crate::ShardId(3),
            "database".to_owned(),
            "demo-shard-0003-term".to_owned(),
            "dddddddd-1111-2222-3333-444444444444".to_owned(),
            "demo-shard-0003-0".to_owned(),
            7,
        )
        .expect("a shard-three generation is valid on its own");
        let canonical =
            String::from_utf8(other_shard.canonical_bytes()).expect("canonical bytes are UTF-8");
        // It round-trips, so the previous check accepted it.
        assert_eq!(
            crate::writable_generation::DurableWritableGeneration::parse_canonical(
                canonical.as_bytes()
            )
            .expect("it parses")
            .canonical_bytes(),
            canonical.as_bytes(),
        );

        let mut mismatched = preparation();
        mismatched.generation = canonical;
        assert_eq!(
            mismatched.validate(),
            Err(ServingPreparationError::InvalidGeneration),
            "a preparation for shard zero accepted another shard's generation"
        );
        assert_eq!(
            mismatched.sha256(),
            Err(ServingPreparationError::InvalidGeneration),
            "a mismatched preparation was hashed anyway"
        );
    }

    /// The digest is a cross-language contract, so it is pinned rather than
    /// merely self-consistent: the operator has to be able to compute it too.
    #[test]
    fn the_canonical_vector_is_pinned() {
        assert_eq!(
            preparation().sha256().expect("the fixture is canonical"),
            "4d2d7bb1b9c2c0405426a1c831c63f7705791c0b77514ea2b481a27cb04f590f",
            "the canonical digest changed; regenerate the operator vector too"
        );
    }
}
