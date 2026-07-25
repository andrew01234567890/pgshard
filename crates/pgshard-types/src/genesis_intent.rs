//! Canonical form for the contract that names one first genesis.
//!
//! This module decides exactly one thing: whether a [`CatalogGenesisIntent`] is
//! canonically well formed and internally consistent. It does no I/O, reads no
//! live state, and grants nothing. Passing every rule here proves the document
//! is spelled correctly and does not contradict itself. It does not prove that
//! anybody may act on it.
//!
//! The contract is worth having because materialization normally reconciles a
//! catalog that is already there, and first genesis is the one case with
//! nothing to reconcile against. An empty database is what a fresh shard looks
//! like; it is equally what a half-restored one, a wrong-cluster mount, and a
//! dropped catalog look like. Nothing about the database separates those, so
//! permission to create a catalog must never be inferred from inspecting it —
//! permission has to be issued elsewhere and carried here. This module fixes
//! the form permission will be expressed in. It does not grant permission.
//!
//! It could not grant permission. Whoever writes an intent chooses every byte
//! in it, so checking one against itself authenticates nothing, however many
//! rules it passes. Authority can only come from agreement with facts the
//! writer does not control, and no such fact is reachable from a types crate.
//!
//! # The obligations this defers
//!
//! Whoever issues genesis authority is whoever can read the real sources.
//! Everything in this section is owed by that component, and none of it
//! happens here.
//!
//! ## Authenticate the whole intent, not one field
//!
//! Recompute [`CatalogGenesisIntent::sha256`] over the intent in hand and
//! authenticate that digest against the durably accepted record. This subsumes
//! comparing `request_sha256` on its own, which binds only itself and leaves
//! every other signed component free to vary. It also survives the contract
//! growing: a field-by-field scheme has to be revisited whenever a field is
//! added, and the field that gets forgotten is the one an attacker may choose.
//!
//! ## Compare each remaining value against its own live source
//!
//! - the intent's generation against the writable generation the attempt
//!   actually holds;
//! - the intent's cluster UID against the live owning object it runs under;
//! - the intent's data directory against the PGDATA incarnation observed on
//!   disk — seed, `pg_control` system identifier and timeline, all three.
//!
//! Each right-hand side has to be read from its own source. A value copied out
//! of the intent and then compared with the intent is a tautology, and a
//! comparison that cannot fail is worse than no comparison at all, because it
//! reads as though something was checked. Care at the comparison site is not
//! the defence, because that is the care that was already tried and lost. The
//! structural defence is to refuse a caller-constructed evidence value
//! entirely, and to read every right-hand side inside the privileged component,
//! so that the tautology has no way to be expressed.
//!
//! An incarnation that is recorded and never read back is not a fence. This
//! contract signs a seed, a system identifier and a timeline; unless the
//! component performing genesis compares all three against the mounted PGDATA
//! on every path that can create a catalog, they are a comment. That is worse
//! than not carrying them at all, because in review they read as protection.
//!
//! ## Refuse unless genesis has never been recorded for this incarnation
//!
//! None of the comparisons above is a freshness check, and without one the
//! ordinary retry path re-creates a catalog that was deliberately destroyed. A
//! controller re-renders this intent on every reconcile. After a `DROP
//! DATABASE` every comparison above still passes: the acceptance record is
//! still live, the generation is still the one held, the cluster UID has not
//! changed, and dropping a database does not touch PGDATA, so the seed, the
//! system identifier and timeline one all still match. The catalog is then
//! silently re-materialized and the loss is masked. "Dropped catalog" is one of
//! the four indistinguishable states this module opens by naming, and it is the
//! one the comparisons above do not reach.
//!
//! The gate that closes it is an assertion about the record and never about the
//! database: genesis is permitted because no genesis has ever been recorded for
//! this incarnation, not because the database looks empty. Reading emptiness as
//! permission is precisely what the opening paragraph rules out. Key that
//! lookup by the incarnation read off the mount, never by the one the intent
//! names: otherwise a caller who invents a fresh `seed_id` invents a fresh key,
//! finds nothing recorded against it, and is waved through — the caller-chosen
//! right-hand side warned about above, reappearing one level up. So the record
//! has to be durable before the catalog is created, and consumable at most
//! once, so that a retry of the same intent finds genesis already recorded and
//! stops.
//!
//! ## Treat absent authority as invalid, never as unknown
//!
//! An absent, empty or zero-valued authority record compares as "invalid,
//! refuse". It never compares as "unknown, proceed", and it is never repaired
//! by writing a fresh one: a store that has been wiped or cannot be reached
//! would then mint its own permission and every fence downstream of it would
//! pass. A zero term that compares as permissive is the same defect one step
//! later, and it is how two nodes end up believing they are both primary.
//!
//! ## Fence the destructive act a second time, in the engine
//!
//! Permission logic can be wrong, so do not let it be the only thing standing
//! between a mistake and a created database. Perform the catalog creation over
//! a role that cannot write while the server is in recovery or
//! `default_transaction_read_only` is set, and do not grant that role the
//! ability to override either. Authority and the engine's own writability then
//! both have to agree, and a node that wrongly believes it may create the
//! catalog gets a failed statement instead of a new database.
//!
//! # The `members` gap
//!
//! [`GenesisTarget::members`] is required to be non-zero and is signed by
//! [`CatalogGenesisIntent::sha256`], but nothing here binds it: an intent may
//! declare any member count at all and stay canonical. A fifth field-by-field
//! comparison could close it, since a durable acceptance record that carries
//! the accepted member count is an independent source for exactly that value.
//! Authenticating the whole intent digest is still the remedy to implement,
//! because it binds all nine signed components at once, `members` among them,
//! and goes on binding fields added after this is written.
//!
//! # The wire form
//!
//! The digest is taken over the fields rather than over the serialized JSON, so
//! a rename moves the encoding without moving the digest:
//! `the_canonical_vector_is_pinned` would keep passing while every recorded
//! intent stopped deserializing. `clusterUid` and `requestSha256` fall out of
//! `rename_all = "camelCase"` and depart from the sibling contracts in
//! [`crate::serving_preparation`] and [`crate::catalog_activation`], which
//! spell the same concepts `clusterUID`, `podUID`, `statefulSetUID` and
//! `requestSHA256`. Tidying that up is a breaking encoding change rather than a
//! cosmetic one, so the wire form is pinned byte for byte alongside the digest.
//!
//! `systemIdentifier` is deliberately decimal text and not a JSON number,
//! because JSON numbers are IEEE 754 doubles to JavaScript and to `jq`'s
//! default parser while a `pg_control` system identifier routinely exceeds
//! 2^53, so a number would let the one field that names an incarnation be
//! silently rounded into naming a different one. Do not tidy it back.

use crate::ShardId;
use crate::writable_generation::DurableWritableGeneration;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Selects the supported canonical encoding.
pub const GENESIS_INTENT_VERSION: &str = "pgshard.catalog-genesis-intent.v1";

/// Domain separator, so a genesis digest can never collide with another
/// contract's digest over the same bytes.
pub const GENESIS_INTENT_DIGEST_DOMAIN: &str = "pgshard-catalog-genesis-intent-v1";

/// Longest accepted bounded text field.
const MAXIMUM_TEXT: usize = 253;

/// The data directory incarnation a genesis intent names.
///
/// `initdb` publishes a fresh PGDATA atomically and stamps it with a seed.
/// These three values are what an intent *claims* about a data directory.
/// Whoever writes an intent chooses them, so they establish which incarnation
/// it names and never which one it was written for. Carrying and signing them
/// fixes the claim so it cannot be varied after the fact; it is not a substitute
/// for reading the mounted PGDATA, and the comparison against the observed
/// incarnation is owed by whoever performs genesis.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GenesisDataDirectory {
    /// Seed written during atomic `initdb` publication.
    pub seed_id: String,
    /// `pg_control` system identifier of that incarnation. Crosses the wire as
    /// decimal text rather than as a JSON number; see the wire form in this
    /// module's documentation before changing that.
    #[serde(with = "canonical_decimal_u64")]
    pub system_identifier: u64,
    /// Timeline the incarnation was published on. Genesis is timeline one; a
    /// later timeline means the cluster has already been promoted at least
    /// once, so this is not first genesis.
    pub timeline: u32,
}

/// The cluster and shard a genesis intent names.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GenesisTarget {
    /// Kubernetes UID of the owning cluster object. Serialized `clusterUid`,
    /// which is not how the sibling contracts spell it — see the wire form in
    /// this module's documentation before renaming it.
    pub cluster_uid: String,
    /// Shard the catalog is being created for.
    pub shard: u32,
    /// Members the shard is declared to have. Signed and required to be
    /// non-zero, but bound to no independent fact — see the `members` gap in
    /// this module's documentation.
    pub members: u32,
}

/// A request to materialize an absent catalog.
///
/// A claim, and only a claim: it is exactly as trustworthy as whoever wrote it.
/// [`Self::validate`] proves the claim is canonical and does not contradict
/// itself. Nothing in this crate can prove it was authorized.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogGenesisIntent {
    /// Canonical encoding selector.
    pub schema_version: String,
    /// Digest of the durably accepted request this intent claims to be issued
    /// for.
    pub request_sha256: String,
    /// Canonical writable generation the request claims to have been accepted
    /// under.
    pub generation: String,
    /// Cluster and shard the intent names.
    pub target: GenesisTarget,
    /// Data directory incarnation the intent names.
    pub data_directory: GenesisDataDirectory,
}

/// Why a genesis intent is not canonical.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GenesisIntentError {
    /// Encoding selector is not the supported one.
    #[error("genesis intent schema version is not supported")]
    UnsupportedVersion,
    /// A bounded text field is empty, oversized, or carries control characters.
    #[error("genesis intent text field is not canonical bounded text")]
    UnsupportedText,
    /// Request digest is not lower-case hexadecimal SHA-256.
    #[error("genesis intent request digest is not a canonical SHA-256")]
    UnsupportedDigest,
    /// Generation does not parse, does not round-trip, or names a cluster or
    /// shard other than the target named beside it.
    #[error("genesis intent generation is not canonical for this target")]
    UnsupportedGeneration,
    /// Only shard zero is materializable in this milestone, and a shard with no
    /// members is not a shard.
    #[error("genesis intent names an unsupported target")]
    UnsupportedTarget,
    /// Genesis is timeline one by definition.
    #[error("genesis intent names a data directory that is not at first genesis")]
    UnsupportedDataDirectory,
}

impl CatalogGenesisIntent {
    /// Proves the contract is canonical and internally consistent.
    ///
    /// This is a check of the document against itself and nothing more.
    /// Whoever wrote the intent chose every byte in it, so passing is not
    /// permission to create anything; the module documentation lists the
    /// comparisons that are still owed and by whom.
    ///
    /// # Errors
    ///
    /// Returns the first violated rule.
    pub fn validate(&self) -> Result<(), GenesisIntentError> {
        // Destructured exhaustively: a field added later is a compile error here
        // rather than a value that is silently neither validated nor signed.
        let Self {
            schema_version,
            request_sha256,
            generation,
            target:
                GenesisTarget {
                    cluster_uid,
                    shard,
                    members,
                },
            data_directory:
                GenesisDataDirectory {
                    seed_id,
                    system_identifier,
                    timeline,
                },
        } = self;

        if schema_version != GENESIS_INTENT_VERSION {
            return Err(GenesisIntentError::UnsupportedVersion);
        }
        for text in [cluster_uid, seed_id] {
            validate_text(text)?;
        }
        validate_digest(request_sha256)?;

        // Parsing is not enough: a canonical generation issued for another shard
        // or another cluster parses perfectly, and an intent naming one target
        // while carrying another target's generation contradicts itself.
        let parsed = DurableWritableGeneration::parse_canonical(generation.as_bytes())
            .ok_or(GenesisIntentError::UnsupportedGeneration)?;
        if parsed.canonical_bytes() != generation.as_bytes() {
            return Err(GenesisIntentError::UnsupportedGeneration);
        }
        if parsed.shard_id() != ShardId(*shard) {
            return Err(GenesisIntentError::UnsupportedGeneration);
        }
        // Signing both cluster UIDs binds what each one is, never that they
        // agree. Without this the intent could name cluster B while carrying a
        // generation issued for cluster A, and each half would then be compared
        // against a different live fact.
        if parsed.cluster_uid() != cluster_uid {
            return Err(GenesisIntentError::UnsupportedGeneration);
        }

        if *shard != 0 || *members == 0 {
            return Err(GenesisIntentError::UnsupportedTarget);
        }
        // A zero system identifier is what an unread or zeroed pg_control looks
        // like, and genesis is timeline one by definition.
        if *system_identifier == 0 || *timeline != 1 {
            return Err(GenesisIntentError::UnsupportedDataDirectory);
        }
        Ok(())
    }

    /// Returns the lowercase SHA-256 digest of the validated, fixed-order,
    /// length-framed contract.
    ///
    /// # Errors
    ///
    /// Returns the first violated rule; an invalid contract has no digest.
    pub fn sha256(&self) -> Result<String, GenesisIntentError> {
        self.validate()?;
        let mut hash = Sha256::new();
        frame(&mut hash, GENESIS_INTENT_DIGEST_DOMAIN);
        self.for_each_component(|component| frame(&mut hash, component));
        Ok(lower_hex(&hash.finalize()))
    }

    /// Visits every signed component in fixed order.
    ///
    /// Exhaustively destructured for the same reason as [`Self::validate`]: a
    /// field that is added but not signed is a field an attacker may vary
    /// freely.
    fn for_each_component(&self, mut visit: impl FnMut(&str)) {
        let Self {
            schema_version,
            request_sha256,
            generation,
            target:
                GenesisTarget {
                    cluster_uid,
                    shard,
                    members,
                },
            data_directory:
                GenesisDataDirectory {
                    seed_id,
                    system_identifier,
                    timeline,
                },
        } = self;
        visit(schema_version);
        visit(request_sha256);
        visit(generation);
        visit(cluster_uid);
        visit(&shard.to_string());
        visit(&members.to_string());
        visit(seed_id);
        visit(&system_identifier.to_string());
        visit(&timeline.to_string());
    }
}

/// Rejects anything that is not bounded, non-empty, printable text.
///
/// `char::is_control` is the whole rule: it covers both control ranges,
/// including U+007F. Naming any single character beside it would suggest the
/// category check has gaps and invite a denylist to grow where a category test
/// belongs.
fn validate_text(text: &str) -> Result<(), GenesisIntentError> {
    if text.is_empty() || text.len() > MAXIMUM_TEXT || text.chars().any(char::is_control) {
        return Err(GenesisIntentError::UnsupportedText);
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), GenesisIntentError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(GenesisIntentError::UnsupportedDigest);
    }
    Ok(())
}

fn frame(hash: &mut Sha256, value: &str) {
    hash.update(
        u64::try_from(value.len())
            .expect("bounded genesis intent component length fits u64")
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

/// Carries a `u64` as decimal text rather than as a JSON number.
///
/// JSON numbers are IEEE 754 doubles to JavaScript and to `jq`'s default
/// parser, and a `pg_control` system identifier is a `uint64` built from a
/// timestamp and a process ID, so it sits above 2^53 as a matter of course.
/// Round-tripping such a document through either silently rewrites the one
/// field whose whole purpose is to name a specific incarnation, and a fence
/// that changes value in transit is worse than no fence. Text has no such
/// range. The catalog activation CRD already spells `systemIdentifier` this
/// way, so this is the repository's existing convention rather than a new one.
mod canonical_decimal_u64 {
    use serde::{Deserialize as _, Deserializer, Serializer, de::Error as _};

    #[allow(clippy::trivially_copy_pass_by_ref)] // serde fixes this signature.
    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    /// Accepts only the text `u64::to_string` would have produced.
    ///
    /// Round-tripping the parse is the whole rule: it admits exactly one
    /// spelling per value, so a bare JSON number, a sign, padding, whitespace,
    /// a digit separator, an overflowing value and an empty string are all
    /// refused without being enumerated. Enumerating them would suggest the
    /// rule has gaps.
    ///
    /// # Errors
    ///
    /// Returns a deserialization error for any other text, and for any JSON
    /// value that is not a string. That is deliberately a different failure
    /// from the zero-identifier rule in [`super::CatalogGenesisIntent::validate`],
    /// which runs afterwards and still owns it: `"0"` parses here and is
    /// refused there.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        let value = text
            .parse::<u64>()
            .map_err(|_| D::Error::custom("expected a canonical decimal u64 string"))?;
        if value.to_string() != text {
            return Err(D::Error::custom("expected a canonical decimal u64 string"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLUSTER_UID: &str = "11111111-2222-3333-4444-555555555555";
    const OTHER_CLUSTER_UID: &str = "99999999-2222-3333-4444-555555555555";
    const SEED_ID: &str = "01hzy4t7q2n8v6c3b9k5m0xw1d";
    const SYSTEM_IDENTIFIER: u64 = 7_248_119_402_113_558_016;

    fn digest(seed: u8) -> String {
        (0..32).fold(String::new(), |mut text, index| {
            use std::fmt::Write as _;
            let _ = write!(text, "{:02x}", seed.wrapping_add(index));
            text
        })
    }

    /// A real canonical generation, rendered by the type that defines what
    /// canonical means. Inventing the text here is what an earlier fixture did,
    /// and it hid a validator that rejected every real value.
    fn generation(cluster_uid: &str, shard: u32, term: u64) -> String {
        let generation = DurableWritableGeneration::new(
            "demo".to_owned(),
            cluster_uid.to_owned(),
            ShardId(shard),
            "database".to_owned(),
            format!("demo-shard-{shard:04}-writable"),
            "dddddddd-1111-2222-3333-444444444444".to_owned(),
            format!("demo-shard-{shard:04}-0"),
            term,
        )
        .expect("the fixture generation is valid");
        String::from_utf8(generation.canonical_bytes()).expect("canonical bytes are UTF-8")
    }

    fn intent() -> CatalogGenesisIntent {
        CatalogGenesisIntent {
            schema_version: GENESIS_INTENT_VERSION.to_owned(),
            request_sha256: digest(1),
            generation: generation(CLUSTER_UID, 0, 7),
            target: GenesisTarget {
                cluster_uid: CLUSTER_UID.to_owned(),
                shard: 0,
                members: 1,
            },
            data_directory: GenesisDataDirectory {
                seed_id: SEED_ID.to_owned(),
                system_identifier: SYSTEM_IDENTIFIER,
                timeline: 1,
            },
        }
    }

    /// Pins the limit of what this module decides. Every field here is chosen
    /// by whoever wanted genesis to happen — sixty-four zeroes for the request
    /// digest, a publicly constructible generation, a made-up seed — and it
    /// passes, because canonical form is a statement about spelling. Anything
    /// that reads a passing `validate` as permission is reading a forgery as
    /// permission.
    #[test]
    fn a_wholly_self_chosen_intent_is_canonical_because_form_is_not_authority() {
        let self_chosen = CatalogGenesisIntent {
            schema_version: GENESIS_INTENT_VERSION.to_owned(),
            request_sha256: "0".repeat(64),
            generation: generation(OTHER_CLUSTER_UID, 0, 1),
            target: GenesisTarget {
                cluster_uid: OTHER_CLUSTER_UID.to_owned(),
                shard: 0,
                members: 1,
            },
            data_directory: GenesisDataDirectory {
                seed_id: "forged-seed".to_owned(),
                system_identifier: 1,
                timeline: 1,
            },
        };
        assert_eq!(self_chosen.validate(), Ok(()));
        assert!(self_chosen.sha256().is_ok());
    }

    #[test]
    fn a_non_canonical_intent_is_never_validated_or_hashed() {
        assert_eq!(
            intent().validate(),
            Ok(()),
            "the fixture has to be canonical, or every rejection below proves nothing"
        );

        for (expected, mutate) in [
            (
                GenesisIntentError::UnsupportedVersion,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.schema_version = "pgshard.catalog-genesis-intent.v2".to_owned();
                }) as Box<dyn Fn(&mut CatalogGenesisIntent)>,
            ),
            (
                GenesisIntentError::UnsupportedText,
                Box::new(|it: &mut CatalogGenesisIntent| it.target.cluster_uid = String::new()),
            ),
            (
                GenesisIntentError::UnsupportedText,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.seed_id = "s".repeat(MAXIMUM_TEXT + 1);
                }),
            ),
            (
                GenesisIntentError::UnsupportedText,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.seed_id = "seed\u{1}id".to_owned();
                }),
            ),
            (
                // U+007F is a control character, so the category test is the
                // whole rule and no character needs naming beside it.
                GenesisIntentError::UnsupportedText,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.seed_id = "seed\u{7f}id".to_owned();
                }),
            ),
            (
                GenesisIntentError::UnsupportedText,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.seed_id = "seed\u{9f}id".to_owned();
                }),
            ),
            (
                GenesisIntentError::UnsupportedDigest,
                Box::new(|it: &mut CatalogGenesisIntent| it.request_sha256.truncate(63)),
            ),
            (
                GenesisIntentError::UnsupportedDigest,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.request_sha256 = digest(1).to_uppercase();
                }),
            ),
            (
                GenesisIntentError::UnsupportedDigest,
                Box::new(|it: &mut CatalogGenesisIntent| it.request_sha256 = "g".repeat(64)),
            ),
            (
                GenesisIntentError::UnsupportedGeneration,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.generation = "cluster-1:holder-a:7".to_owned();
                }),
            ),
            (
                GenesisIntentError::UnsupportedTarget,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    // The generation moves with the shard, so the intent fails
                    // on the target rather than on a generation mismatch.
                    it.target.shard = 1;
                    it.generation = generation(CLUSTER_UID, 1, 7);
                }),
            ),
            (
                GenesisIntentError::UnsupportedTarget,
                Box::new(|it: &mut CatalogGenesisIntent| it.target.members = 0),
            ),
            (
                GenesisIntentError::UnsupportedDataDirectory,
                Box::new(|it: &mut CatalogGenesisIntent| {
                    it.data_directory.system_identifier = 0;
                }),
            ),
            (
                GenesisIntentError::UnsupportedDataDirectory,
                Box::new(|it: &mut CatalogGenesisIntent| it.data_directory.timeline = 2),
            ),
        ] {
            let mut mutated = intent();
            mutate(&mut mutated);
            assert_eq!(mutated.validate(), Err(expected));
            assert_eq!(
                mutated.sha256(),
                Err(expected),
                "a non-canonical intent was hashed anyway"
            );
        }
    }

    /// A canonical generation for another shard round-trips perfectly, so
    /// parsing proves nothing about which shard it names. Without the
    /// comparison this intent would describe two different shards at once.
    #[test]
    fn a_generation_for_another_shard_is_rejected() {
        let other_shard = generation(CLUSTER_UID, 3, 7);
        assert_eq!(
            DurableWritableGeneration::parse_canonical(other_shard.as_bytes())
                .expect("a shard-three generation is canonical on its own")
                .canonical_bytes(),
            other_shard.as_bytes(),
            "the fixture has to round-trip, or it proves nothing",
        );

        let mut mismatched = intent();
        mismatched.generation = other_shard;
        assert_eq!(
            mismatched.validate(),
            Err(GenesisIntentError::UnsupportedGeneration),
            "an intent for shard zero accepted another shard's generation"
        );
        assert_eq!(
            mismatched.sha256(),
            Err(GenesisIntentError::UnsupportedGeneration),
            "a mismatched intent was hashed anyway"
        );
    }

    /// A canonical generation issued for another cluster round-trips just as
    /// perfectly, and its shard agrees with the target, so every other rule
    /// passes. Signing both cluster UIDs binds what each one is and never that
    /// they name the same cluster, so they are compared.
    #[test]
    fn a_generation_for_another_cluster_is_rejected() {
        let other_cluster = generation(OTHER_CLUSTER_UID, 0, 7);
        let parsed = DurableWritableGeneration::parse_canonical(other_cluster.as_bytes())
            .expect("another cluster's generation is canonical on its own");
        assert_eq!(
            parsed.canonical_bytes(),
            other_cluster.as_bytes(),
            "the fixture has to round-trip, or it proves nothing",
        );
        assert_eq!(
            parsed.shard_id(),
            ShardId(0),
            "the fixture has to agree on the shard, or the shard check is what rejects it",
        );

        let mut mismatched = intent();
        mismatched.generation = other_cluster;
        assert_ne!(
            mismatched.target.cluster_uid, OTHER_CLUSTER_UID,
            "the target has to keep naming the original cluster"
        );
        assert_eq!(
            mismatched.validate(),
            Err(GenesisIntentError::UnsupportedGeneration),
            "an intent for one cluster accepted another cluster's generation"
        );
        assert_eq!(
            mismatched.sha256(),
            Err(GenesisIntentError::UnsupportedGeneration),
            "a mismatched intent was hashed anyway"
        );
    }

    /// `shard` and `schema_version` are pinned by validation, so they cannot be
    /// varied through `sha256`. Pinning the component list is what proves they
    /// are signed at all.
    #[test]
    fn every_field_is_signed_in_a_fixed_order() {
        let intent = intent();
        let mut components = Vec::new();
        intent.for_each_component(|component| components.push(component.to_owned()));
        assert_eq!(
            components,
            vec![
                intent.schema_version.clone(),
                intent.request_sha256.clone(),
                intent.generation.clone(),
                intent.target.cluster_uid.clone(),
                intent.target.shard.to_string(),
                intent.target.members.to_string(),
                intent.data_directory.seed_id.clone(),
                intent.data_directory.system_identifier.to_string(),
                intent.data_directory.timeline.to_string(),
            ]
        );
    }

    /// A field that is bound but not signed is a field that may be varied
    /// freely, so every one that validation permits to vary has to move the
    /// digest. The cluster UID may no longer vary on its own — validation pins
    /// it to the generation — so the pair moves together.
    #[test]
    fn the_digest_is_sensitive_to_every_variable_binding() {
        let baseline = intent().sha256().expect("the fixture is canonical");
        assert_eq!(
            baseline,
            intent().sha256().expect("the fixture is canonical"),
            "the digest is unstable for an unchanged value"
        );

        let mut mutations: Vec<(&str, CatalogGenesisIntent)> = Vec::new();
        let mut it = intent();
        it.request_sha256 = digest(9);
        mutations.push(("request", it));
        let mut it = intent();
        it.generation = generation(CLUSTER_UID, 0, 8);
        mutations.push(("generation term", it));
        let mut it = intent();
        it.target.cluster_uid = OTHER_CLUSTER_UID.to_owned();
        it.generation = generation(OTHER_CLUSTER_UID, 0, 7);
        mutations.push(("cluster", it));
        let mut it = intent();
        it.target.members = 3;
        mutations.push(("members", it));
        let mut it = intent();
        it.data_directory.seed_id = "01hzy4t7q2n8v6c3b9k5m0xw1e".to_owned();
        mutations.push(("seed", it));
        let mut it = intent();
        it.data_directory.system_identifier = 1;
        mutations.push(("system identifier", it));

        for (binding, mutated) in mutations {
            let moved = mutated.sha256().expect("the mutation stays canonical");
            assert_ne!(baseline, moved, "the digest ignores the {binding} binding");
        }
    }

    /// Length framing, so two adjacent components cannot be re-split between
    /// each other to produce the same bytes.
    #[test]
    fn adjacent_bindings_cannot_be_reflowed_into_each_other() {
        let mut left = intent();
        left.target.members = 10;
        left.data_directory.seed_id = "abc".to_owned();
        let mut right = intent();
        right.target.members = 1;
        right.data_directory.seed_id = "0abc".to_owned();
        assert_ne!(
            left.sha256().expect("canonical"),
            right.sha256().expect("canonical"),
            "moving a byte across a component boundary produced the same digest"
        );
    }

    /// Pinned rather than merely self-consistent: the digest is what a later
    /// stage will authenticate against a durable record, so reframing it
    /// silently would strand every intent already recorded under the old
    /// encoding.
    #[test]
    fn the_canonical_vector_is_pinned() {
        assert_eq!(
            intent().sha256().expect("the fixture is canonical"),
            "d456d1913594700ad2fdfb0f7ba3eb06f6f0706f954286da2b16fc2c42f702ad",
            "the canonical digest changed; every recorded intent digest moved with it"
        );
    }

    /// The digest is taken over the fields, so renaming one moves the wire form
    /// while `the_canonical_vector_is_pinned` keeps passing and every recorded
    /// intent stops deserializing. Pinning the JSON byte for byte is what makes
    /// a rename — `clusterUid` to `clusterUID`, say — announce itself as the
    /// breaking encoding change it is. `deny_unknown_fields` is pinned beside
    /// it, so a field this version cannot see is refused rather than dropped.
    #[test]
    fn the_wire_form_is_pinned_and_refuses_unknown_fields() {
        let expected = concat!(
            "{\"schemaVersion\":\"pgshard.catalog-genesis-intent.v1\",",
            "\"requestSha256\":\"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20\",",
            "\"generation\":\"format=1\\ncluster_name=demo\\n",
            "cluster_uid=11111111-2222-3333-4444-555555555555\\nshard=0\\n",
            "lease_namespace=database\\nlease_name=demo-shard-0000-writable\\n",
            "lease_uid=dddddddd-1111-2222-3333-444444444444\\n",
            "holder=demo-shard-0000-0\\nterm=7\\n\",",
            "\"target\":{\"clusterUid\":\"11111111-2222-3333-4444-555555555555\",",
            "\"shard\":0,\"members\":1},",
            "\"dataDirectory\":{\"seedId\":\"01hzy4t7q2n8v6c3b9k5m0xw1d\",",
            "\"systemIdentifier\":\"7248119402113558016\",\"timeline\":1}}"
        );
        let encoded = serde_json::to_string(&intent()).expect("serialize intent");
        assert_eq!(
            encoded, expected,
            "the wire form changed; every recorded intent stopped deserializing"
        );
        assert_eq!(
            serde_json::from_str::<CatalogGenesisIntent>(&encoded).expect("deserialize intent"),
            intent()
        );

        for with_unknown in [
            format!(
                "{},\"unexpected\":true}}",
                encoded.strip_suffix('}').expect("object JSON")
            ),
            encoded.replace(
                "\"dataDirectory\":{",
                "\"dataDirectory\":{\"unexpected\":true,",
            ),
            encoded.replace("\"target\":{", "\"target\":{\"unexpected\":true,"),
        ] {
            let error = serde_json::from_str::<CatalogGenesisIntent>(&with_unknown)
                .expect_err("an unknown field must be refused, not dropped");
            assert!(
                error.to_string().contains("unknown field"),
                "unexpected rejection reason: {error}"
            );
        }
    }

    /// A `pg_control` system identifier is a `uint64` built from a timestamp
    /// and a process ID, so it lives above 2^53 where a JSON number stops being
    /// exact: JavaScript and `jq`'s default parser both round it, and the field
    /// whose whole job is to name one incarnation would come out naming a
    /// different one. It crosses the wire as text, and only the text
    /// `u64::to_string` would have produced is accepted back — a bare number is
    /// refused rather than coerced, so the hazard cannot return through
    /// deserialization either.
    #[test]
    fn the_system_identifier_crosses_the_wire_as_canonical_decimal_text() {
        const {
            assert!(
                SYSTEM_IDENTIFIER > (1 << 53),
                "the fixture has to exceed exact JSON number range, or this proves nothing"
            );
        }

        let encoded = serde_json::to_string(&intent()).expect("serialize intent");
        let quoted = "\"7248119402113558016\"";
        assert!(
            encoded.contains(&format!("\"systemIdentifier\":{quoted}")),
            "the identifier is not decimal text: {encoded}"
        );
        assert_eq!(
            serde_json::from_str::<CatalogGenesisIntent>(&encoded)
                .expect("deserialize intent")
                .data_directory
                .system_identifier,
            SYSTEM_IDENTIFIER,
            "the identifier did not survive the round trip exactly"
        );

        for rejected in [
            "7248119402113558016",
            "7.248119402113558e18",
            "\"007248119402113558016\"",
            "\"+7248119402113558016\"",
            "\"-1\"",
            "\" 7248119402113558016\"",
            "\"7248119402113558016 \"",
            "\"7_248_119_402_113_558_016\"",
            "\"7248119402113558016.0\"",
            "\"18446744073709551616\"",
            "\"0x64\"",
            "\"\"",
            "null",
        ] {
            let document = encoded.replace(quoted, rejected);
            assert_ne!(
                document, encoded,
                "the {rejected} case never reached the parser"
            );
            assert!(
                serde_json::from_str::<CatalogGenesisIntent>(&document).is_err(),
                "a noncanonical system identifier was accepted: {rejected}"
            );
        }

        // Zero is canonical decimal text, so it parses and is then refused by
        // the rule that owns it. Neither failure hides behind the other.
        assert_eq!(
            serde_json::from_str::<CatalogGenesisIntent>(&encoded.replace(quoted, "\"0\""))
                .expect("zero is canonical decimal text")
                .validate(),
            Err(GenesisIntentError::UnsupportedDataDirectory),
            "a zero identifier has to be refused by validation, not by the parser"
        );
    }
}
