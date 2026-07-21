//! Shared catalog-material fingerprint contract.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// Domain separating the pooler's catalog password and CA fingerprint.
pub const CATALOG_CLIENT_DIGEST_DOMAIN: &str = "pgshard-catalog-client-v1";

/// Domain separating the shard-zero catalog certificate fingerprint.
pub const CATALOG_SERVER_DIGEST_DOMAIN: &str = "pgshard-catalog-server-v1";

/// Domain separating the orchestrator's operation-writer password and CA.
pub const OPERATION_WRITER_DIGEST_DOMAIN: &str = "pgshard-operation-writer-client-v1";

/// Domain separating the per-shard physical-replication password fingerprint.
pub const POSTGRESQL_REPLICATION_DIGEST_DOMAIN: &str = "pgshard-postgresql-replication-v1";

/// Computes the canonical length-framed HMAC-SHA-256 material fingerprint.
///
/// # Panics
///
/// Panics only if HMAC-SHA-256 rejects a key (it accepts keys of every length)
/// or a component length does not fit the contract's `u64` frame.
#[must_use]
pub fn catalog_material_sha256<'a>(
    domain: &str,
    key: &[u8],
    values: impl IntoIterator<Item = &'a [u8]>,
) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any length");
    update_mac(&mut mac, domain.as_bytes());
    for component in values {
        update_mac(&mut mac, component);
    }
    lower_hex(&mac.finalize().into_bytes())
}

fn update_mac(mac: &mut Hmac<Sha256>, component: &[u8]) {
    mac.update(
        &u64::try_from(component.len())
            .expect("catalog material component length fits u64")
            .to_be_bytes(),
    );
    mac.update(component);
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn material_fingerprint_is_domain_separated_and_length_framed() {
        let client =
            catalog_material_sha256(CATALOG_CLIENT_DIGEST_DOMAIN, b"", [&b"catalog-ca"[..]]);
        assert_eq!(
            client,
            "f25d89531a7aa9937005eb56aab838662145cadff1315196229e0cd334ece559"
        );
        assert_ne!(
            client,
            catalog_material_sha256(CATALOG_SERVER_DIGEST_DOMAIN, b"", [&b"catalog-ca"[..]],)
        );
        assert_eq!(
            catalog_material_sha256(
                POSTGRESQL_REPLICATION_DIGEST_DOMAIN,
                b"password",
                std::iter::empty(),
            ),
            "f28e708e623164f153012f8f21e13d4bbd3ad2de150d3181b69316275bb49f7e"
        );
        assert_eq!(
            catalog_material_sha256(
                OPERATION_WRITER_DIGEST_DOMAIN,
                b"writer-password",
                [&b"catalog-ca"[..]],
            ),
            "62592029f6dfabdf02e2ad5cdcd3f030107f69decfd7363a44efd61d7e6597ee"
        );
        assert_ne!(
            client,
            catalog_material_sha256(
                POSTGRESQL_REPLICATION_DIGEST_DOMAIN,
                b"",
                [&b"catalog-ca"[..]],
            )
        );
        assert_ne!(
            catalog_material_sha256("ab", b"key", [&b"c"[..]]),
            catalog_material_sha256("a", b"key", [&b"bc"[..]])
        );
    }
}
