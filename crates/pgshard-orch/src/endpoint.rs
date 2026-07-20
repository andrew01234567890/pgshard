//! Portable credential-free HTTP(S) endpoint grammar.

pub(crate) const MAXIMUM_HTTP_ENDPOINT_BYTES: usize = 2_048;

pub(crate) fn valid_credential_free_http_endpoint(value: &str) -> bool {
    if value.is_empty() || value.len() > MAXIMUM_HTTP_ENDPOINT_BYTES {
        return false;
    }
    if value
        .bytes()
        .any(|byte| byte <= 0x20 || byte >= 0x7f || byte == b'\\')
        || value.bytes().any(|byte| matches!(byte, b'@' | b'?' | b'#'))
    {
        return false;
    }
    let remainder = value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"));
    let Some(remainder) = remainder else {
        return false;
    };
    let (authority, path) = remainder.find('/').map_or((remainder, ""), |index| {
        (&remainder[..index], &remainder[index..])
    });
    valid_authority(authority) && valid_path(path)
}

fn valid_authority(authority: &str) -> bool {
    if authority.is_empty()
        || authority.bytes().any(|byte| matches!(byte, b'[' | b']'))
        || authority.bytes().filter(|byte| *byte == b':').count() > 1
    {
        return false;
    }
    let (host, port) = authority
        .rsplit_once(':')
        .map_or((authority, None), |(host, port)| (host, Some(port)));
    if port.is_some_and(|port| !valid_decimal(port, 65_535, false))
        || host.is_empty()
        || host.len() > 253
    {
        return false;
    }
    if host
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        let mut parts = host.split('.');
        return (0..4).all(|_| {
            parts
                .next()
                .is_some_and(|part| valid_decimal(part, 255, true))
        }) && parts.next().is_none();
    }
    let final_label = host.rsplit('.').next().expect("nonempty host");
    !whatwg_ipv4_number_spelling(final_label) && host.split('.').all(valid_dns_label)
}

fn whatwg_ipv4_number_spelling(label: &str) -> bool {
    if let Some(hexadecimal) = label.strip_prefix("0x") {
        return hexadecimal.bytes().all(|byte| byte.is_ascii_hexdigit());
    }
    label.bytes().all(|byte| byte.is_ascii_digit())
}

fn valid_dns_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 63
        && !label
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("xn--"))
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_decimal(value: &str, maximum: u32, allow_zero: bool) -> bool {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return false;
    }
    let mut parsed = 0_u32;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            return false;
        }
        parsed = parsed * 10 + u32::from(byte - b'0');
        if parsed > maximum {
            return false;
        }
    }
    allow_zero || parsed > 0
}

fn valid_path(path: &str) -> bool {
    if path.is_empty() || path == "/" {
        return true;
    }
    if !path.starts_with('/') || path.ends_with('/') {
        return false;
    }
    path[1..].split('/').all(valid_path_segment)
}

fn valid_path_segment(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            if !unreserved(bytes[index]) {
                return false;
            }
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return false;
        }
        let Some(high) = hexadecimal_nibble(bytes[index + 1]) else {
            return false;
        };
        let Some(low) = hexadecimal_nibble(bytes[index + 2]) else {
            return false;
        };
        let byte = high << 4 | low;
        if !unreserved(byte) {
            return false;
        }
        decoded.push(byte);
        index += 3;
    }
    decoded != b"." && decoded != b".."
}

const fn unreserved(byte: u8) -> bool {
    byte.is_ascii_lowercase()
        || byte.is_ascii_uppercase()
        || byte.is_ascii_digit()
        || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

const fn hexadecimal_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct EndpointFixture {
        version: String,
        maximum_length: usize,
        cases: Vec<EndpointCase>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct EndpointCase {
        name: String,
        value: String,
        valid: bool,
    }

    #[test]
    fn matches_the_shared_go_and_rust_golden_contract() {
        let fixture: EndpointFixture =
            serde_json::from_str(include_str!("../../../contracts/http-endpoints-v1.json"))
                .expect("valid endpoint fixture");
        assert_eq!(fixture.version, "pgshard.http-endpoint.v1");
        assert_eq!(fixture.maximum_length, MAXIMUM_HTTP_ENDPOINT_BYTES);
        assert!(!fixture.cases.is_empty());
        for test in fixture.cases {
            assert_eq!(
                valid_credential_free_http_endpoint(&test.value),
                test.valid,
                "{}: {:?}",
                test.name,
                test.value
            );
        }
    }

    #[test]
    fn enforces_the_exact_maximum_length() {
        let prefix = "https://collector.example.invalid/";
        let maximum = format!(
            "{prefix}{}",
            "x".repeat(MAXIMUM_HTTP_ENDPOINT_BYTES - prefix.len())
        );
        assert!(valid_credential_free_http_endpoint(&maximum));
        assert!(!valid_credential_free_http_endpoint(&(maximum + "x")));
    }
}
