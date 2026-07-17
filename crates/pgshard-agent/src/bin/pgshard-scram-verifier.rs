//! Internal `PostgreSQL` SCRAM verifier generator.

use std::io::{self, Read, Write};

const CATALOG_PASSWORD_LENGTH: usize = 64;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut password = Vec::with_capacity(CATALOG_PASSWORD_LENGTH + 1);
    io::stdin()
        .take((CATALOG_PASSWORD_LENGTH + 1) as u64)
        .read_to_end(&mut password)?;
    if password.len() != CATALOG_PASSWORD_LENGTH
        || !password
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("catalog password must be exactly 64 lowercase hexadecimal bytes".into());
    }

    let verifier = postgres_protocol::password::scram_sha_256(&password);
    let mut stdout = io::stdout().lock();
    stdout.write_all(verifier.as_bytes())?;
    stdout.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_shape_matches_operator_generated_catalog_credentials() {
        let password = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(password.len(), CATALOG_PASSWORD_LENGTH);
        let verifier = postgres_protocol::password::scram_sha_256(password);
        assert!(verifier.starts_with("SCRAM-SHA-256$4096:"));
        assert_eq!(verifier.matches('$').count(), 2);
        assert!(!verifier.contains(std::str::from_utf8(password).expect("ASCII password")));
    }
}
