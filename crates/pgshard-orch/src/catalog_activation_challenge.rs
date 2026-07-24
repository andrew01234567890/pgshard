//! Bounded, server-authenticated probe of the dormant activation consumer.
//!
//! This client has no Kubernetes mutation handle and no catalog credential. It
//! connects directly to the selected target agent while authenticating the
//! catalog-service DNS identity from one projected CA certificate.

use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pgshard_types::catalog_activation::{
    CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_REQUEST_VERSION, CatalogActivationCapabilityCarrier,
    CatalogActivationCapabilityChallenge, CatalogActivationCapabilityChallengeError,
    CatalogActivationCapabilityChallengeResponse, CatalogActivationCapabilityCluster,
    CatalogActivationCapabilityTarget, postgresql_member_pod_name,
};
use rustix::fs::{Mode, OFlags};
use rustls::client::Resumption;
use rustls::{ClientConfig, ProtocolVersion, RootCertStore};
use rustls_pki_types::{CertificateDer, ServerName, pem::PemObject};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::catalog_materialization::PreparedCatalogActivationRequest;

const CATALOG_ACTIVATION_TLS_PORT: u16 = 8_443;
const CATALOG_ACTIVATION_CAPABILITY_PATH: &str = "/capabilities/catalog-activation";
const CATALOG_ACTIVATION_CA_FILE: &str = "/etc/pgshard/catalog-activation/ca.crt";
const MAXIMUM_CA_BYTES: usize = 64 * 1_024;
const MAXIMUM_BODY_BYTES: usize = 4 * 1_024;
const MAXIMUM_HEADER_BYTES: usize = 8 * 1_024;
const MAXIMUM_RESPONSE_BYTES: usize = MAXIMUM_HEADER_BYTES + MAXIMUM_BODY_BYTES + 4;
const MINIMUM_TIMEOUT: Duration = Duration::from_millis(100);
const MAXIMUM_TIMEOUT: Duration = Duration::from_secs(5);

/// Exact identities expected from the selected activation consumer.
#[allow(dead_code)] // Input to the deliberately uncomposed publisher client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExpectedCatalogActivationCapability {
    pub(crate) cluster: CatalogActivationCapabilityCluster,
    pub(crate) carrier: CatalogActivationCapabilityCarrier,
    pub(crate) target: CatalogActivationCapabilityTarget,
}

#[allow(dead_code)] // Composed only after the runtime supervisor is reviewed.
impl ExpectedCatalogActivationCapability {
    /// Derives the challenge identity solely from the exact prepared request.
    /// This prevents the network target and challenge body from being assembled
    /// from a second, weaker identity source.
    pub(crate) fn from_prepared(
        prepared: &PreparedCatalogActivationRequest,
    ) -> Result<Self, CatalogActivationChallengeError> {
        let request = prepared.request();
        if request.source.shard != 0
            || request.source.member != 0
            || request.source.instance_id != request.source.pod_name
            || request.sha256().as_deref() != Ok(prepared.sha256())
        {
            return Err(CatalogActivationChallengeError::InvalidTarget);
        }
        request
            .validate()
            .map_err(|_| CatalogActivationChallengeError::InvalidTarget)?;
        Ok(Self {
            cluster: CatalogActivationCapabilityCluster {
                name: request.cluster.name.clone(),
                uid: request.cluster.uid.clone(),
            },
            carrier: CatalogActivationCapabilityCarrier {
                namespace: request.cluster.namespace.clone(),
                name: request.carrier.name.clone(),
                uid: request.carrier.uid.clone(),
            },
            target: CatalogActivationCapabilityTarget {
                shard: request.source.shard,
                member: request.source.member,
                instance_id: request.source.instance_id.clone(),
                pod_name: request.source.pod_name.clone(),
                pod_uid: request.source.pod_uid.clone(),
            },
        })
    }
}

/// TLS 1.3 and HTTP/1.1 client with one explicit CA and no ambient identity.
#[allow(dead_code)] // Deliberately inert until publisher composition is reviewed.
pub(crate) struct CatalogActivationChallengeClient {
    tls: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    http_host: String,
    cluster_name: String,
    namespace: String,
    timeout: Duration,
}

#[allow(dead_code)] // Deliberately inert until publisher composition is reviewed.
impl CatalogActivationChallengeClient {
    /// Loads one exact CA-only trust anchor and fixes catalog-service SNI.
    pub(crate) fn new(
        cluster_name: &str,
        namespace: &str,
        timeout: Duration,
    ) -> Result<Self, CatalogActivationChallengeError> {
        if !valid_dns_name(cluster_name)
            || !valid_dns_name(namespace)
            || !(MINIMUM_TIMEOUT..=MAXIMUM_TIMEOUT).contains(&timeout)
        {
            return Err(CatalogActivationChallengeError::InvalidConfiguration);
        }
        let http_host = format!("{cluster_name}-shardschema.{namespace}.svc");
        if !valid_dns_name(&http_host) {
            return Err(CatalogActivationChallengeError::InvalidConfiguration);
        }
        let server_name = ServerName::try_from(http_host.clone())
            .map_err(|_| CatalogActivationChallengeError::InvalidConfiguration)?;
        let ca_path = PathBuf::from(CATALOG_ACTIVATION_CA_FILE);
        debug_assert!(absolute_normal_path(&ca_path));
        let ca = read_ca_file(&ca_path)?;
        let tls = client_config_from_ca(ca)?;
        Ok(Self {
            tls,
            server_name,
            http_host,
            cluster_name: cluster_name.to_owned(),
            namespace: namespace.to_owned(),
            timeout,
        })
    }

    /// Generates a fresh nonce and performs one finite, non-retried challenge.
    pub(crate) async fn challenge(
        &self,
        target_agent_dns: &str,
        prepared: &PreparedCatalogActivationRequest,
    ) -> Result<CatalogActivationCapabilityChallengeResponse, CatalogActivationChallengeError> {
        let expected = ExpectedCatalogActivationCapability::from_prepared(prepared)?;
        if !valid_dns_name(target_agent_dns)
            || expected.cluster.name != self.cluster_name
            || expected.carrier.namespace != self.namespace
            || expected.carrier.name != format!("{}-catalog-activation", self.cluster_name)
            || expected.target.instance_id != expected.target.pod_name
            || !target_dns_matches(
                target_agent_dns,
                &expected.target.pod_name,
                &self.cluster_name,
                &self.namespace,
            )
        {
            return Err(CatalogActivationChallengeError::InvalidTarget);
        }
        let challenge = CatalogActivationCapabilityChallenge {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_REQUEST_VERSION.to_owned(),
            nonce: fresh_nonce()?,
            request_sha256: prepared.sha256().to_owned(),
            cluster: expected.cluster,
            carrier: expected.carrier,
            target: expected.target,
        };
        challenge
            .validate()
            .map_err(CatalogActivationChallengeError::InvalidChallenge)?;
        tokio::time::timeout(self.timeout, self.exchange(target_agent_dns, &challenge))
            .await
            .map_err(|_| CatalogActivationChallengeError::TimedOut)?
    }

    async fn exchange(
        &self,
        target_agent_dns: &str,
        challenge: &CatalogActivationCapabilityChallenge,
    ) -> Result<CatalogActivationCapabilityChallengeResponse, CatalogActivationChallengeError> {
        let tcp = TcpStream::connect((target_agent_dns, CATALOG_ACTIVATION_TLS_PORT))
            .await
            .map_err(CatalogActivationChallengeError::Connect)?;
        self.exchange_tcp(tcp, challenge).await
    }

    async fn exchange_tcp(
        &self,
        tcp: TcpStream,
        challenge: &CatalogActivationCapabilityChallenge,
    ) -> Result<CatalogActivationCapabilityChallengeResponse, CatalogActivationChallengeError> {
        let connector = TlsConnector::from(Arc::clone(&self.tls));
        let mut tls = connector
            .connect(self.server_name.clone(), tcp)
            .await
            .map_err(CatalogActivationChallengeError::Tls)?;
        if tls.get_ref().1.protocol_version() != Some(ProtocolVersion::TLSv1_3)
            || tls.get_ref().1.alpn_protocol() != Some(b"http/1.1".as_slice())
        {
            return Err(CatalogActivationChallengeError::NegotiatedProtocol);
        }
        let body = serde_json::to_vec(challenge)
            .map_err(CatalogActivationChallengeError::SerializeChallenge)?;
        if body.is_empty() || body.len() > MAXIMUM_BODY_BYTES {
            return Err(CatalogActivationChallengeError::InvalidChallengeBody);
        }
        let request = format!(
            "POST {CATALOG_ACTIVATION_CAPABILITY_PATH} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.http_host,
            body.len()
        );
        if request.len() > MAXIMUM_HEADER_BYTES {
            return Err(CatalogActivationChallengeError::InvalidChallengeBody);
        }
        tls.write_all(request.as_bytes())
            .await
            .map_err(CatalogActivationChallengeError::Write)?;
        tls.write_all(&body)
            .await
            .map_err(CatalogActivationChallengeError::Write)?;
        tls.flush()
            .await
            .map_err(CatalogActivationChallengeError::Write)?;

        let mut response = Vec::with_capacity(MAXIMUM_RESPONSE_BYTES.min(4 * 1_024));
        tls.take((MAXIMUM_RESPONSE_BYTES + 1) as u64)
            .read_to_end(&mut response)
            .await
            .map_err(CatalogActivationChallengeError::Read)?;
        if response.len() > MAXIMUM_RESPONSE_BYTES {
            return Err(CatalogActivationChallengeError::ResponseTooLarge);
        }
        parse_response(&response, challenge)
    }
}

fn client_config_from_ca(
    ca: CertificateDer<'static>,
) -> Result<Arc<ClientConfig>, CatalogActivationChallengeError> {
    let mut roots = RootCertStore::empty();
    roots
        .add(ca)
        .map_err(|_| CatalogActivationChallengeError::InvalidCA)?;
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(CatalogActivationChallengeError::TlsConfiguration)?
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config.enable_early_data = false;
    config.resumption = Resumption::disabled();
    Ok(Arc::new(config))
}

fn read_ca_file(path: &Path) -> Result<CertificateDer<'static>, CatalogActivationChallengeError> {
    let metadata = fs::metadata(path).map_err(CatalogActivationChallengeError::ReadCA)?;
    if !metadata.file_type().is_file() {
        return Err(CatalogActivationChallengeError::CAIsNotRegularFile);
    }
    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
        Mode::empty(),
    )
    .map_err(|source| CatalogActivationChallengeError::ReadCA(source.into()))?;
    let file = File::from(descriptor);
    if !file
        .metadata()
        .map_err(CatalogActivationChallengeError::ReadCA)?
        .file_type()
        .is_file()
    {
        return Err(CatalogActivationChallengeError::CAIsNotRegularFile);
    }
    let mut contents = Vec::with_capacity(MAXIMUM_CA_BYTES + 1);
    file.take((MAXIMUM_CA_BYTES + 1) as u64)
        .read_to_end(&mut contents)
        .map_err(CatalogActivationChallengeError::ReadCA)?;
    if contents.len() > MAXIMUM_CA_BYTES {
        return Err(CatalogActivationChallengeError::CATooLarge);
    }
    parse_ca(&contents)
}

fn parse_ca(contents: &[u8]) -> Result<CertificateDer<'static>, CatalogActivationChallengeError> {
    if !contents.starts_with(b"-----BEGIN CERTIFICATE-----\n") {
        return Err(CatalogActivationChallengeError::InvalidCA);
    }
    let mut certificates = CertificateDer::pem_slice_iter(contents);
    let certificate = certificates
        .next()
        .ok_or(CatalogActivationChallengeError::InvalidCA)?
        .map_err(|_| CatalogActivationChallengeError::InvalidCA)?
        .into_owned();
    if certificates.next().is_some() || !certificates.remainder().is_empty() {
        return Err(CatalogActivationChallengeError::InvalidCA);
    }
    Ok(certificate)
}

fn fresh_nonce() -> Result<String, CatalogActivationChallengeError> {
    for _ in 0..2 {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).map_err(CatalogActivationChallengeError::Random)?;
        if bytes.iter().any(|byte| *byte != 0) {
            let mut encoded = String::with_capacity(64);
            for byte in bytes {
                let _ = write!(encoded, "{byte:02x}");
            }
            return Ok(encoded);
        }
    }
    Err(CatalogActivationChallengeError::ZeroNonce)
}

fn parse_response(
    response: &[u8],
    challenge: &CatalogActivationCapabilityChallenge,
) -> Result<CatalogActivationCapabilityChallengeResponse, CatalogActivationChallengeError> {
    let delimiter = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(CatalogActivationChallengeError::InvalidResponseFraming)?;
    let header_end = delimiter + 4;
    if header_end > MAXIMUM_HEADER_BYTES || response.len() - header_end > MAXIMUM_BODY_BYTES {
        return Err(CatalogActivationChallengeError::ResponseTooLarge);
    }
    let header = std::str::from_utf8(&response[..delimiter])
        .map_err(|_| CatalogActivationChallengeError::InvalidResponseFraming)?;
    if header
        .split("\r\n")
        .any(|line| line.bytes().any(|byte| byte.is_ascii_control()))
    {
        return Err(CatalogActivationChallengeError::InvalidResponseFraming);
    }
    let mut lines = header.split("\r\n");
    if lines.next() != Some("HTTP/1.1 200 OK") {
        return Err(CatalogActivationChallengeError::InvalidResponseStatus);
    }
    let mut content_type = None;
    let mut cache_control = None;
    let mut connection = None;
    let mut content_length = None;
    let mut count = 0_usize;
    for line in lines {
        count += 1;
        let (name, value) = line
            .split_once(": ")
            .ok_or(CatalogActivationChallengeError::InvalidResponseHeaders)?;
        let slot = match name.to_ascii_lowercase().as_str() {
            "content-type" => &mut content_type,
            "cache-control" => &mut cache_control,
            "connection" => &mut connection,
            "content-length" => &mut content_length,
            _ => return Err(CatalogActivationChallengeError::InvalidResponseHeaders),
        };
        if slot.replace(value).is_some() {
            return Err(CatalogActivationChallengeError::InvalidResponseHeaders);
        }
    }
    if count != 4
        || content_type != Some("application/json")
        || cache_control != Some("no-store")
        || connection != Some("close")
    {
        return Err(CatalogActivationChallengeError::InvalidResponseHeaders);
    }
    let declared = content_length
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or(CatalogActivationChallengeError::InvalidResponseHeaders)?;
    let length = declared
        .parse::<usize>()
        .map_err(|_| CatalogActivationChallengeError::InvalidResponseHeaders)?;
    let body = &response[header_end..];
    if length == 0
        || length > MAXIMUM_BODY_BYTES
        || length.to_string() != declared
        || length != body.len()
    {
        return Err(CatalogActivationChallengeError::InvalidResponseFraming);
    }
    let parsed: CatalogActivationCapabilityChallengeResponse = serde_json::from_slice(body)
        .map_err(CatalogActivationChallengeError::InvalidResponseJSON)?;
    parsed
        .validate_for(challenge)
        .map_err(CatalogActivationChallengeError::InvalidResponse)?;
    if serde_json::to_vec(&parsed).map_err(CatalogActivationChallengeError::SerializeResponse)?
        != body
    {
        return Err(CatalogActivationChallengeError::NonCanonicalResponse);
    }
    Ok(parsed)
}

fn absolute_normal_path(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .any(|component| matches!(component, Component::Normal(_)))
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

fn valid_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
        })
}

fn target_dns_matches(
    target_dns: &str,
    pod_name: &str,
    cluster_name: &str,
    namespace: &str,
) -> bool {
    let target_stateful_set = format!("{cluster_name}-shard-0000");
    let canonical_pod = postgresql_member_pod_name(cluster_name, 0, 0);
    pod_name == canonical_pod
        && target_dns == format!("{canonical_pod}.{target_stateful_set}.{namespace}.svc")
}

/// Fail-closed challenge client error. No variant contains CA or response data.
#[derive(Debug, Error)]
pub(crate) enum CatalogActivationChallengeError {
    #[error("invalid catalog-activation challenge client configuration")]
    InvalidConfiguration,
    #[error("catalog-activation CA could not be read")]
    ReadCA(#[source] io::Error),
    #[error("catalog-activation CA is not a regular file")]
    CAIsNotRegularFile,
    #[error("catalog-activation CA exceeds 64 KiB")]
    CATooLarge,
    #[error("catalog-activation CA is not one exact PEM certificate")]
    InvalidCA,
    #[error("catalog-activation TLS client configuration is invalid")]
    TlsConfiguration(#[source] rustls::Error),
    #[error("catalog-activation target identity is invalid")]
    InvalidTarget,
    #[error("generate catalog-activation challenge nonce")]
    Random(#[source] getrandom::Error),
    #[error("catalog-activation crypto RNG returned an all-zero nonce twice")]
    ZeroNonce,
    #[error("catalog-activation challenge is invalid")]
    InvalidChallenge(#[source] CatalogActivationCapabilityChallengeError),
    #[error("catalog-activation challenge body is invalid")]
    InvalidChallengeBody,
    #[error("serialize catalog-activation challenge")]
    SerializeChallenge(#[source] serde_json::Error),
    #[error("catalog-activation challenge timed out")]
    TimedOut,
    #[error("connect directly to catalog-activation target agent")]
    Connect(#[source] io::Error),
    #[error("catalog-activation TLS handshake failed")]
    Tls(#[source] io::Error),
    #[error("catalog-activation connection did not negotiate TLS 1.3 with HTTP/1.1")]
    NegotiatedProtocol,
    #[error("write catalog-activation challenge")]
    Write(#[source] io::Error),
    #[error("read catalog-activation challenge response")]
    Read(#[source] io::Error),
    #[error("catalog-activation challenge response exceeds bounded limits")]
    ResponseTooLarge,
    #[error("catalog-activation challenge response framing is invalid")]
    InvalidResponseFraming,
    #[error("catalog-activation challenge response status is not exactly HTTP/1.1 200 OK")]
    InvalidResponseStatus,
    #[error("catalog-activation challenge response headers are invalid")]
    InvalidResponseHeaders,
    #[error("catalog-activation challenge response JSON is invalid")]
    InvalidResponseJSON(#[source] serde_json::Error),
    #[error("catalog-activation challenge response does not exactly match the request")]
    InvalidResponse(#[source] CatalogActivationCapabilityChallengeError),
    #[error("serialize validated catalog-activation challenge response")]
    SerializeResponse(#[source] serde_json::Error),
    #[error("catalog-activation challenge response is not canonical JSON")]
    NonCanonicalResponse,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pgshard_types::catalog_activation::{
        CATALOG_ACTIVATION_ACCEPTANCE_VERSION,
        CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_RESPONSE_VERSION,
        CATALOG_ACTIVATION_CAPABILITY_VERSION, CATALOG_ACTIVATION_CONSUMER_VERSION,
        CATALOG_ACTIVATION_FSYNC_PERSISTENCE, CATALOG_ACTIVATION_REQUEST_VERSION,
        CatalogActivationCapability,
    };
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::ServerConfig;
    use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    use super::*;

    fn expected() -> ExpectedCatalogActivationCapability {
        ExpectedCatalogActivationCapability {
            cluster: CatalogActivationCapabilityCluster {
                name: "demo".to_owned(),
                uid: "cluster-uid".to_owned(),
            },
            carrier: CatalogActivationCapabilityCarrier {
                namespace: "database".to_owned(),
                name: "demo-catalog-activation".to_owned(),
                uid: "carrier-uid".to_owned(),
            },
            target: CatalogActivationCapabilityTarget {
                shard: 0,
                member: 0,
                instance_id: "demo-shard-0000-0".to_owned(),
                pod_name: "demo-shard-0000-0".to_owned(),
                pod_uid: "target-pod-uid".to_owned(),
            },
        }
    }

    fn challenge() -> CatalogActivationCapabilityChallenge {
        let expected = expected();
        CatalogActivationCapabilityChallenge {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_REQUEST_VERSION.to_owned(),
            nonce: "1".repeat(64),
            request_sha256: "a".repeat(64),
            cluster: expected.cluster,
            carrier: expected.carrier,
            target: expected.target,
        }
    }

    fn response(
        challenge: &CatalogActivationCapabilityChallenge,
    ) -> CatalogActivationCapabilityChallengeResponse {
        CatalogActivationCapabilityChallengeResponse {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_RESPONSE_VERSION.to_owned(),
            nonce: challenge.nonce.clone(),
            request_sha256: challenge.request_sha256.clone(),
            capability: CatalogActivationCapability {
                schema_version: CATALOG_ACTIVATION_CAPABILITY_VERSION.to_owned(),
                capability: CATALOG_ACTIVATION_CONSUMER_VERSION.to_owned(),
                request_schema_version: CATALOG_ACTIVATION_REQUEST_VERSION.to_owned(),
                acceptance_schema_version: CATALOG_ACTIVATION_ACCEPTANCE_VERSION.to_owned(),
                persistence: CATALOG_ACTIVATION_FSYNC_PERSISTENCE.to_owned(),
                cluster: challenge.cluster.clone(),
                carrier: challenge.carrier.clone(),
                target: challenge.target.clone(),
            },
        }
    }

    fn wire_response(challenge: &CatalogActivationCapabilityChallenge) -> Vec<u8> {
        let body = serde_json::to_vec(&response(challenge)).expect("response JSON");
        [
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: no-store\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes(),
            body,
        ]
        .concat()
    }

    #[tokio::test]
    async fn tls13_http11_round_trip_uses_catalog_sni_and_exact_challenge() {
        let catalog_name = "demo-shardschema.database.svc";
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec![catalog_name.to_owned()]).expect("test certificate");
        let certificate = cert.der().clone();
        let private_key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
        let mut server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .expect("TLS 1.3 server policy")
                .with_no_client_auth()
                .with_single_cert(vec![certificate.clone()], private_key)
                .expect("test server certificate");
        server.alpn_protocols = vec![b"http/1.1".to_vec()];
        server.send_tls13_tickets = 0;
        let acceptor = TlsAcceptor::from(Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test address");
        let expected_challenge = challenge();
        let server_challenge = expected_challenge.clone();
        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept test client");
            let mut tls = acceptor.accept(tcp).await.expect("accept TLS 1.3");
            assert_eq!(
                tls.get_ref().1.protocol_version(),
                Some(ProtocolVersion::TLSv1_3)
            );
            assert_eq!(
                tls.get_ref().1.alpn_protocol(),
                Some(b"http/1.1".as_slice())
            );
            let mut request = Vec::new();
            let body = loop {
                let mut block = [0_u8; 1_024];
                let read = tls.read(&mut block).await.expect("read request");
                assert_ne!(read, 0, "client closed before complete request");
                request.extend_from_slice(&block[..read]);
                assert!(request.len() <= MAXIMUM_HEADER_BYTES + MAXIMUM_BODY_BYTES);
                let Some(delimiter) = request.windows(4).position(|window| window == b"\r\n\r\n")
                else {
                    continue;
                };
                let header = std::str::from_utf8(&request[..delimiter]).expect("request header");
                assert!(header.starts_with(&format!(
                    "POST {CATALOG_ACTIVATION_CAPABILITY_PATH} HTTP/1.1\r\nHost: {catalog_name}\r\n"
                )));
                let length = header
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("Content-Length: "))
                    .expect("content length")
                    .parse::<usize>()
                    .expect("numeric content length");
                let body_start = delimiter + 4;
                if request.len() == body_start + length {
                    break request[body_start..].to_vec();
                }
            };
            let observed: CatalogActivationCapabilityChallenge =
                serde_json::from_slice(&body).expect("challenge JSON");
            assert_eq!(observed, server_challenge);
            let response = wire_response(&observed);
            tls.write_all(&response).await.expect("write response");
            tls.shutdown().await.expect("close response");
        });

        let client = CatalogActivationChallengeClient {
            tls: client_config_from_ca(certificate).expect("test client config"),
            server_name: ServerName::try_from(catalog_name.to_owned()).expect("test SNI"),
            http_host: catalog_name.to_owned(),
            cluster_name: "demo".to_owned(),
            namespace: "database".to_owned(),
            timeout: Duration::from_secs(1),
        };
        let tcp = TcpStream::connect(address)
            .await
            .expect("connect test server");
        let observed = client
            .exchange_tcp(tcp, &expected_challenge)
            .await
            .expect("bounded challenge exchange");
        assert_eq!(observed, response(&expected_challenge));
        server_task.await.expect("server task");
    }

    #[test]
    fn exact_response_is_accepted() {
        let challenge = challenge();
        assert_eq!(
            parse_response(&wire_response(&challenge), &challenge)
                .expect("exact response")
                .capability
                .target,
            challenge.target
        );
    }

    #[test]
    fn response_framing_and_identity_drift_fail_closed() {
        let challenge = challenge();
        let valid = wire_response(&challenge);
        let cases = [
            String::from_utf8(valid.clone())
                .expect("UTF-8")
                .replacen("HTTP/1.1 200 OK", "HTTP/1.0 200 OK", 1)
                .into_bytes(),
            String::from_utf8(valid.clone())
                .expect("UTF-8")
                .replacen("cache-control: no-store\r\n", "", 1)
                .into_bytes(),
            String::from_utf8(valid.clone())
                .expect("UTF-8")
                .replacen("connection: close", "transfer-encoding: chunked", 1)
                .into_bytes(),
            String::from_utf8(valid.clone())
                .expect("UTF-8")
                .replacen(
                    "connection: close",
                    "connection: close\r\nconnection: close",
                    1,
                )
                .into_bytes(),
            String::from_utf8(valid.clone())
                .expect("UTF-8")
                .replacen("connection: close", "x-unexpected: value", 1)
                .into_bytes(),
            String::from_utf8(valid.clone())
                .expect("UTF-8")
                .replacen("content-length: ", "content-length: 0", 1)
                .into_bytes(),
            [valid.clone(), b"x".to_vec()].concat(),
        ];
        for malformed in cases {
            assert!(parse_response(&malformed, &challenge).is_err());
        }

        let mut wrong_challenge = challenge.clone();
        wrong_challenge.nonce = "2".repeat(64);
        assert!(parse_response(&valid, &wrong_challenge).is_err());

        let oversized_header = [
            b"HTTP/1.1 200 OK\r\n".as_slice(),
            vec![b'a'; MAXIMUM_HEADER_BYTES].as_slice(),
            b"\r\n\r\n{}".as_slice(),
        ]
        .concat();
        assert!(matches!(
            parse_response(&oversized_header, &challenge),
            Err(CatalogActivationChallengeError::ResponseTooLarge)
        ));
    }

    #[test]
    fn nonce_is_fresh_canonical_and_nonzero() {
        let first = fresh_nonce().expect("first nonce");
        let second = fresh_nonce().expect("second nonce");
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first.bytes().any(|byte| byte != b'0'));
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
    }

    #[test]
    fn paths_and_dns_are_strictly_bounded() {
        assert!(absolute_normal_path(Path::new(
            "/etc/pgshard/catalog-activation/ca.crt"
        )));
        assert!(!absolute_normal_path(Path::new("relative/ca.crt")));
        assert!(!absolute_normal_path(Path::new("/etc/../ca.crt")));
        assert!(valid_dns_name("demo-shardschema.database.svc"));
        assert!(!valid_dns_name("Demo_shardschema.database.svc"));
        assert!(!valid_dns_name(&format!("{}.svc", "a".repeat(64))));
        assert!(target_dns_matches(
            "demo-shard-0000-0.demo-shard-0000.database.svc",
            "demo-shard-0000-0",
            "demo",
            "database"
        ));
        for cluster_length in [41, 42, 50] {
            let cluster_name = "a".repeat(cluster_length);
            let pod_name = postgresql_member_pod_name(&cluster_name, 0, 0);
            let service_name = format!("{cluster_name}-shard-0000");
            let target_dns = format!("{pod_name}.{service_name}.database.svc");

            assert!(
                target_dns_matches(&target_dns, &pod_name, &cluster_name, "database"),
                "rejected canonical target DNS at cluster length {cluster_length}"
            );
            if cluster_length >= 42 {
                assert_ne!(pod_name, format!("{cluster_name}-shard-0000-0"));
            }
        }
        for (dns_name, pod_name, cluster_name, namespace) in [
            (
                "demo-shardschema.database.svc",
                "demo-shard-0000-0",
                "demo",
                "database",
            ),
            (
                "demo-shard-0000-0.evil.database.svc",
                "demo-shard-0000-0",
                "demo",
                "database",
            ),
            (
                "demo-shard-0000-0.demo-shard-0001.database.svc",
                "demo-shard-0000-0",
                "demo",
                "database",
            ),
            (
                "demo-shard-0000-0.demo-shard-0000.evil.svc",
                "demo-shard-0000-0",
                "demo",
                "database",
            ),
            (
                "other-shard-0000-0.demo-shard-0000.database.svc",
                "other-shard-0000-0",
                "demo",
                "database",
            ),
            (
                "demo-shard-0000-0.demo-shard-0000.database.svc.evil",
                "demo-shard-0000-0",
                "demo",
                "database",
            ),
        ] {
            assert!(
                !target_dns_matches(dns_name, pod_name, cluster_name, namespace),
                "accepted near-miss target DNS {dns_name}"
            );
        }

        let long_cluster_name = "a".repeat(50);
        let canonical_pod = postgresql_member_pod_name(&long_cluster_name, 0, 0);
        let raw_service = format!("{long_cluster_name}-shard-0000");
        let raw_pod = format!("{raw_service}-0");
        for (dns_name, pod_name) in [
            (
                format!("{raw_pod}.{raw_service}.database.svc"),
                raw_pod.clone(),
            ),
            (
                format!("{canonical_pod}.evil.database.svc"),
                canonical_pod.clone(),
            ),
            (
                format!("{canonical_pod}.{raw_service}.database.svc.evil"),
                canonical_pod.clone(),
            ),
            (
                format!("{canonical_pod}-evil.{raw_service}.database.svc"),
                format!("{canonical_pod}-evil"),
            ),
        ] {
            assert!(
                !target_dns_matches(&dns_name, &pod_name, &long_cluster_name, "database"),
                "accepted long-name near-miss target DNS {dns_name}"
            );
        }
    }
}
