use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use rcgen::{
    CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256, PKCS_ECDSA_P384_SHA384, SignatureAlgorithm,
};
use sha2::{Digest, Sha256, Sha384, Sha512};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, HandshakeKind, RootCertStore,
        crypto::ring,
        pki_types::{CertificateDer, ServerName},
    },
};

use super::*;

/// A temporary directory, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("pseudogress-tls-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    fn write(&self, file: &str, contents: &str) -> PathBuf {
        let path = self.0.join(file);
        std::fs::write(&path, contents).unwrap();
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A self-signed certificate for localhost and its key, written to `dir`.
fn identity(
    dir: &TempDir,
    algorithm: &'static SignatureAlgorithm,
) -> (config::Tls, CertificateDer<'static>) {
    let key = KeyPair::generate_for(algorithm).unwrap();
    let certificate = CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let config = config::Tls {
        certificate: dir.write("certificate.pem", &certificate.pem()),
        key: dir.write("key.pem", &key.serialize_pem()),
    };
    (config, certificate.der().clone())
}

/// A client that trusts `certificate` only.
fn connector(certificate: &CertificateDer<'static>, alpn: &[&[u8]]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    let mut config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    TlsConnector::from(Arc::new(config))
}

/// Connects `connector` to `tls` over TCP and exchanges bytes both ways.
async fn exchange(
    tls: &Tls,
    connector: &TlsConnector,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = tls.accept(stream).await.unwrap();
        let mut request = [0; 5];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"hello");
        stream.write_all(b"world").await.unwrap();
        stream.flush().await.unwrap();
    };
    let client = async {
        let stream = TcpStream::connect(address).await.unwrap();
        let name = ServerName::try_from("localhost").unwrap();
        let mut stream = connector.connect(name, stream).await.unwrap();
        stream.write_all(b"hello").await.unwrap();
        stream.flush().await.unwrap();
        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"world");
        stream
    };
    let ((), stream) = tokio::join!(server, client);
    stream
}

fn load_error(config: &config::Tls) -> String {
    Tls::load(config).err().expect("loading fails")
}

fn assert_names(error: &str, path: &Path) {
    assert!(error.contains(&path.display().to_string()), "{error}");
}

#[tokio::test]
#[ignore = "xfail: tls is not implemented yet"]
async fn exchanges_bytes_with_a_client_that_sends_alpn() {
    let dir = TempDir::new("alpn");
    let (config, certificate) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    let tls = Tls::load(&config).unwrap();
    let stream = exchange(&tls, &connector(&certificate, &[b"postgresql"])).await;
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"postgresql"[..]));
}

#[tokio::test]
#[ignore = "xfail: tls is not implemented yet"]
async fn exchanges_bytes_with_a_client_that_sends_no_alpn() {
    let dir = TempDir::new("no-alpn");
    let (config, certificate) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    let tls = Tls::load(&config).unwrap();
    let stream = exchange(&tls, &connector(&certificate, &[])).await;
    assert_eq!(stream.get_ref().1.alpn_protocol(), None);
}

/// A resumed handshake would hide the cost a client pays against PostgreSQL,
/// which has no session tickets or cache.
#[tokio::test]
#[ignore = "xfail: tls is not implemented yet"]
async fn never_resumes_a_session() {
    let dir = TempDir::new("resumption");
    let (config, certificate) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    let tls = Tls::load(&config).unwrap();
    let connector = connector(&certificate, &[b"postgresql"]);
    exchange(&tls, &connector).await;
    let stream = exchange(&tls, &connector).await;
    assert_eq!(
        stream.get_ref().1.handshake_kind(),
        Some(HandshakeKind::Full)
    );
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn end_point_is_the_sha256_of_a_p256_certificate() {
    let dir = TempDir::new("p256");
    let (config, certificate) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    let tls = Tls::load(&config).unwrap();
    assert_eq!(tls.end_point()[..], Sha256::digest(&certificate)[..]);
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn end_point_is_the_sha384_of_a_p384_certificate() {
    let dir = TempDir::new("p384");
    let (config, certificate) = identity(&dir, &PKCS_ECDSA_P384_SHA384);
    let tls = Tls::load(&config).unwrap();
    assert_eq!(tls.end_point()[..], Sha384::digest(&certificate)[..]);
}

/// No ECDSA certificate with SHA-512 can be generated, so the signature
/// algorithm of a P-256 certificate is changed. Nothing checks the signature
/// when a certificate is loaded.
#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn end_point_is_the_sha512_of_a_certificate_signed_with_sha512() {
    let dir = TempDir::new("sha512");
    let (mut config, certificate) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    let ecdsa_with_sha256 = [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02];
    let mut der = certificate.to_vec();
    // The signature algorithm after the signed part is the last one.
    let oid = der
        .windows(ecdsa_with_sha256.len())
        .rposition(|window| window == ecdsa_with_sha256)
        .unwrap();
    der[oid + ecdsa_with_sha256.len() - 1] = 0x04;
    let pem = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        STANDARD.encode(&der)
    );
    config.certificate = dir.write("sha512.pem", &pem);
    let tls = Tls::load(&config).unwrap();
    assert_eq!(tls.end_point()[..], Sha512::digest(&der)[..]);
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn a_missing_certificate_is_an_error() {
    let dir = TempDir::new("missing-certificate");
    let (config, _) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    std::fs::remove_file(&config.certificate).unwrap();
    assert_names(&load_error(&config), &config.certificate);
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn a_missing_key_is_an_error() {
    let dir = TempDir::new("missing-key");
    let (config, _) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    std::fs::remove_file(&config.key).unwrap();
    assert_names(&load_error(&config), &config.key);
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn a_certificate_file_without_a_certificate_is_an_error() {
    let dir = TempDir::new("invalid-certificate");
    let (mut config, _) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    config.certificate = dir.write("invalid.pem", "not a certificate\n");
    assert_names(&load_error(&config), &config.certificate);
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn a_key_file_without_a_key_is_an_error() {
    let dir = TempDir::new("invalid-key");
    let (mut config, _) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    config.key = dir.write("invalid.pem", "not a key\n");
    assert_names(&load_error(&config), &config.key);
}

#[test]
#[ignore = "xfail: tls is not implemented yet"]
fn a_key_of_another_certificate_is_an_error() {
    let dir = TempDir::new("mismatch");
    let (mut config, _) = identity(&dir, &PKCS_ECDSA_P256_SHA256);
    let other = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    config.key = dir.write("other.pem", &other.serialize_pem());
    assert_names(&load_error(&config), &config.key);
}
