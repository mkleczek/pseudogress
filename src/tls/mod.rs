//! TLS for connections that send SSLRequest.

use std::{fmt::Display, path::Path, sync::Arc};

use bytes::Bytes;
use sha2::{Digest, Sha256, Sha384, Sha512};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        crypto::ring,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        server::NoServerSessionStorage,
    },
    server::TlsStream,
};

use crate::config;

#[cfg(test)]
mod tests;

pub struct Tls {
    acceptor: TlsAcceptor,
    end_point: Bytes,
}

impl Tls {
    pub fn load(config: &config::Tls) -> Result<Self, String> {
        let chain = CertificateDer::pem_file_iter(&config.certificate)
            .and_then(|certificates| certificates.collect::<Result<Vec<_>, _>>())
            .map_err(|e| named(&config.certificate, e))?;
        let leaf = chain
            .first()
            .ok_or_else(|| named(&config.certificate, "no certificate found"))?;
        let end_point = Bytes::from(end_point_hash(leaf));
        let key = PrivateKeyDer::from_pem_file(&config.key).map_err(|e| named(&config.key, e))?;
        let mut server = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .and_then(|builder| builder.with_no_client_auth().with_single_cert(chain, key))
            .map_err(|e| named(&config.key, e))?;
        // libpq 17 and later offer it.
        server.alpn_protocols = vec![b"postgresql".to_vec()];
        // PostgreSQL has neither session tickets nor a session cache, and a
        // resumed handshake would make a client cheaper than it is against it.
        server.session_storage = Arc::new(NoServerSessionStorage {});
        server.send_tls13_tickets = 0;
        Ok(Tls {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            end_point,
        })
    }

    pub async fn accept(&self, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
        self.acceptor.accept(stream).await
    }

    /// The tls-server-end-point channel binding data of the certificate, for
    /// SCRAM-SHA-256-PLUS.
    pub fn end_point(&self) -> Bytes {
        self.end_point.clone()
    }
}

fn named(path: &Path, error: impl Display) -> String {
    format!("{}: {error}", path.display())
}

/// The hash of the certificate with the hash function of its signature
/// algorithm, SHA-256 when that is MD5 or SHA-1 (RFC 5929). Only the SHA-384
/// and SHA-512 algorithms of RSA and ECDSA are told apart: any other gets
/// SHA-256.
fn end_point_hash(certificate: &[u8]) -> Vec<u8> {
    const SHA384_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c];
    const SHA512_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d];
    const SHA384_ECDSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03];
    const SHA512_ECDSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04];
    match signature_algorithm(certificate) {
        Some(SHA384_RSA | SHA384_ECDSA) => Sha384::digest(certificate).to_vec(),
        Some(SHA512_RSA | SHA512_ECDSA) => Sha512::digest(certificate).to_vec(),
        _ => Sha256::digest(certificate).to_vec(),
    }
}

/// The object identifier of the signature algorithm of a certificate:
///
/// ```text
/// Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
/// AlgorithmIdentifier ::= SEQUENCE { algorithm OBJECT IDENTIFIER, parameters }
/// ```
fn signature_algorithm(certificate: &[u8]) -> Option<&[u8]> {
    let (certificate, _) = element(certificate)?;
    let (_, after_tbs) = element(certificate)?;
    let (algorithm, _) = element(after_tbs)?;
    element(algorithm).map(|(oid, _)| oid)
}

/// Splits the DER element at the start of `der` into its content and the
/// bytes after it.
fn element(der: &[u8]) -> Option<(&[u8], &[u8])> {
    let (_tag, der) = der.split_first()?;
    let (&first, der) = der.split_first()?;
    let (length, der) = if first < 0x80 {
        (Some(usize::from(first)), der)
    } else {
        let (bytes, der) = der.split_at_checked(usize::from(first & 0x7f))?;
        let length = bytes.iter().try_fold(0usize, |length, &byte| {
            length.checked_mul(256)?.checked_add(usize::from(byte))
        });
        (length, der)
    };
    der.split_at_checked(length?)
}
