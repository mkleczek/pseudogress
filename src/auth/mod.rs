//! SCRAM-SHA-256 authentication, server side, without I/O: the session passes
//! the client's SASL messages in and sends the returned answers.

use std::sync::Arc;

use bytes::Bytes;

#[cfg(test)]
mod tests;

/// The password of `user` in `database`, derived from the secret.
pub fn derive_password(secret: &str, database: &str, user: &str) -> String {
    let _ = (secret, database, user);
    todo!()
}

/// SASL mechanisms to offer. SCRAM-SHA-256-PLUS needs TLS.
pub fn mechanisms(channel_binding: bool) -> Vec<String> {
    let _ = channel_binding;
    todo!()
}

/// What SCRAM keeps of a password: salt, iteration count, stored key and
/// server key.
pub struct Verifier {}

/// Verifiers by password. Making one costs PostgreSQL's 4096 PBKDF2
/// iterations, so every password is hashed once, while the cache keeps it.
pub struct Verifiers {}

impl Verifiers {
    pub fn new() -> Self {
        todo!()
    }

    /// Blocks for the hashing on a miss; call it off the runtime's worker
    /// threads.
    pub fn get(&self, password: &str) -> Arc<Verifier> {
        let _ = password;
        todo!()
    }
}

#[derive(Debug, PartialEq)]
pub enum AuthError {
    /// The client broke the SASL exchange.
    Protocol(String),
    WrongPassword,
}

/// One SCRAM exchange.
pub struct Scram {}

impl Scram {
    /// `channel_binding` is the tls-server-end-point data of a TLS
    /// connection.
    pub fn new(verifier: Arc<Verifier>, channel_binding: Option<Bytes>) -> Self {
        let _ = (verifier, channel_binding);
        todo!()
    }

    /// Takes SASLInitialResponse and returns server-first-message.
    pub fn client_first(&mut self, mechanism: &str, data: &[u8]) -> Result<Bytes, AuthError> {
        let _ = (mechanism, data);
        todo!()
    }

    /// Takes SASLResponse and returns server-final-message.
    pub fn client_final(&mut self, data: &[u8]) -> Result<Bytes, AuthError> {
        let _ = data;
        todo!()
    }
}
