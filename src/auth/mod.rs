//! SCRAM-SHA-256 authentication, server side, without I/O: the session passes
//! the client's SASL messages in and sends the returned answers.

use std::{
    fmt::Write as _,
    hash::BuildHasher,
    mem,
    sync::{Arc, Mutex, OnceLock},
};

use base64::prelude::{BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use rustc_hash::{FxBuildHasher, FxHashMap};
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests;

const PLAIN: &str = "SCRAM-SHA-256";
const PLUS: &str = "SCRAM-SHA-256-PLUS";
/// The gs2 channel binding flag of PLUS, the only type PostgreSQL supports.
const END_POINT: &str = "p=tls-server-end-point";

/// PostgreSQL's default `scram_iterations`: the cost that a connection
/// has for the client.
const ITERATIONS: u32 = 4096;
const SALT_LEN: usize = 16;
/// Random bytes that the server adds to the client's nonce.
const NONCE_LEN: usize = 18;

const OUT_OF_ORDER: &str = "SCRAM messages out of order";
const NOT_UTF8: &str = "SCRAM message is not UTF-8";
const MALFORMED_FIRST: &str = "malformed client-first-message";

/// The password of `user` in `database`, derived from the secret: the
/// lowercase hex (64 digits) of HMAC-SHA-256 keyed with the bytes of
/// `secret`, over the bytes of `database`, a zero byte and the bytes of
/// `user`. The secret may be empty. A harness that knows it computes the
/// same with
///
/// ```sh
/// printf '%s\0%s' "$database" "$user" | openssl dgst -sha256 -hmac "$secret"
/// ```
pub fn derive_password(secret: &str, database: &str, user: &str) -> String {
    let tag = hmac(
        secret.as_bytes(),
        &[database.as_bytes(), &[0], user.as_bytes()],
    );
    tag.iter()
        .fold(String::with_capacity(2 * tag.len()), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String succeeds");
            hex
        })
}

/// SASL mechanisms to offer. SCRAM-SHA-256-PLUS needs TLS.
pub fn mechanisms(channel_binding: bool) -> Vec<String> {
    let mut mechanisms = Vec::with_capacity(2);
    if channel_binding {
        mechanisms.push(PLUS.to_owned());
    }
    mechanisms.push(PLAIN.to_owned());
    mechanisms
}

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes keys of any length");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

/// What SCRAM keeps of a password: salt, iteration count, stored key and
/// server key.
pub struct Verifier {
    salt: [u8; SALT_LEN],
    stored_key: [u8; 32],
    server_key: [u8; 32],
}

impl Verifier {
    // PostgreSQL and clients apply SASLprep to a password, which changes
    // only non-ASCII characters. Passwords here are configuration strings
    // or hex, so it is left out.
    fn new(password: &str) -> Self {
        let mut salt = [0; SALT_LEN];
        rand::fill(&mut salt[..]);
        let mut salted_password = [0; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ITERATIONS, &mut salted_password);
        Verifier {
            salt,
            stored_key: Sha256::digest(hmac(&salted_password, &[b"Client Key"])).into(),
            server_key: hmac(&salted_password, &[b"Server Key"]),
        }
    }
}

const SHARDS: usize = 64;
/// About 100,000 verifiers in all.
const SHARD_CAPACITY: usize = 1600;

/// The verifier of a password, which whoever comes first makes.
type Cell = Arc<OnceLock<Arc<Verifier>>>;

/// Verifiers by password. Making one costs PostgreSQL's 4096 PBKDF2
/// iterations, so every password is hashed once, while the cache keeps it.
pub struct Verifiers {
    // A full shard is cleared instead of evicting from: dropping a verifier
    // only costs hashing it again, and nothing needs bookkeeping.
    shards: [Mutex<FxHashMap<Box<str>, Cell>>; SHARDS],
}

impl Verifiers {
    pub fn new() -> Self {
        Verifiers {
            shards: std::array::from_fn(|_| Mutex::default()),
        }
    }

    /// Blocks for the hashing on a miss; call it off the runtime's worker
    /// threads.
    pub fn get(&self, password: &str) -> Arc<Verifier> {
        // A shard's map uses the low bits of this hash for buckets and the
        // top ones for its control bytes, so the shard takes bits between.
        let shard = (FxBuildHasher.hash_one(password) >> 32) as usize % SHARDS;
        let cell = {
            let mut passwords = self.shards[shard].lock().unwrap();
            match passwords.get(password) {
                Some(cell) => cell.clone(),
                None => {
                    if passwords.len() >= SHARD_CAPACITY {
                        passwords.clear();
                    }
                    let cell = Cell::default();
                    passwords.insert(password.into(), cell.clone());
                    cell
                }
            }
        };
        // Outside the lock, so that only the clients of this password wait.
        cell.get_or_init(|| Arc::new(Verifier::new(password)))
            .clone()
    }
}

#[derive(Debug, PartialEq)]
pub enum AuthError {
    /// The client broke the SASL exchange.
    Protocol(String),
    WrongPassword,
}

fn protocol(reason: &str) -> AuthError {
    AuthError::Protocol(reason.to_owned())
}

/// One SCRAM exchange.
pub struct Scram {
    verifier: Arc<Verifier>,
    channel_binding: Option<Bytes>,
    state: State,
}

enum State {
    ClientFirst,
    ClientFinal(Exchange),
    /// Finished, or failed: a client gets one try.
    Done,
}

/// What client-final-message is checked against.
struct Exchange {
    /// What `c=` repeats: the gs2 header, and with `p=` the channel binding
    /// data.
    channel_binding: Vec<u8>,
    nonce: String,
    /// AuthMessage so far: client-first-message-bare and server-first-message,
    /// each followed by a comma, so that client-final-message without proof
    /// can be appended.
    auth_message: String,
}

impl Scram {
    /// `channel_binding` is the tls-server-end-point data of a TLS
    /// connection, and is given exactly when `mechanisms` offered PLUS.
    pub fn new(verifier: Arc<Verifier>, channel_binding: Option<Bytes>) -> Self {
        Scram {
            verifier,
            channel_binding,
            state: State::ClientFirst,
        }
    }

    /// Takes SASLInitialResponse and returns server-first-message.
    pub fn client_first(&mut self, mechanism: &str, data: &[u8]) -> Result<Bytes, AuthError> {
        self.first(mechanism, data).map_err(protocol)
    }

    /// Takes SASLResponse and returns server-final-message.
    pub fn client_final(&mut self, data: &[u8]) -> Result<Bytes, AuthError> {
        let State::ClientFinal(mut exchange) = mem::replace(&mut self.state, State::Done) else {
            return Err(protocol(OUT_OF_ORDER));
        };
        let (without_proof, proof) = exchange.proof(data).map_err(protocol)?;
        exchange.auth_message.push_str(without_proof);
        let auth_message = exchange.auth_message.as_bytes();

        let Verifier {
            stored_key,
            server_key,
            ..
        } = &*self.verifier;
        let signature = hmac(stored_key, &[auth_message]);
        let client_key: [u8; 32] = std::array::from_fn(|i| proof[i] ^ signature[i]);
        if Sha256::digest(client_key)[..] != stored_key[..] {
            return Err(AuthError::WrongPassword);
        }
        let signature = hmac(server_key, &[auth_message]);
        Ok(format!("v={}", BASE64_STANDARD.encode(signature)).into())
    }

    fn first(&mut self, mechanism: &str, data: &[u8]) -> Result<Bytes, &'static str> {
        if !matches!(
            mem::replace(&mut self.state, State::Done),
            State::ClientFirst
        ) {
            return Err(OUT_OF_ORDER);
        }
        let plus = match mechanism {
            PLAIN => false,
            PLUS if self.channel_binding.is_some() => true,
            _ => return Err("unsupported SASL mechanism"),
        };

        // gs2-header "," client-first-message-bare. PostgreSQL takes the
        // user of the startup message, so the SCRAM username is not read,
        // and neither is the authzid.
        let message = std::str::from_utf8(data).map_err(|_| NOT_UTF8)?;
        let (flag, rest) = message.split_once(',').ok_or(MALFORMED_FIRST)?;
        let (authzid, bare) = rest.split_once(',').ok_or(MALFORMED_FIRST)?;
        if !authzid.is_empty() && !authzid.starts_with("a=") {
            return Err(MALFORMED_FIRST);
        }
        match (flag, plus) {
            (END_POINT, true) | ("n", false) => {}
            ("y", false) if self.channel_binding.is_none() => {}
            ("y", false) => {
                return Err("the client supports channel binding but thinks the server does not");
            }
            ("n" | "y", true) => return Err("SCRAM-SHA-256-PLUS needs channel binding data"),
            (END_POINT, false) => return Err("channel binding data with SCRAM-SHA-256"),
            _ => return Err("unsupported channel binding"),
        }
        let mut attributes = bare.split(',');
        let (Some(username), Some(nonce)) = (attributes.next(), attributes.next()) else {
            return Err(MALFORMED_FIRST);
        };
        let client_nonce = match (username.starts_with("n="), nonce.strip_prefix("r=")) {
            (true, Some(nonce))
                if !nonce.is_empty() && nonce.bytes().all(|b| b.is_ascii_graphic()) =>
            {
                nonce
            }
            _ => return Err(MALFORMED_FIRST),
        };

        let header = &message[..message.len() - bare.len()];
        let binding_data = self.channel_binding.as_deref().filter(|_| plus);
        let mut random = [0; NONCE_LEN];
        rand::fill(&mut random[..]);
        let nonce = format!("{client_nonce}{}", BASE64_STANDARD.encode(random));
        let salt = BASE64_STANDARD.encode(self.verifier.salt);
        let server_first = format!("r={nonce},s={salt},i={ITERATIONS}");
        self.state = State::ClientFinal(Exchange {
            channel_binding: [header.as_bytes(), binding_data.unwrap_or_default()].concat(),
            auth_message: format!("{bare},{server_first},"),
            nonce,
        });
        Ok(server_first.into())
    }
}

impl Exchange {
    /// Checks all of client-final-message but the proof, and returns the
    /// message without it, and the proof.
    fn proof<'a>(&self, data: &'a [u8]) -> Result<(&'a str, [u8; 32]), &'static str> {
        const MALFORMED: &str = "malformed client-final-message";
        let message = std::str::from_utf8(data).map_err(|_| NOT_UTF8)?;
        let (without_proof, proof) = message.rsplit_once(",p=").ok_or(MALFORMED)?;
        let mut attributes = without_proof.split(',');
        let channel_binding = attributes.next().and_then(|a| a.strip_prefix("c="));
        let nonce = attributes.next().and_then(|a| a.strip_prefix("r="));
        let (Some(channel_binding), Some(nonce)) = (channel_binding, nonce) else {
            return Err(MALFORMED);
        };
        if BASE64_STANDARD.decode(channel_binding).ok().as_ref() != Some(&self.channel_binding) {
            return Err("SCRAM channel binding check failed");
        }
        if nonce != self.nonce {
            return Err("SCRAM nonce does not match");
        }
        let proof = BASE64_STANDARD
            .decode(proof)
            .ok()
            .and_then(|p| p.try_into().ok());
        Ok((without_proof, proof.ok_or(MALFORMED)?))
    }
}
