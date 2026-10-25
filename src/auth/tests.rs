use std::{sync::LazyLock, thread};

use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};

use super::*;

const PLAIN: &str = "SCRAM-SHA-256";
const PLUS: &str = "SCRAM-SHA-256-PLUS";
const END_POINT: &[u8] = b"hash of the server certificate";

/// Hashing costs 4096 iterations, so the tests share the verifier of "pw".
static PW: LazyLock<Arc<Verifier>> = LazyLock::new(|| Verifiers::new().get("pw"));

fn scram(channel_binding: Option<&'static [u8]>) -> Scram {
    Scram::new(PW.clone(), channel_binding.map(Bytes::from_static))
}

fn client(channel_binding: ChannelBinding) -> ScramSha256 {
    ScramSha256::new(b"pw", channel_binding)
}

fn protocol_error<T: std::fmt::Debug>(result: Result<T, AuthError>) {
    assert!(
        matches!(result, Err(AuthError::Protocol(_))),
        "expected a protocol error, got {result:?}"
    );
}

/// Runs the whole exchange of `client` with `server`.
fn exchange(
    server: &mut Scram,
    mechanism: &str,
    client: &mut ScramSha256,
) -> Result<(), AuthError> {
    let first = server.client_first(mechanism, client.message())?;
    client.update(&first).unwrap();
    let last = server.client_final(client.message())?;
    client.finish(&last).unwrap();
    Ok(())
}

/// A server and its client after server-first-message, so the client's
/// message is the client-final-message.
fn after_first() -> (Scram, ScramSha256) {
    let mut server = scram(None);
    let mut client = client(ChannelBinding::unsupported());
    let first = server.client_first(PLAIN, client.message()).unwrap();
    client.update(&first).unwrap();
    (server, client)
}

/// The answer to a client-final-message made from the right one's parts:
/// `c=…`, `r=…` and `p=…`.
fn final_message(edit: impl FnOnce([&str; 3]) -> String) -> Result<Bytes, AuthError> {
    let (mut server, client) = after_first();
    let message = std::str::from_utf8(client.message()).unwrap();
    let [c, r, p] = message.split(',').collect::<Vec<_>>()[..] else {
        panic!("{message}");
    };
    server.client_final(edit([c, r, p]).as_bytes())
}

#[test]
fn a_client_with_the_password_authenticates_and_verifies_the_server() {
    for binding in [ChannelBinding::unsupported(), ChannelBinding::unrequested()] {
        let mut client = client(binding);
        assert_eq!(exchange(&mut scram(None), PLAIN, &mut client), Ok(()));
    }
}

#[test]
fn server_first_message_is_what_postgresql_sends() {
    let client = client(ChannelBinding::unsupported());
    let client_first = std::str::from_utf8(client.message()).unwrap();
    // The attribute, not an "r=" inside the random nonce.
    let client_nonce = client_first.split_once(",r=").unwrap().1;

    let first = scram(None).client_first(PLAIN, client_first.as_bytes());
    let first = String::from_utf8(first.unwrap().to_vec()).unwrap();
    let [nonce, salt, iterations] = first.split(',').collect::<Vec<_>>()[..] else {
        panic!("{first}");
    };
    let server_nonce = nonce.strip_prefix("r=").unwrap().strip_prefix(client_nonce);
    assert_eq!(
        BASE64_STANDARD.decode(server_nonce.unwrap()).unwrap().len(),
        18
    );
    let salt = BASE64_STANDARD.decode(salt.strip_prefix("s=").unwrap());
    assert_eq!(salt.unwrap().len(), 16);
    assert_eq!(iterations, "i=4096");
}

#[test]
fn a_wrong_password_is_rejected() {
    let mut client = ScramSha256::new(b"wrong", ChannelBinding::unsupported());
    assert_eq!(
        exchange(&mut scram(None), PLAIN, &mut client),
        Err(AuthError::WrongPassword)
    );
}

#[test]
fn channel_binding_succeeds_when_the_end_point_data_matches() {
    let mut client = client(ChannelBinding::tls_server_end_point(END_POINT.to_vec()));
    assert_eq!(
        exchange(&mut scram(Some(END_POINT)), PLUS, &mut client),
        Ok(())
    );
}

#[test]
fn channel_binding_fails_when_the_end_point_data_differs() {
    let other = ChannelBinding::tls_server_end_point(b"another certificate".to_vec());
    let mut client = client(other);
    protocol_error(exchange(&mut scram(Some(END_POINT)), PLUS, &mut client));
}

#[test]
fn a_client_may_decline_channel_binding_it_was_offered() {
    let mut client = client(ChannelBinding::unsupported());
    assert_eq!(
        exchange(&mut scram(Some(END_POINT)), PLAIN, &mut client),
        Ok(())
    );
}

#[test]
fn a_client_that_thinks_the_server_lacks_channel_binding_is_refused() {
    // The gs2 flag `y` against a server that offered PLUS is a downgrade.
    let client = client(ChannelBinding::unrequested());
    protocol_error(scram(Some(END_POINT)).client_first(PLAIN, client.message()));
}

#[test]
fn the_mechanism_must_agree_with_the_gs2_flag() {
    let with_data = client(ChannelBinding::tls_server_end_point(END_POINT.to_vec()));
    let declined = client(ChannelBinding::unsupported());
    let unrequested = client(ChannelBinding::unrequested());
    protocol_error(scram(Some(END_POINT)).client_first(PLAIN, with_data.message()));
    protocol_error(scram(Some(END_POINT)).client_first(PLUS, declined.message()));
    protocol_error(scram(Some(END_POINT)).client_first(PLUS, unrequested.message()));
}

#[test]
fn channel_binding_is_refused_without_end_point_data() {
    let client = client(ChannelBinding::tls_server_end_point(END_POINT.to_vec()));
    protocol_error(scram(None).client_first(PLUS, client.message()));
    protocol_error(scram(None).client_first(PLAIN, client.message()));
}

#[test]
fn an_unknown_mechanism_is_refused() {
    let client = client(ChannelBinding::unsupported());
    for mechanism in ["", "PLAIN", "SCRAM-SHA-1", "scram-sha-256"] {
        protocol_error(scram(None).client_first(mechanism, client.message()));
    }
}

#[test]
fn a_malformed_client_first_message_is_refused() {
    let messages: &[&[u8]] = &[
        b"",
        b"n",
        b"n,,",
        b"n,,n=",
        b"n,,n=,r=",
        b"n,,n=,r=a\x01b",
        b"n,,n=,r=a b",
        b"n,,r=abc",
        b"n,,n=,x=abc",
        b"n,x=1,n=,r=abc",
        b"x,,n=,r=abc",
        b"n,,n=\xff,r=abc",
    ];
    for message in messages {
        protocol_error(scram(None).client_first(PLAIN, message));
    }
    let unknown_type = b"p=tls-unique,,n=,r=abc";
    protocol_error(scram(Some(END_POINT)).client_first(PLUS, unknown_type));
}

#[test]
fn the_username_authzid_and_extensions_are_ignored() {
    let first = scram(None).client_first(PLAIN, b"n,a=someone,n=whoever,r=abc,x=1");
    assert!(first.is_ok(), "{first:?}");
}

#[test]
fn a_malformed_client_final_message_is_refused() {
    protocol_error(final_message(|_| String::new()));
    protocol_error(final_message(|[c, r, _]| format!("{c},{r}")));
    protocol_error(final_message(|[c, r, _]| format!("{c},{r},p=")));
    protocol_error(final_message(|[c, r, _]| format!("{c},{r},p=!!!")));
    protocol_error(final_message(|[c, r, _]| format!("{c},{r},p=AAAA")));
    protocol_error(final_message(|[c, r, p]| format!("{c},{r},{p},x=1")));
    protocol_error(final_message(|[_, r, p]| format!("{r},{p}")));
    protocol_error(final_message(|[c, _, p]| format!("{c},{p}")));
    protocol_error(final_message(|[c, r, p]| format!("{r},{c},{p}")));
}

#[test]
fn client_final_must_repeat_the_nonce_and_the_channel_binding() {
    protocol_error(final_message(|[c, r, p]| format!("{c},{r}x,{p}")));
    protocol_error(final_message(|[c, _, p]| format!("{c},r=x,{p}")));
    // "y,," where the client sent "n,,"
    protocol_error(final_message(|[_, r, p]| format!("c=eSws,{r},{p}")));
    protocol_error(final_message(|[_, r, p]| format!("c=!,{r},{p}")));
}

#[test]
fn messages_out_of_order_are_refused() {
    protocol_error(scram(None).client_final(b"c=biws,r=abc,p=AAAA"));

    let (mut server, client) = after_first();
    protocol_error(server.client_first(PLAIN, client.message()));

    let (mut server, client) = after_first();
    assert!(server.client_final(client.message()).is_ok());
    protocol_error(server.client_final(client.message()));
    protocol_error(server.client_first(PLAIN, b"n,,n=,r=abc"));

    let mut wrong = ScramSha256::new(b"wrong", ChannelBinding::unsupported());
    let mut server = scram(None);
    let first = server.client_first(PLAIN, wrong.message()).unwrap();
    wrong.update(&first).unwrap();
    assert_eq!(
        server.client_final(wrong.message()),
        Err(AuthError::WrongPassword)
    );
    protocol_error(server.client_final(wrong.message()));
}

#[test]
fn mechanisms_offer_channel_binding_only_with_tls() {
    assert_eq!(mechanisms(true), [PLUS, PLAIN]);
    assert_eq!(mechanisms(false), [PLAIN]);
}

#[test]
fn derived_passwords_are_stable_and_differ_per_secret_database_and_user() {
    let password = derive_password("secret", "db", "user");
    assert_eq!(password, derive_password("secret", "db", "user"));
    assert_eq!(password.len(), 64);
    assert!(
        password
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    );
    let others = [
        derive_password("other", "db", "user"),
        derive_password("secret", "other", "user"),
        derive_password("secret", "db", "other"),
        // The zero byte keeps the split between database and user.
        derive_password("secret", "d", "buser"),
        derive_password("secret", "", "dbuser"),
    ];
    for other in &others {
        assert_ne!(other, &password);
    }
}

#[test]
fn derived_passwords_are_hmac_sha256_of_database_nul_user() {
    // printf 'small-1\0authenticator' | openssl dgst -sha256 -hmac 'change me'
    assert_eq!(
        derive_password("change me", "small-1", "authenticator"),
        "9b7615d1046a43c235deee30a23968c9951498306c7a812c51275b08b5d302f8"
    );
    // printf 'db\0user' | openssl dgst -sha256 -hmac ''
    assert_eq!(
        derive_password("", "db", "user"),
        "87be8f16a964f8f2aa99183f7c112ce57c633126da60657279c9f83526d5fc02"
    );
}

#[test]
fn a_repeated_password_gets_the_same_verifier() {
    let verifiers = Verifiers::new();
    let first = verifiers.get("pw");
    assert!(Arc::ptr_eq(&first, &verifiers.get("pw")));
    assert!(!Arc::ptr_eq(&first, &verifiers.get("another")));
}

#[test]
fn concurrent_gets_of_one_password_share_one_verifier() {
    let verifiers = Verifiers::new();
    let all: Vec<Arc<Verifier>> = thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| verifiers.get("pw")))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(all.iter().all(|v| Arc::ptr_eq(v, &all[0])));
}
