//! From accepting a connection to its first batch: SSL negotiation, protocol
//! version, database, user, SCRAM and the start of the backend.
//!
//! Every answer that the client waits for comes one round trip after its
//! request, so that setup costs what it would over the network.

use std::{
    io,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use pgwire::{
    error::PgWireError,
    messages::{
        PgWireFrontendMessage, ProtocolVersion, SslNegotiationMetaMessage,
        response::TransactionStatus,
        startup::{
            Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus,
            PasswordMessageFamily, SecretKey, Startup,
        },
    },
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use super::{
    Backend, Connector, Shared, batches, cancel,
    cancel::Registration,
    chance,
    peer::{BUFFER, Peer, Stream},
};
use crate::{
    auth::{self, AuthError, Scram, Verifier},
    class::Class,
    databases::ConnectError,
    metrics::ClassMetrics,
    timer,
    tls::Tls,
    wire::{put, put_ready},
};

/// What a connection's first messages lead to.
enum Opening<'a> {
    Startup(Startup),
    /// The client asked for TLS, and was told to go ahead.
    Tls(&'a Tls),
    /// The connection is finished: a cancel request, or an error.
    Done,
}

pub(super) async fn accept<C: Connector>(tcp: TcpStream, shared: Arc<Shared<C>>) -> io::Result<()> {
    let mut peer = Peer::new(tcp);
    match opening(&mut peer, shared.tls.as_ref()).await? {
        Opening::Startup(startup) => start(peer, startup, None, &shared).await,
        Opening::Tls(tls) => {
            let stream = tls.accept(peer.stream).await?;
            let mut peer = Peer {
                stream,
                input: peer.input,
                ctx: peer.ctx,
            };
            match opening(&mut peer, None).await? {
                Opening::Startup(startup) => {
                    start(peer, startup, Some(tls.end_point()), &shared).await
                }
                _ => Ok(()),
            }
        }
        Opening::Done => Ok(()),
    }
}

/// Reads up to the startup message. Without `tls`, SSLRequest is refused.
async fn opening<'a, S: Stream>(
    peer: &mut Peer<S>,
    tls: Option<&'a Tls>,
) -> io::Result<Opening<'a>> {
    loop {
        // What the version of a refused startup message was.
        let head = peer.input.first_chunk::<8>().copied();
        match peer.decode() {
            Ok(None) => {
                peer.fill().await?;
            }
            Ok(Some(PgWireFrontendMessage::SslNegotiation(request))) => match request {
                SslNegotiationMetaMessage::PostgresSsl(_) => {
                    let Some(tls) = tls else {
                        peer.stream.write_all(b"N").await?;
                        continue;
                    };
                    peer.stream.write_all(b"S").await?;
                    // Anything before the handshake was not encrypted: it
                    // could be an attempt to inject into the TLS session.
                    return Ok(if peer.input.is_empty() {
                        Opening::Tls(tls)
                    } else {
                        Opening::Done
                    });
                }
                SslNegotiationMetaMessage::PostgresGss(_) => peer.stream.write_all(b"N").await?,
                // Neither: the startup message or a cancel request follows.
                SslNegotiationMetaMessage::None => peer.ctx.awaiting_frontend_ssl = false,
            },
            Ok(Some(PgWireFrontendMessage::Startup(startup))) => {
                peer.ctx.awaiting_frontend_startup = false;
                return Ok(Opening::Startup(startup));
            }
            Ok(Some(PgWireFrontendMessage::CancelRequest(request))) => {
                cancel::cancel(request.pid, request.secret_key.as_i32());
                return Ok(Opening::Done);
            }
            Ok(Some(_)) => return Ok(Opening::Done),
            Err(error) => {
                let (code, text) = match (&error, head) {
                    // pgwire only decodes major version 3.
                    (PgWireError::InvalidStartupMessage, Some(head)) if head[4..6] != [0, 3] => (
                        "0A000",
                        format!(
                            "unsupported frontend protocol {}.{}: server supports 3.0",
                            u16::from_be_bytes([head[4], head[5]]),
                            u16::from_be_bytes([head[6], head[7]])
                        ),
                    ),
                    _ => ("08P01", error.to_string()),
                };
                peer.fatal(&mut BytesMut::new(), code, &text).await?;
                return Ok(Opening::Done);
            }
        }
    }
}

fn round_trip(class: &Class) -> Duration {
    class.settings.load().round_trip
}

/// Counts an open connection until it ends, whatever the reason.
struct Open<'a>(&'a ClassMetrics);

impl<'a> Open<'a> {
    fn new(metrics: &'a ClassMetrics) -> Self {
        metrics.connects.fetch_add(1, Relaxed);
        metrics.connections.fetch_add(1, Relaxed);
        Open(metrics)
    }
}

impl Drop for Open<'_> {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Relaxed);
    }
}

/// Everything after the startup message. `end_point` is the channel binding
/// data of a TLS connection.
async fn start<C: Connector, S: Stream>(
    mut peer: Peer<S>,
    startup: Startup,
    end_point: Option<Bytes>,
    shared: &Arc<Shared<C>>,
) -> io::Result<()> {
    let started = Instant::now();
    let mut out = BytesMut::with_capacity(BUFFER);
    let Some(user) = startup.parameters.get("user") else {
        let message = "no PostgreSQL user name specified in startup packet";
        return peer.fatal(&mut out, "28000", message).await;
    };
    let database = startup.parameters.get("database").unwrap_or(user);

    // Protocol 3.0 is all there is, as the connection continues in it.
    let options: Vec<String> = startup
        .parameters
        .keys()
        .filter(|name| name.starts_with("_pq_."))
        .cloned()
        .collect();
    if startup.protocol_number_minor > 0 || !options.is_empty() {
        // PostgreSQL sends the whole version number here, not the minor.
        let version = Startup::PROTOCOL_VERSION_3_0;
        put(&mut out, &NegotiateProtocolVersion::new(version, options));
    }
    peer.ctx.protocol_version = ProtocolVersion::PROTOCOL3_0;

    let connection = match shared.databases.connect(database) {
        Ok(connection) => connection,
        Err(ConnectError::NoSuchDatabase) => {
            let message = format!("database \"{database}\" does not exist");
            return peer.fatal(&mut out, "3D000", &message).await;
        }
        Err(ConnectError::TooManyConnections) => {
            return peer
                .fatal(&mut out, "53300", "sorry, too many clients already")
                .await;
        }
    };
    let class = &shared.classes[connection.class()];
    if chance(class.settings.load().faults.refuse) {
        class.metrics.faults.fetch_add(1, Relaxed);
        return Ok(());
    }
    if chance(class.settings.load().faults.hang) {
        class.metrics.faults.fetch_add(1, Relaxed);
        return hang(peer).await;
    }
    if !class.users.contains(user) {
        timer::sleep_until(started + round_trip(class)).await;
        let message = format!("role \"{user}\" does not exist");
        return peer.fatal(&mut out, "28000", &message).await;
    }

    let password = class
        .password
        .clone()
        .unwrap_or_else(|| auth::derive_password(&shared.secret, database, user));
    // Hashing takes milliseconds.
    let verifier = {
        let shared = shared.clone();
        tokio::task::spawn_blocking(move || shared.verifiers.get(&password))
            .await
            .map_err(io::Error::other)?
    };
    let Some((client_final_at, server_final)) = authenticate(
        &mut peer, &mut out, class, user, verifier, end_point, started,
    )
    .await?
    else {
        return Ok(());
    };

    let backend = match shared
        .connector
        .connect(connection.class(), database, user)
        .await
    {
        Ok(backend) => backend,
        Err(error) => {
            timer::sleep_until(client_final_at + round_trip(class)).await;
            return peer.fatal(&mut out, "08006", &error).await;
        }
    };
    let cancel = Registration::new();
    put(&mut out, &Authentication::SASLFinal(server_final));
    put(&mut out, &Authentication::Ok);
    for (name, value) in backend.parameter_statuses() {
        put(&mut out, &ParameterStatus::new(name.clone(), value.clone()));
    }
    put(
        &mut out,
        &BackendKeyData::new(cancel.pid, SecretKey::I32(cancel.secret)),
    );
    put_ready(&mut out, TransactionStatus::Idle);
    timer::sleep_until(client_final_at + round_trip(class)).await;
    peer.send(&mut out).await?;

    let _open = Open::new(&class.metrics);
    batches::run(peer, out, connection, cancel, backend, class).await
}

/// Holds the connection until the client closes it.
async fn hang<S: Stream>(mut peer: Peer<S>) -> io::Result<()> {
    loop {
        peer.input.clear();
        peer.fill().await?;
    }
}

/// Runs the SCRAM exchange. Returns when the client's final message came, and
/// the server's final message; or `None` when the connection is finished.
async fn authenticate<S: Stream>(
    peer: &mut Peer<S>,
    out: &mut BytesMut,
    class: &Class,
    user: &str,
    verifier: Arc<Verifier>,
    end_point: Option<Bytes>,
    started: Instant,
) -> io::Result<Option<(Instant, Bytes)>> {
    put(
        out,
        &Authentication::SASL(auth::mechanisms(end_point.is_some())),
    );
    timer::sleep_until(started + round_trip(class)).await;
    peer.send(out).await?;

    let mut scram = Scram::new(verifier, end_point);
    let Some((at, message)) = password_message(peer, out).await? else {
        return Ok(None);
    };
    let Ok(initial) = message.into_sasl_initial_response() else {
        return protocol_error(peer, out, "malformed SASLInitialResponse").await;
    };
    let data = initial.data.as_deref().unwrap_or_default();
    let first = match scram.client_first(&initial.auth_method, data) {
        Ok(first) => first,
        Err(error) => return refuse(peer, out, class, user, at, error).await,
    };
    put(out, &Authentication::SASLContinue(first));
    timer::sleep_until(at + round_trip(class)).await;
    peer.send(out).await?;

    let Some((at, message)) = password_message(peer, out).await? else {
        return Ok(None);
    };
    let Ok(response) = message.into_sasl_response() else {
        return protocol_error(peer, out, "malformed SASLResponse").await;
    };
    match scram.client_final(&response.data) {
        Ok(last) if !chance(class.settings.load().faults.reject_password) => Ok(Some((at, last))),
        Ok(_) => {
            class.metrics.faults.fetch_add(1, Relaxed);
            refuse(peer, out, class, user, at, AuthError::WrongPassword).await
        }
        Err(error) => refuse(peer, out, class, user, at, error).await,
    }
}

/// Reads a message of the password family, and when it came. Anything else
/// finishes the connection.
async fn password_message<S: Stream>(
    peer: &mut Peer<S>,
    out: &mut BytesMut,
) -> io::Result<Option<(Instant, PasswordMessageFamily)>> {
    loop {
        match peer.decode() {
            Ok(Some(PgWireFrontendMessage::PasswordMessageFamily(message))) => {
                return Ok(Some((Instant::now(), message)));
            }
            Ok(Some(PgWireFrontendMessage::Terminate(_))) => return Ok(None),
            Ok(Some(_)) => return protocol_error(peer, out, "expected a SASL message").await,
            Ok(None) => {
                peer.fill().await?;
            }
            Err(error) => return protocol_error(peer, out, &error.to_string()).await,
        }
    }
}

async fn protocol_error<S: Stream, T>(
    peer: &mut Peer<S>,
    out: &mut BytesMut,
    message: &str,
) -> io::Result<Option<T>> {
    peer.fatal(out, "08P01", message).await?;
    Ok(None)
}

/// Answers a failed exchange to the request that came at `at`.
async fn refuse<S: Stream>(
    peer: &mut Peer<S>,
    out: &mut BytesMut,
    class: &Class,
    user: &str,
    at: Instant,
    error: AuthError,
) -> io::Result<Option<(Instant, Bytes)>> {
    let answer_at = at + round_trip(class);
    match error {
        AuthError::Protocol(message) => {
            timer::sleep_until(answer_at).await;
            protocol_error(peer, out, &message).await
        }
        AuthError::WrongPassword => {
            class.metrics.auth_failures.fetch_add(1, Relaxed);
            timer::sleep_until(answer_at).await;
            let message = format!("password authentication failed for user \"{user}\"");
            peer.fatal(out, "28P01", &message).await?;
            Ok(None)
        }
    }
}
