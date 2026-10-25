//! The connection to a class's upstream: framing, and the startup that
//! precedes the first batch.

use std::io;

use bytes::{Bytes, BytesMut};
use pgwire::messages::{
    DecodeContext, Message, PgWireBackendMessage,
    response::ErrorResponse,
    startup::{Authentication, Password, SASLInitialResponse, SASLResponse, Startup},
};
use postgres_protocol::authentication::{
    md5_hash,
    sasl::{ChannelBinding, ScramSha256},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::config::Upstream;

pub struct Conn {
    stream: TcpStream,
    read: BytesMut,
}

impl Conn {
    /// Connects, authenticates and reads the ParameterStatus messages up to
    /// ReadyForQuery.
    pub async fn open(
        upstream: &Upstream,
        database: &str,
    ) -> io::Result<(Conn, Vec<(String, String)>)> {
        let stream = TcpStream::connect((upstream.host.as_str(), upstream.port)).await?;
        stream.set_nodelay(true)?;
        let mut conn = Conn {
            stream,
            read: BytesMut::new(),
        };
        let mut startup = Startup::new();
        startup
            .parameters
            .insert("user".to_owned(), upstream.user.clone());
        startup
            .parameters
            .insert("database".to_owned(), database.to_owned());
        conn.send(startup).await?;

        let mut scram = None;
        let mut parameters = Vec::new();
        loop {
            match conn.decoded().await? {
                PgWireBackendMessage::Authentication(authentication) => {
                    conn.authenticate(upstream, &mut scram, authentication)
                        .await?;
                }
                PgWireBackendMessage::ParameterStatus(status) => {
                    parameters.push((status.name, status.value));
                }
                PgWireBackendMessage::ReadyForQuery(_) => return Ok((conn, parameters)),
                PgWireBackendMessage::ErrorResponse(error) => {
                    return Err(io::Error::other(error_message(&error)));
                }
                _ => {}
            }
        }
    }

    async fn authenticate(
        &mut self,
        upstream: &Upstream,
        scram: &mut Option<ScramSha256>,
        authentication: Authentication,
    ) -> io::Result<()> {
        let password = || {
            upstream.password.as_deref().ok_or_else(|| {
                io::Error::other("the upstream asks for a password, and the class has none")
            })
        };
        match authentication {
            Authentication::Ok => Ok(()),
            Authentication::CleartextPassword => {
                self.send(Password::new(password()?.to_owned())).await
            }
            Authentication::MD5Password(salt) => {
                let salt = salt
                    .try_into()
                    .map_err(|_| io::Error::other("the MD5 salt is not 4 bytes"))?;
                let hash = md5_hash(upstream.user.as_bytes(), password()?.as_bytes(), salt);
                self.send(Password::new(hash)).await
            }
            Authentication::SASL(_) => {
                let first = scram.insert(ScramSha256::new(
                    password()?.as_bytes(),
                    ChannelBinding::unsupported(),
                ));
                let message = SASLInitialResponse::new(
                    "SCRAM-SHA-256".to_owned(),
                    Some(Bytes::copy_from_slice(first.message())),
                );
                self.send(message).await
            }
            Authentication::SASLContinue(data) => {
                let scram = scram.as_mut().ok_or_else(out_of_order)?;
                scram.update(&data)?;
                self.send(SASLResponse::new(Bytes::copy_from_slice(scram.message())))
                    .await
            }
            Authentication::SASLFinal(data) => {
                scram.as_mut().ok_or_else(out_of_order)?.finish(&data)
            }
            _ => Err(io::Error::other("unsupported authentication method")),
        }
    }

    async fn send(&mut self, message: impl Message) -> io::Result<()> {
        let mut bytes = BytesMut::new();
        message.encode(&mut bytes).map_err(io::Error::other)?;
        self.write(&bytes).await
    }

    pub async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes).await
    }

    /// The next message as PostgreSQL sent it, type and length included.
    pub async fn raw(&mut self) -> io::Result<Bytes> {
        loop {
            if let Some([_, length @ ..]) = self.read.first_chunk::<5>() {
                let length = 1 + u32::from_be_bytes(*length) as usize;
                if self.read.len() >= length {
                    return Ok(self.read.split_to(length).freeze());
                }
            }
            if self.stream.read_buf(&mut self.read).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the connection was closed",
                ));
            }
        }
    }

    async fn decoded(&mut self) -> io::Result<PgWireBackendMessage> {
        let mut message = BytesMut::from(&self.raw().await?[..]);
        PgWireBackendMessage::decode(&mut message, &DecodeContext::default())
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("truncated message"))
    }
}

fn out_of_order() -> io::Error {
    io::Error::other("SASL messages out of order")
}

fn error_message(error: &ErrorResponse) -> String {
    error
        .fields
        .iter()
        .find(|(field, _)| *field == b'M')
        .map_or_else(
            || "unknown error".to_owned(),
            |(_, message)| message.clone(),
        )
}
