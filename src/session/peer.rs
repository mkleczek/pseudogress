//! The client's end of a connection: bytes in, messages out.

use std::{io, time::Duration};

use bytes::BytesMut;
use pgwire::{
    error::PgWireResult,
    messages::{DecodeContext, PgWireFrontendMessage},
};
use socket2::SockRef;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::server::TlsStream;

use crate::wire::put_error;

/// The size of PostgreSQL's send buffer, which is also what a read gets.
pub(super) const BUFFER: usize = 8192;

/// A connection's stream, plain or TLS. The rest of the session is generic
/// over it, so that neither costs dynamic dispatch.
pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    fn tcp(&self) -> &TcpStream;
}

impl Stream for TcpStream {
    fn tcp(&self) -> &TcpStream {
        self
    }
}

impl Stream for TlsStream<TcpStream> {
    fn tcp(&self) -> &TcpStream {
        self.get_ref().0
    }
}

pub(super) struct Peer<S> {
    pub stream: S,
    /// Bytes read, that are not decoded yet.
    pub input: BytesMut,
    pub ctx: DecodeContext,
}

impl<S: Stream> Peer<S> {
    pub fn new(stream: S) -> Self {
        Peer {
            stream,
            input: BytesMut::with_capacity(BUFFER),
            ctx: DecodeContext::default(),
        }
    }

    pub fn decode(&mut self) -> PgWireResult<Option<PgWireFrontendMessage>> {
        PgWireFrontendMessage::decode(&mut self.input, &self.ctx)
    }

    /// Reads more input, and returns how many bytes. The client closing the
    /// connection is an error.
    pub async fn fill(&mut self) -> io::Result<usize> {
        // BytesMut would grow by 64 bytes when it is full.
        if self.input.capacity() - self.input.len() < BUFFER / 8 {
            self.input.reserve(BUFFER);
        }
        match self.stream.read_buf(&mut self.input).await? {
            0 => Err(io::ErrorKind::UnexpectedEof.into()),
            read => Ok(read),
        }
    }

    /// Writes and empties `out`, in pieces of the size PostgreSQL's send
    /// buffer has. Each write waits for the client when it does not read: a
    /// connection that nobody reads from stalls, as it would with PostgreSQL.
    pub async fn send(&mut self, out: &mut BytesMut) -> io::Result<()> {
        for piece in out.chunks(BUFFER) {
            self.stream.write_all(piece).await?;
        }
        self.stream.flush().await?;
        out.clear();
        Ok(())
    }

    /// Appends FATAL to `out` and sends it. The connection ends after it.
    pub async fn fatal(&mut self, out: &mut BytesMut, code: &str, message: &str) -> io::Result<()> {
        put_error(out, "FATAL", code, message);
        self.send(out).await
    }

    /// Makes closing the connection a TCP reset.
    pub fn reset_on_close(&self) {
        let _ = SockRef::from(self.stream.tcp()).set_linger(Some(Duration::ZERO));
    }
}
