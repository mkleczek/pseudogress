//! TLS for connections that send SSLRequest.

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

use crate::config;

#[cfg(test)]
mod tests;

pub struct Tls {}

impl Tls {
    pub fn load(config: &config::Tls) -> Result<Self, String> {
        let _ = config;
        todo!()
    }

    pub async fn accept(&self, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
        let _ = stream;
        todo!()
    }

    /// The tls-server-end-point channel binding data of the certificate, for
    /// SCRAM-SHA-256-PLUS.
    pub fn end_point(&self) -> Bytes {
        todo!()
    }
}
