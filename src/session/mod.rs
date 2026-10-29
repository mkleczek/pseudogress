//! Connections: the PostgreSQL protocol as a backend speaks it, with simulated
//! latency and faults. What a statement gets back comes from a [`Backend`],
//! so the protocol handling is the same for replay and record mode.
//!
//! Every connection is one task. It reads a batch (messages up to a Sync, a
//! Flush or a simple Query), has the backend answer it, waits until the
//! simulated time has passed and writes the answer before it reads on, as a
//! PostgreSQL backend does.

use std::{
    future::Future,
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use bytes::BytesMut;
use pgwire::messages::PgWireFrontendMessage;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{net::TcpListener, task::JoinSet};

use crate::{auth::Verifiers, class::Classes, databases::Databases, loader::Loader, tls::Tls};

mod batches;
mod cancel;
mod listen;
mod peer;
mod setup;
#[cfg(test)]
mod tests;

/// Answers statements for one connection.
pub trait Backend: Send + 'static {
    /// The ParameterStatus messages to send after authentication.
    fn parameter_statuses(&self) -> &[(String, String)];

    /// Appends the answer to `batch` to `out`, ReadyForQuery included when
    /// the batch ends in a Sync or a simple Query, and returns its execution
    /// time.
    fn batch(
        &mut self,
        batch: &[PgWireFrontendMessage],
        out: &mut BytesMut,
    ) -> impl Future<Output = Duration> + Send;

    /// Appends the answer to a batch that was cancelled before its answer was
    /// sent: an ErrorResponse with SQLSTATE 57014, and ReadyForQuery when the
    /// batch ended in a Sync or a simple Query. After a batch that ended in a
    /// Flush the backend ignores the messages up to the next Sync, as after
    /// any error: the client's Sync gets the ReadyForQuery.
    fn cancelled(&mut self, out: &mut BytesMut);

    /// Whether the connection must end once the answer of the last batch is
    /// sent, as PostgreSQL's does after a FATAL: the backend can't go on.
    fn broken(&self) -> bool {
        false
    }
}

/// Makes the backends of a mode.
pub trait Connector: Send + Sync + 'static {
    type Backend: Backend;

    /// Opens the backend of an authenticated connection. An error ends the
    /// connection with FATAL 08006.
    fn connect(
        &self,
        class: usize,
        database: &str,
        user: &str,
    ) -> impl Future<Output = Result<Self::Backend, String>> + Send;
}

/// What every connection shares.
pub struct Shared<C> {
    pub classes: Classes,
    pub databases: Arc<Databases>,
    pub verifiers: Verifiers,
    pub secret: String,
    pub tls: Option<Tls>,
    pub connector: C,
    /// Record mode with a class service: finds the class of a database that
    /// no class matches.
    #[allow(dead_code, reason = "nothing loads classes on demand yet")]
    pub loader: Option<Loader>,
}

/// Binds an address for PostgreSQL connections, with an accept queue long
/// enough for storms of new connections. Port 0 binds a free port.
pub fn listen(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(4096)?;
    TcpListener::from_std(socket.into())
}

/// Accepts connections on every listener, until the process ends.
pub async fn serve<C: Connector>(
    listeners: Vec<TcpListener>,
    shared: Arc<Shared<C>>,
) -> std::io::Result<()> {
    let mut accepting = JoinSet::new();
    for listener in listeners {
        accepting.spawn(accept(listener, shared.clone()));
    }
    match accepting.join_next().await {
        Some(Ok(error)) => Err(error),
        Some(Err(error)) => Err(io::Error::other(error)),
        None => Err(io::Error::new(
            ErrorKind::InvalidInput,
            "no address to listen on",
        )),
    }
}

/// Returns the error that ends the listener.
async fn accept<C: Connector>(listener: TcpListener, shared: Arc<Shared<C>>) -> io::Error {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                // PostgreSQL sets it too: without it, delayed ACKs add tens
                // of milliseconds to small answers.
                let _ = stream.set_nodelay(true);
                tokio::spawn(setup::accept(stream, shared.clone()));
            }
            Err(error) if transient(&error) => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(error) => return error,
        }
    }
}

/// Errors of the connection being accepted, and running out of file
/// descriptors (ENFILE and EMFILE: 23 and 24 on Linux and macOS) end only
/// that connection: others close and free descriptors.
fn transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset | ErrorKind::Interrupted
    ) || matches!(error.raw_os_error(), Some(23 | 24))
}

/// Whether a fault with probability `p` happens. Most faults are off, so the
/// random number is only drawn for the others.
fn chance(p: f64) -> bool {
    p > 0.0 && rand::random::<f64>() < p
}
