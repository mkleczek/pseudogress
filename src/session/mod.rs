//! Connections: the PostgreSQL protocol as a backend speaks it, with simulated
//! latency and faults. What a statement gets back comes from a [`Backend`],
//! so the protocol handling is the same for replay and record mode.
//!
//! Every connection is one task. It reads a batch (messages up to a Sync, a
//! Flush or a simple Query), has the backend answer it, waits until the
//! simulated time has passed and writes the answer before it reads on, as a
//! PostgreSQL backend does.

use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use bytes::BytesMut;
use pgwire::messages::PgWireFrontendMessage;
use tokio::net::TcpListener;

use crate::{auth::Verifiers, class::Classes, databases::Databases, tls::Tls};

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
}

/// Binds an address for PostgreSQL connections, with an accept queue long
/// enough for storms of new connections. Port 0 binds a free port.
pub fn listen(address: SocketAddr) -> std::io::Result<TcpListener> {
    let _ = address;
    todo!()
}

/// Accepts connections on every listener, until the process ends.
pub async fn serve<C: Connector>(
    listeners: Vec<TcpListener>,
    shared: Arc<Shared<C>>,
) -> std::io::Result<()> {
    let _ = (listeners, shared);
    todo!()
}
