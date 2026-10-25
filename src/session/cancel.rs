//! Cancel requests come on a new connection of their own, so connections are
//! found by process id.

use std::{
    collections::hash_map::Entry,
    sync::{LazyLock, Mutex},
};

use rustc_hash::FxHashMap;
use tokio::sync::watch;

struct Cancellable {
    secret: i32,
    signal: watch::Sender<()>,
}

/// Locked once when a connection starts or ends, and by cancel requests:
/// never by a batch.
static REGISTRY: LazyLock<Mutex<FxHashMap<i32, Cancellable>>> = LazyLock::new(Default::default);

/// A connection's entry in the registry, removed on drop.
pub(super) struct Registration {
    pub pid: i32,
    pub secret: i32,
    /// Changes when the connection is cancelled. A connection that marks it
    /// unchanged when a batch starts only sees the cancels that arrive while
    /// the batch runs, as PostgreSQL ignores a cancel when it is idle.
    pub signal: watch::Receiver<()>,
}

impl Registration {
    pub fn new() -> Self {
        let secret = rand::random();
        let (sender, signal) = watch::channel(());
        let mut registry = REGISTRY.lock().unwrap();
        loop {
            let pid = rand::random_range(1..=i32::MAX);
            if let Entry::Vacant(free) = registry.entry(pid) {
                free.insert(Cancellable {
                    secret,
                    signal: sender,
                });
                return Registration {
                    pid,
                    secret,
                    signal,
                };
            }
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        REGISTRY.lock().unwrap().remove(&self.pid);
    }
}

/// Cancels the connection if the secret is right.
pub(super) fn cancel(pid: i32, secret: Option<i32>) {
    if let Some(connection) = REGISTRY.lock().unwrap().get(&pid)
        && secret == Some(connection.secret)
    {
        connection.signal.send_replace(());
    }
}
