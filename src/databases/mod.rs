//! Virtual databases: a database is a name that matches a class's pattern. A
//! database has state only while it has connections: the connections and
//! the channels they listen on. Notifications and terminations reach
//! connections through their events.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config;

#[cfg(test)]
mod tests;

pub struct Databases {}

#[derive(Debug, PartialEq)]
pub enum ConnectError {
    /// No class matches the name: FATAL 3D000.
    NoSuchDatabase,
    /// The class's `max_connections` is reached: FATAL 53300.
    TooManyConnections,
}

/// Which databases a notification or termination is for.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Target {
    Database {
        database: String,
    },
    /// A fraction of the class's open databases, chosen at random.
    Class {
        class: String,
        #[serde(default = "all")]
        fraction: f64,
    },
}

fn all() -> f64 {
    1.0
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Notification {
        channel: String,
        payload: String,
    },
    /// End the connection with FATAL 57P01.
    Terminate,
}

/// What the admin API shows of an open database.
#[derive(Debug, PartialEq, Serialize)]
pub struct DatabaseInfo {
    pub class: String,
    pub connections: usize,
    /// Listening connections per channel.
    pub listening: BTreeMap<String, usize>,
}

impl Databases {
    pub fn new(classes: &[config::Class]) -> Self {
        let _ = classes;
        todo!()
    }

    pub fn connect(&self, database: &str) -> Result<Connection, ConnectError> {
        let _ = database;
        todo!()
    }

    /// Returns the number of connections notified.
    pub fn notify(&self, target: &Target, channel: &str, payload: &str) -> usize {
        let _ = (target, channel, payload);
        todo!()
    }

    /// Returns the number of connections told to terminate.
    pub fn terminate(&self, target: &Target) -> usize {
        let _ = target;
        todo!()
    }

    /// `None` when the database has no connections.
    pub fn info(&self, database: &str) -> Option<DatabaseInfo> {
        let _ = database;
        todo!()
    }
}

/// A connection's place in its database, given up on drop.
pub struct Connection {}

impl Connection {
    /// Index of the database's class.
    pub fn class(&self) -> usize {
        todo!()
    }

    pub fn listen(&self, channel: &str) {
        let _ = channel;
        todo!()
    }

    /// `*` stops listening on every channel.
    pub fn unlisten(&self, channel: &str) {
        let _ = channel;
        todo!()
    }

    /// Waits for the next notification or termination.
    pub async fn event(&mut self) -> Event {
        todo!()
    }
}
