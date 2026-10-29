//! Loading classes on demand: record mode asks the class service about a
//! database that no class matches, and adds the class that it returns, so
//! that the connection, and later ones to databases of the class, find it.
//!
//! Adding a class goes to the connector, then to the classes, then to the
//! databases, under one lock, so that the three have it at the same index.
//! The databases come last because they make the class reachable.

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Mutex,
};

use tokio::sync::watch;

use crate::{
    config,
    options::Service,
    service::Template,
    session::{Connector, Shared},
};

/// Why no class was loaded for a database.
#[allow(dead_code, reason = "nothing loads classes on demand yet")]
#[derive(Debug, Clone, PartialEq)]
pub enum LoadError {
    /// The service has no class for the database: FATAL 3D000.
    NotFound,
    /// The lookup failed, or what it found can't be added: FATAL 08006, with
    /// the reason in the message.
    Failed(String),
}

#[allow(dead_code, reason = "nothing loads classes on demand yet")]
pub struct Loader {
    template: Template,
    /// Where the classes are recorded, and `classes.toml` goes.
    recordings: PathBuf,
    max_classes: usize,
    state: Mutex<State>,
}

#[allow(dead_code, reason = "nothing loads classes on demand yet")]
struct State {
    /// The classes that the service returned, by name.
    added: BTreeMap<String, config::Class>,
    /// The lookups in flight, by database. A caller that finds its database
    /// here waits for the outcome instead of asking again.
    lookups: HashMap<String, watch::Sender<Option<Result<(), LoadError>>>>,
}

#[allow(dead_code, reason = "nothing loads classes on demand yet")]
impl Loader {
    /// Creates the directory of the recordings, so that a path that can't be
    /// written fails before any connection does.
    pub fn new(service: Service) -> Result<Loader, String> {
        let _ = service;
        todo!()
    }

    /// Makes a class match `database`, if the service has one. When it is
    /// `Ok`, connecting to the database again finds a class.
    ///
    /// Matching goes first: a class added meanwhile may cover the database.
    /// Callers for one database share a lookup, and its outcome.
    pub async fn load<C: Connector>(
        &self,
        shared: &Shared<C>,
        database: &str,
        user: &str,
    ) -> Result<(), LoadError> {
        let _ = (shared, database, user);
        todo!()
    }

    /// Writes `classes.toml` into the directory of the recordings: the classes
    /// that were added. Called once, at shutdown, after the recordings.
    pub fn write(&self) -> Result<(), String> {
        todo!()
    }
}
