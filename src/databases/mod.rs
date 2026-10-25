//! Virtual databases: a database is a name that matches a class's pattern. A
//! database has state only while it has connections: the connections and
//! the channels they listen on. Notifications and terminations reach
//! connections through their events.

use std::{
    collections::BTreeMap,
    hash::BuildHasher,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rand::RngExt;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::config;

mod pattern;
#[cfg(test)]
mod tests;

use pattern::Pattern;

const SHARDS: usize = 64;

/// The open databases are split into shards by the hash of their names:
/// connections are made and dropped thousands of times per second, and each
/// one takes the lock of its database's shard only.
pub struct Databases {
    classes: Vec<Rule>,
    shards: Box<[Arc<Shard>]>,
}

/// What it takes to find a database's class and admit its connections.
struct Rule {
    name: String,
    pattern: Pattern,
    /// `usize::MAX` without a limit.
    max_connections: usize,
}

/// Aligned to a cache line, so that shards used by different cores do not
/// share one. Connections hold their shard, to leave it on drop.
#[derive(Default)]
#[repr(align(64))]
struct Shard(Mutex<FxHashMap<Arc<str>, Database>>);

impl Shard {
    /// Ignores poisoning, so that dropping a connection never panics.
    fn lock(&self) -> MutexGuard<'_, FxHashMap<Arc<str>, Database>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Database {
    class: usize,
    /// The map's key, which connections share instead of copying it.
    name: Arc<str>,
    /// Connection ids are never reused, so a stale id finds nobody.
    next_id: u64,
    connections: FxHashMap<u64, Peer>,
}

struct Peer {
    events: mpsc::UnboundedSender<Event>,
    channels: FxHashSet<String>,
}

impl Database {
    fn new(class: usize, name: &str) -> Self {
        Database {
            class,
            name: name.into(),
            next_id: 0,
            connections: FxHashMap::default(),
        }
    }

    /// Returns the name and the id of the new connection.
    fn admit(
        &mut self,
        events: mpsc::UnboundedSender<Event>,
        limit: usize,
    ) -> Result<(Arc<str>, u64), ConnectError> {
        if self.connections.len() >= limit {
            return Err(ConnectError::TooManyConnections);
        }
        let id = self.next_id;
        self.next_id += 1;
        let channels = FxHashSet::default();
        self.connections.insert(id, Peer { events, channels });
        Ok((self.name.clone(), id))
    }

    /// Returns the number of connections the event was sent to.
    fn send(&self, event: &Event, wants: impl Fn(&Peer) -> bool) -> usize {
        let wanting = self.connections.values().filter(|peer| wants(peer));
        wanting
            .filter(|peer| peer.events.send(event.clone()).is_ok())
            .count()
    }
}

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
        Databases {
            classes: classes
                .iter()
                .map(|class| Rule {
                    name: class.name.clone(),
                    pattern: Pattern::new(&class.databases),
                    max_connections: class.max_connections.map_or(usize::MAX, |n| n as usize),
                })
                .collect(),
            shards: (0..SHARDS).map(|_| Arc::default()).collect(),
        }
    }

    pub fn connect(&self, database: &str) -> Result<Connection, ConnectError> {
        let (class, rule) = self
            .classes
            .iter()
            .enumerate()
            .find(|(_, rule)| rule.pattern.matches(database))
            .ok_or(ConnectError::NoSuchDatabase)?;
        let shard = self.shard(database);
        let (sender, events) = mpsc::unbounded_channel();
        let (name, id) = {
            let mut open = shard.lock();
            match open.get_mut(database) {
                Some(open) => open.admit(sender, rule.max_connections)?,
                None => {
                    let mut new = Database::new(class, database);
                    let admitted = new.admit(sender, rule.max_connections)?;
                    open.insert(new.name.clone(), new);
                    admitted
                }
            }
        };
        Ok(Connection {
            shard: shard.clone(),
            database: name,
            id,
            class,
            events,
        })
    }

    /// Returns the number of connections notified.
    pub fn notify(&self, target: &Target, channel: &str, payload: &str) -> usize {
        let event = Event::Notification {
            channel: channel.to_owned(),
            payload: payload.to_owned(),
        };
        let mut reached = 0;
        self.each(target, |database| {
            reached += database.send(&event, |peer| peer.channels.contains(channel));
        });
        reached
    }

    /// Returns the number of connections told to terminate.
    pub fn terminate(&self, target: &Target) -> usize {
        let mut reached = 0;
        self.each(target, |database| {
            reached += database.send(&Event::Terminate, |_| true);
        });
        reached
    }

    /// `None` when the database has no connections.
    pub fn info(&self, database: &str) -> Option<DatabaseInfo> {
        let open = self.shard(database).lock();
        let database = open.get(database)?;
        let mut listening = BTreeMap::new();
        for channel in database
            .connections
            .values()
            .flat_map(|peer| &peer.channels)
        {
            *listening.entry(channel.clone()).or_default() += 1;
        }
        Some(DatabaseInfo {
            class: self.classes[database.class].name.clone(),
            connections: database.connections.len(),
            listening,
        })
    }

    fn shard(&self, database: &str) -> &Arc<Shard> {
        // A shard's map takes the bucket from the low bits of the same hash
        // and a tag from the top seven; choosing the shard by those would
        // leave the databases of a shard with few buckets and tags.
        let hash = FxBuildHasher.hash_one(database);
        &self.shards[(hash >> 32) as usize % SHARDS]
    }

    /// Calls `f` for every open database the target picks.
    fn each(&self, target: &Target, mut f: impl FnMut(&Database)) {
        match target {
            Target::Database { database } => {
                if let Some(database) = self.shard(database).lock().get(database.as_str()) {
                    f(database);
                }
            }
            Target::Class { class, fraction } => {
                let Some(class) = self.classes.iter().position(|rule| rule.name == *class) else {
                    return;
                };
                let mut rng = rand::rng();
                for shard in &self.shards {
                    for database in shard.lock().values().filter(|d| d.class == class) {
                        // `random_bool` panics outside 0..=1, and NaN picks nothing.
                        if *fraction >= 1.0 || *fraction > 0.0 && rng.random_bool(*fraction) {
                            f(database);
                        }
                    }
                }
            }
        }
    }
}

/// A connection's place in its database, given up on drop.
pub struct Connection {
    shard: Arc<Shard>,
    database: Arc<str>,
    id: u64,
    class: usize,
    events: mpsc::UnboundedReceiver<Event>,
}

impl Connection {
    /// Index of the database's class.
    pub fn class(&self) -> usize {
        self.class
    }

    pub fn listen(&self, channel: &str) {
        self.with_peer(|peer| {
            if !peer.channels.contains(channel) {
                peer.channels.insert(channel.to_owned());
            }
        });
    }

    /// `*` stops listening on every channel.
    pub fn unlisten(&self, channel: &str) {
        self.with_peer(|peer| {
            if channel == "*" {
                peer.channels.clear();
            } else {
                peer.channels.remove(channel);
            }
        });
    }

    /// Waits for the next notification or termination.
    pub async fn event(&mut self) -> Event {
        match self.events.recv().await {
            Some(event) => event,
            // The database keeps the sender for as long as this connection
            // exists, so this is not reached.
            None => std::future::pending().await,
        }
    }

    fn with_peer(&self, f: impl FnOnce(&mut Peer)) {
        let mut open = self.shard.lock();
        let database = open.get_mut(&*self.database);
        if let Some(peer) = database.and_then(|d| d.connections.get_mut(&self.id)) {
            f(peer);
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let mut open = self.shard.lock();
        let Some(database) = open.get_mut(&*self.database) else {
            return;
        };
        database.connections.remove(&self.id);
        if database.connections.is_empty() {
            open.remove(&*self.database);
        }
    }
}
