//! Loading classes on demand: record mode asks the class service about a
//! database that no class matches, and adds the class that it returns, so
//! that the connection, and later ones to databases of the class, find it.
//!
//! Adding a class goes to the connector, then to the classes, then to the
//! databases, under one lock, so that the three have it at the same index.
//! The databases come last because they make the class reachable.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, PoisonError, atomic::Ordering::Relaxed},
    time::Instant,
};

use tokio::sync::watch;

use crate::{
    class::Class,
    config,
    metrics::LookupMetrics,
    options::Service,
    service::{self, Lookup, Template},
    session::{Connector, Shared},
};

#[cfg(test)]
mod tests;

/// Why no class was loaded for a database.
#[derive(Debug, Clone, PartialEq)]
pub enum LoadError {
    /// The service has no class for the database: FATAL 3D000.
    NotFound,
    /// The lookup failed, or what it found can't be added: FATAL 08006, with
    /// the reason in the message.
    Failed(String),
}

pub struct Loader {
    template: Template,
    /// Where the classes are recorded, and `classes.toml` goes.
    recordings: PathBuf,
    max_classes: usize,
    /// Counts the requests to the service: callers that wait for one don't.
    metrics: Arc<LookupMetrics>,
    state: Mutex<State>,
}

struct State {
    /// The classes that the service returned, by name.
    added: BTreeMap<String, config::Class>,
    /// The lookups in flight, by database. A caller that finds its database
    /// here waits for the outcome instead of asking again.
    lookups: HashMap<String, watch::Sender<Option<Result<(), LoadError>>>>,
}

impl Loader {
    /// Creates the directory of the recordings, so that a path that can't be
    /// written fails before any connection does.
    pub fn new(service: Service) -> Result<Loader, String> {
        fs::create_dir_all(&service.recordings)
            .map_err(|e| format!("{}: {e}", service.recordings.display()))?;
        Ok(Loader {
            template: service.url,
            recordings: service.recordings,
            max_classes: service.max_classes,
            metrics: Arc::default(),
            state: Mutex::new(State {
                added: BTreeMap::new(),
                lookups: HashMap::new(),
            }),
        })
    }

    /// What the lookups did, for the admin API.
    pub fn metrics(&self) -> Arc<LookupMetrics> {
        self.metrics.clone()
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
        loop {
            let mut waiting = {
                let mut state = self.lock();
                if shared.databases.matches(database) {
                    return Ok(());
                }
                match state.lookups.get(database) {
                    Some(lookup) => lookup.subscribe(),
                    None => {
                        let (lookup, _) = watch::channel(None);
                        state.lookups.insert(database.to_owned(), lookup);
                        break;
                    }
                }
            };
            // Without an outcome the lookup was cancelled: match again, and
            // ask if nobody does.
            if let Ok(outcome) = waiting
                .wait_for(Option::is_some)
                .await
                .map(|outcome| outcome.clone())
            {
                return outcome.expect("the outcome is there");
            }
        }
        let mut lookup = InFlight {
            loader: self,
            database,
            outcome: None,
        };
        let started = Instant::now();
        let outcome = self.find(shared, database, user).await;
        let counter = match &outcome {
            Ok(()) => &self.metrics.found,
            Err(LoadError::NotFound) => &self.metrics.not_found,
            Err(LoadError::Failed(_)) => &self.metrics.errors,
        };
        counter.fetch_add(1, Relaxed);
        self.metrics.duration.record(started.elapsed());
        if let Err(LoadError::Failed(reason)) = &outcome {
            eprintln!("pseudogress: class service: {database}: {reason}");
        }
        lookup.outcome = Some(outcome.clone());
        outcome
    }

    /// Writes `classes.toml` into the directory of the recordings: the classes
    /// that were added. Called once, at shutdown, after the recordings.
    pub fn write(&self) -> Result<(), String> {
        config::write_classes(&self.recordings, self.lock().added.values())
    }

    async fn find<C: Connector>(
        &self,
        shared: &Shared<C>,
        database: &str,
        user: &str,
    ) -> Result<(), LoadError> {
        match service::lookup(&self.template, database, user, service::TIMEOUT).await {
            Lookup::Found(class) => self.add(shared, *class).map_err(LoadError::Failed),
            Lookup::NotFound => Err(LoadError::NotFound),
            Lookup::Failed(reason) => Err(LoadError::Failed(reason)),
        }
    }

    /// Adds the class that the service returned, unless it is one that was
    /// added before.
    fn add<C: Connector>(
        &self,
        shared: &Shared<C>,
        mut class: config::Class,
    ) -> Result<(), String> {
        class.recording = self.recordings.join(format!("{}.jsonl", class.name));
        let mut state = self.lock();
        if let Some(added) = state.added.get(&class.name) {
            return if *added == class {
                Ok(())
            } else {
                Err(format!(
                    "class {}: the service returned a different definition before",
                    class.name
                ))
            };
        }
        if shared.classes.iter().any(|other| other.name == class.name) {
            return Err(format!(
                "class {}: a class of the configuration has that name",
                class.name
            ));
        }
        if state.added.len() >= self.max_classes {
            return Err(format!(
                "{} classes were added, as many as --max-classes allows: the service \
                 should return classes whose pattern covers a group of databases",
                self.max_classes
            ));
        }
        // The connector first, as it can refuse the class. The databases go
        // last, as they make the class reachable.
        let index = shared.connector.add_class(&class)?;
        assert_eq!(
            shared.classes.push(Class::from_service(&class)),
            Some(index),
            "the classes are at the indexes of the connector's"
        );
        assert_eq!(
            shared.databases.add(&class),
            Some(index),
            "the databases are at the indexes of the classes"
        );
        state.added.insert(class.name.clone(), class);
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing the guard protects is left half done by a panic.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A lookup that this caller makes. When it is dropped, the callers that wait
/// for it go on: with its outcome, or, when it was cancelled, to match again.
struct InFlight<'a> {
    loader: &'a Loader,
    database: &'a str,
    outcome: Option<Result<(), LoadError>>,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if let Some(lookup) = self.loader.lock().lookups.remove(self.database)
            && let Some(outcome) = self.outcome.take()
        {
            lookup.send_replace(Some(outcome));
        }
    }
}
