//! What a class's connections recorded, one entry per thing a client asked.

use std::{collections::HashMap, sync::Mutex};

use bytes::Bytes;

use crate::recording::Entry;

/// Samples kept per entry: enough to show the distribution, and a bound on
/// the recording's size.
const MAX_SAMPLES: usize = 1000;

/// A repeated ask only adds samples to the entry of the first.
pub struct Sink {
    entries: Mutex<Entries>,
    /// Prefixes of the statements whose Executes are asked for by their text
    /// alone: the same ask, whatever their parameters.
    ignore_parameters: Vec<String>,
}

#[derive(Default)]
struct Entries {
    /// In the order first seen, while the recording file sorts them.
    entries: Vec<Entry>,
    index: HashMap<Key, usize>,
}

/// What an entry answers.
#[derive(PartialEq, Eq, Hash)]
enum Key {
    Startup,
    Execute(String, Vec<i16>, Vec<Option<Bytes>>, Vec<i16>),
    /// The statement's text and result formats.
    ExecuteIgnoringParameters(String, Vec<i16>),
    Describe(String, Vec<u32>),
    Query(String),
}

impl Key {
    fn of(entry: &Entry, ignore_parameters: &[String]) -> Key {
        match entry {
            Entry::Startup { .. } => Key::Startup,
            Entry::Execute {
                sql,
                result_formats,
                ..
            } if ignore_parameters
                .iter()
                .any(|prefix| sql.starts_with(prefix.as_str())) =>
            {
                Key::ExecuteIgnoringParameters(sql.clone(), result_formats.clone())
            }
            Entry::Execute {
                sql,
                param_formats,
                params,
                result_formats,
                ..
            } => Key::Execute(
                sql.clone(),
                param_formats.clone(),
                params.clone(),
                result_formats.clone(),
            ),
            Entry::Describe {
                sql, param_types, ..
            } => Key::Describe(sql.clone(), param_types.clone()),
            Entry::Query { sql, .. } => Key::Query(sql.clone()),
        }
    }
}

/// The times an entry has seen.
fn samples(entry: &mut Entry) -> Option<&mut Vec<u32>> {
    match entry {
        Entry::Startup { start_us, .. } => Some(start_us),
        Entry::Execute { exec_us, .. } | Entry::Query { exec_us, .. } => Some(exec_us),
        Entry::Describe { .. } => None,
    }
}

impl Sink {
    pub fn new(ignore_parameters: Vec<String>) -> Self {
        Sink {
            entries: Mutex::default(),
            ignore_parameters,
        }
    }

    pub fn add(&self, mut entry: Entry) {
        // Outside the lock, which every connection of the class takes.
        let key = Key::of(&entry, &self.ignore_parameters);
        let mut guard = self.entries.lock().expect("adding an entry doesn't panic");
        let Entries { entries, index } = &mut *guard;
        match index.get(&key) {
            Some(&at) => {
                if let (Some(kept), Some(new)) = (samples(&mut entries[at]), samples(&mut entry)) {
                    let room = MAX_SAMPLES.saturating_sub(kept.len());
                    kept.extend(new.iter().take(room));
                }
            }
            None => {
                index.insert(key, entries.len());
                entries.push(entry);
            }
        }
    }

    pub fn entries(&self) -> Vec<Entry> {
        self.entries
            .lock()
            .expect("adding an entry doesn't panic")
            .entries
            .clone()
    }
}
