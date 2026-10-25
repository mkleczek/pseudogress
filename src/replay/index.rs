//! A class's recording, indexed for answering without allocating: statements
//! by SQL text, their Executes by a hash of the parameters, and the effect of
//! every answer on the transaction status computed once.

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use bytes::Bytes;
use pgwire::messages::{
    extendedquery::Bind,
    response::TransactionStatus::{self, Error, Idle, Transaction},
};
use rustc_hash::{FxHashMap, FxHasher};

use super::{column, frames::frames};
use crate::{
    config,
    recording::{self, Entry},
};

/// What every connection of a class answers from. Immutable.
pub struct Index {
    pub parameters: Vec<(String, String)>,
    /// Backend start times, in microseconds.
    pub start_us: Vec<u32>,
    pub statements: FxHashMap<Box<str>, Arc<Statement>>,
    pub queries: FxHashMap<Box<str>, Query>,
}

/// What was recorded of one SQL text in the extended protocol.
pub struct Statement {
    pub sql: Box<str>,
    /// The class says the answer doesn't depend on the parameters.
    ignores_parameters: bool,
    /// Candidates by the hash of their parameters and formats.
    executes: FxHashMap<u64, Vec<Arc<Execute>>>,
    /// The first Execute in the recording, which is sorted, per result
    /// formats, for parameters that were never recorded.
    by_result_formats: FxHashMap<Box<[i16]>, Arc<Execute>>,
    pub describes: Vec<Describe>,
}

pub struct Execute {
    param_formats: Box<[i16]>,
    params: Box<[Option<Bytes>]>,
    result_formats: Box<[i16]>,
    pub description: Bytes,
    pub rows: Bytes,
    pub effect: Effect,
    /// COMMIT, ROLLBACK or ABORT: all that a failed transaction still takes.
    pub exit: bool,
    pub exec_us: Box<[u32]>,
}

pub struct Describe {
    pub param_types: Box<[u32]>,
    pub response: Bytes,
}

pub struct Query {
    pub response: Bytes,
    pub effect: Effect,
    pub exec_us: Box<[u32]>,
}

/// What a recorded answer does to the connection.
#[derive(Clone, Copy)]
pub struct Effect {
    /// The transaction status afterwards, by the one before: idle, or in a
    /// transaction.
    after: [TransactionStatus; 2],
    /// The answer has an ErrorResponse.
    pub error: bool,
}

impl Effect {
    fn of(response: &[u8]) -> Result<Effect, &'static str> {
        let mut effect = Effect {
            after: [Idle, Transaction],
            error: false,
        };
        for frame in frames(response) {
            match frame? {
                (b'C', b"BEGIN\0" | b"START TRANSACTION\0") => effect.after = [Transaction; 2],
                (b'C', b"COMMIT\0" | b"ROLLBACK\0") => effect.after = [Idle; 2],
                (b'E', _) => {
                    effect.error = true;
                    for status in &mut effect.after {
                        if *status == Transaction {
                            *status = Error;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(effect)
    }

    /// `status` is idle or in a transaction, not failed.
    pub fn apply(&self, status: TransactionStatus) -> TransactionStatus {
        self.after[(status == Transaction) as usize]
    }
}

impl Statement {
    /// `ignore_parameters` are the class's prefixes of statements that
    /// ignore them, matched once here and not per Execute.
    fn new(sql: &str, ignore_parameters: &[String]) -> Self {
        Statement {
            sql: sql.into(),
            ignores_parameters: ignore_parameters
                .iter()
                .any(|prefix| sql.starts_with(prefix.as_str())),
            executes: FxHashMap::default(),
            by_result_formats: FxHashMap::default(),
            describes: Vec::new(),
        }
    }

    /// The Execute recorded for these parameters, else, with `true`, the
    /// first one in the recording, which is sorted, for these result formats.
    /// A statement that ignores its parameters gets that one, with `false`:
    /// it is not a fallback.
    pub fn execute(&self, bind: &Bind) -> Option<(&Arc<Execute>, bool)> {
        let (param_formats, params, result_formats) = (
            &bind.parameter_format_codes[..],
            &bind.parameters[..],
            &bind.result_column_format_codes[..],
        );
        if self.ignores_parameters {
            return self
                .by_result_formats
                .get(result_formats)
                .map(|execute| (execute, false));
        }
        self.executes
            .get(&key_hash(param_formats, params, result_formats))
            .and_then(|candidates| {
                candidates
                    .iter()
                    .find(|e| e.has_key(param_formats, params, result_formats))
            })
            .map(|exact| (exact, false))
            .or_else(|| {
                let fallback = self.by_result_formats.get(result_formats);
                fallback.map(|fallback| (fallback, true))
            })
    }

    /// The position of the Describe recorded for these parameter types.
    pub fn describe(&self, param_types: &[u32]) -> Option<usize> {
        self.describes
            .iter()
            .position(|d| *d.param_types == *param_types)
    }

    fn add_execute(&mut self, execute: Execute) {
        let candidates = self.executes.entry(execute.key_hash()).or_default();
        // Record mode merges repeats, so a repeat here is not worth a second
        // entry.
        if candidates.iter().any(|c| {
            c.has_key(
                &execute.param_formats,
                &execute.params,
                &execute.result_formats,
            )
        }) {
            return;
        }
        let execute = Arc::new(execute);
        self.by_result_formats
            .entry(execute.result_formats.clone())
            .or_insert_with(|| execute.clone());
        candidates.push(execute);
    }
}

impl Execute {
    fn key_hash(&self) -> u64 {
        key_hash(&self.param_formats, &self.params, &self.result_formats)
    }

    fn has_key(
        &self,
        param_formats: &[i16],
        params: &[Option<Bytes>],
        result_formats: &[i16],
    ) -> bool {
        *self.param_formats == *param_formats
            && *self.params == *params
            && *self.result_formats == *result_formats
    }
}

fn key_hash(param_formats: &[i16], params: &[Option<Bytes>], result_formats: &[i16]) -> u64 {
    let mut hasher = FxHasher::default();
    param_formats.hash(&mut hasher);
    params.hash(&mut hasher);
    result_formats.hash(&mut hasher);
    hasher.finish()
}

/// Whether PostgreSQL still takes the statement in a failed transaction.
pub fn ends_transaction(sql: &str) -> bool {
    let word = sql
        .trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    ["commit", "rollback", "abort", "end"]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
}

/// The start of a statement, for messages.
pub fn excerpt(sql: &str) -> String {
    match sql.char_indices().nth(200) {
        Some((end, _)) => format!("{}...", &sql[..end]),
        None => sql.to_owned(),
    }
}

impl Index {
    pub fn load(class: &config::Class) -> Result<Index, String> {
        let path = &class.recording;
        let context =
            |sql: &str, error: &str| format!("{}: {}: {error}", path.display(), excerpt(sql));
        let mut parameters = None;
        let mut start_us = Vec::new();
        let mut statements: FxHashMap<Box<str>, Statement> = FxHashMap::default();
        let mut queries = FxHashMap::default();
        for entry in recording::read(path)? {
            match entry {
                Entry::Startup {
                    parameters: seen,
                    start_us: times,
                } => {
                    parameters.get_or_insert(seen);
                    start_us.extend(times);
                }
                Entry::Execute {
                    sql,
                    param_formats,
                    params,
                    result_formats,
                    description,
                    rows,
                    exec_us,
                } => {
                    let rows = match &class.column_override {
                        Some(over) => column::apply(over, &description, &result_formats, &rows)
                            .map_err(|e| context(&sql, &e))?
                            .unwrap_or(rows),
                        None => rows,
                    };
                    let effect = Effect::of(&rows).map_err(|e| context(&sql, e))?;
                    let execute = Execute {
                        param_formats: param_formats.into(),
                        params: params.into(),
                        result_formats: result_formats.into(),
                        description,
                        rows,
                        effect,
                        exit: ends_transaction(&sql),
                        exec_us: exec_us.into(),
                    };
                    statements
                        .entry(sql.as_str().into())
                        .or_insert_with(|| Statement::new(&sql, &class.ignore_parameters))
                        .add_execute(execute);
                }
                Entry::Describe {
                    sql,
                    param_types,
                    response,
                } => {
                    let statement = statements
                        .entry(sql.as_str().into())
                        .or_insert_with(|| Statement::new(&sql, &class.ignore_parameters));
                    if statement.describe(&param_types).is_none() {
                        statement.describes.push(Describe {
                            param_types: param_types.into(),
                            response,
                        });
                    }
                }
                Entry::Query {
                    sql,
                    response,
                    exec_us,
                } => {
                    let effect = Effect::of(&response).map_err(|e| context(&sql, e))?;
                    queries.entry(sql.into()).or_insert(Query {
                        response,
                        effect,
                        exec_us: exec_us.into(),
                    });
                }
            }
        }
        let parameters =
            parameters.ok_or_else(|| format!("{}: no Startup entry", path.display()))?;
        Ok(Index {
            parameters,
            start_us,
            statements: statements
                .into_iter()
                .map(|(sql, statement)| (sql, Arc::new(statement)))
                .collect(),
            queries,
        })
    }
}
