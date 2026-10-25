//! Replay mode: backends that answer from the classes' recordings.
//!
//! The recordings are indexed once per class. A connection resolves a
//! statement at Parse and its recorded Execute at Bind, so that answering
//! Execute is a copy of pre-encoded bytes: no lock and no allocation.

use std::{
    borrow::Cow,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use bytes::BytesMut;
use pgwire::messages::{
    PgWireFrontendMessage as Frontend,
    extendedquery::{
        Bind, Close, Describe, Execute as ExecuteMessage, Parse, TARGET_TYPE_BYTE_PORTAL,
        TARGET_TYPE_BYTE_STATEMENT,
    },
    response::TransactionStatus::{self, Error, Idle, Transaction},
};
use rand::{RngExt, rngs::SmallRng};
use rustc_hash::FxHashMap;

use self::index::{Execute, Index, Statement, ends_transaction, excerpt};
use crate::{
    class::Classes,
    config::{Config, Execution, Settings},
    session::{Backend, Connector},
    wire::{put_error, put_ready},
};

mod column;
mod frames;
mod index;
#[cfg(test)]
mod tests;

// Messages without a body, and what a failed transaction answers COMMIT with.
const PARSE_COMPLETE: &[u8] = b"1\0\0\0\x04";
const BIND_COMPLETE: &[u8] = b"2\0\0\0\x04";
const CLOSE_COMPLETE: &[u8] = b"3\0\0\0\x04";
const EMPTY_QUERY_RESPONSE: &[u8] = b"I\0\0\0\x04";
const ROLLBACK_COMPLETE: &[u8] = b"C\0\0\0\x0dROLLBACK\0";

pub struct Replay {
    classes: Classes,
    /// By class.
    indexes: Vec<Arc<Index>>,
}

impl Replay {
    /// Reads and indexes every class's recording.
    pub fn load(config: &Config, classes: Classes) -> Result<Self, String> {
        let indexes = config
            .classes
            .iter()
            .map(|class| Index::load(class).map(Arc::new))
            .collect::<Result<_, _>>()?;
        Ok(Replay { classes, indexes })
    }
}

impl Connector for Replay {
    type Backend = Replayer;

    async fn connect(
        &self,
        class: usize,
        _database: &str,
        _user: &str,
    ) -> Result<Replayer, String> {
        let index = self.indexes[class].clone();
        let mut rng: SmallRng = rand::make_rng();
        let start = scaled(
            sample(&mut rng, &index.start_us),
            self.classes[class].settings.load().execution_scale,
        );
        if !start.is_zero() {
            tokio::time::sleep(start).await;
        }
        Ok(Replayer {
            classes: self.classes.clone(),
            class,
            index,
            rng,
            status: Idle,
            skipping: false,
            flushed: false,
            statements: Named::default(),
            portals: Named::default(),
        })
    }
}

pub struct Replayer {
    classes: Classes,
    class: usize,
    index: Arc<Index>,
    rng: SmallRng,
    status: TransactionStatus,
    /// After an error, a backend ignores messages up to the next Sync.
    skipping: bool,
    /// Whether the last batch ended in a Flush: its answer has no
    /// ReadyForQuery, and neither has the one to its cancel.
    flushed: bool,
    statements: Named<Prepared>,
    portals: Named<Arc<Execute>>,
}

struct Prepared {
    statement: Arc<Statement>,
    /// The recorded Describe for the parameter types of the Parse.
    describe: Option<usize>,
}

/// Statements or portals by name. The unnamed one gets a field of its own,
/// since it is by far the most used.
struct Named<T> {
    unnamed: Option<T>,
    named: FxHashMap<String, T>,
}

impl<T> Default for Named<T> {
    fn default() -> Self {
        Named {
            unnamed: None,
            named: FxHashMap::default(),
        }
    }
}

impl<T> Named<T> {
    /// Both no name and an empty one are the unnamed.
    fn get(&self, name: &Option<String>) -> Option<&T> {
        match name.as_deref() {
            None | Some("") => self.unnamed.as_ref(),
            Some(name) => self.named.get(name),
        }
    }

    fn insert(&mut self, name: &Option<String>, value: T) {
        match name.as_deref() {
            None | Some("") => self.unnamed = Some(value),
            Some(name) => match self.named.get_mut(name) {
                Some(slot) => *slot = value,
                None => {
                    self.named.insert(name.to_owned(), value);
                }
            },
        }
    }

    fn remove(&mut self, name: &Option<String>) {
        match name.as_deref() {
            None | Some("") => self.unnamed = None,
            Some(name) => {
                self.named.remove(name);
            }
        }
    }
}

/// An ErrorResponse, and what ends up in it.
struct Failure {
    code: &'static str,
    message: Cow<'static, str>,
}

const ABORTED: Failure = Failure {
    code: "25P02",
    message: Cow::Borrowed(
        "current transaction is aborted, commands ignored until end of transaction block",
    ),
};

const UNSUPPORTED: Failure = Failure {
    code: "0A000",
    message: Cow::Borrowed("pseudogress: this message is not supported"),
};

impl Failure {
    fn missing(kind: &str, name: &Option<String>, code: &'static str) -> Failure {
        let name = name.as_deref().unwrap_or_default();
        Failure {
            code,
            message: format!("{kind} \"{name}\" does not exist").into(),
        }
    }
}

impl Backend for Replayer {
    fn parameter_statuses(&self) -> &[(String, String)] {
        &self.index.parameters
    }

    async fn batch(&mut self, batch: &[Frontend], out: &mut BytesMut) -> Duration {
        let timing = Timing::of(&self.classes[self.class].settings.load());
        let mut time = Duration::ZERO;
        self.flushed = matches!(batch.last(), Some(Frontend::Flush(_)));
        for message in batch {
            if self.skipping && !matches!(message, Frontend::Sync(_)) {
                continue;
            }
            if let Err(failure) = self.message(message, out, &timing, &mut time) {
                self.fail(failure, out);
                self.skipping = true;
            }
        }
        time
    }

    fn cancelled(&mut self, out: &mut BytesMut) {
        put_error(
            out,
            "ERROR",
            "57014",
            "canceling statement due to user request",
        );
        if self.status != Idle {
            self.status = Error;
        }
        // The client sends its Sync after the Flush, and that gets the
        // ReadyForQuery.
        self.skipping = self.flushed;
        if !self.flushed {
            put_ready(out, self.status);
        }
    }
}

impl Replayer {
    /// Appends the answer to one message. `time` adds up execution times.
    fn message(
        &mut self,
        message: &Frontend,
        out: &mut BytesMut,
        timing: &Timing,
        time: &mut Duration,
    ) -> Result<(), Failure> {
        match message {
            Frontend::Parse(parse) => self.parse(parse, out),
            Frontend::Bind(bind) => self.bind(bind, out),
            Frontend::Describe(describe) => self.describe(describe, out),
            Frontend::Execute(execute) => self.execute(execute, out, timing, time),
            Frontend::Close(close) => self.close(close, out),
            Frontend::Flush(_) => Ok(()),
            Frontend::Sync(_) => {
                self.skipping = false;
                put_ready(out, self.status);
                Ok(())
            }
            Frontend::Query(query) => {
                // Whatever happens, a simple query ends in ReadyForQuery, so
                // its error does not skip anything.
                if let Err(failure) = self.query(&query.query, out, timing, time) {
                    self.fail(failure, out);
                }
                put_ready(out, self.status);
                Ok(())
            }
            _ => Err(UNSUPPORTED),
        }
    }

    /// An error fails the transaction it happens in.
    fn fail(&mut self, failure: Failure, out: &mut BytesMut) {
        put_error(out, "ERROR", failure.code, &failure.message);
        if self.status == Transaction {
            self.status = Error;
        }
    }

    /// Counts a statement without a recording. There is no default answer: a
    /// load test that misses has not measured what it was meant to.
    fn miss(&self, sql: &str) -> Failure {
        self.classes[self.class]
            .metrics
            .misses
            .fetch_add(1, Relaxed);
        Failure {
            code: "XX000",
            message: format!("pseudogress: no recording of: {}", excerpt(sql)).into(),
        }
    }

    fn parse(&mut self, parse: &Parse, out: &mut BytesMut) -> Result<(), Failure> {
        // PostgreSQL refuses a statement in a failed transaction when it is
        // parsed, not only when it is executed.
        if self.status == Error && !ends_transaction(&parse.query) {
            return Err(ABORTED);
        }
        let statement = self
            .index
            .statements
            .get(parse.query.as_str())
            .ok_or_else(|| self.miss(&parse.query))?;
        // Only the unnamed statement is replaced without a Close.
        if let Some(name) = parse.name.as_deref().filter(|name| !name.is_empty())
            && self.statements.named.contains_key(name)
        {
            return Err(Failure {
                code: "42P05",
                message: format!("prepared statement \"{name}\" already exists").into(),
            });
        }
        let prepared = Prepared {
            describe: statement.describe(&parse.type_oids),
            statement: statement.clone(),
        };
        self.statements.insert(&parse.name, prepared);
        out.extend_from_slice(PARSE_COMPLETE);
        Ok(())
    }

    fn bind(&mut self, bind: &Bind, out: &mut BytesMut) -> Result<(), Failure> {
        let prepared = self
            .statements
            .get(&bind.statement_name)
            .ok_or_else(|| Failure::missing("prepared statement", &bind.statement_name, "26000"))?;
        let (execute, fallback) = prepared
            .statement
            .execute(bind)
            .ok_or_else(|| self.miss(&prepared.statement.sql))?;
        if self.status == Error && !execute.exit {
            return Err(ABORTED);
        }
        if fallback {
            self.classes[self.class]
                .metrics
                .fallbacks
                .fetch_add(1, Relaxed);
        }
        self.portals.insert(&bind.portal_name, execute.clone());
        out.extend_from_slice(BIND_COMPLETE);
        Ok(())
    }

    fn describe(&self, describe: &Describe, out: &mut BytesMut) -> Result<(), Failure> {
        match describe.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                let prepared = self.statements.get(&describe.name).ok_or_else(|| {
                    Failure::missing("prepared statement", &describe.name, "26000")
                })?;
                let recorded = prepared
                    .describe
                    .ok_or_else(|| self.miss(&prepared.statement.sql))?;
                out.extend_from_slice(&prepared.statement.describes[recorded].response);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                let execute = self
                    .portals
                    .get(&describe.name)
                    .ok_or_else(|| Failure::missing("portal", &describe.name, "34000"))?;
                out.extend_from_slice(&execute.description);
            }
            _ => return Err(UNSUPPORTED),
        }
        Ok(())
    }

    fn execute(
        &mut self,
        execute: &ExecuteMessage,
        out: &mut BytesMut,
        timing: &Timing,
        time: &mut Duration,
    ) -> Result<(), Failure> {
        let recorded = self
            .portals
            .get(&execute.name)
            .ok_or_else(|| Failure::missing("portal", &execute.name, "34000"))?;
        if self.status == Error {
            if !recorded.exit {
                return Err(ABORTED);
            }
            out.extend_from_slice(ROLLBACK_COMPLETE);
            self.status = Idle;
            return Ok(());
        }
        out.extend_from_slice(&recorded.rows);
        *time += timing.statement(&mut self.rng, &recorded.exec_us, recorded.rows.len());
        self.status = recorded.effect.apply(self.status);
        // A recorded error is part of the answer already.
        self.skipping = recorded.effect.error;
        Ok(())
    }

    fn close(&mut self, close: &Close, out: &mut BytesMut) -> Result<(), Failure> {
        match close.target_type {
            TARGET_TYPE_BYTE_STATEMENT => self.statements.remove(&close.name),
            TARGET_TYPE_BYTE_PORTAL => self.portals.remove(&close.name),
            _ => return Err(UNSUPPORTED),
        }
        out.extend_from_slice(CLOSE_COMPLETE);
        Ok(())
    }

    fn query(
        &mut self,
        sql: &str,
        out: &mut BytesMut,
        timing: &Timing,
        time: &mut Duration,
    ) -> Result<(), Failure> {
        if sql.trim().is_empty() {
            out.extend_from_slice(EMPTY_QUERY_RESPONSE);
            return Ok(());
        }
        if self.status == Error {
            if !ends_transaction(sql) {
                return Err(ABORTED);
            }
            out.extend_from_slice(ROLLBACK_COMPLETE);
            self.status = Idle;
            return Ok(());
        }
        let query = self.index.queries.get(sql).ok_or_else(|| self.miss(sql))?;
        out.extend_from_slice(&query.response);
        *time += timing.statement(&mut self.rng, &query.exec_us, query.response.len());
        self.status = query.effect.apply(self.status);
        Ok(())
    }
}

/// One of the recorded times, in microseconds; 0 without any.
fn sample(rng: &mut SmallRng, times: &[u32]) -> u64 {
    match times {
        [] => 0,
        [only] => u64::from(*only),
        _ => u64::from(times[rng.random_range(..times.len())]),
    }
}

/// A negative or NaN scale gives no time.
fn scaled(micros: u64, scale: f64) -> Duration {
    Duration::from_nanos((micros as f64 * 1000.0 * scale) as u64)
}

/// The settings that decide how long a statement takes, which a batch reads
/// once.
#[derive(Clone, Copy)]
struct Timing {
    scale: f64,
    execution: Option<Execution>,
    per_mib: Duration,
}

impl Timing {
    fn of(settings: &Settings) -> Self {
        Timing {
            scale: settings.execution_scale,
            execution: settings.execution,
            per_mib: settings.execution_per_mib,
        }
    }

    /// What a statement takes: its recorded time scaled, a draw from the
    /// execution model, and a time per MiB of its `answer` bytes.
    fn statement(&self, rng: &mut SmallRng, recorded_us: &[u32], answer: usize) -> Duration {
        const MIB: f64 = 1024.0 * 1024.0;
        let mut time = scaled(sample(rng, recorded_us), self.scale);
        if let Some(execution) = self.execution {
            time += log_normal(rng, execution);
        }
        time + Duration::from_nanos((self.per_mib.as_nanos() as f64 * answer as f64 / MIB) as u64)
    }
}

/// A draw from the log-normal distribution with this median and 99th
/// percentile.
fn log_normal(rng: &mut SmallRng, Execution { median, p99 }: Execution) -> Duration {
    // The 99th percentile of the standard normal distribution.
    const Z99: f64 = 2.3263;
    let (median, p99) = (median.as_secs_f64(), p99.as_secs_f64());
    let sigma = (p99 / median).ln() / Z99;
    // Box-Muller; 1 - u is in (0, 1], so its logarithm is finite.
    let (u, v): (f64, f64) = (rng.random(), rng.random());
    let z = (-2.0 * (1.0 - u).ln()).sqrt() * (std::f64::consts::TAU * v).cos();
    // exp(mu + sigma * z), where mu is the logarithm of the median.
    Duration::from_nanos((median * 1e9 * (sigma * z).exp()) as u64)
}
