//! Record mode: backends that forward to each class's upstream PostgreSQL and
//! record what it answers.
//!
//! A backend sends every batch upstream and appends the answers to the
//! client's output exactly as PostgreSQL sent them. To turn answers into
//! recording entries it follows what the client sent: the statements it
//! parsed and the portals it bound, and which message each answer is for.

use std::{
    collections::{HashMap, VecDeque},
    fs, io,
    mem::take,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use pgwire::messages::{
    Message, PgWireFrontendMessage,
    extendedquery::{
        Bind, Close, Describe, Flush, Parse, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
    },
    response::TransactionStatus,
};

use crate::{
    config::{Config, Upstream},
    recording::{self, Entry},
    session::{Backend, Connector},
    wire,
};

use conn::Conn;
use sink::Sink;

mod conn;
mod sink;
#[cfg(test)]
mod tests;

pub struct Record {
    /// In configuration order, like the classes.
    classes: Vec<Arc<Target>>,
}

/// A class's upstream, and what its connections recorded.
struct Target {
    upstream: Upstream,
    recording: PathBuf,
    sink: Sink,
}

impl Record {
    /// Every class needs an upstream.
    pub fn new(config: &Config) -> Result<Self, String> {
        // Every class writes its recording at shutdown: two on one file would
        // write over each other.
        let mut recordings = HashMap::new();
        for class in &config.classes {
            if let Some(other) = recordings.insert(&class.recording, &class.name) {
                return Err(format!(
                    "class {}: recording {} is also the recording of class {other}",
                    class.name,
                    class.recording.display()
                ));
            }
        }
        let classes = config
            .classes
            .iter()
            .map(|class| {
                let upstream = class.upstream.as_ref().ok_or_else(|| {
                    format!("class {}: record mode needs an upstream", class.name)
                })?;
                Ok(Arc::new(Target {
                    upstream: Upstream {
                        host: upstream.host.clone(),
                        port: upstream.port,
                        user: upstream.user.clone(),
                        password: upstream.password.clone(),
                        database: upstream.database.clone(),
                    },
                    recording: class.recording.clone(),
                    sink: Sink::new(class.ignore_parameters.clone()),
                }))
            })
            .collect::<Result<_, String>>()?;
        Ok(Record { classes })
    }

    /// Writes every class's recording. Called once, at shutdown.
    pub fn write(&self) -> Result<(), String> {
        let errors: Vec<String> = self
            .classes
            .iter()
            .filter_map(|class| class.write().err())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl Target {
    fn write(&self) -> Result<(), String> {
        let entries = self.sink.entries();
        // A class that nobody connected to keeps the recording it has.
        if entries.is_empty() {
            return Ok(());
        }
        if let Some(dir) = self.recording.parent() {
            fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        recording::write(&self.recording, &entries)
    }
}

impl Connector for Record {
    type Backend = Recorder;

    async fn connect(&self, class: usize, database: &str, _user: &str) -> Result<Recorder, String> {
        let target = self.classes[class].clone();
        let mut started = Instant::now();
        let database = target.upstream.database.replace("{database}", database);
        let (conn, parameters) = Conn::open(&target.upstream, &database).await.map_err(|e| {
            format!(
                "upstream {}:{}: {e}",
                target.upstream.host, target.upstream.port
            )
        })?;
        target.sink.add(Entry::Startup {
            parameters: parameters.clone(),
            start_us: vec![lap(&mut started)],
        });
        Ok(Recorder {
            target,
            conn,
            parameters,
            status: TransactionStatus::Idle,
            skipping: false,
            flushed: false,
            broken: false,
            statements: HashMap::new(),
            portals: HashMap::new(),
        })
    }
}

pub struct Recorder {
    target: Arc<Target>,
    conn: Conn,
    parameters: Vec<(String, String)>,
    /// The last one upstream sent, for `cancelled`.
    status: TransactionStatus,
    /// After an error, PostgreSQL ignores the client's messages up to the
    /// next Sync, so they are not sent to it.
    skipping: bool,
    /// Whether the last batch ended in a Flush: its answer has no
    /// ReadyForQuery, and neither has the one to its cancel.
    flushed: bool,
    /// Whether the upstream failed.
    broken: bool,
    /// By name, `None` for the unnamed ones.
    statements: HashMap<Option<String>, Statement>,
    portals: HashMap<Option<String>, Portal>,
}

struct Statement {
    sql: String,
    param_types: Vec<u32>,
}

struct Portal {
    sql: String,
    param_formats: Vec<i16>,
    params: Vec<Option<Bytes>>,
    result_formats: Vec<i16>,
    /// The answer to Describe of the portal, once it has one.
    description: Option<Bytes>,
}

/// What the next answers from upstream are for, in the order of the batch.
enum Expect<'a> {
    Parse(&'a Parse),
    Bind(&'a Bind),
    Close(&'a Close),
    /// With the answer so far: ParameterDescription, then RowDescription or
    /// NoData.
    DescribeStatement(&'a Option<String>, BytesMut),
    /// `true` for a Describe that the client didn't send.
    DescribePortal(&'a Option<String>, bool),
    /// With the rows so far.
    Execute(&'a Option<String>, BytesMut),
    /// With the response so far.
    Query(&'a str, BytesMut),
    Sync,
}

enum Step {
    Wait,
    Done,
    /// PostgreSQL answers nothing else up to the Sync.
    Failed,
}

impl Backend for Recorder {
    fn parameter_statuses(&self) -> &[(String, String)] {
        &self.parameters
    }

    /// Takes no simulated time: it was spent upstream already.
    async fn batch(&mut self, batch: &[PgWireFrontendMessage], out: &mut BytesMut) -> Duration {
        self.flushed = matches!(batch.last(), Some(PgWireFrontendMessage::Flush(_)));
        if let Err(error) = self.forward(batch, out).await {
            wire::put_error(out, "FATAL", "08006", &format!("upstream: {error}"));
            self.broken = true;
        }
        Duration::ZERO
    }

    fn cancelled(&mut self, out: &mut BytesMut) {
        wire::put_error(
            out,
            "ERROR",
            "57014",
            "canceling statement due to user request",
        );
        // The client sends its Sync after the Flush, and that gets the
        // ReadyForQuery. The upstream never saw an error, so what comes
        // before the Sync must not be sent to it.
        self.skipping = self.flushed;
        if !self.flushed {
            wire::put_ready(out, self.status);
        }
    }

    fn broken(&self) -> bool {
        self.broken
    }
}

impl Recorder {
    async fn forward(
        &mut self,
        batch: &[PgWireFrontendMessage],
        out: &mut BytesMut,
    ) -> io::Result<()> {
        let (wire, mut expected) = self.encode(batch)?;
        self.conn.write(&wire).await?;
        // Where the time of the next Execute starts.
        let mut mark = Instant::now();
        while let Some(head) = expected.front_mut() {
            let message = self.conn.raw().await?;
            // These answer nothing.
            if matches!(message[0], b'N' | b'S' | b'A') {
                out.extend_from_slice(&message);
                continue;
            }
            // The client sees what it asked for: no answer to its Describe
            // of a portal it never described, but an error it got.
            if !matches!(head, Expect::DescribePortal(_, true)) || message[0] == b'E' {
                out.extend_from_slice(&message);
            }
            match self.answer(head, &message, &mut mark) {
                Step::Wait => {}
                Step::Done => {
                    expected.pop_front();
                }
                Step::Failed => {
                    expected.retain(|expect| matches!(expect, Expect::Sync));
                    // The next Sync is in a later batch when this one has none.
                    self.skipping = expected.is_empty();
                }
            }
        }
        Ok(())
    }

    /// The batch as it is sent upstream, and what its answers are for.
    ///
    /// An Execute needs the description of its portal in the recording, but
    /// clients like tokio-postgres never describe a portal: it is described
    /// here, and the client doesn't see the answer.
    fn encode<'a>(
        &self,
        batch: &'a [PgWireFrontendMessage],
    ) -> io::Result<(BytesMut, VecDeque<Expect<'a>>)> {
        let mut wire = BytesMut::new();
        let mut expected = VecDeque::new();
        // Whether a portal is described: Bind makes a new one that isn't.
        let mut described: HashMap<&Option<String>, bool> = HashMap::new();
        for message in batch {
            if self.skipping && !matches!(message, PgWireFrontendMessage::Sync(_)) {
                continue;
            }
            match message {
                PgWireFrontendMessage::Parse(parse) => expected.push_back(Expect::Parse(parse)),
                PgWireFrontendMessage::Bind(bind) => {
                    described.insert(&bind.portal_name, false);
                    expected.push_back(Expect::Bind(bind));
                }
                PgWireFrontendMessage::Close(close) => expected.push_back(Expect::Close(close)),
                PgWireFrontendMessage::Describe(describe)
                    if describe.target_type == TARGET_TYPE_BYTE_STATEMENT =>
                {
                    expected.push_back(Expect::DescribeStatement(&describe.name, BytesMut::new()));
                }
                PgWireFrontendMessage::Describe(describe) => {
                    described.insert(&describe.name, true);
                    expected.push_back(Expect::DescribePortal(&describe.name, false));
                }
                PgWireFrontendMessage::Execute(execute) => {
                    let known = described.get(&execute.name).copied().unwrap_or_else(|| {
                        self.portals
                            .get(&execute.name)
                            .is_some_and(|portal| portal.description.is_some())
                    });
                    if !known {
                        Describe::new(TARGET_TYPE_BYTE_PORTAL, execute.name.clone())
                            .encode(&mut wire)
                            .map_err(io::Error::other)?;
                        described.insert(&execute.name, true);
                        expected.push_back(Expect::DescribePortal(&execute.name, true));
                    }
                    expected.push_back(Expect::Execute(&execute.name, BytesMut::new()));
                }
                PgWireFrontendMessage::Query(query) => {
                    expected.push_back(Expect::Query(&query.query, BytesMut::new()));
                }
                PgWireFrontendMessage::Sync(_) => expected.push_back(Expect::Sync),
                _ => {}
            }
            message.encode(&mut wire).map_err(io::Error::other)?;
            if matches!(message, PgWireFrontendMessage::Execute(_)) {
                // PostgreSQL holds its answers back until a Flush or a Sync.
                // Without one, the Executes of a batch would all seem to end
                // together, and only the last one to take any time.
                Flush::new().encode(&mut wire).map_err(io::Error::other)?;
            }
        }
        Ok((wire, expected))
    }

    /// Records what `message` answers, when it completes an entry. `mark`
    /// is when the previous Execute of the batch ended, or the batch was
    /// sent.
    fn answer(&mut self, head: &mut Expect, message: &Bytes, mark: &mut Instant) -> Step {
        match (head, message[0]) {
            (Expect::Query(sql, response), b'Z') => {
                self.ready(message);
                self.target.sink.add(Entry::Query {
                    sql: (*sql).to_owned(),
                    response: take(response).freeze(),
                    exec_us: vec![lap(mark)],
                });
                Step::Done
            }
            (Expect::Query(_, response), _) => {
                response.extend_from_slice(message);
                Step::Wait
            }
            (Expect::Sync, b'Z') => {
                self.ready(message);
                self.skipping = false;
                Step::Done
            }
            (Expect::Execute(_, rows), b'D') => {
                rows.extend_from_slice(message);
                Step::Wait
            }
            // The format has no place for a PortalSuspended answer.
            (Expect::Execute(..), b's') => {
                lap(mark);
                Step::Done
            }
            (Expect::Execute(name, rows), b'C' | b'I' | b'E') => {
                rows.extend_from_slice(message);
                let exec_us = lap(mark);
                if let Some(Portal {
                    sql,
                    param_formats,
                    params,
                    result_formats,
                    description: Some(description),
                }) = self.portals.get(*name)
                {
                    self.target.sink.add(Entry::Execute {
                        sql: sql.clone(),
                        param_formats: param_formats.clone(),
                        params: params.clone(),
                        result_formats: result_formats.clone(),
                        description: description.clone(),
                        rows: take(rows).freeze(),
                        exec_us: vec![exec_us],
                    });
                }
                if message[0] == b'E' {
                    Step::Failed
                } else {
                    Step::Done
                }
            }
            (_, b'E') => Step::Failed,
            (Expect::Parse(parse), b'1') => {
                self.statements.insert(
                    parse.name.clone(),
                    Statement {
                        sql: parse.query.clone(),
                        param_types: parse.type_oids.clone(),
                    },
                );
                Step::Done
            }
            (Expect::Bind(bind), b'2') => {
                if let Some(statement) = self.statements.get(&bind.statement_name) {
                    self.portals.insert(
                        bind.portal_name.clone(),
                        Portal {
                            sql: statement.sql.clone(),
                            param_formats: bind.parameter_format_codes.clone(),
                            params: bind.parameters.clone(),
                            result_formats: bind.result_column_format_codes.clone(),
                            description: None,
                        },
                    );
                }
                Step::Done
            }
            (Expect::Close(close), b'3') => {
                if close.target_type == TARGET_TYPE_BYTE_STATEMENT {
                    self.statements.remove(&close.name);
                } else {
                    self.portals.remove(&close.name);
                }
                Step::Done
            }
            (Expect::DescribeStatement(_, response), b't') => {
                response.extend_from_slice(message);
                Step::Wait
            }
            (Expect::DescribeStatement(name, response), b'T' | b'n') => {
                response.extend_from_slice(message);
                if let Some(statement) = self.statements.get(*name) {
                    self.target.sink.add(Entry::Describe {
                        sql: statement.sql.clone(),
                        param_types: statement.param_types.clone(),
                        response: take(response).freeze(),
                    });
                }
                Step::Done
            }
            (Expect::DescribePortal(name, _), b'T' | b'n') => {
                if let Some(portal) = self.portals.get_mut(*name) {
                    portal.description = Some(message.clone());
                }
                Step::Done
            }
            _ => Step::Wait,
        }
    }

    fn ready(&mut self, message: &Bytes) {
        if let Some(status) = message
            .get(5)
            .and_then(|&status| TransactionStatus::try_from(status).ok())
        {
            self.status = status;
        }
    }
}

/// Microseconds since `*since`, which becomes now.
fn lap(since: &mut Instant) -> u32 {
    let now = Instant::now();
    let elapsed = now.duration_since(*since);
    *since = now;
    u32::try_from(elapsed.as_micros()).unwrap_or(u32::MAX)
}
