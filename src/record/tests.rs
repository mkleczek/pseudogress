//! Tests against a real PostgreSQL. Each starts a throwaway cluster with
//! `initdb`, and returns at once when it isn't on the PATH (outside the nix
//! shell).

use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use pgwire::messages::{PgWireFrontendMessage as Frontend, extendedquery, simplequery::Query};

use super::*;
use crate::{
    config::Config,
    recording::{self, Entry},
    session::{Backend, Connector},
};

/// A PostgreSQL that is stopped and deleted when dropped.
pub(crate) struct Cluster {
    dir: PathBuf,
    pub(crate) port: u16,
    /// Only one cluster runs at a time: every PostgreSQL takes a System V
    /// shared memory segment, and macOS allows 32 of them in all, many taken
    /// by other PostgreSQLs on a development machine.
    _exclusive: MutexGuard<'static, ()>,
}

impl Cluster {
    /// Trusts every user unless `hba` is the content of pg_hba.conf. `None`
    /// when PostgreSQL isn't installed.
    pub(crate) fn start(hba: Option<&str>) -> Option<Cluster> {
        if Command::new("initdb")
            .arg("--version")
            .stdout(Stdio::null())
            .status()
            .is_err()
        {
            eprintln!("initdb is not on the PATH: run the tests in nix-shell");
            return None;
        }
        static EXCLUSIVE: Mutex<()> = Mutex::new(());
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let exclusive = EXCLUSIVE.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = std::env::temp_dir().join(format!(
            "pseudogress-record-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        // Dropping it cleans up, even when the start fails.
        let mut cluster = Cluster {
            dir,
            port: free_port(),
            _exclusive: exclusive,
        };
        let data = cluster.dir.join("data");
        run(Command::new("initdb")
            .args(["--no-sync", "-U", "postgres", "--auth=trust", "-D"])
            .arg(&data));
        if let Some(hba) = hba {
            fs::write(data.join("pg_hba.conf"), hba).unwrap();
        }
        // Another process can take the port before PostgreSQL binds it: then
        // the start fails, and the next attempt uses another port.
        for attempt in 1..=5 {
            let started = try_run(
                Command::new("pg_ctl")
                    .arg("-D")
                    .arg(&data)
                    .arg("-l")
                    .arg(cluster.dir.join("log"))
                    .arg("-o")
                    .arg(format!(
                        "-p {} -k {} -c listen_addresses=127.0.0.1 -c fsync=off",
                        cluster.port,
                        cluster.dir.display()
                    ))
                    .args(["-w", "start"]),
            );
            match started {
                Ok(()) => break,
                Err(error) if attempt == 5 => panic!("{error}"),
                Err(_) => cluster.port = free_port(),
            }
        }
        Some(cluster)
    }

    fn psql(&self, sql: &str) {
        run(Command::new("psql")
            .args([
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                "127.0.0.1",
                "-U",
                "postgres",
            ])
            .args(["-p", &self.port.to_string(), "-d", "postgres", "-c", sql]));
    }

    /// A class `app` that connects to this cluster as `user`.
    fn record(&self, user: &str, password: Option<&str>) -> Record {
        self.record_with(user, password, "")
    }

    /// `record`, with more TOML of the class.
    fn record_with(&self, user: &str, password: Option<&str>, class: &str) -> Record {
        let password = password
            .map(|password| format!(", password = \"{password}\""))
            .unwrap_or_default();
        let config: Config = toml::from_str(&format!(
            r#"
            listen = []
            [[class]]
            name = "app"
            databases = "*"
            users = ["client"]
            recording = "{}"
            upstream = {{ host = "127.0.0.1", port = {}, user = "{user}", database = "postgres"{password} }}
            {class}
            "#,
            self.dir.join("app.jsonl").display(),
            self.port
        ))
        .unwrap();
        Record::new(&config).unwrap()
    }

    /// The recording that `record` writes.
    fn recording(&self, record: &Record) -> Vec<Entry> {
        record.write().unwrap();
        recording::read(&self.dir.join("app.jsonl")).unwrap()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new("pg_ctl")
            .args(["-m", "immediate", "stop", "-D"])
            .arg(self.dir.join("data"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// The port is free only until something takes it: `Cluster::start` retries
/// on another one when that happens first.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port exists")
        .port()
}

fn run(command: &mut Command) {
    try_run(command).unwrap_or_else(|error| panic!("{error}"));
}

fn try_run(command: &mut Command) -> Result<(), String> {
    let output = command.output().expect("the command can be started");
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "{command:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    ))
}

async fn connect(record: &Record) -> Recorder {
    record.connect(0, "app", "client").await.unwrap()
}

/// Sends a batch, and returns what the client would get.
async fn send(recorder: &mut Recorder, batch: Vec<Frontend>) -> BytesMut {
    let mut out = BytesMut::new();
    tokio::time::timeout(Duration::from_secs(10), recorder.batch(&batch, &mut out))
        .await
        .expect("the batch is answered");
    out
}

/// The type bytes of backend messages: `1`, `2`, `T`, `D`, `C`, `Z`...
fn tags(mut messages: &[u8]) -> String {
    let mut tags = String::new();
    while !messages.is_empty() {
        tags.push(messages[0] as char);
        let length = u32::from_be_bytes(messages[1..5].try_into().unwrap()) as usize;
        messages = &messages[1 + length..];
    }
    tags
}

/// The only entry of the recording that `is`. The recording is sorted by what
/// the entries say, not by when they were first seen, so tests can't find
/// them by position.
fn the(entries: &[Entry], is: impl Fn(&Entry) -> bool) -> &Entry {
    let mut found = entries.iter().filter(|entry| is(entry));
    let entry = found.next().expect("the recording has the entry");
    assert!(found.next().is_none(), "more than one entry: {entries:?}");
    entry
}

fn is_startup(entry: &Entry) -> bool {
    matches!(entry, Entry::Startup { .. })
}

fn is_execute(entry: &Entry) -> bool {
    matches!(entry, Entry::Execute { .. })
}

fn name(name: &str) -> Option<String> {
    (!name.is_empty()).then(|| name.to_owned())
}

fn query(sql: &str) -> Frontend {
    Frontend::Query(Query::new(sql.to_owned()))
}

fn parse(statement: &str, sql: &str) -> Frontend {
    Frontend::Parse(extendedquery::Parse::new(
        name(statement),
        sql.to_owned(),
        Vec::new(),
    ))
}

/// Binds to the unnamed portal.
fn bind(statement: &str, params: &[&[u8]], binary: bool) -> Frontend {
    let format = i16::from(binary);
    Frontend::Bind(extendedquery::Bind::new(
        None,
        name(statement),
        vec![format],
        params
            .iter()
            .map(|param| Some(Bytes::copy_from_slice(param)))
            .collect(),
        vec![format],
    ))
}

fn describe(kind: u8, target: &str) -> Frontend {
    Frontend::Describe(extendedquery::Describe::new(kind, name(target)))
}

/// Executes the unnamed portal.
fn execute() -> Frontend {
    Frontend::Execute(extendedquery::Execute::new(None, 0))
}

fn sync() -> Frontend {
    Frontend::Sync(extendedquery::Sync::new())
}

fn flush() -> Frontend {
    Frontend::Flush(extendedquery::Flush::new())
}

#[tokio::test]
async fn a_simple_query_is_recorded_as_a_query_entry() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;
    assert!(
        recorder
            .parameter_statuses()
            .iter()
            .any(|(name, _)| name == "server_version")
    );

    let out = send(&mut recorder, vec![query("select 1")]).await;

    assert_eq!(tags(&out), "TDCZ");
    let entries = cluster.recording(&record);
    assert_eq!(entries.len(), 2);
    the(&entries, is_startup);
    let query = the(&entries, |entry| matches!(entry, Entry::Query { .. }));
    let Entry::Query {
        sql,
        response,
        exec_us,
    } = query
    else {
        panic!("{query:?}");
    };
    assert_eq!(sql, "select 1");
    // Everything before ReadyForQuery, as PostgreSQL sent it.
    assert_eq!(response[..], out[..out.len() - 6]);
    assert_eq!(exec_us.len(), 1);
}

#[tokio::test]
async fn an_execute_is_recorded_with_its_params_and_description() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    let out = send(
        &mut recorder,
        vec![
            parse("", "select $1::int4 + 1"),
            bind("", &[&[0, 0, 0, 41]], true),
            describe(b'P', ""),
            execute(),
            sync(),
        ],
    )
    .await;

    assert_eq!(tags(&out), "12TDCZ");
    let entries = cluster.recording(&record);
    assert_eq!(entries.len(), 2);
    let execute = the(&entries, is_execute);
    let Entry::Execute {
        sql,
        param_formats,
        params,
        result_formats,
        description,
        rows,
        exec_us,
    } = execute
    else {
        panic!("{execute:?}");
    };
    assert_eq!(sql, "select $1::int4 + 1");
    assert_eq!(param_formats, &[1]);
    assert_eq!(params, &[Some(Bytes::from_static(&[0, 0, 0, 41]))]);
    assert_eq!(result_formats, &[1]);
    assert_eq!(tags(description), "T");
    // A DataRow with the int4 42, then CommandComplete.
    assert!(rows.starts_with(&[b'D', 0, 0, 0, 14, 0, 1, 0, 0, 0, 4, 0, 0, 0, 42]));
    assert_eq!(tags(rows), "DC");
    assert_eq!(exec_us.len(), 1);
}

#[tokio::test]
async fn an_execute_without_describe_portal_still_gets_its_description() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    // What tokio-postgres sends: no Describe of the portal.
    let out = send(
        &mut recorder,
        vec![
            parse("", "select $1::text"),
            bind("", &[b"a"], false),
            execute(),
            // Rebinding the unnamed portal must not mix the two Executes up.
            bind("", &[b"b"], false),
            execute(),
            sync(),
        ],
    )
    .await;

    // The client doesn't see the answers to the Describe it never sent.
    assert_eq!(tags(&out), "12DC2DCZ");
    let entries = cluster.recording(&record);
    assert_eq!(entries.len(), 3);
    for value in [b"a", b"b"] {
        let params = [Some(Bytes::from_static(value))];
        let entry = the(
            &entries,
            |entry| matches!(entry, Entry::Execute { params: seen, .. } if *seen == params),
        );
        let Entry::Execute {
            description, rows, ..
        } = entry
        else {
            panic!("{entry:?}");
        };
        assert_eq!(tags(description), "T");
        assert_eq!(tags(rows), "DC");
    }
}

#[tokio::test]
async fn statements_are_tracked_across_batches() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    let described = send(
        &mut recorder,
        vec![parse("s1", "select $1::int4"), describe(b'S', "s1"), sync()],
    )
    .await;
    let executed = send(
        &mut recorder,
        vec![bind("s1", &[b"7"], false), execute(), sync()],
    )
    .await;

    assert_eq!(tags(&described), "1tTZ");
    assert_eq!(tags(&executed), "2DCZ");
    let entries = cluster.recording(&record);
    assert_eq!(entries.len(), 3);
    let describe = the(&entries, |entry| matches!(entry, Entry::Describe { .. }));
    let Entry::Describe {
        sql,
        param_types,
        response,
    } = describe
    else {
        panic!("{describe:?}");
    };
    assert_eq!(sql, "select $1::int4");
    assert!(param_types.is_empty());
    assert_eq!(tags(response), "tT");
    let execute = the(&entries, is_execute);
    let Entry::Execute { sql, params, .. } = execute else {
        panic!("{execute:?}");
    };
    assert_eq!(sql, "select $1::int4");
    assert_eq!(params, &[Some(Bytes::from_static(b"7"))]);
}

#[tokio::test]
async fn an_error_response_is_recorded_in_the_rows() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    let extended = send(
        &mut recorder,
        vec![
            parse("", "select 1 / x from generate_series(0, 0) x"),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;
    let simple = send(&mut recorder, vec![query("select 1 / 0")]).await;
    let after = send(&mut recorder, vec![query("select 1")]).await;

    assert_eq!(tags(&extended), "12EZ");
    assert_eq!(tags(&simple), "EZ");
    assert_eq!(tags(&after), "TDCZ");
    let entries = cluster.recording(&record);
    assert_eq!(entries.len(), 4);
    let execute = the(&entries, is_execute);
    let Entry::Execute {
        description, rows, ..
    } = execute
    else {
        panic!("{execute:?}");
    };
    assert_eq!(tags(description), "T");
    assert_eq!(tags(rows), "E");
    assert!(String::from_utf8_lossy(rows).contains("22012"));
    let query = the(
        &entries,
        |entry| matches!(entry, Entry::Query { sql, .. } if sql == "select 1 / 0"),
    );
    let Entry::Query { response, .. } = query else {
        panic!("{query:?}");
    };
    assert_eq!(tags(response), "E");
}

#[tokio::test]
async fn a_flush_batch_ends_when_every_message_is_answered() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    let described = send(
        &mut recorder,
        vec![
            parse("", "select 1"),
            bind("", &[], false),
            describe(b'P', ""),
            flush(),
        ],
    )
    .await;
    let executed = send(&mut recorder, vec![execute(), flush()]).await;
    let synced = send(&mut recorder, vec![sync()]).await;
    // The Execute fails, so the Flush that follows is never answered.
    let failed = send(
        &mut recorder,
        vec![
            parse("", "select 1 / x from generate_series(0, 0) x"),
            bind("", &[], false),
            execute(),
            flush(),
        ],
    )
    .await;
    let recovered = send(&mut recorder, vec![sync()]).await;

    assert_eq!(tags(&described), "12T");
    assert_eq!(tags(&executed), "DC");
    assert_eq!(tags(&synced), "Z");
    assert_eq!(tags(&failed), "12E");
    assert_eq!(tags(&recovered), "Z");
    let entries = cluster.recording(&record);
    assert_eq!(entries.len(), 3);
    let execute = the(
        &entries,
        |entry| matches!(entry, Entry::Execute { sql, .. } if sql == "select 1"),
    );
    let Entry::Execute {
        description, rows, ..
    } = execute
    else {
        panic!("{execute:?}");
    };
    assert_eq!(tags(description), "T");
    assert_eq!(tags(rows), "DC");
}

#[tokio::test]
async fn after_an_error_without_a_sync_the_messages_up_to_the_sync_are_ignored() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    let failed = send(
        &mut recorder,
        vec![
            parse("", "select 1 / x from generate_series(0, 0) x"),
            bind("", &[], false),
            execute(),
            flush(),
        ],
    )
    .await;
    // PostgreSQL ignores these, and so answers only the Sync.
    let ignored = send(
        &mut recorder,
        vec![
            parse("", "select 2"),
            bind("", &[], false),
            execute(),
            flush(),
        ],
    )
    .await;
    let recovered = send(
        &mut recorder,
        vec![
            parse("", "select 3"),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;
    let after = send(
        &mut recorder,
        vec![
            parse("", "select 4"),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;

    assert_eq!(tags(&failed), "12E");
    assert_eq!(tags(&ignored), "");
    assert_eq!(tags(&recovered), "Z");
    assert_eq!(tags(&after), "12DCZ");
    let entries = cluster.recording(&record);
    // The startup, the failed Execute and the last one.
    assert_eq!(entries.len(), 3);
    the(
        &entries,
        |entry| matches!(entry, Entry::Execute { sql, .. } if sql == "select 4"),
    );
}

#[tokio::test]
async fn repeated_statements_only_add_samples() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    for _ in 0..2 {
        let mut recorder = connect(&record).await;
        for _ in 0..2 {
            send(&mut recorder, vec![query("select 1")]).await;
            for param in [b"1", b"2"] {
                send(
                    &mut recorder,
                    vec![
                        parse("", "select $1::int4"),
                        bind("", &[param], false),
                        execute(),
                        sync(),
                    ],
                )
                .await;
            }
        }
    }

    let entries = cluster.recording(&record);

    // Every key once.
    assert_eq!(entries.len(), 4);
    let startup = the(&entries, is_startup);
    let Entry::Startup {
        parameters,
        start_us,
    } = startup
    else {
        panic!("{startup:?}");
    };
    assert!(parameters.iter().any(|(name, _)| name == "server_version"));
    assert_eq!(start_us.len(), 2);
    let query = the(&entries, |entry| matches!(entry, Entry::Query { .. }));
    let Entry::Query { exec_us, .. } = query else {
        panic!("{query:?}");
    };
    assert_eq!(exec_us.len(), 4);
    let executes: Vec<_> = entries.iter().filter(|entry| is_execute(entry)).collect();
    assert_eq!(executes.len(), 2);
    for entry in executes {
        let Entry::Execute { exec_us, .. } = entry else {
            panic!("{entry:?}");
        };
        assert_eq!(exec_us.len(), 4);
    }
}

#[tokio::test]
async fn every_execute_of_a_batch_gets_its_own_time() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;

    // PostgreSQL answers a batch at its Sync, unless asked to flush: the
    // second Execute would seem to take no time.
    send(
        &mut recorder,
        vec![
            parse("", "select pg_sleep(0.05)"),
            bind("", &[], false),
            execute(),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;

    let entries = cluster.recording(&record);
    let execute = the(&entries, is_execute);
    let Entry::Execute { exec_us, .. } = execute else {
        panic!("{execute:?}");
    };
    assert_eq!(exec_us.len(), 2);
    assert!(exec_us.iter().all(|&us| us >= 50_000), "{exec_us:?}");
}

#[tokio::test]
async fn cancelled_answers_57014_and_the_status_upstream_had() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;
    send(&mut recorder, vec![query("begin")]).await;

    let mut out = BytesMut::new();
    recorder.cancelled(&mut out);

    assert_eq!(tags(&out), "EZ");
    assert!(String::from_utf8_lossy(&out).contains("57014"));
    assert_eq!(out[out.len() - 1], b'T');
}

#[tokio::test]
async fn cancelled_after_a_flush_batch_answers_no_ready_for_query_and_skips_to_the_sync() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;
    let executed = send(
        &mut recorder,
        vec![
            parse("", "select 1"),
            bind("", &[], false),
            execute(),
            flush(),
        ],
    )
    .await;

    let mut out = BytesMut::new();
    recorder.cancelled(&mut out);
    // The client has not sent its Sync yet: it gets its only ReadyForQuery
    // for that, and nothing for the messages before it.
    let ignored = send(
        &mut recorder,
        vec![
            parse("", "select 2"),
            bind("", &[], false),
            execute(),
            flush(),
        ],
    )
    .await;
    let synced = send(
        &mut recorder,
        vec![
            parse("", "select 3"),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;
    let after = send(
        &mut recorder,
        vec![
            parse("", "select 4"),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;

    assert_eq!(tags(&executed), "12DC");
    assert_eq!(tags(&out), "E");
    assert!(String::from_utf8_lossy(&out).contains("57014"));
    assert_eq!(tags(&ignored), "");
    assert_eq!(tags(&synced), "Z");
    assert_eq!(tags(&after), "12DCZ");
}

#[tokio::test]
async fn cancelled_answers_a_ready_for_query_when_the_batch_ended_in_a_sync() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;
    // The Flush batch before it does not matter.
    send(
        &mut recorder,
        vec![
            parse("", "select 1"),
            bind("", &[], false),
            execute(),
            flush(),
        ],
    )
    .await;
    send(&mut recorder, vec![sync()]).await;
    send(
        &mut recorder,
        vec![
            parse("", "select 1"),
            bind("", &[], false),
            execute(),
            sync(),
        ],
    )
    .await;

    let mut out = BytesMut::new();
    recorder.cancelled(&mut out);

    assert_eq!(tags(&out), "EZ");
}

#[tokio::test]
async fn an_upstream_that_fails_breaks_the_connection() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record("postgres", None);
    let mut recorder = connect(&record).await;
    send(&mut recorder, vec![query("select 1")]).await;
    assert!(!recorder.broken());

    // The recorder's is the only other connection. It is gone when the call
    // returns.
    cluster.psql(
        "select pg_terminate_backend(pid, 5000) from pg_stat_activity
         where backend_type = 'client backend' and pid <> pg_backend_pid()",
    );
    let out = send(&mut recorder, vec![query("select 1")]).await;

    let error = String::from_utf8_lossy(&out);
    assert!(
        error.contains("FATAL") && error.contains("08006"),
        "{error}"
    );
    assert!(recorder.broken());
}

#[tokio::test]
async fn upstream_passwords_are_answered() {
    let hba = "\
        host all postgres 127.0.0.1/32 trust\n\
        host all scram_user 127.0.0.1/32 scram-sha-256\n\
        host all md5_user 127.0.0.1/32 md5\n\
        host all clear_user 127.0.0.1/32 password\n";
    let Some(cluster) = Cluster::start(Some(hba)) else {
        return;
    };
    cluster.psql(
        "create role scram_user login password 'scram-secret';
         create role clear_user login password 'clear-secret';
         set password_encryption = 'md5';
         create role md5_user login password 'md5-secret';",
    );

    for (user, password) in [
        ("scram_user", "scram-secret"),
        ("md5_user", "md5-secret"),
        ("clear_user", "clear-secret"),
    ] {
        let record = cluster.record(user, Some(password));
        let mut recorder = connect(&record).await;
        let out = send(&mut recorder, vec![query("select 1")]).await;
        assert_eq!(tags(&out), "TDCZ", "{user}");
    }

    let wrong = cluster.record("scram_user", Some("not the password"));
    let error = wrong.connect(0, "app", "client").await.err().unwrap();
    assert!(error.contains("password authentication failed"), "{error}");
    let missing = cluster.record("scram_user", None);
    let error = missing.connect(0, "app", "client").await.err().unwrap();
    assert!(error.contains("password"), "{error}");
}

#[tokio::test]
async fn connecting_to_nothing_fails() {
    // A port whose listener is closed refuses connections at once. A socket
    // that is bound but never listens would keep the port from others, but
    // macOS doesn't refuse connections to it: they wait out the SYN timeout.
    let port = free_port();
    let config: Config = toml::from_str(&format!(
        r#"
        listen = []
        [[class]]
        name = "app"
        databases = "*"
        users = ["client"]
        recording = "unused.jsonl"
        upstream = {{ host = "127.0.0.1", port = {port}, user = "postgres", database = "postgres" }}
        "#
    ))
    .unwrap();
    let record = Record::new(&config).unwrap();

    assert!(record.connect(0, "app", "client").await.is_err());
}

#[tokio::test]
async fn executes_that_ignore_parameters_are_recorded_once_with_every_time() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record_with(
        "postgres",
        None,
        r#"ignore_parameters = ["select set_config("]"#,
    );
    let mut recorder = connect(&record).await;
    let sql = "select set_config('app.tenant', $1, true)";
    for param in [b"a", b"b", b"c"] {
        send(
            &mut recorder,
            vec![parse("", sql), bind("", &[param], false), execute(), sync()],
        )
        .await;
    }
    // Other result formats are another ask.
    send(
        &mut recorder,
        vec![parse("", sql), bind("", &[b"d"], true), execute(), sync()],
    )
    .await;

    let entries = cluster.recording(&record);

    assert_eq!(entries.len(), 3);
    let mut executes: Vec<_> = entries.iter().filter(|entry| is_execute(entry)).collect();
    executes.sort_by_key(|entry| match entry {
        Entry::Execute { result_formats, .. } => result_formats.clone(),
        _ => unreachable!(),
    });
    let [text, binary] = executes[..] else {
        panic!("{entries:?}");
    };
    let Entry::Execute {
        params,
        result_formats,
        rows,
        exec_us,
        ..
    } = text
    else {
        panic!("{text:?}");
    };
    // The first answer, with the times of every execution.
    assert_eq!(*result_formats, [0]);
    assert_eq!(*params, [Some(Bytes::from_static(b"a"))]);
    assert!(
        rows.starts_with(b"D\0\0\0\x0b\0\x01\0\0\0\x01a"),
        "{rows:?}"
    );
    assert_eq!(exec_us.len(), 3);
    let Entry::Execute {
        result_formats,
        exec_us,
        ..
    } = binary
    else {
        panic!("{binary:?}");
    };
    assert_eq!(*result_formats, [1]);
    assert_eq!(exec_us.len(), 1);
}

#[tokio::test]
async fn executes_without_a_matching_prefix_are_still_recorded_per_parameters() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = cluster.record_with(
        "postgres",
        None,
        r#"ignore_parameters = ["select set_config("]"#,
    );
    let mut recorder = connect(&record).await;
    for param in [b"1", b"2", b"1"] {
        send(
            &mut recorder,
            vec![
                parse("", "select $1::int4"),
                bind("", &[param], false),
                execute(),
                sync(),
            ],
        )
        .await;
    }

    let entries = cluster.recording(&record);

    let mut times: Vec<_> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Execute {
                params, exec_us, ..
            } => Some((params.clone(), exec_us.len())),
            _ => None,
        })
        .collect();
    times.sort();
    assert_eq!(
        times,
        [
            (vec![Some(Bytes::from_static(b"1"))], 2),
            (vec![Some(Bytes::from_static(b"2"))], 1)
        ]
    );
}

#[test]
fn every_class_needs_an_upstream() {
    let config: Config = toml::from_str(
        r#"
        listen = []
        [[class]]
        name = "no-upstream"
        databases = "*"
        users = ["client"]
        recording = "unused.jsonl"
        "#,
    )
    .unwrap();

    let Err(error) = Record::new(&config) else {
        panic!("a class without an upstream is refused");
    };
    assert!(error.contains("no-upstream"), "{error}");
}

#[test]
fn two_classes_cannot_record_to_the_same_file() {
    let config: Config = toml::from_str(
        r#"
        listen = []
        [[class]]
        name = "first"
        databases = "first-*"
        users = ["client"]
        recording = "shared.jsonl"
        upstream = { host = "localhost", port = 5432, user = "postgres", database = "{database}" }
        [[class]]
        name = "second"
        databases = "second-*"
        users = ["client"]
        recording = "shared.jsonl"
        upstream = { host = "localhost", port = 5432, user = "postgres", database = "{database}" }
        "#,
    )
    .unwrap();

    let Err(error) = Record::new(&config) else {
        panic!("two classes recording to one file are refused");
    };
    assert!(error.contains("second"), "{error}");
    assert!(error.contains("first"), "{error}");
    assert!(error.contains("shared.jsonl"), "{error}");
}

/// A class with an upstream that is never connected to, unless it has none.
fn class(name: &str, recording: &str, upstream: bool) -> config::Class {
    let upstream = if upstream {
        r#"upstream = { host = "localhost", port = 5432, user = "postgres", database = "{database}" }"#
    } else {
        ""
    };
    toml::from_str(&format!(
        r#"
        name = "{name}"
        databases = "{name}-*"
        users = ["client"]
        recording = "{recording}"
        {upstream}
        "#
    ))
    .unwrap()
}

/// A record of one class, `first`, with room for more.
fn record_with_room(room: usize) -> Record {
    let config: Config = toml::from_str(
        r#"
        listen = []
        [[class]]
        name = "first"
        databases = "first-*"
        users = ["client"]
        recording = "first.jsonl"
        upstream = { host = "localhost", port = 5432, user = "postgres", database = "{database}" }
        "#,
    )
    .unwrap();
    Record::with_room(&config, room).unwrap()
}

#[test]
fn an_added_class_gets_the_next_index_and_a_target_of_its_own() {
    let record = record_with_room(2);

    assert_eq!(record.add(&class("second", "second.jsonl", true)), Ok(1));
    assert_eq!(record.add(&class("third", "third.jsonl", true)), Ok(2));

    let recordings: Vec<_> = record.classes.iter().map(|t| t.recording.clone()).collect();
    assert_eq!(
        recordings,
        ["first.jsonl", "second.jsonl", "third.jsonl"].map(PathBuf::from)
    );
}

#[test]
fn an_added_class_cannot_record_to_the_file_of_another_and_leaves_nothing() {
    let record = record_with_room(2);

    let error = record
        .add(&class("second", "first.jsonl", true))
        .unwrap_err();

    assert!(error.contains("second"), "{error}");
    assert!(error.contains("first"), "{error}");
    assert!(error.contains("first.jsonl"), "{error}");
    assert_eq!(record.classes.iter().count(), 1);
    // The failed class took no index and no recording.
    assert_eq!(record.add(&class("second", "second.jsonl", true)), Ok(1));
}

#[test]
fn an_added_class_needs_an_upstream_and_leaves_nothing() {
    let record = record_with_room(2);

    let error = record
        .add(&class("second", "second.jsonl", false))
        .unwrap_err();

    assert!(error.contains("second"), "{error}");
    assert_eq!(record.classes.iter().count(), 1);
    assert_eq!(record.add(&class("second", "second.jsonl", true)), Ok(1));
}

#[test]
fn classes_are_added_only_up_to_the_room() {
    let none = record_with_room(0);
    assert!(none.add(&class("second", "second.jsonl", true)).is_err());

    let one = record_with_room(1);
    assert_eq!(one.add(&class("second", "second.jsonl", true)), Ok(1));
    let error = one.add(&class("third", "third.jsonl", true)).unwrap_err();
    assert!(error.contains("third"), "{error}");
    assert_eq!(one.classes.iter().count(), 2);
    // A class that did not fit did not take its recording either.
    assert_eq!(
        one.add(&class("fourth", "third.jsonl", true)).unwrap_err(),
        "class fourth: there is no room for another class"
    );
}

#[tokio::test]
async fn a_class_added_later_records_to_its_own_file() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let record = record_with_room(1);
    let second: config::Class = toml::from_str(&format!(
        r#"
        name = "second"
        databases = "second-*"
        users = ["client"]
        recording = "{}"
        upstream = {{ host = "127.0.0.1", port = {}, user = "postgres", database = "postgres" }}
        "#,
        cluster.dir.join("second.jsonl").display(),
        cluster.port
    ))
    .unwrap();
    assert_eq!(record.add(&second), Ok(1));

    let mut recorder = record.connect(1, "second-1", "client").await.unwrap();
    let out = send(&mut recorder, vec![query("select 1")]).await;
    record.write().unwrap();

    assert_eq!(tags(&out), "TDCZ");
    let entries = recording::read(&cluster.dir.join("second.jsonl")).unwrap();
    assert_eq!(entries.len(), 2);
}
