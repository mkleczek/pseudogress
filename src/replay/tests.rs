use std::{
    collections::BTreeSet,
    fs,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use pgwire::messages::{
    PgWireFrontendMessage as Frontend,
    copy::CopyData,
    data::{DataRow, FieldDescription, NoData, ParameterDescription, RowDescription},
    extendedquery::{
        Bind, BindComplete, Close, CloseComplete, Describe, Execute as ExecuteMessage, Flush,
        Parse, ParseComplete, Sync as SyncMessage,
    },
    response::{
        CommandComplete, EmptyQueryResponse,
        TransactionStatus::{self, Error, Idle, Transaction},
    },
    simplequery::Query as QueryMessage,
};

use super::*;
use crate::{
    class::Class,
    config::Settings,
    recording::{self, Entry},
    wire::{put, put_error, put_ready},
};

const INT4: u32 = 23;
const TEXT: u32 = 25;
const VARCHAR: u32 = 1043;
const JSON: u32 = 114;
const JSONB: u32 = 3802;
const UNKNOWN: u32 = 705;

const ABORTED: &str =
    "current transaction is aborted, commands ignored until end of transaction block";

// What PostgreSQL answers, as record mode writes it.

fn encode(build: impl FnOnce(&mut BytesMut)) -> Bytes {
    let mut out = BytesMut::new();
    build(&mut out);
    out.freeze()
}

fn concat(parts: &[&[u8]]) -> Bytes {
    Bytes::from(parts.concat())
}

/// Columns are name, type and format.
fn description(columns: &[(&str, u32, i16)]) -> Bytes {
    let fields = columns
        .iter()
        .map(|&(name, type_id, format)| {
            FieldDescription::new(name.to_owned(), 0, 0, type_id, -1, -1, format)
        })
        .collect();
    encode(|out| put(out, &RowDescription::new(fields)))
}

fn no_data() -> Bytes {
    encode(|out| put(out, &NoData::new()))
}

type Value<'a> = Option<&'a [u8]>;

fn data_row(values: &[Value]) -> DataRow {
    let mut data = BytesMut::new();
    for value in values {
        match value {
            Some(value) => {
                data.put_i32(value.len() as i32);
                data.extend_from_slice(value);
            }
            None => data.put_i32(-1),
        }
    }
    DataRow::new(data, values.len() as i16)
}

/// DataRows, then the command tag.
fn table(rows: &[&[Value]], tag: &str) -> Bytes {
    encode(|out| {
        for row in rows {
            put(out, &data_row(row));
        }
        put(out, &CommandComplete::new(tag.to_owned()));
    })
}

/// DataRows of one text column, then the command tag.
fn rows(values: &[&str], tag: &str) -> Bytes {
    encode(|out| {
        for value in values {
            put(out, &data_row(&[Some(value.as_bytes())]));
        }
        put(out, &CommandComplete::new(tag.to_owned()));
    })
}

fn complete(tag: &str) -> Bytes {
    rows(&[], tag)
}

fn failure(code: &str, message: &str) -> Bytes {
    encode(|out| put_error(out, "ERROR", code, message))
}

fn parse_complete() -> Bytes {
    encode(|out| put(out, &ParseComplete::new()))
}

fn bind_complete() -> Bytes {
    encode(|out| put(out, &BindComplete::new()))
}

fn close_complete() -> Bytes {
    encode(|out| put(out, &CloseComplete::new()))
}

fn ready(status: TransactionStatus) -> Bytes {
    encode(|out| put_ready(out, status))
}

// Recordings.

fn startup() -> Entry {
    Entry::Startup {
        parameters: vec![("server_version".into(), "18.0".into())],
        start_us: vec![],
    }
}

fn execute_entry(
    sql: &str,
    params: &[&str],
    result_formats: &[i16],
    description: Bytes,
    rows: Bytes,
    exec_us: &[u32],
) -> Entry {
    Entry::Execute {
        sql: sql.into(),
        param_formats: vec![],
        params: params
            .iter()
            .map(|p| Some(Bytes::copy_from_slice(p.as_bytes())))
            .collect(),
        result_formats: result_formats.to_vec(),
        description,
        rows,
        exec_us: exec_us.to_vec(),
    }
}

/// An Execute without parameters and columns.
fn command(sql: &str, rows: Bytes) -> Entry {
    execute_entry(sql, &[], &[], no_data(), rows, &[])
}

fn query_entry(sql: &str, response: Bytes, exec_us: &[u32]) -> Entry {
    Entry::Query {
        sql: sql.into(),
        response,
        exec_us: exec_us.to_vec(),
    }
}

fn describe_entry(sql: &str, param_types: &[u32], response: Bytes) -> Entry {
    Entry::Describe {
        sql: sql.into(),
        param_types: param_types.to_vec(),
        response,
    }
}

// What a client sends.

fn parse(name: Option<&str>, sql: &str, types: &[u32]) -> Frontend {
    Frontend::Parse(Parse::new(
        name.map(str::to_owned),
        sql.to_owned(),
        types.to_vec(),
    ))
}

fn bind(
    portal: Option<&str>,
    statement: Option<&str>,
    params: &[&str],
    result_formats: &[i16],
) -> Frontend {
    Frontend::Bind(Bind::new(
        portal.map(str::to_owned),
        statement.map(str::to_owned),
        vec![],
        params
            .iter()
            .map(|p| Some(Bytes::copy_from_slice(p.as_bytes())))
            .collect(),
        result_formats.to_vec(),
    ))
}

fn describe(target: u8, name: Option<&str>) -> Frontend {
    Frontend::Describe(Describe::new(target, name.map(str::to_owned)))
}

fn execute(portal: Option<&str>) -> Frontend {
    Frontend::Execute(ExecuteMessage::new(portal.map(str::to_owned), 0))
}

fn flush() -> Frontend {
    Frontend::Flush(Flush::new())
}

fn sync() -> Frontend {
    Frontend::Sync(SyncMessage::new())
}

fn simple(sql: &str) -> Frontend {
    Frontend::Query(QueryMessage::new(sql.to_owned()))
}

/// Parse, Bind, Execute and Sync of a statement without parameters.
fn run(sql: &str) -> Vec<Frontend> {
    vec![
        parse(None, sql, &[]),
        bind(None, None, &[], &[]),
        execute(None),
        sync(),
    ]
}

/// Parse, Bind, Execute and Sync of a statement with text parameters, and
/// text results.
fn run_with(sql: &str, params: &[&str]) -> Vec<Frontend> {
    vec![
        parse(None, sql, &[]),
        bind(None, None, params, &[0]),
        execute(None),
        sync(),
    ]
}

// A Replay over a recording.

struct Loaded {
    replay: Replay,
    classes: Classes,
}

impl Loaded {
    async fn replayer(&self) -> Replayer {
        self.replay.connect(0, "db", "u").await.unwrap()
    }

    fn misses(&self) -> u64 {
        self.classes[0].metrics.misses.load(Relaxed)
    }

    fn fallbacks(&self) -> u64 {
        self.classes[0].metrics.fallbacks.load(Relaxed)
    }
}

/// `class` is more TOML of the class: an override, or settings.
fn try_load(entries: &[Entry], class: &str) -> Result<Loaded, String> {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pseudogress-replay-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Relaxed)
    ));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("recording.jsonl");
    recording::write(&path, entries).unwrap();
    let config: Config = toml::from_str(&format!(
        "listen = ['127.0.0.1:0']\n[[class]]\nname = 'test'\ndatabases = '*'\n\
         users = ['u']\nrecording = '{}'\n{class}",
        path.display()
    ))
    .unwrap();
    let classes: Classes = config.classes.iter().map(Class::new).collect();
    let replay = Replay::load(&config, classes.clone());
    fs::remove_dir_all(&dir).unwrap();
    Ok(Loaded {
        replay: replay?,
        classes,
    })
}

fn load(entries: &[Entry]) -> Loaded {
    try_load(entries, "").unwrap()
}

async fn answer(replayer: &mut Replayer, batch: &[Frontend]) -> (Bytes, Duration) {
    let mut out = BytesMut::new();
    let time = replayer.batch(batch, &mut out).await;
    (out.freeze(), time)
}

#[tokio::test]
async fn answers_parse_bind_describe_execute_and_sync() {
    let columns = description(&[("t", TEXT, 0)]);
    let answer_rows = rows(&["a", "b"], "SELECT 2");
    let loaded = load(&[
        startup(),
        execute_entry(
            "select $1::text",
            &["a"],
            &[0],
            columns.clone(),
            answer_rows.clone(),
            &[100],
        ),
    ]);
    let mut replayer = loaded.replayer().await;
    let batch = [
        parse(Some("s1"), "select $1::text", &[TEXT]),
        bind(None, Some("s1"), &["a"], &[0]),
        describe(b'P', None),
        // max_rows does not limit the rows
        Frontend::Execute(ExecuteMessage::new(None, 1)),
        sync(),
    ];
    let (out, time) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &bind_complete(),
            &columns,
            &answer_rows,
            &ready(Idle)
        ])
    );
    assert_eq!(time, Duration::from_micros(100));
    assert_eq!(loaded.misses(), 0);
    assert_eq!(
        replayer.parameter_statuses(),
        [("server_version".to_owned(), "18.0".to_owned())]
    );
}

#[tokio::test]
async fn executes_the_statement_a_bind_names_in_the_portal_it_names() {
    let columns = description(&[("t", TEXT, 0)]);
    let (one, two) = (rows(&["1"], "SELECT 1"), rows(&["2"], "SELECT 1"));
    let loaded = load(&[
        startup(),
        execute_entry("select 1", &[], &[0], columns.clone(), one.clone(), &[]),
        execute_entry("select 2", &[], &[0], columns, two.clone(), &[]),
    ]);
    let mut replayer = loaded.replayer().await;
    let batch = [
        parse(Some("s1"), "select 1", &[]),
        parse(Some("s2"), "select 2", &[]),
        bind(Some("p1"), Some("s1"), &[], &[0]),
        bind(Some("p2"), Some("s2"), &[], &[0]),
        execute(Some("p2")),
        execute(Some("p1")),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &parse_complete(),
            &bind_complete(),
            &bind_complete(),
            &two,
            &one,
            &ready(Idle)
        ])
    );
}

#[tokio::test]
async fn falls_back_to_the_result_formats_when_parameters_differ() {
    let columns = description(&[("t", TEXT, 0)]);
    let (first, second) = (rows(&["a"], "SELECT 1"), rows(&["b"], "SELECT 1"));
    let loaded = load(&[
        startup(),
        execute_entry(
            "select $1",
            &["a"],
            &[0],
            columns.clone(),
            first.clone(),
            &[],
        ),
        execute_entry("select $1", &["b"], &[0], columns, second.clone(), &[]),
    ]);
    let mut replayer = loaded.replayer().await;
    // The first answer in the recording, which is sorted, stands in for
    // parameters that were not seen.
    for (param, expected) in [("a", &first), ("b", &second), ("c", &first)] {
        let batch = [
            parse(None, "select $1", &[]),
            bind(None, None, &[param], &[0]),
            execute(None),
            sync(),
        ];
        let (out, _) = answer(&mut replayer, &batch).await;
        assert_eq!(
            out,
            concat(&[&parse_complete(), &bind_complete(), expected, &ready(Idle)]),
            "parameter {param}"
        );
        // Only the last one was not recorded.
        assert_eq!(loaded.fallbacks(), u64::from(param == "c"), "after {param}");
    }
    assert_eq!(loaded.misses(), 0);
}

const SET_CONFIG: &str = "select set_config('request.jwt.claims', $1, true)";
const IGNORING_SET_CONFIG: &str = "ignore_parameters = ['select set_config(']";

#[tokio::test]
async fn a_statement_that_ignores_its_parameters_gets_the_recorded_answer() {
    let columns = description(&[("set_config", TEXT, 0)]);
    let answer_rows = rows(&["{}"], "SELECT 1");
    let loaded = try_load(
        &[
            startup(),
            execute_entry(SET_CONFIG, &["{}"], &[0], columns, answer_rows.clone(), &[]),
        ],
        IGNORING_SET_CONFIG,
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    // Parameters that were never recorded get the answer to the recorded
    // ones, and it is no fallback.
    for params in [&["{}"][..], &["{\"role\":\"anon\"}"], &["x"]] {
        let (out, _) = answer(&mut replayer, &run_with(SET_CONFIG, params)).await;
        assert_eq!(
            out,
            concat(&[
                &parse_complete(),
                &bind_complete(),
                &answer_rows,
                &ready(Idle)
            ]),
            "parameters {params:?}"
        );
    }
    assert_eq!(loaded.fallbacks(), 0);
    assert_eq!(loaded.misses(), 0);
}

#[tokio::test]
async fn a_statement_without_a_matching_prefix_still_counts_a_fallback() {
    let columns = description(&[("t", TEXT, 0)]);
    let (config_rows, first) = (rows(&["{}"], "SELECT 1"), rows(&["a"], "SELECT 1"));
    let loaded = try_load(
        &[
            startup(),
            execute_entry(SET_CONFIG, &["{}"], &[0], columns.clone(), config_rows, &[]),
            execute_entry("select $1", &["a"], &[0], columns, first.clone(), &[]),
        ],
        IGNORING_SET_CONFIG,
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    answer(&mut replayer, &run_with(SET_CONFIG, &["x"])).await;
    assert_eq!(loaded.fallbacks(), 0);
    let (out, _) = answer(&mut replayer, &run_with("select $1", &["b"])).await;
    assert_eq!(
        out,
        concat(&[&parse_complete(), &bind_complete(), &first, &ready(Idle)])
    );
    assert_eq!(loaded.fallbacks(), 1);
    assert_eq!(loaded.misses(), 0);
}

#[tokio::test]
async fn a_statement_that_ignores_its_parameters_is_still_a_miss_without_a_recording() {
    let columns = description(&[("set_config", TEXT, 0)]);
    let loaded = try_load(
        &[
            startup(),
            execute_entry(
                SET_CONFIG,
                &["{}"],
                &[0],
                columns,
                rows(&["{}"], "SELECT 1"),
                &[],
            ),
        ],
        IGNORING_SET_CONFIG,
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    // A statement that was never recorded.
    let other = "select set_config('request.headers', $1, true)";
    let (out, _) = answer(&mut replayer, &run_with(other, &["{}"])).await;
    assert_eq!(
        out,
        concat(&[
            &failure("XX000", &format!("pseudogress: no recording of: {other}")),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 1);
    // Result formats that were never recorded.
    let batch = [
        parse(None, SET_CONFIG, &[]),
        bind(None, None, &["{}"], &[1]),
        execute(None),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &failure(
                "XX000",
                &format!("pseudogress: no recording of: {SET_CONFIG}")
            ),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 2);
    assert_eq!(loaded.fallbacks(), 0);
}

#[tokio::test]
async fn a_miss_is_an_error_and_skips_to_the_sync() {
    let loaded = load(&[startup(), command("select 1", complete("SELECT 0"))]);
    let mut replayer = loaded.replayer().await;
    let (out, time) = answer(&mut replayer, &run("select 2")).await;
    assert_eq!(
        out,
        concat(&[
            &failure("XX000", "pseudogress: no recording of: select 2"),
            &ready(Idle)
        ])
    );
    assert_eq!(time, Duration::ZERO);
    assert_eq!(loaded.misses(), 1);
    // The connection goes on.
    let (out, _) = answer(&mut replayer, &run("select 1")).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &bind_complete(),
            &complete("SELECT 0"),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 1);
}

#[tokio::test]
async fn a_bind_without_a_recording_is_a_miss() {
    let loaded = load(&[startup(), command("select 1", complete("SELECT 0"))]);
    let mut replayer = loaded.replayer().await;
    // Only the result formats [] were recorded.
    let batch = [
        parse(None, "select 1", &[]),
        bind(None, None, &[], &[1]),
        execute(None),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &failure("XX000", "pseudogress: no recording of: select 1"),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 1);
}

#[tokio::test]
async fn skips_messages_up_to_the_sync_even_across_batches() {
    let loaded = load(&[startup(), command("select 1", complete("SELECT 0"))]);
    let mut replayer = loaded.replayer().await;
    let (out, _) = answer(&mut replayer, &[parse(None, "select 2", &[]), flush()]).await;
    assert_eq!(
        out,
        failure("XX000", "pseudogress: no recording of: select 2")
    );
    let (out, _) = answer(&mut replayer, &[describe(b'P', None), sync()]).await;
    assert_eq!(out, ready(Idle));
    assert_eq!(loaded.misses(), 1);
}

#[tokio::test]
async fn truncates_the_sql_of_a_miss() {
    let loaded = load(&[startup()]);
    let mut replayer = loaded.replayer().await;
    let sql = format!("select {}", "x".repeat(1000));
    let (out, _) = answer(&mut replayer, &[simple(&sql)]).await;
    assert!(out.len() < 400, "{} bytes", out.len());
    assert!(out.windows(10).any(|w| w == b"select xxx"));
    assert_eq!(out[out.len() - 6..], ready(Idle)[..]);
}

#[tokio::test]
async fn an_error_in_the_batch_skips_what_follows_it() {
    let boom = failure("22012", "division by zero");
    let loaded = load(&[
        startup(),
        command("select 1/0", boom.clone()),
        command("select 1", complete("SELECT 0")),
    ]);
    let mut replayer = loaded.replayer().await;
    let batch = [
        parse(None, "select 1/0", &[]),
        bind(None, None, &[], &[]),
        execute(None),
        parse(None, "select 1", &[]),
        bind(None, None, &[], &[]),
        execute(None),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[&parse_complete(), &bind_complete(), &boom, &ready(Idle)])
    );
    assert_eq!(loaded.misses(), 0);
}

#[tokio::test]
async fn follows_the_transaction_status_of_extended_statements() {
    let loaded = load(&[
        startup(),
        command("begin", complete("BEGIN")),
        command("select 1", complete("SELECT 0")),
        command("commit", complete("COMMIT")),
    ]);
    let mut replayer = loaded.replayer().await;
    for (sql, tag, status) in [
        ("begin", "BEGIN", Transaction),
        ("select 1", "SELECT 0", Transaction),
        ("commit", "COMMIT", Idle),
        ("select 1", "SELECT 0", Idle),
    ] {
        let (out, _) = answer(&mut replayer, &run(sql)).await;
        assert_eq!(
            out,
            concat(&[
                &parse_complete(),
                &bind_complete(),
                &complete(tag),
                &ready(status)
            ]),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn follows_the_transaction_status_of_simple_queries() {
    let loaded = load(&[
        startup(),
        query_entry("begin", complete("BEGIN"), &[10]),
        query_entry("select 1", rows(&["1"], "SELECT 1"), &[20]),
        query_entry("rollback", complete("ROLLBACK"), &[30]),
    ]);
    let mut replayer = loaded.replayer().await;
    for (sql, response, status, micros) in [
        ("begin", complete("BEGIN"), Transaction, 10),
        ("select 1", rows(&["1"], "SELECT 1"), Transaction, 20),
        ("rollback", complete("ROLLBACK"), Idle, 30),
        ("select 1", rows(&["1"], "SELECT 1"), Idle, 20),
    ] {
        let (out, time) = answer(&mut replayer, &[simple(sql)]).await;
        assert_eq!(out, concat(&[&response, &ready(status)]), "{sql}");
        assert_eq!(time, Duration::from_micros(micros));
    }
}

#[tokio::test]
async fn an_error_in_a_transaction_fails_it_until_it_ends() {
    let boom = failure("22012", "division by zero");
    let loaded = load(&[
        startup(),
        command("begin", complete("BEGIN")),
        command("select 1/0", boom.clone()),
        command("select 1", complete("SELECT 0")),
        command("commit", complete("COMMIT")),
    ]);
    let mut replayer = loaded.replayer().await;
    let (begun, done) = (complete("BEGIN"), complete("SELECT 0"));
    let aborted = failure("25P02", ABORTED);
    let steps = [
        (
            "begin",
            concat(&[
                &parse_complete(),
                &bind_complete(),
                &begun,
                &ready(Transaction),
            ]),
        ),
        (
            "select 1/0",
            concat(&[&parse_complete(), &bind_complete(), &boom, &ready(Error)]),
        ),
        ("select 1", concat(&[&aborted, &ready(Error)])),
        // COMMIT of a failed transaction rolls it back
        (
            "commit",
            concat(&[
                &parse_complete(),
                &bind_complete(),
                &complete("ROLLBACK"),
                &ready(Idle),
            ]),
        ),
        (
            "select 1",
            concat(&[&parse_complete(), &bind_complete(), &done, &ready(Idle)]),
        ),
    ];
    for (sql, expected) in steps {
        let (out, _) = answer(&mut replayer, &run(sql)).await;
        assert_eq!(out, expected, "{sql}");
    }
}

#[tokio::test]
async fn a_failed_transaction_rejects_simple_queries_until_it_ends() {
    let boom = failure("22012", "division by zero");
    let loaded = load(&[
        startup(),
        query_entry("begin", complete("BEGIN"), &[]),
        query_entry("select 1/0", boom.clone(), &[]),
        query_entry("select 1", rows(&["1"], "SELECT 1"), &[]),
        query_entry("rollback", complete("ROLLBACK"), &[]),
    ]);
    let mut replayer = loaded.replayer().await;
    let aborted = failure("25P02", ABORTED);
    let steps = [
        ("begin", concat(&[&complete("BEGIN"), &ready(Transaction)])),
        ("select 1/0", concat(&[&boom, &ready(Error)])),
        ("select 1", concat(&[&aborted, &ready(Error)])),
        ("rollback", concat(&[&complete("ROLLBACK"), &ready(Idle)])),
        (
            "select 1",
            concat(&[&rows(&["1"], "SELECT 1"), &ready(Idle)]),
        ),
    ];
    for (sql, expected) in steps {
        let (out, _) = answer(&mut replayer, &[simple(sql)]).await;
        assert_eq!(out, expected, "{sql}");
    }
}

#[tokio::test]
async fn a_multi_statement_query_ends_in_the_status_of_its_last_statement() {
    let started = concat(&[&complete("BEGIN"), &rows(&["1"], "SELECT 1")]);
    let failed = concat(&[&complete("BEGIN"), &failure("22012", "division by zero")]);
    let switched = concat(&[&complete("COMMIT"), &complete("BEGIN")]);
    let loaded = load(&[
        startup(),
        query_entry("begin; select 1", started.clone(), &[]),
        query_entry("begin; select 1/0", failed.clone(), &[]),
        query_entry("commit; begin", switched.clone(), &[]),
        query_entry("rollback", complete("ROLLBACK"), &[]),
    ]);
    let mut replayer = loaded.replayer().await;
    let (out, _) = answer(&mut replayer, &[simple("begin; select 1")]).await;
    assert_eq!(out, concat(&[&started, &ready(Transaction)]));
    let (out, _) = answer(&mut replayer, &[simple("commit; begin")]).await;
    assert_eq!(out, concat(&[&switched, &ready(Transaction)]));
    answer(&mut replayer, &[simple("rollback")]).await;
    let (out, _) = answer(&mut replayer, &[simple("begin; select 1/0")]).await;
    assert_eq!(out, concat(&[&failed, &ready(Error)]));
}

#[tokio::test]
async fn an_empty_query_is_an_empty_query_response() {
    let loaded = load(&[startup()]);
    let mut replayer = loaded.replayer().await;
    let (out, _) = answer(&mut replayer, &[simple("")]).await;
    assert_eq!(
        out,
        concat(&[
            &encode(|out| put(out, &EmptyQueryResponse::new())),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 0);
}

#[tokio::test]
async fn describes_a_statement_by_its_parameter_types() {
    let response = concat(&[
        &encode(|out| put(out, &ParameterDescription::new(vec![TEXT]))),
        &description(&[("t", TEXT, 0)]),
    ]);
    let loaded = load(&[
        startup(),
        describe_entry("select $1::text", &[TEXT], response.clone()),
    ]);
    let mut replayer = loaded.replayer().await;
    let batch = [
        parse(Some("s"), "select $1::text", &[TEXT]),
        describe(b'S', Some("s")),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(out, concat(&[&parse_complete(), &response, &ready(Idle)]));
    assert_eq!(loaded.misses(), 0);

    // Other parameter types were not recorded.
    let batch = [
        parse(Some("t"), "select $1::text", &[INT4]),
        describe(b'S', Some("t")),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &failure("XX000", "pseudogress: no recording of: select $1::text"),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 1);
}

#[tokio::test]
async fn closes_statements_and_portals() {
    let loaded = load(&[startup(), command("select 1", complete("SELECT 0"))]);
    let mut replayer = loaded.replayer().await;
    let batch = [
        parse(Some("s"), "select 1", &[]),
        Frontend::Close(Close::new(b'S', Some("s".to_owned()))),
        Frontend::Close(Close::new(b'P', None)),
        flush(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[&parse_complete(), &close_complete(), &close_complete()])
    );
    let (out, _) = answer(&mut replayer, &[bind(None, Some("s"), &[], &[]), sync()]).await;
    assert_eq!(
        out,
        concat(&[
            &failure("26000", "prepared statement \"s\" does not exist"),
            &ready(Idle)
        ])
    );
    assert_eq!(loaded.misses(), 0);
}

#[tokio::test]
async fn a_named_statement_must_be_closed_before_it_is_parsed_again() {
    let loaded = load(&[
        startup(),
        command("select 1", complete("SELECT 1")),
        command("select 2", complete("SELECT 2")),
    ]);
    let mut replayer = loaded.replayer().await;
    let (out, _) = answer(&mut replayer, &[parse(Some("s"), "select 1", &[]), sync()]).await;
    assert_eq!(out, concat(&[&parse_complete(), &ready(Idle)]));

    // The error skips the rest up to the Sync, and the statement stays.
    let batch = [
        parse(Some("s"), "select 2", &[]),
        bind(None, Some("s"), &[], &[]),
        execute(None),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &failure("42P05", "prepared statement \"s\" already exists"),
            &ready(Idle)
        ])
    );
    let (out, _) = answer(
        &mut replayer,
        &[bind(None, Some("s"), &[], &[]), execute(None), sync()],
    )
    .await;
    assert_eq!(
        out,
        concat(&[&bind_complete(), &complete("SELECT 1"), &ready(Idle)])
    );

    let batch = [
        Frontend::Close(Close::new(b'S', Some("s".to_owned()))),
        parse(Some("s"), "select 2", &[]),
        bind(None, Some("s"), &[], &[]),
        execute(None),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(
        out,
        concat(&[
            &close_complete(),
            &parse_complete(),
            &bind_complete(),
            &complete("SELECT 2"),
            &ready(Idle)
        ])
    );
}

#[tokio::test]
async fn the_unnamed_statement_is_replaced_without_a_close() {
    let loaded = load(&[
        startup(),
        command("select 1", complete("SELECT 1")),
        command("select 2", complete("SELECT 2")),
    ]);
    let mut replayer = loaded.replayer().await;
    for (sql, tag) in [("select 1", "SELECT 1"), ("select 2", "SELECT 2")] {
        let (out, _) = answer(&mut replayer, &run(sql)).await;
        assert_eq!(
            out,
            concat(&[
                &parse_complete(),
                &bind_complete(),
                &complete(tag),
                &ready(Idle)
            ]),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn refuses_copy() {
    let loaded = load(&[startup()]);
    let mut replayer = loaded.replayer().await;
    let batch = [
        Frontend::CopyData(CopyData::new(Bytes::from_static(b"x"))),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    assert_eq!(out[0], b'E', "an ErrorResponse comes first");
    assert!(out.windows(6).any(|w| w == b"C0A000"), "{out:?}");
    assert_eq!(out[out.len() - 6..], ready(Idle)[..]);
}

// The column override.

/// The JSON a column is overridden with.
fn json(bytes: usize) -> String {
    format!(r#"[{{"x":"{}"}}]"#, "a".repeat(bytes - 10))
}

/// What a batch answers between BindComplete and ReadyForQuery.
async fn rows_of(loaded: &Loaded, sql: &str, result_formats: &[i16]) -> Bytes {
    let mut replayer = loaded.replayer().await;
    let batch = [
        parse(None, sql, &[]),
        bind(None, None, &[], result_formats),
        execute(None),
        sync(),
    ];
    let (out, _) = answer(&mut replayer, &batch).await;
    let head = parse_complete().len() + bind_complete().len();
    out.slice(head..out.len() - ready(Idle).len())
}

fn override_body(bytes: usize) -> String {
    format!("override = {{ column = 'body', bytes = {bytes} }}")
}

#[tokio::test]
async fn overrides_a_text_column_with_json_of_the_configured_size() {
    let columns = description(&[("id", INT4, 0), ("body", JSON, 0)]);
    let recorded = table(
        &[&[Some(b"1"), Some(b"[]")], &[Some(b"2"), Some(b"[1,2,3]")]],
        "SELECT 2",
    );
    let other = rows(&["1"], "SELECT 1");
    let loaded = try_load(
        &[
            startup(),
            execute_entry("select id, body", &[], &[0], columns, recorded, &[]),
            execute_entry(
                "select 1 as n",
                &[],
                &[0],
                description(&[("n", INT4, 0)]),
                other.clone(),
                &[],
            ),
        ],
        &override_body(40),
    )
    .unwrap();
    let json = json(40);
    let expected = table(
        &[
            &[Some(b"1"), Some(json.as_bytes())],
            &[Some(b"2"), Some(json.as_bytes())],
        ],
        "SELECT 2",
    );
    assert_eq!(rows_of(&loaded, "select id, body", &[0]).await, expected);
    // Without the column, rows stay as recorded.
    assert_eq!(rows_of(&loaded, "select 1 as n", &[0]).await, other);
}

#[tokio::test]
async fn an_override_is_at_least_the_minimal_json() {
    let columns = description(&[("body", TEXT, 0)]);
    let loaded = try_load(
        &[
            startup(),
            execute_entry("select", &[], &[0], columns, rows(&["x"], "SELECT 1"), &[]),
        ],
        &override_body(1),
    )
    .unwrap();
    let expected = rows(&[r#"[{"x":""}]"#], "SELECT 1");
    assert_eq!(rows_of(&loaded, "select", &[0]).await, expected);
}

#[tokio::test]
async fn overrides_a_binary_jsonb_column_with_its_version_byte() {
    let columns = description(&[("body", JSONB, 1)]);
    let loaded = try_load(
        &[
            startup(),
            execute_entry(
                "select",
                &[],
                &[1],
                columns,
                table(&[&[Some(b"\x01[]")]], "SELECT 1"),
                &[],
            ),
        ],
        &override_body(24),
    )
    .unwrap();
    let value = [&b"\x01"[..], json(24).as_bytes()].concat();
    let expected = table(&[&[Some(&value)]], "SELECT 1");
    assert_eq!(rows_of(&loaded, "select", &[1]).await, expected);
}

#[tokio::test]
async fn overrides_binary_text_columns_as_utf8() {
    for type_id in [TEXT, VARCHAR, JSON, UNKNOWN] {
        let columns = description(&[("body", type_id, 1)]);
        let loaded = try_load(
            &[
                startup(),
                execute_entry(
                    "select",
                    &[],
                    &[1],
                    columns,
                    table(&[&[Some(b"[]")]], "SELECT 1"),
                    &[],
                ),
            ],
            &override_body(16),
        )
        .unwrap();
        let expected = table(&[&[Some(json(16).as_bytes())]], "SELECT 1");
        assert_eq!(
            rows_of(&loaded, "select", &[1]).await,
            expected,
            "{type_id}"
        );
    }
}

#[tokio::test]
async fn refuses_to_override_a_binary_column_of_another_type() {
    let columns = description(&[("id", INT4, 0), ("body", INT4, 1)]);
    let recorded = table(&[&[Some(b"1"), Some(&[0, 0, 0, 1])]], "SELECT 1");
    let error = try_load(
        &[
            startup(),
            execute_entry("select", &[], &[0, 1], columns, recorded, &[]),
        ],
        &override_body(40),
    )
    .err()
    .expect("loading fails");
    assert!(error.contains("body") && error.contains("23"), "{error}");
}

#[tokio::test]
async fn scales_execution_times() {
    let loaded = load(&[
        startup(),
        execute_entry(
            "select 1",
            &[],
            &[],
            no_data(),
            complete("SELECT 0"),
            &[200],
        ),
        execute_entry(
            "select 2",
            &[],
            &[],
            no_data(),
            complete("SELECT 0"),
            &[1000, 3000],
        ),
    ]);
    let mut replayer = loaded.replayer().await;
    let (_, time) = answer(&mut replayer, &run("select 1")).await;
    assert_eq!(time, Duration::from_micros(200));

    let scale = Settings {
        execution_scale: 0.5,
        ..Settings::default()
    };
    loaded.classes[0].settings.store(Arc::new(scale));
    let (_, time) = answer(&mut replayer, &run("select 1")).await;
    assert_eq!(time, Duration::from_micros(100));

    // Each Execute picks one of its samples, and a batch adds them up.
    let twice = [
        parse(None, "select 2", &[]),
        bind(None, None, &[], &[]),
        execute(None),
        bind(None, None, &[], &[]),
        execute(None),
        sync(),
    ];
    let mut seen = BTreeSet::new();
    for _ in 0..200 {
        seen.insert(answer(&mut replayer, &twice).await.1);
    }
    let expected = [1000, 2000, 3000].map(Duration::from_micros);
    assert_eq!(seen, BTreeSet::from(expected));
}

/// A recording of statements that take 200 microseconds, and a statement
/// whose answer is `answer` bytes.
fn latency_recording(answer: usize) -> Vec<Entry> {
    // The DataRow and the command tag around a value.
    let overhead = rows(&[""], "SELECT 1").len();
    let big = rows(&["x".repeat(answer - overhead).as_str()], "SELECT 1");
    assert_eq!(big.len(), answer);
    vec![
        startup(),
        execute_entry(
            "select 1",
            &[],
            &[],
            no_data(),
            complete("SELECT 0"),
            &[200],
        ),
        execute_entry("select big", &[], &[], no_data(), big.clone(), &[200]),
        query_entry("select big", big, &[200]),
    ]
}

/// Parse and Bind of a statement, and its Execute, `times` times, then Sync.
fn executions(sql: &str, times: usize) -> Vec<Frontend> {
    let mut batch = vec![parse(None, sql, &[])];
    for _ in 0..times {
        batch.push(bind(None, None, &[], &[]));
        batch.push(execute(None));
    }
    batch.push(sync());
    batch
}

#[tokio::test]
async fn a_constant_model_gives_every_statement_its_median() {
    let loaded = try_load(
        &latency_recording(100),
        "[class.settings]\nexecution_scale = 0\n\
         execution = { median = '2ms', p99 = '2ms' }",
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    // The recorded 200 us don't count, and the other messages take nothing.
    for k in [1, 2, 5] {
        let (_, time) = answer(&mut replayer, &executions("select 1", k)).await;
        assert_eq!(time, k as u32 * Duration::from_millis(2), "{k} Executes");
    }
    let (_, time) = answer(&mut replayer, &[simple("select big")]).await;
    assert_eq!(time, Duration::from_millis(2));
    let (_, time) = answer(&mut replayer, &[parse(None, "select 1", &[]), sync()]).await;
    assert_eq!(time, Duration::ZERO);
}

#[tokio::test]
async fn a_model_with_a_p99_varies_the_time_of_statements() {
    let loaded = try_load(
        &latency_recording(100),
        "[class.settings]\nexecution_scale = 0\n\
         execution = { median = '1ms', p99 = '20ms' }",
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    let mut seen = BTreeSet::new();
    for _ in 0..100 {
        seen.insert(answer(&mut replayer, &executions("select 1", 1)).await.1);
    }
    assert!(seen.len() > 50, "{seen:?}");
}

#[tokio::test]
async fn execution_per_mib_adds_time_by_the_size_of_the_answer() {
    const MIB: usize = 1024 * 1024;
    let loaded = try_load(
        &latency_recording(MIB),
        "[class.settings]\nexecution_scale = 0\nexecution_per_mib = '10ms'",
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    // The Execute's rows, and the response of a simple query.
    let (_, time) = answer(&mut replayer, &executions("select big", 1)).await;
    assert_eq!(time, Duration::from_millis(10));
    let (_, time) = answer(&mut replayer, &executions("select big", 3)).await;
    assert_eq!(time, Duration::from_millis(30));
    let (_, time) = answer(&mut replayer, &[simple("select big")]).await;
    assert_eq!(time, Duration::from_millis(10));

    // A smaller answer takes proportionally less.
    let small = latency_recording(MIB / 4);
    let loaded = try_load(
        &small,
        "[class.settings]\nexecution_scale = 0\nexecution_per_mib = '10ms'",
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    let (_, time) = answer(&mut replayer, &executions("select big", 1)).await;
    assert_eq!(time, Duration::from_micros(2500));
}

#[tokio::test]
async fn a_statements_time_is_recorded_scaled_plus_modelled_plus_per_mib() {
    const MIB: usize = 1024 * 1024;
    let loaded = try_load(
        &latency_recording(MIB),
        "[class.settings]\nexecution_scale = 0.5\nexecution_per_mib = '10ms'\n\
         execution = { median = '3ms', p99 = '3ms' }",
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    // 100 us of the recorded 200, 3 ms of the model and 10 ms of the answer.
    let (_, time) = answer(&mut replayer, &executions("select big", 2)).await;
    assert_eq!(time, 2 * Duration::from_micros(13_100));
    let (_, time) = answer(&mut replayer, &[simple("select big")]).await;
    assert_eq!(time, Duration::from_micros(13_100));
    // The settings are read again by every batch.
    let settings = Settings {
        execution_per_mib: Duration::from_millis(1),
        ..Settings::default()
    };
    loaded.classes[0].settings.store(Arc::new(settings));
    let (_, time) = answer(&mut replayer, &executions("select big", 1)).await;
    assert_eq!(time, Duration::from_micros(1200));
}

#[tokio::test]
async fn execution_times_are_log_normal_with_the_configured_median_and_p99() {
    let loaded = try_load(
        &latency_recording(100),
        "[class.settings]\nexecution_scale = 0\n\
         execution = { median = '1ms', p99 = '5ms' }",
    )
    .unwrap();
    let mut replayer = loaded.replayer().await;
    let batch = executions("select 1", 1);
    let mut times = Vec::new();
    for _ in 0..10_000 {
        times.push(answer(&mut replayer, &batch).await.1);
    }
    times.sort();
    // The bounds are many standard errors wide, so that chance never fails
    // the test.
    let (median, p99) = (times[5_000], times[9_900]);
    assert!(
        (900..=1100).contains(&median.as_micros()),
        "median {median:?}"
    );
    assert!((3750..=6250).contains(&p99.as_micros()), "p99 {p99:?}");
}

#[test]
fn a_recording_without_a_startup_entry_fails_to_load() {
    let error = try_load(&[command("select 1", complete("SELECT 0"))], "")
        .err()
        .expect("loading fails");
    assert!(error.contains("Startup"), "{error}");
}

#[tokio::test]
async fn connecting_takes_a_recorded_start_time() {
    let loaded = load(&[Entry::Startup {
        parameters: vec![("TimeZone".into(), "UTC".into())],
        start_us: vec![3000],
    }]);
    let started = Instant::now();
    let replayer = loaded.replayer().await;
    assert!(started.elapsed() >= Duration::from_millis(3));
    assert_eq!(
        replayer.parameter_statuses(),
        [("TimeZone".to_owned(), "UTC".to_owned())]
    );
}

#[tokio::test]
async fn a_cancelled_batch_is_an_error_and_a_ready_for_query() {
    let loaded = load(&[startup(), command("begin", complete("BEGIN"))]);
    let mut replayer = loaded.replayer().await;
    let cancelled = failure("57014", "canceling statement due to user request");

    let mut out = BytesMut::new();
    replayer.cancelled(&mut out);
    assert_eq!(out.freeze(), concat(&[&cancelled, &ready(Idle)]));

    answer(&mut replayer, &run("begin")).await;
    let mut out = BytesMut::new();
    replayer.cancelled(&mut out);
    assert_eq!(out.freeze(), concat(&[&cancelled, &ready(Error)]));
    // The transaction stays failed.
    let (out, _) = answer(&mut replayer, &[simple("select 1")]).await;
    assert_eq!(out, concat(&[&failure("25P02", ABORTED), &ready(Error)]));
}

#[tokio::test]
async fn a_cancelled_flush_batch_is_an_error_and_skips_to_the_sync() {
    let loaded = load(&[startup(), command("select 1", complete("SELECT 1"))]);
    let mut replayer = loaded.replayer().await;
    let cancelled = failure("57014", "canceling statement due to user request");
    let batch = [
        parse(None, "select 1", &[]),
        bind(None, None, &[], &[]),
        execute(None),
        flush(),
    ];
    answer(&mut replayer, &batch).await;

    // The client has not sent its Sync yet: it gets its only ReadyForQuery
    // for that, and nothing for the messages before it.
    let mut out = BytesMut::new();
    replayer.cancelled(&mut out);
    assert_eq!(out.freeze(), cancelled);
    let (out, _) = answer(&mut replayer, &[parse(None, "select 1", &[]), flush()]).await;
    assert_eq!(out, Bytes::new());
    let (out, _) = answer(&mut replayer, &run("select 1")).await;
    assert_eq!(out, ready(Idle));
    let (out, _) = answer(&mut replayer, &run("select 1")).await;
    assert_eq!(
        out,
        concat(&[
            &parse_complete(),
            &bind_complete(),
            &complete("SELECT 1"),
            &ready(Idle)
        ])
    );
}

#[tokio::test]
async fn a_cancelled_batch_ends_in_a_ready_for_query_when_the_batch_did() {
    let loaded = load(&[
        startup(),
        command("begin", complete("BEGIN")),
        query_entry("select 1", complete("SELECT 1"), &[]),
    ]);
    let mut replayer = loaded.replayer().await;
    let cancelled = failure("57014", "canceling statement due to user request");
    // The batches before the cancelled one end in a Flush, a Sync, and a
    // simple Query: only the last batch decides.
    let flushed = [
        parse(None, "begin", &[]),
        bind(None, None, &[], &[]),
        execute(None),
        flush(),
    ];
    answer(&mut replayer, &flushed).await;
    answer(&mut replayer, &[sync()]).await;
    let mut out = BytesMut::new();
    replayer.cancelled(&mut out);
    assert_eq!(out.freeze(), concat(&[&cancelled, &ready(Error)]));

    answer(&mut replayer, &[simple("rollback")]).await;
    answer(&mut replayer, &[simple("select 1")]).await;
    let mut out = BytesMut::new();
    replayer.cancelled(&mut out);
    assert_eq!(out.freeze(), concat(&[&cancelled, &ready(Idle)]));
}
