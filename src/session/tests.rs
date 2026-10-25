//! The tests run `serve` on a free port with a backend that answers every
//! statement, and talk to it with tokio-postgres or, to control the bytes on
//! the wire, with raw sockets.

use std::{
    future::poll_fn,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pgwire::messages::{
    PgWireFrontendMessage,
    data::{DataRow, FieldDescription, ParameterDescription, RowDescription},
    extendedquery::{BindComplete, CloseComplete, Flush, Parse, ParseComplete, Sync},
    response::{CommandComplete, TransactionStatus},
    startup::{PasswordMessageFamily, SASLInitialResponse, SASLResponse, Startup},
};
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_postgres::{AsyncMessage, Client, Error, NoTls, error::SqlState};

use super::*;
use crate::{
    auth::{self, Verifiers},
    class::Class,
    config,
    databases::{Databases, Target},
    metrics::{self, ClassMetrics},
    wire::{put, put_error, put_ready},
};

const SECRET: &str = "test secret";
const USER: &str = "authenticator";

/// Makes backends that answer any statement with one row of `1`.
struct Answering {
    /// Execution time of every batch, in nanoseconds.
    exec: Arc<AtomicU64>,
}

struct Answerer {
    exec: Arc<AtomicU64>,
    parameters: Vec<(String, String)>,
    /// Format of the result column, from the last Bind.
    format: i16,
    /// After the query `break`.
    broken: bool,
}

impl Connector for Answering {
    type Backend = Answerer;

    async fn connect(
        &self,
        _class: usize,
        _database: &str,
        _user: &str,
    ) -> Result<Answerer, String> {
        let parameters = [
            ("server_version", "18.0"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
        ];
        Ok(Answerer {
            exec: self.exec.clone(),
            parameters: parameters
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .to_vec(),
            format: 0,
            broken: false,
        })
    }
}

fn column(format: i16) -> RowDescription {
    // An int4 column.
    RowDescription::new(vec![FieldDescription::new(
        "n".into(),
        0,
        0,
        23,
        4,
        -1,
        format,
    )])
}

fn row(format: i16) -> DataRow {
    let mut data = BytesMut::new();
    if format == 1 {
        data.put_i32(4);
        data.put_i32(1);
    } else {
        data.put_i32(1);
        data.put_u8(b'1');
    }
    DataRow::new(data, 1)
}

impl Backend for Answerer {
    fn parameter_statuses(&self) -> &[(String, String)] {
        &self.parameters
    }

    async fn batch(&mut self, batch: &[PgWireFrontendMessage], out: &mut BytesMut) -> Duration {
        for message in batch {
            match message {
                PgWireFrontendMessage::Parse(_) => put(out, &ParseComplete::new()),
                PgWireFrontendMessage::Describe(describe) if describe.target_type == b'S' => {
                    put(out, &ParameterDescription::new(Vec::new()));
                    put(out, &column(0));
                }
                PgWireFrontendMessage::Describe(_) => put(out, &column(self.format)),
                PgWireFrontendMessage::Bind(bind) => {
                    self.format = bind
                        .result_column_format_codes
                        .first()
                        .copied()
                        .unwrap_or(0);
                    put(out, &BindComplete::new());
                }
                PgWireFrontendMessage::Execute(_) => {
                    put(out, &row(self.format));
                    put(out, &CommandComplete::new("SELECT 1".into()));
                }
                PgWireFrontendMessage::Close(_) => put(out, &CloseComplete::new()),
                PgWireFrontendMessage::Sync(_) => put_ready(out, TransactionStatus::Idle),
                PgWireFrontendMessage::Query(query) if query.query == "break" => {
                    put_error(out, "FATAL", "08006", "the backend is broken");
                    self.broken = true;
                }
                PgWireFrontendMessage::Query(_) => {
                    put(out, &column(0));
                    put(out, &row(0));
                    put(out, &CommandComplete::new("SELECT 1".into()));
                    put_ready(out, TransactionStatus::Idle);
                }
                _ => {}
            }
        }
        Duration::from_nanos(self.exec.load(Relaxed))
    }

    fn cancelled(&mut self, out: &mut BytesMut) {
        put_error(
            out,
            "ERROR",
            "57014",
            "canceling statement due to user request",
        );
        put_ready(out, TransactionStatus::Idle);
    }

    fn broken(&self) -> bool {
        self.broken
    }
}

struct Server {
    port: u16,
    shared: Arc<Shared<Answering>>,
    exec: Arc<AtomicU64>,
}

impl Server {
    fn set_exec(&self, exec: Duration) {
        self.exec.store(exec.as_nanos() as u64, Relaxed);
    }

    /// Replaces the settings of the first class.
    fn set_settings(&self, settings: config::Settings) {
        self.shared.classes[0].settings.store(Arc::new(settings));
    }

    /// The metrics of the first class.
    fn metrics(&self) -> &ClassMetrics {
        &self.shared.classes[0].metrics
    }
}

fn class(name: &str, databases: &str) -> config::Class {
    config::Class {
        name: name.into(),
        databases: databases.into(),
        users: vec![USER.into()],
        password: None,
        recording: PathBuf::new(),
        upstream: None,
        ignore_parameters: vec![],
        max_connections: None,
        column_override: None,
        settings: config::Settings::default(),
    }
}

/// Serves the classes on a free port, once it accepts connections.
async fn start(classes: Vec<config::Class>) -> Server {
    // Tests open thousands of sockets.
    let _ = rlimit::increase_nofile_limit(u64::MAX);
    let exec = Arc::new(AtomicU64::new(0));
    let shared = Arc::new(Shared {
        classes: classes.iter().map(Class::new).collect(),
        databases: Arc::new(Databases::new(&classes)),
        verifiers: Verifiers::new(),
        secret: SECRET.into(),
        tls: None,
        connector: Answering { exec: exec.clone() },
    });
    // Bound before `serve` gets it, so that no other test can take the port.
    let listener = listen(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = listener.local_addr().unwrap().port();
    let served = shared.clone();
    tokio::spawn(async move { serve(vec![listener], served).await });
    Server { port, shared, exec }
}

/// Derived password of the test user.
fn derived(database: &str) -> String {
    auth::derive_password(SECRET, database, USER)
}

fn config(server: &Server, user: &str, database: &str, password: &str) -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(server.port)
        .user(user)
        .dbname(database)
        .password(password);
    config
}

async fn connect_as(
    server: &Server,
    user: &str,
    database: &str,
    password: &str,
) -> Result<Client, Error> {
    let (client, connection) = config(server, user, database, password)
        .connect(NoTls)
        .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn connect(server: &Server, database: &str, password: &str) -> Result<Client, Error> {
    connect_as(server, USER, database, password).await
}

type Messages = mpsc::UnboundedReceiver<Result<AsyncMessage, Error>>;

/// Like `connect` with the derived password, but also returns what the server
/// sends outside of responses: notifications, and the error that ends the
/// connection.
async fn connect_listening(server: &Server, database: &str) -> (Client, Messages) {
    let (client, mut connection) = config(server, USER, database, &derived(database))
        .connect(NoTls)
        .await
        .unwrap();
    let (sender, messages) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
            let ended = message.is_err();
            if sender.send(message).is_err() || ended {
                break;
            }
        }
    });
    (client, messages)
}

fn code(error: &Error) -> &SqlState {
    error.code().expect("a database error")
}

fn message(error: &Error) -> &str {
    error.as_db_error().expect("a database error").message()
}

/// The count and the sum in seconds of the lateness samples of the only class,
/// as its metrics render them.
fn lateness(server: &Server) -> (u64, f64) {
    let mut out = String::new();
    metrics::render(&server.shared.classes, &mut out);
    let sample = |suffix: &str| {
        let prefix = format!(r#"pseudogress_answer_lateness_seconds_{suffix}{{class="app"}} "#);
        let line = out.lines().find_map(|line| line.strip_prefix(&prefix));
        line.unwrap_or_else(|| panic!("no {prefix:?} in:\n{out}"))
            .to_owned()
    };
    (
        sample("count").parse().unwrap(),
        sample("sum").parse().unwrap(),
    )
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn derived_and_fixed_passwords_work() {
    let mut fixed = class("fixed", "fixed-*");
    fixed.password = Some("fixed password".into());
    let server = start(vec![fixed, class("derived", "derived-*")]).await;

    let client = connect(&server, "fixed-1", "fixed password").await.unwrap();
    let row = client.query_one("select 1", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);

    let client = connect(&server, "derived-1", &derived("derived-1"))
        .await
        .unwrap();
    let row = client.query_one("select 1", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    let messages = client.simple_query("select 1").await.unwrap();
    assert_eq!(messages.len(), 2, "a row and the command's completion");

    // A password derived for another database does not work.
    let error = connect(&server, "derived-1", &derived("derived-2"))
        .await
        .err()
        .unwrap();
    assert_eq!(code(&error), &SqlState::INVALID_PASSWORD);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_wrong_password_is_rejected() {
    let server = start(vec![class("app", "app-*")]).await;
    let error = connect(&server, "app-1", "wrong").await.err().unwrap();
    assert_eq!(code(&error), &SqlState::INVALID_PASSWORD);
    assert_eq!(
        message(&error),
        format!("password authentication failed for user \"{USER}\"")
    );
    assert_eq!(server.metrics().auth_failures.load(Relaxed), 1);
    assert_eq!(server.metrics().connects.load(Relaxed), 0);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn an_unknown_database_is_rejected() {
    let server = start(vec![class("app", "app-*")]).await;
    let error = connect(&server, "nowhere", "any").await.err().unwrap();
    assert_eq!(code(&error), &SqlState::INVALID_CATALOG_NAME);
    assert_eq!(message(&error), "database \"nowhere\" does not exist");
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_user_outside_the_class_is_rejected() {
    let server = start(vec![class("app", "app-*")]).await;
    let error = connect_as(&server, "stranger", "app-1", "any")
        .await
        .err()
        .unwrap();
    assert_eq!(code(&error), &SqlState::INVALID_AUTHORIZATION_SPECIFICATION);
    assert_eq!(message(&error), "role \"stranger\" does not exist");
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn max_connections_limits_the_connections_of_a_database() {
    let mut limited = class("app", "app-*");
    limited.max_connections = Some(2);
    let server = start(vec![limited]).await;
    let password = derived("app-1");
    let _first = connect(&server, "app-1", &password).await.unwrap();
    let _second = connect(&server, "app-1", &password).await.unwrap();

    let error = connect(&server, "app-1", &password).await.err().unwrap();
    assert_eq!(code(&error), &SqlState::TOO_MANY_CONNECTIONS);
    assert_eq!(message(&error), "sorry, too many clients already");

    // Another database has its own connections.
    connect(&server, "app-2", &derived("app-2")).await.unwrap();
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn the_round_trip_delays_setup_and_every_batch() {
    let round_trip = Duration::from_millis(20);
    let mut slow = class("app", "app-*");
    slow.settings.round_trip = round_trip;
    let server = start(vec![slow]).await;

    // The startup message, and both steps of SCRAM, wait for an answer.
    let begun = Instant::now();
    let client = connect(&server, "app-1", &derived("app-1")).await.unwrap();
    assert!(begun.elapsed() >= 3 * round_trip, "{:?}", begun.elapsed());

    let begun = Instant::now();
    client.simple_query("select 1").await.unwrap();
    assert!(begun.elapsed() >= round_trip, "{:?}", begun.elapsed());
    assert_eq!(server.metrics().connections.load(Relaxed), 1);
    assert_eq!(server.metrics().statements.load(Relaxed), 1);
}

#[tokio::test]
#[ignore = "xfail: batches does not record how late answers are sent yet"]
async fn every_batch_records_how_late_its_answer_is_sent() {
    let server = start(vec![class("app", "app-*")]).await;
    let client = connect(&server, "app-1", &derived("app-1")).await.unwrap();
    // The answers of setup are not batches'.
    assert_eq!(lateness(&server), (0, 0.0));

    // Due at once, the answer is sent without waiting.
    client.simple_query("select 1").await.unwrap();
    let (count, sum) = lateness(&server);
    assert_eq!(count, 1);
    assert!(sum < 0.05, "{sum} s");

    // A timer that wakes on time leaves the answer a little late at most.
    let round_trip = Duration::from_millis(100);
    server.set_settings(config::Settings {
        round_trip,
        ..config::Settings::default()
    });
    client.simple_query("select 1").await.unwrap();
    let (count, total) = lateness(&server);
    assert_eq!(count, 2);
    assert!(total - sum < round_trip.as_secs_f64() / 2.0, "{total} s");
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_notification_reaches_a_listening_client() {
    let server = start(vec![class("app", "app-*")]).await;
    let (client, mut messages) = connect_listening(&server, "app-1").await;
    client.batch_execute("LISTEN updates").await.unwrap();

    let target = Target::Database {
        database: "app-1".into(),
    };
    assert_eq!(server.shared.databases.notify(&target, "other", "no"), 0);
    assert_eq!(server.shared.databases.notify(&target, "updates", "yes"), 1);
    let Ok(Some(Ok(AsyncMessage::Notification(notification)))) =
        timeout(Duration::from_secs(5), messages.recv()).await
    else {
        panic!("no notification");
    };
    assert_eq!(notification.channel(), "updates");
    assert_eq!(notification.payload(), "yes");

    // A notification that arrives while a statement runs is not lost.
    server.set_exec(Duration::from_millis(200));
    let query = client.simple_query("select 1");
    let notify = async {
        sleep(Duration::from_millis(50)).await;
        server.shared.databases.notify(&target, "updates", "late")
    };
    let (query, notified) = tokio::join!(query, notify);
    query.unwrap();
    assert_eq!(notified, 1);
    let Ok(Some(Ok(AsyncMessage::Notification(notification)))) =
        timeout(Duration::from_secs(5), messages.recv()).await
    else {
        panic!("no notification");
    };
    assert_eq!(notification.payload(), "late");

    client.batch_execute("UNLISTEN *").await.unwrap();
    assert_eq!(server.shared.databases.notify(&target, "updates", "no"), 0);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn terminate_ends_connections_with_an_admin_shutdown() {
    let server = start(vec![class("app", "app-*")]).await;
    let (client, mut messages) = connect_listening(&server, "app-1").await;
    client.simple_query("select 1").await.unwrap();

    let target = Target::Database {
        database: "app-1".into(),
    };
    assert_eq!(server.shared.databases.terminate(&target), 1);
    let Ok(Some(Err(error))) = timeout(Duration::from_secs(5), messages.recv()).await else {
        panic!("the connection did not end with an error");
    };
    assert_eq!(code(&error), &SqlState::ADMIN_SHUTDOWN);
    assert_eq!(
        message(&error),
        "terminating connection due to administrator command"
    );
    assert!(client.simple_query("select 1").await.is_err());
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn terminate_ends_a_connection_waiting_for_its_answer() {
    let server = start(vec![class("app", "app-*")]).await;
    let (client, mut messages) = connect_listening(&server, "app-1").await;
    server.set_exec(Duration::from_secs(30));
    let query = tokio::spawn(async move { client.simple_query("select 1").await });
    sleep(Duration::from_millis(100)).await;

    let target = Target::Database {
        database: "app-1".into(),
    };
    assert_eq!(server.shared.databases.terminate(&target), 1);
    let Ok(Some(Err(error))) = timeout(Duration::from_secs(5), messages.recv()).await else {
        panic!("the connection did not end with an error");
    };
    assert_eq!(code(&error), &SqlState::ADMIN_SHUTDOWN);
    assert!(query.await.unwrap().is_err());
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_cancel_interrupts_a_long_execution() {
    let server = start(vec![class("app", "app-*")]).await;
    let client = Arc::new(connect(&server, "app-1", &derived("app-1")).await.unwrap());
    server.set_exec(Duration::from_secs(30));
    let query = {
        let client = client.clone();
        tokio::spawn(async move { client.simple_query("select 1").await })
    };
    sleep(Duration::from_millis(100)).await;
    client.cancel_token().cancel_query(NoTls).await.unwrap();

    let result = timeout(Duration::from_secs(5), query)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code(&result.unwrap_err()), &SqlState::QUERY_CANCELED);

    // A cancel that arrives while nothing runs does not affect the next batch.
    server.set_exec(Duration::from_millis(100));
    client.cancel_token().cancel_query(NoTls).await.unwrap();
    client.simple_query("select 1").await.unwrap();
}

#[tokio::test]
#[ignore = "xfail: render does not know the lateness histogram yet"]
async fn a_cancelled_answer_records_no_lateness() {
    let server = start(vec![class("app", "app-*")]).await;
    let client = Arc::new(connect(&server, "app-1", &derived("app-1")).await.unwrap());
    server.set_exec(Duration::from_secs(30));
    let query = {
        let client = client.clone();
        tokio::spawn(async move { client.simple_query("select 1").await })
    };
    timeout(Duration::from_secs(5), async {
        while server.metrics().in_flight.load(Relaxed) != 1 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the batch is running");
    client.cancel_token().cancel_query(NoTls).await.unwrap();

    let result = timeout(Duration::from_secs(5), query)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code(&result.unwrap_err()), &SqlState::QUERY_CANCELED);
    assert_eq!(lateness(&server).0, 0);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_statement_timeout_cancels_slow_statements() {
    let mut limited = class("app", "app-*");
    limited.settings.faults.statement_timeout = Some(Duration::from_millis(50));
    let server = start(vec![limited]).await;
    let client = connect(&server, "app-1", &derived("app-1")).await.unwrap();
    client.simple_query("select 1").await.unwrap();

    server.set_exec(Duration::from_secs(30));
    let begun = Instant::now();
    let error = client.simple_query("select 1").await.unwrap_err();
    assert_eq!(code(&error), &SqlState::QUERY_CANCELED);
    assert!(begun.elapsed() >= Duration::from_millis(50));
    assert!(begun.elapsed() < Duration::from_secs(5));
    assert_eq!(server.metrics().faults.load(Relaxed), 1);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_stall_holds_back_the_answer() {
    let server = start(vec![class("app", "app-*")]).await;
    let client = connect(&server, "app-1", &derived("app-1")).await.unwrap();
    let mut settings = config::Settings::default();
    settings.faults.stall = 1.0;
    settings.faults.stall_for = Duration::from_millis(100);
    server.set_settings(settings);

    let begun = Instant::now();
    client.simple_query("select 1").await.unwrap();
    assert!(begun.elapsed() >= Duration::from_millis(100));
    assert_eq!(server.metrics().faults.load(Relaxed), 1);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_reset_ends_the_connection_without_an_answer() {
    let server = start(vec![class("app", "app-*")]).await;
    let client = connect(&server, "app-1", &derived("app-1")).await.unwrap();
    let mut settings = config::Settings::default();
    settings.faults.reset = 1.0;
    server.set_settings(settings);

    let error = client.simple_query("select 1").await.unwrap_err();
    assert!(error.as_db_error().is_none(), "{error}");
    assert_eq!(server.metrics().faults.load(Relaxed), 1);
}

#[tokio::test]
#[ignore = "xfail: the connection stays open after the answer of a broken backend"]
async fn a_broken_backend_ends_its_connection_after_its_answer() {
    let server = start(vec![class("app", "app-*")]).await;
    let (client, mut messages) = connect_listening(&server, "app-1").await;
    client.simple_query("select 1").await.unwrap();

    let error = client.simple_query("break").await.unwrap_err();
    assert_eq!(code(&error), &SqlState::CONNECTION_FAILURE);
    let Ok(Some(Err(_))) = timeout(Duration::from_secs(5), messages.recv()).await else {
        panic!("the connection did not end");
    };
    assert!(client.simple_query("select 1").await.is_err());
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn the_refuse_fault_closes_connections_at_once() {
    let mut refusing = class("app", "app-*");
    refusing.settings.faults.refuse = 1.0;
    let server = start(vec![refusing]).await;
    let error = connect(&server, "app-1", &derived("app-1"))
        .await
        .err()
        .unwrap();
    assert!(error.as_db_error().is_none(), "{error}");
    assert_eq!(server.metrics().faults.load(Relaxed), 1);
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn the_reject_password_fault_fails_correct_passwords() {
    let mut rejecting = class("app", "app-*");
    rejecting.settings.faults.reject_password = 1.0;
    let server = start(vec![rejecting]).await;
    let error = connect(&server, "app-1", &derived("app-1"))
        .await
        .err()
        .unwrap();
    assert_eq!(code(&error), &SqlState::INVALID_PASSWORD);
    assert_eq!(server.metrics().auth_failures.load(Relaxed), 1);
    assert_eq!(server.metrics().faults.load(Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "xfail: session is not implemented yet"]
async fn a_thousand_clients_run_ten_queries_each() {
    let mut fixed = class("app", "app-*");
    // One password, so that the clients share a verifier.
    fixed.password = Some("fixed password".into());
    let server = Arc::new(start(vec![fixed]).await);

    let clients: Vec<_> = (0..1000)
        .map(|i| {
            let server = server.clone();
            tokio::spawn(async move {
                let client = connect(&server, &format!("app-{}", i % 100), "fixed password")
                    .await
                    .unwrap();
                for _ in 0..5 {
                    client.simple_query("select 1").await.unwrap();
                    let row = client.query_one("select 1", &[]).await.unwrap();
                    assert_eq!(row.get::<_, i32>(0), 1);
                }
            })
        })
        .collect();
    for client in clients {
        client.await.unwrap();
    }
    // Every connection ends with its client.
    for _ in 0..100 {
        if server.metrics().connections.load(Relaxed) == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(server.metrics().connections.load(Relaxed), 0);
    assert_eq!(server.metrics().connects.load(Relaxed), 1000);
}

/// A client that writes and reads the bytes of the protocol itself.
struct Raw {
    stream: TcpStream,
    input: BytesMut,
}

impl Raw {
    async fn connect(server: &Server) -> Raw {
        Raw {
            stream: TcpStream::connect((Ipv4Addr::LOCALHOST, server.port))
                .await
                .unwrap(),
            input: BytesMut::new(),
        }
    }

    /// Connects and logs in with SCRAM-SHA-256, up to ReadyForQuery.
    async fn login(server: &Server, database: &str, password: &str) -> Raw {
        let mut raw = Raw::connect(server).await;
        raw.startup(3, 0, database).await;
        let (tag, body) = raw.next().await;
        assert_eq!((tag, (&body[..4]).get_i32()), (b'R', 10), "SASL is offered");

        let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unrequested());
        raw.send(&[PgWireFrontendMessage::PasswordMessageFamily(
            PasswordMessageFamily::SASLInitialResponse(SASLInitialResponse::new(
                SCRAM_SHA_256.into(),
                Some(Bytes::copy_from_slice(scram.message())),
            )),
        )])
        .await;
        let (tag, body) = raw.next().await;
        assert_eq!((tag, (&body[..4]).get_i32()), (b'R', 11), "SASL continues");
        scram.update(&body[4..]).unwrap();
        raw.send(&[PgWireFrontendMessage::PasswordMessageFamily(
            PasswordMessageFamily::SASLResponse(SASLResponse::new(Bytes::copy_from_slice(
                scram.message(),
            ))),
        )])
        .await;
        let (tag, body) = raw.next().await;
        assert_eq!((tag, (&body[..4]).get_i32()), (b'R', 12), "SASL ends");
        scram.finish(&body[4..]).unwrap();
        loop {
            if raw.next().await.0 == b'Z' {
                return raw;
            }
        }
    }

    async fn startup(&mut self, major: u16, minor: u16, database: &str) {
        let mut startup = Startup::new();
        startup.protocol_number_major = major;
        startup.protocol_number_minor = minor;
        startup.parameters.insert("user".into(), USER.into());
        startup
            .parameters
            .insert("database".into(), database.into());
        self.send(&[PgWireFrontendMessage::Startup(startup)]).await;
    }

    /// Sends the messages in one write.
    async fn send(&mut self, messages: &[PgWireFrontendMessage]) {
        let mut out = BytesMut::new();
        for message in messages {
            message.encode(&mut out).unwrap();
        }
        self.stream.write_all(&out).await.unwrap();
    }

    /// Reads a message of the server: its tag and body.
    async fn next(&mut self) -> (u8, Bytes) {
        loop {
            if self.input.len() >= 5 {
                let length = (&self.input[1..5]).get_i32() as usize;
                if self.input.len() > length {
                    let tag = self.input[0];
                    self.input.advance(5);
                    return (tag, self.input.split_to(length - 4).freeze());
                }
            }
            let read = self.stream.read_buf(&mut self.input).await.unwrap();
            assert!(read > 0, "the server closed the connection");
        }
    }

    async fn next_within(&mut self, wait: Duration) -> Option<(u8, Bytes)> {
        timeout(wait, self.next()).await.ok()
    }
}

fn parse() -> PgWireFrontendMessage {
    PgWireFrontendMessage::Parse(Parse::new(None, "select 1".into(), Vec::new()))
}

fn sync() -> PgWireFrontendMessage {
    PgWireFrontendMessage::Sync(Sync::new())
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn pipelined_batches_are_answered_in_order() {
    let server = start(vec![class("app", "app-*")]).await;
    let mut raw = Raw::login(&server, "app-1", &derived("app-1")).await;

    raw.send(&[parse(), sync(), parse(), parse(), sync()]).await;
    let mut tags = Vec::new();
    for _ in 0..5 {
        tags.push(raw.next().await.0);
    }
    assert_eq!(tags, [b'1', b'Z', b'1', b'1', b'Z']);
    assert!(raw.next_within(Duration::from_millis(100)).await.is_none());
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn pipelined_batches_pay_the_round_trip_once() {
    let mut slow = class("app", "app-*");
    slow.settings.round_trip = Duration::from_millis(100);
    let server = start(vec![slow]).await;
    let mut raw = Raw::login(&server, "app-1", &derived("app-1")).await;
    server.set_exec(Duration::from_millis(50));

    // The network is paid once, but the executions add up.
    let begun = Instant::now();
    raw.send(&[parse(), sync(), parse(), sync()]).await;
    raw.next().await;
    raw.next().await;
    let first = begun.elapsed();
    raw.next().await;
    raw.next().await;
    let second = begun.elapsed();
    assert!(first >= Duration::from_millis(150), "{first:?}");
    assert!(second >= Duration::from_millis(200), "{second:?}");
    assert!(second < Duration::from_millis(300), "{second:?}");
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_flush_batch_is_answered_without_ready_for_query() {
    let server = start(vec![class("app", "app-*")]).await;
    let mut raw = Raw::login(&server, "app-1", &derived("app-1")).await;

    raw.send(&[parse(), PgWireFrontendMessage::Flush(Flush::new())])
        .await;
    assert_eq!(raw.next().await.0, b'1');
    assert!(raw.next_within(Duration::from_millis(100)).await.is_none());

    raw.send(&[sync()]).await;
    assert_eq!(raw.next().await.0, b'Z');
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn a_newer_protocol_minor_is_negotiated_down() {
    let server = start(vec![class("app", "app-*")]).await;
    let mut raw = Raw::connect(&server).await;
    let mut startup = Startup::new();
    startup.protocol_number_minor = 2;
    startup.parameters.insert("user".into(), USER.into());
    startup.parameters.insert("database".into(), "app-1".into());
    startup
        .parameters
        .insert("_pq_.feature".into(), "on".into());
    raw.send(&[PgWireFrontendMessage::Startup(startup)]).await;

    let (tag, body) = raw.next().await;
    assert_eq!(tag, b'v');
    let mut expected = BytesMut::new();
    expected.put_i32(0);
    expected.put_i32(1);
    expected.put_slice(b"_pq_.feature\0");
    assert_eq!(body, expected);
    assert_eq!(raw.next().await.0, b'R');
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn another_protocol_major_is_refused() {
    let server = start(vec![class("app", "app-*")]).await;
    let mut raw = Raw::connect(&server).await;
    raw.startup(4, 0, "app-1").await;

    let (tag, body) = raw.next().await;
    assert_eq!(tag, b'E');
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("0A000"), "{text}");
}

#[tokio::test]
#[ignore = "xfail: session is not implemented yet"]
async fn the_hang_fault_never_answers() {
    let mut hanging = class("app", "app-*");
    hanging.settings.faults.hang = 1.0;
    let server = start(vec![hanging]).await;
    let mut raw = Raw::connect(&server).await;
    raw.startup(3, 0, "app-1").await;

    assert!(raw.next_within(Duration::from_millis(300)).await.is_none());
    assert_eq!(server.metrics().faults.load(Relaxed), 1);
}
