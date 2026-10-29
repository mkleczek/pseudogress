//! Record mode with a class service, in this process: a service that answers
//! from a table and counts the requests, a server wired as `main` wires record
//! mode, and clients that connect to it. The upstream is the PostgreSQL of
//! the nix shell, where a connection gets as far as running statements, and
//! a port nobody listens on otherwise.

use std::{
    collections::HashMap,
    fs,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_postgres::{Client, Error, NoTls, SimpleQueryMessage};

use super::*;
use crate::{
    auth::Verifiers,
    class::Class,
    config::{self, Config},
    databases::Databases,
    options::{self, Mode},
    record::{Record, tests::Cluster},
    recording::{self, Entry},
    replay::Replay,
    session::{self, Connector, Shared},
};

const SECRET: &str = "secret";
const PASSWORD: &str = "secret";
const USER: &str = "client";

// The class service.

/// What the stub says about a database.
#[derive(Clone)]
enum Answer {
    Class(Value),
    /// A status line, such as `500 Internal Server Error`, and a body.
    Status(&'static str, &'static str),
}

/// A class service that answers `GET /classes/{database}?user={user}` from a
/// table. A database that isn't in it is a 404.
struct Stub {
    port: u16,
    answers: Arc<Mutex<HashMap<String, Answer>>>,
    /// The targets of the requests that arrived.
    requests: Arc<Mutex<Vec<String>>>,
}

impl Stub {
    /// Answers after `delay`, so that requests can overlap.
    async fn start(delay: Duration) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stub = Stub {
            port: listener.local_addr().unwrap().port(),
            answers: Arc::default(),
            requests: Arc::default(),
        };
        let (answers, requests) = (stub.answers.clone(), stub.requests.clone());
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (answers, requests) = (answers.clone(), requests.clone());
                tokio::spawn(async move {
                    let head = read_head(&mut stream).await;
                    let target = head.split(' ').nth(1).unwrap_or_default().to_owned();
                    requests.lock().unwrap().push(target.clone());
                    let database = target
                        .strip_prefix("/classes/")
                        .and_then(|rest| rest.split('?').next())
                        .unwrap_or_default();
                    let answer = answers.lock().unwrap().get(database).cloned();
                    tokio::time::sleep(delay).await;
                    let (status, body) = match answer {
                        Some(Answer::Class(class)) => ("200 OK", class.to_string()),
                        Some(Answer::Status(status, body)) => (status, body.to_owned()),
                        None => ("404 Not Found", String::new()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    // The client may be gone.
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        stub
    }

    fn url(&self) -> String {
        format!(
            "http://127.0.0.1:{}/classes/{{database}}?user={{user}}",
            self.port
        )
    }

    fn say(&self, database: &str, answer: Answer) {
        self.answers
            .lock()
            .unwrap()
            .insert(database.to_owned(), answer);
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

async fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read_buf(&mut head).await.unwrap() == 0 {
            break;
        }
    }
    String::from_utf8(head).unwrap()
}

/// The class of a database, as the service answers: users and password for
/// the clients of these tests, and the upstream.
fn class_json(name: &str, databases: &str, upstream: u16) -> Value {
    json!({
        "name": name,
        "databases": databases,
        "users": [USER],
        "password": PASSWORD,
        "upstream": {
            "host": "127.0.0.1",
            "port": upstream,
            "user": "postgres",
            "database": "postgres",
        },
    })
}

// The server.

/// Record mode, wired as `main` wires it, listening on a free port.
struct Server {
    port: u16,
    shared: Arc<Shared<Record>>,
    /// Where the classes of the service are recorded.
    dir: PathBuf,
}

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "pseudogress-loader-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Relaxed)
    ))
}

/// A `[[class]]` table of the configuration.
fn configured(dir: &Path, name: &str, databases: &str, upstream: u16) -> String {
    format!(
        r#"
        [[class]]
        name = "{name}"
        databases = "{databases}"
        users = ["{USER}"]
        password = "{PASSWORD}"
        recording = "{}"
        upstream = {{ host = "127.0.0.1", port = {upstream}, user = "postgres", database = "postgres" }}
        "#,
        dir.join(format!("{name}.jsonl")).display()
    )
}

/// Serves `shared` on a free port.
fn serve<C: Connector>(shared: &Arc<Shared<C>>) -> u16 {
    let listener = session::listen(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(session::serve(vec![listener], shared.clone()));
    port
}

/// A server for the `classes` of the configuration (name and databases), and
/// the service of `stub`, whose classes are at most `max_classes`.
async fn start(stub: &Stub, upstream: u16, classes: &[(&str, &str)], max_classes: usize) -> Server {
    let dir = temp_dir();
    let tables: String = classes
        .iter()
        .map(|(name, databases)| configured(&dir, name, databases, upstream))
        .collect();
    let config: Config = toml::from_str(&format!(
        "listen = []\nsecret = \"{SECRET}\"\n{}",
        if tables.is_empty() {
            "class = []"
        } else {
            &tables
        }
    ))
    .unwrap();
    let arguments: Vec<String> = [
        "pseudogress",
        "record",
        "pseudogress.toml",
        "--class-url",
        &stub.url(),
        "--recordings",
        dir.to_str().unwrap(),
        "--max-classes",
        &max_classes.to_string(),
    ]
    .map(str::to_owned)
    .into();
    let Mode::Record {
        service: Some(service),
    } = options::parse(&arguments).unwrap().mode
    else {
        panic!("the arguments are for a class service");
    };
    let room = service.max_classes;
    let shared = Arc::new(Shared {
        classes: Class::registry(&config.classes, room),
        databases: Arc::new(Databases::with_room(&config.classes, room)),
        verifiers: Verifiers::new(),
        secret: SECRET.into(),
        tls: None,
        connector: Record::with_room(&config, room).unwrap(),
        loader: Some(Loader::new(service).unwrap()),
    });
    Server {
        port: serve(&shared),
        shared,
        dir,
    }
}

impl Server {
    /// What `main` does at shutdown: the recordings, then `classes.toml`.
    fn finish(&self) {
        self.shared.connector.write().unwrap();
        self.shared.loader.as_ref().unwrap().write().unwrap();
    }

    /// The names of the classes, in matching order.
    fn classes(&self) -> Vec<String> {
        self.shared
            .classes
            .iter()
            .map(|class| class.name.clone())
            .collect()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

// The clients.

async fn connect_to(port: u16, database: &str) -> Result<Client, Error> {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user(USER)
        .dbname(database)
        .password(PASSWORD)
        .connect(NoTls)
        .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn connect(server: &Server, database: &str) -> Result<Client, Error> {
    connect_to(server.port, database).await
}

/// The SQLSTATE and the message of the error that refused a connection.
fn refusal(result: Result<Client, Error>) -> (String, String) {
    let error = result.expect_err("the connection is refused");
    let error = error.as_db_error().expect("a database error");
    (error.code().code().to_owned(), error.message().to_owned())
}

/// Statements of both kinds: a simple query, and a prepared one with a
/// parameter. Their answers are PostgreSQL's, or a replay of them.
async fn statements(client: &Client) -> (String, i32) {
    let messages = client.simple_query("select 'a' || 'b'").await.unwrap();
    let text = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("a row");
    let row = client
        .query_one("select $1::int + 1", &[&41_i32])
        .await
        .unwrap();
    (text, row.get(0))
}

/// A port that nobody listens on, for an upstream that is never reached.
fn closed_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// Lookups.

#[tokio::test]
async fn an_unknown_database_is_looked_up_and_its_class_added() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[], 4).await;
    assert_eq!(server.classes(), Vec::<String>::new());

    // The client connects, authenticates and runs statements.
    let client = connect(&server, "tenant-1").await.unwrap();
    assert_eq!(statements(&client).await, ("ab".to_owned(), 42));

    assert_eq!(server.classes(), ["tenants"]);
    assert_eq!(stub.requests(), ["/classes/tenant-1?user=client"]);
}

#[tokio::test]
async fn the_recording_of_a_class_of_the_service_is_in_the_recordings_directory() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[], 4).await;
    let client = connect(&server, "tenant-1").await.unwrap();
    statements(&client).await;
    drop(client);

    server.finish();

    let entries = recording::read(&server.dir.join("tenants.jsonl")).unwrap();
    assert!(
        entries
            .iter()
            .any(|entry| matches!(entry, Entry::Query { sql, .. } if sql == "select 'a' || 'b'")),
        "{entries:?}"
    );
    assert!(
        entries.iter().any(
            |entry| matches!(entry, Entry::Execute { sql, .. } if sql == "select $1::int + 1")
        ),
        "{entries:?}"
    );
}

/// What `classes.toml` is made of.
#[derive(Deserialize)]
struct File {
    #[serde(rename = "class")]
    classes: Vec<config::Class>,
}

fn read_classes_file(server: &Server) -> File {
    let text = fs::read_to_string(server.dir.join("classes.toml")).unwrap();
    toml::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
}

#[tokio::test]
async fn classes_toml_is_written_at_shutdown_sorted_by_name_with_relative_recordings() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    for (database, name) in [("z-1", "zeta"), ("a-1", "alpha"), ("m-1", "Mid")] {
        let databases = format!("{}-*", &database[..1]);
        stub.say(
            database,
            Answer::Class(class_json(name, &databases, cluster.port)),
        );
    }
    let server = start(&stub, cluster.port, &[], 4).await;
    // Added in this order, which is not the order of the file.
    for database in ["z-1", "a-1", "m-1"] {
        connect(&server, database).await.unwrap();
    }
    assert_eq!(server.classes(), ["zeta", "alpha", "Mid"]);

    server.finish();

    let file = read_classes_file(&server);
    let written: Vec<_> = file
        .classes
        .iter()
        .map(|class| (class.name.as_str(), class.recording.to_str().unwrap()))
        .collect();
    assert_eq!(
        written,
        [
            ("Mid", "Mid.jsonl"),
            ("alpha", "alpha.jsonl"),
            ("zeta", "zeta.jsonl")
        ]
    );
    // What the service said is in the file.
    assert_eq!(file.classes[2].databases, "z-*");
    assert_eq!(file.classes[2].password.as_deref(), Some(PASSWORD));
    assert_eq!(
        file.classes[2].upstream.as_ref().unwrap().port,
        cluster.port
    );
}

#[tokio::test]
async fn without_classes_from_the_service_classes_toml_is_empty_but_valid() {
    let stub = Stub::start(Duration::ZERO).await;
    let server = start(&stub, closed_port(), &[("app", "app-*")], 4).await;

    server.finish();

    assert!(read_classes_file(&server).classes.is_empty());
}

#[tokio::test]
async fn a_class_of_the_configuration_wins_over_the_service() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    // The service has another class for the database of the configuration's.
    stub.say(
        "app-1",
        Answer::Class(class_json("shadow", "app-*", closed_port())),
    );
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[("app", "app-*")], 4).await;

    let client = connect(&server, "app-1").await.unwrap();
    assert_eq!(statements(&client).await.1, 42);
    // The service isn't asked about it.
    assert_eq!(stub.requests(), Vec::<String>::new());

    connect(&server, "tenant-1").await.unwrap();
    assert_eq!(stub.requests().len(), 1);
    // The configuration's classes are first, whatever they matched.
    assert_eq!(server.classes(), ["app", "tenants"]);
}

#[tokio::test]
async fn a_database_that_a_loaded_class_matches_is_not_looked_up() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[], 4).await;

    connect(&server, "tenant-1").await.unwrap();
    let client = connect(&server, "tenant-2").await.unwrap();
    assert_eq!(statements(&client).await.1, 42);

    assert_eq!(stub.requests(), ["/classes/tenant-1?user=client"]);
    assert_eq!(server.classes(), ["tenants"]);
}

#[tokio::test]
async fn a_database_that_a_class_matches_by_the_time_of_the_lookup_is_not_asked_about() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[], 4).await;
    connect(&server, "tenant-1").await.unwrap();
    let loader = server.shared.loader.as_ref().unwrap();

    // What a connection does that found no class, and then a class that
    // matches was added.
    let loaded = loader.load(&server.shared, "tenant-2", USER).await;

    assert_eq!(loaded, Ok(()));
    assert_eq!(stub.requests().len(), 1);
}

#[tokio::test]
async fn loading_says_why_no_class_was_found() {
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "broken-1",
        Answer::Status("503 Service Unavailable", "try later"),
    );
    let server = start(&stub, closed_port(), &[], 4).await;
    let loader = server.shared.loader.as_ref().unwrap();

    let unknown = loader.load(&server.shared, "unknown-1", USER).await;
    let broken = loader.load(&server.shared, "broken-1", USER).await;

    assert_eq!(unknown, Err(LoadError::NotFound));
    let Err(LoadError::Failed(reason)) = broken else {
        panic!("{broken:?}");
    };
    assert!(reason.contains("503"), "{reason}");
    assert!(reason.contains("try later"), "{reason}");
}

#[tokio::test]
async fn concurrent_connects_to_one_unknown_database_make_one_request() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    // Slow enough for every client to arrive before the answer.
    let stub = Stub::start(Duration::from_millis(500)).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[], 4).await;

    let mut connecting = JoinSet::new();
    for _ in 0..8 {
        let port = server.port;
        connecting.spawn(async move {
            let client = connect_to(port, "tenant-1").await.unwrap();
            statements(&client).await
        });
    }
    while let Some(done) = connecting.join_next().await {
        assert_eq!(done.unwrap(), ("ab".to_owned(), 42));
    }

    assert_eq!(stub.requests().len(), 1, "{:?}", stub.requests());
    assert_eq!(server.classes(), ["tenants"]);
}

#[tokio::test]
async fn concurrent_connects_to_a_database_that_does_not_exist_make_one_request() {
    let stub = Stub::start(Duration::from_millis(500)).await;
    let server = start(&stub, closed_port(), &[], 4).await;

    let mut connecting = JoinSet::new();
    for _ in 0..8 {
        let port = server.port;
        connecting.spawn(async move { refusal(connect_to(port, "nobody").await) });
    }
    while let Some(done) = connecting.join_next().await {
        let (code, message) = done.unwrap();
        assert_eq!(code, "3D000");
        assert_eq!(message, "database \"nobody\" does not exist");
    }

    assert_eq!(stub.requests().len(), 1, "{:?}", stub.requests());
}

#[tokio::test]
async fn two_databases_that_get_the_same_class_make_one_class() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    // Both are asked about before either answer, so the second answer finds
    // the class of the first.
    let stub = Stub::start(Duration::from_millis(500)).await;
    for database in ["tenant-1", "tenant-2"] {
        stub.say(
            database,
            Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
        );
    }
    let server = start(&stub, cluster.port, &[], 4).await;

    let (first, second) = tokio::join!(connect(&server, "tenant-1"), connect(&server, "tenant-2"));
    first.unwrap();
    second.unwrap();

    assert_eq!(stub.requests().len(), 2);
    assert_eq!(server.classes(), ["tenants"]);
    server.finish();
    assert_eq!(read_classes_file(&server).classes.len(), 1);
}

#[tokio::test]
async fn a_class_that_differs_from_the_one_returned_before_fails_the_connection() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    // The same name, and another definition.
    stub.say(
        "other-1",
        Answer::Class(class_json("tenants", "other-*", cluster.port)),
    );
    let server = start(&stub, cluster.port, &[], 4).await;
    connect(&server, "tenant-1").await.unwrap();

    let (code, message) = refusal(connect(&server, "other-1").await);

    assert_eq!(code, "08006");
    assert!(message.starts_with("class service: "), "{message}");
    assert!(
        message.contains("class tenants: the service returned a different definition before"),
        "{message}"
    );
    assert_eq!(server.classes(), ["tenants"]);
}

#[tokio::test]
async fn a_name_of_the_configuration_fails_the_connection() {
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "other-1",
        Answer::Class(class_json("app", "other-*", closed_port())),
    );
    let server = start(&stub, closed_port(), &[("app", "app-*")], 4).await;

    let (code, message) = refusal(connect(&server, "other-1").await);

    assert_eq!(code, "08006");
    assert!(message.starts_with("class service: "), "{message}");
    assert!(
        message.contains("class app: a class of the configuration has that name"),
        "{message}"
    );
    assert_eq!(server.classes(), ["app"]);
}

#[tokio::test]
async fn a_database_that_the_service_does_not_know_does_not_exist() {
    let stub = Stub::start(Duration::ZERO).await;
    let server = start(&stub, closed_port(), &[], 4).await;

    let (code, message) = refusal(connect(&server, "nobody").await);

    assert_eq!(code, "3D000");
    assert_eq!(message, "database \"nobody\" does not exist");
    assert_eq!(stub.requests(), ["/classes/nobody?user=client"]);
}

#[tokio::test]
async fn a_database_that_was_not_found_is_looked_up_again() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    let server = start(&stub, cluster.port, &[], 4).await;
    assert_eq!(refusal(connect(&server, "late-1").await).0, "3D000");

    stub.say(
        "late-1",
        Answer::Class(class_json("late", "late-*", cluster.port)),
    );
    connect(&server, "late-1").await.unwrap();

    assert_eq!(stub.requests().len(), 2);
}

#[tokio::test]
async fn a_failing_service_fails_the_connection_with_the_reason() {
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Status(
            "500 Internal Server Error",
            "the database of classes is down",
        ),
    );
    let server = start(&stub, closed_port(), &[], 4).await;

    let (code, message) = refusal(connect(&server, "tenant-1").await);

    assert_eq!(code, "08006");
    assert!(message.starts_with("class service: "), "{message}");
    assert!(message.contains("500"), "{message}");
    assert!(
        message.contains("the database of classes is down"),
        "{message}"
    );
    assert_eq!(server.classes(), Vec::<String>::new());
}

#[tokio::test]
async fn an_invalid_class_fails_the_connection() {
    let stub = Stub::start(Duration::ZERO).await;
    // Its databases don't include the one that was asked about.
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "other-*", closed_port())),
    );
    let server = start(&stub, closed_port(), &[], 4).await;

    let (code, message) = refusal(connect(&server, "tenant-1").await);

    assert_eq!(code, "08006");
    assert!(message.starts_with("class service: "), "{message}");
    assert!(message.contains("databases"), "{message}");
    assert_eq!(server.classes(), Vec::<String>::new());
}

#[tokio::test]
async fn a_service_that_would_add_more_classes_than_the_most_fails_the_connection() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    for (database, name) in [("a-1", "a"), ("b-1", "b")] {
        let databases = format!("{name}-*");
        stub.say(
            database,
            Answer::Class(class_json(name, &databases, cluster.port)),
        );
    }
    let server = start(&stub, cluster.port, &[], 1).await;
    connect(&server, "a-1").await.unwrap();

    let (code, message) = refusal(connect(&server, "b-1").await);

    assert_eq!(code, "08006");
    assert!(message.starts_with("class service: "), "{message}");
    assert!(message.contains("--max-classes"), "{message}");
    assert!(
        message.contains("pattern covers a group of databases"),
        "{message}"
    );
    assert_eq!(server.classes(), ["a"]);
    // The class that was added still serves its databases.
    connect(&server, "a-2").await.unwrap();
    assert_eq!(stub.requests().len(), 2);
}

#[tokio::test]
async fn the_most_classes_do_not_limit_the_classes_of_the_configuration() {
    let stub = Stub::start(Duration::ZERO).await;
    let classes = [("a", "a-*"), ("b", "b-*"), ("c", "c-*")];
    let server = start(&stub, closed_port(), &classes, 1).await;

    assert_eq!(server.classes(), ["a", "b", "c"]);
}

// The directory of recordings.

fn service(dir: &Path) -> options::Service {
    options::Service {
        url: crate::service::Template::parse("http://127.0.0.1:1/{database}").unwrap(),
        recordings: dir.to_owned(),
        max_classes: 1,
    }
}

#[test]
fn the_directory_of_recordings_is_created_at_startup() {
    let dir = temp_dir().join("nested").join("recordings");

    Loader::new(service(&dir)).unwrap();

    assert!(dir.is_dir());
    fs::remove_dir_all(dir.parent().unwrap().parent().unwrap()).unwrap();
}

#[test]
fn a_directory_of_recordings_that_cannot_be_created_fails_at_startup() {
    let dir = temp_dir();
    fs::create_dir_all(&dir).unwrap();
    let file = dir.join("file");
    fs::write(&file, "").unwrap();

    let error = Loader::new(service(&file.join("recordings")))
        .err()
        .unwrap();

    fs::remove_dir_all(&dir).unwrap();
    assert!(error.contains("file"), "{error}");
}

// Replay.

#[tokio::test]
async fn replay_answers_a_database_of_a_service_class_from_its_recording() {
    let Some(cluster) = Cluster::start(None) else {
        return;
    };
    let stub = Stub::start(Duration::ZERO).await;
    stub.say(
        "tenant-1",
        Answer::Class(class_json("tenants", "tenant-*", cluster.port)),
    );
    let recorder = start(&stub, cluster.port, &[], 4).await;
    let client = connect(&recorder, "tenant-1").await.unwrap();
    let recorded = statements(&client).await;
    drop(client);
    recorder.finish();
    assert_eq!(recorded, ("ab".to_owned(), 42));

    // Replay, with the recordings directory, and no PostgreSQL or service.
    drop(cluster);
    let mut config: Config = toml::from_str("listen = []\nclass = []").unwrap();
    config::load_classes(&mut config, &recorder.dir).unwrap();
    let classes: crate::class::Classes = Arc::new(config.classes.iter().map(Class::new).collect());
    let shared = Arc::new(Shared {
        classes: classes.clone(),
        databases: Arc::new(Databases::new(&config.classes)),
        verifiers: Verifiers::new(),
        secret: SECRET.into(),
        tls: None,
        connector: Replay::load(&config, classes).unwrap(),
        loader: None,
    });
    let port = serve(&shared);

    // The database that was recorded, and another of the class.
    for database in ["tenant-1", "tenant-2"] {
        let client = connect_to(port, database).await.unwrap();
        assert_eq!(statements(&client).await, recorded, "{database}");
    }
    // A database that no class matches is not looked up.
    assert_eq!(refusal(connect_to(port, "nobody").await).0, "3D000");
    assert_eq!(stub.requests().len(), 1);
}
