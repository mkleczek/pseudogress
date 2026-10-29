//! Black-box test of the binary: what a client's traffic gets from a real
//! PostgreSQL in record mode, thousands of connections over hundreds of
//! virtual databases get again in replay mode, without PostgreSQL.
//!
//! It starts a throwaway cluster, so it needs `initdb` and `pg_ctl` on PATH
//! (`nix-shell` has them) and passes without doing anything otherwise.

use std::{
    fs,
    net::{Ipv4Addr, SocketAddr, TcpListener as StdListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    thread,
    time::{Duration, Instant},
};

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::sleep,
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const SECRET: &str = "end-to-end secret";
const USER: &str = "app";
const DATABASES: usize = 500;
const CONNECTIONS: usize = 2000;
/// Connections made at once. The listener's accept queue is capped at 128 on
/// macOS, and the clients' SCRAM hashing is what takes the time anyway.
const BURST: usize = 50;
/// How often every connection repeats the reading statements.
const ROUNDS: usize = 3;
const STARTUP: Duration = Duration::from_secs(10);

const SCHEMA: &str = "
    CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL, price numeric(10, 2) NOT NULL);
    INSERT INTO items (name, price) VALUES ('apple', 1.50), ('banana', 0.25), ('cherry', 12.00);
    CREATE TABLE audit (id serial PRIMARY KEY, note text NOT NULL);";

#[tokio::test(flavor = "multi_thread")]
async fn replays_what_postgres_answered_to_thousands_of_connections() {
    if Command::new("initdb").arg("--version").output().is_err() {
        eprintln!("initdb is not on PATH: skipping the end-to-end test (run it in nix-shell)");
        return;
    }
    // A connection is a file descriptor here as well as in the server.
    let _ = rlimit::increase_nofile_limit(u64::MAX);

    let dir = std::env::temp_dir().join(format!("pseudogress-e2e-{}", std::process::id()));
    // Declared first, so that it is dropped last: after the processes that
    // use the directory are gone.
    let postgres = Postgres::start(&dir);
    postgres.createdb("app");
    postgres.psql("app", SCHEMA);

    let recording = dir.join("app.jsonl");
    let config = dir.join("pseudogress.toml");
    fs::write(
        &config,
        format!(
            r#"listen = ["127.0.0.1:0"]
admin = "127.0.0.1:0"
secret = "{SECRET}"

[[class]]
name = "app"
databases = "app-*"
users = ["{USER}"]
recording = "{recording}"
upstream = {{ host = "127.0.0.1", port = {postgres_port}, user = "postgres", database = "app" }}
"#,
            recording = recording.display(),
            postgres_port = postgres.port
        ),
    )
    .unwrap();

    let mut record = Pseudogress::spawn("record", &config, &[], &dir.join("record.log"));
    let (listen, _) = record.wait_until_listening();
    let recorded = workload(listen.port(), "app-0").await;
    assert_eq!(recorded, postgres_answers());
    let status = record.terminate();
    assert!(
        status.success(),
        "record mode exited with {status}:\n{}",
        record.stderr()
    );
    assert!(
        fs::metadata(&recording).is_ok_and(|file| file.len() > 0),
        "record mode wrote no recording:\n{}",
        record.stderr()
    );

    let mut replay = Pseudogress::spawn("replay", &config, &[], &dir.join("replay.log"));
    let (listen, admin) = replay.wait_until_listening();
    assert_eq!(workload(listen.port(), "app-0").await, recorded);

    // Every connection is open at once, and all of them read at once.
    let clients = connect_all(listen.port()).await;
    wait_for_metric(admin, "pseudogress_connections", CONNECTIONS as i64).await;
    let mut reading = JoinSet::new();
    for (database, client) in clients {
        let expected = recorded.reads.clone();
        reading.spawn(async move {
            for _ in 0..ROUNDS {
                assert_eq!(reads(&client).await, expected, "answers in {database}");
            }
            client
        });
    }
    let mut open = Vec::with_capacity(CONNECTIONS);
    while let Some(done) = reading.join_next().await {
        open.push(done.unwrap());
    }
    assert_eq!(metric(admin, "pseudogress_misses_total").await, 0);

    drop(open);
    wait_for_metric(admin, "pseudogress_connections", 0).await;
}

/// Records through a class service and replays from the recordings: the
/// service gives the class of `svc-*` databases, which the configuration lacks.
#[tokio::test(flavor = "multi_thread")]
async fn replays_what_a_class_service_gave_classes_to_record() {
    if Command::new("initdb").arg("--version").output().is_err() {
        eprintln!("initdb is not on PATH: skipping the end-to-end test (run it in nix-shell)");
        return;
    }
    let _ = rlimit::increase_nofile_limit(u64::MAX);

    let dir = std::env::temp_dir().join(format!("pseudogress-e2e-service-{}", std::process::id()));
    let postgres = Postgres::start(&dir);
    postgres.createdb("app");
    postgres.psql("app", SCHEMA);
    let (service, requests) = class_service(postgres.port).await;

    let recordings = dir.join("recordings");
    let config = dir.join("pseudogress.toml");
    fs::write(
        &config,
        format!(
            r#"listen = ["127.0.0.1:0"]
admin = "127.0.0.1:0"
secret = "{SECRET}"

[[class]]
name = "app"
databases = "app-*"
users = ["{USER}"]
recording = "{recording}"
upstream = {{ host = "127.0.0.1", port = {postgres_port}, user = "postgres", database = "app" }}
"#,
            recording = dir.join("app.jsonl").display(),
            postgres_port = postgres.port
        ),
    )
    .unwrap();

    let url = format!("http://127.0.0.1:{service}/classes/{{database}}?user={{user}}");
    let flags = [
        "--class-url",
        &url,
        "--recordings",
        recordings.to_str().unwrap(),
        "--max-classes",
        "8",
    ];
    let mut record = Pseudogress::spawn("record", &config, &flags, &dir.join("record.log"));
    let (listen, _) = record.wait_until_listening();
    assert!(recordings.is_dir(), "the directory of recordings is made");
    // A database of the configuration's class is not asked about, and the
    // service's class answers what PostgreSQL does.
    let app = connect(listen.port(), "app-0").await;
    assert_eq!(reads(&app).await, postgres_answers().reads);
    assert_eq!(requests.load(Relaxed), 0);
    let recorded = workload(listen.port(), "svc-0").await;
    assert_eq!(recorded, postgres_answers());
    assert_eq!(requests.load(Relaxed), 1);
    drop(app);
    let status = record.terminate();
    assert!(
        status.success(),
        "record mode exited with {status}:\n{}",
        record.stderr()
    );
    let classes = fs::read_to_string(recordings.join("classes.toml")).unwrap();
    assert!(classes.contains("name = \"svc\""), "{classes}");
    assert!(
        fs::metadata(recordings.join("svc.jsonl")).is_ok_and(|file| file.len() > 0),
        "record mode wrote no recording of the service's class:\n{}",
        record.stderr()
    );

    // Replay needs neither PostgreSQL nor the service: only the files.
    let flags = ["--recordings", recordings.to_str().unwrap()];
    let mut replay = Pseudogress::spawn("replay", &config, &flags, &dir.join("replay.log"));
    let (listen, _) = replay.wait_until_listening();
    assert_eq!(workload(listen.port(), "svc-0").await, recorded);
    // Another database of the class, which was never recorded.
    assert_eq!(workload(listen.port(), "svc-1").await, recorded);
    let app = connect(listen.port(), "app-0").await;
    assert_eq!(reads(&app).await, recorded.reads);
    assert_eq!(requests.load(Relaxed), 1);
}

/// A class service for the databases `svc-*`, which knows no other. Returns its
/// port, and how many requests it has had.
async fn class_service(postgres_port: u16) -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let counted = counted.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    if stream.read_buf(&mut head).await.unwrap_or(0) == 0 {
                        return;
                    }
                }
                counted.fetch_add(1, Relaxed);
                let head = String::from_utf8_lossy(&head);
                let target = head.split(' ').nth(1).unwrap_or_default();
                let (status, body) = if target.starts_with("/classes/svc-") {
                    let class = format!(
                        r#"{{"name": "svc", "databases": "svc-*", "users": ["{USER}"],
                        "upstream": {{"host": "127.0.0.1", "port": {postgres_port},
                                      "user": "postgres", "database": "app"}}}}"#
                    );
                    ("200 OK", class)
                } else {
                    ("404 Not Found", String::new())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    (port, requests)
}

#[test]
fn replay_with_a_directory_of_recordings_needs_its_classes_file() {
    let dir = std::env::temp_dir().join(format!("pseudogress-e2e-nofile-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let config = dir.join("pseudogress.toml");
    fs::write(&config, "listen = [\"127.0.0.1:0\"]\nclass = []\n").unwrap();
    let flags = ["--recordings", dir.to_str().unwrap()];

    let mut replay = Pseudogress::spawn("replay", &config, &flags, &dir.join("replay.log"));
    let status = replay.exited();
    let stderr = replay.stderr();
    fs::remove_dir_all(&dir).unwrap();

    assert_eq!(status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("classes.toml"), "{stderr}");
}

#[test]
fn flags_that_do_not_go_with_the_mode_are_usage_errors() {
    for (arguments, reason) in [
        (
            ["replay", "c.toml", "--class-url", "http://h/{database}"],
            "--class-url is for record mode",
        ),
        (
            ["record", "c.toml", "--recordings", "recordings"],
            "--recordings needs --class-url",
        ),
        (
            ["record", "c.toml", "--max-classes", "3"],
            "--max-classes needs --class-url",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_pseudogress"))
            .args(arguments)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{arguments:?}: {stderr}");
        assert!(stderr.contains(reason), "{arguments:?}: {stderr}");
        assert!(
            stderr.contains("usage: pseudogress"),
            "{arguments:?}: {stderr}"
        );
    }
}

/// PostgreSQL's own answers to the workload of an empty `SCHEMA`, so that a
/// replay can't pass by matching a recording of failures.
fn postgres_answers() -> Answers {
    Answers {
        reads: vec![
            "1|apple|1.50;3|cherry|12.00".to_owned(),
            "3|apple,banana,cherry".to_owned(),
            "22012 division by zero".to_owned(),
        ],
        transaction: "1|seen".to_owned(),
    }
}

/// What a client got from the statements of the workload.
#[derive(Debug, PartialEq)]
struct Answers {
    reads: Vec<String>,
    transaction: String,
}

/// Runs the workload on one connection to `database`.
async fn workload(port: u16, database: &str) -> Answers {
    let mut client = connect(port, database).await;
    let reads = reads(&client).await;
    // After the failed statement, so that it shows the connection still works.
    let transaction = transaction(&mut client).await;
    Answers { reads, transaction }
}

/// The statements that don't change anything, with their answers as text:
/// a prepared one with parameters, a simple query, and one that fails.
async fn reads(client: &Client) -> Vec<String> {
    let statement = client
        .prepare(
            "SELECT id, name, price::text FROM items WHERE id >= $1 AND name <> $2 ORDER BY id",
        )
        .await
        .unwrap();
    let rows = client
        .query(&statement, &[&1_i32, &"banana"])
        .await
        .unwrap();
    let items: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{}|{}|{}",
                row.get::<_, i32>(0),
                row.get::<_, &str>(1),
                row.get::<_, &str>(2)
            )
        })
        .collect();

    let messages = client
        .simple_query("SELECT count(*), string_agg(name, ',' ORDER BY name) FROM items")
        .await
        .unwrap();
    let summary: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(format!(
                "{}|{}",
                row.get(0).unwrap_or_default(),
                row.get(1).unwrap_or_default()
            )),
            _ => None,
        })
        .collect();

    // Fails when it runs, not when it is prepared.
    let statement = client
        .prepare("SELECT id / $1::int FROM items")
        .await
        .unwrap();
    let error = client.query(&statement, &[&0_i32]).await.unwrap_err();
    let error = error.as_db_error().expect("a database error");

    vec![
        items.join(";"),
        summary.join(";"),
        format!("{} {}", error.code().code(), error.message()),
    ]
}

async fn transaction(client: &mut Client) -> String {
    let transaction = client.transaction().await.unwrap();
    let row = transaction
        .query_one(
            "INSERT INTO audit (note) VALUES ($1) RETURNING id, note",
            &[&"seen"],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    format!("{}|{}", row.get::<_, i32>(0), row.get::<_, &str>(1))
}

/// Opens `CONNECTIONS` connections, `DATABASES` databases' worth, and keeps
/// them open.
async fn connect_all(port: u16) -> Vec<(String, Client)> {
    let mut clients = Vec::with_capacity(CONNECTIONS);
    for first in (0..CONNECTIONS).step_by(BURST) {
        let mut connecting = JoinSet::new();
        for n in first..first + BURST {
            let database = format!("app-{}", n % DATABASES);
            connecting.spawn(async move {
                let client = connect(port, &database).await;
                (database, client)
            });
        }
        while let Some(done) = connecting.join_next().await {
            clients.push(done.unwrap());
        }
    }
    clients
}

async fn connect(port: u16, database: &str) -> Client {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user(USER)
        .dbname(database)
        .password(derived_password(database));
    let (client, connection) = config
        .connect(NoTls)
        .await
        .unwrap_or_else(|error| panic!("connecting to {database}: {error}"));
    // Ends when the client is dropped.
    tokio::spawn(connection);
    client
}

/// What `auth::derive_password` documents: the hex of the HMAC-SHA-256 of
/// the database, a zero byte and the user, keyed with the secret.
fn derived_password(database: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(database.as_bytes());
    mac.update(&[0]);
    mac.update(USER.as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A request to the admin API, as a client without an HTTP library makes it.
async fn admin_get(admin: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(admin).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {admin}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP response");
    assert!(head.starts_with("HTTP/1.1 200"), "GET {path}: {head}");
    body.to_owned()
}

/// The value of a metric of the class `app`.
async fn metric(admin: SocketAddr, name: &str) -> i64 {
    let body = admin_get(admin, "/metrics").await;
    let prefix = format!("{name}{{class=\"app\"}} ");
    body.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {name} in the metrics:\n{body}"))
        .parse()
        .unwrap()
}

/// The server counts a connection only after it answered the client, and a
/// closed one when it notices, so a gauge takes a moment to show either.
async fn wait_for_metric(admin: SocketAddr, name: &str, value: i64) {
    let mut seen = metric(admin, name).await;
    for _ in 0..200 {
        if seen == value {
            return;
        }
        sleep(Duration::from_millis(50)).await;
        seen = metric(admin, name).await;
    }
    panic!("{name} is {seen}, not {value}");
}

/// A port nobody listens on. PostgreSQL can't be told to pick one, so a port
/// taken before PostgreSQL binds it is a lost race, which `Postgres::start`
/// retries.
fn free_port() -> u16 {
    StdListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Runs a PostgreSQL tool, which has to succeed.
fn run(command: &mut Command) {
    if let Err(error) = try_run(command) {
        panic!("{error}");
    }
}

/// Runs a PostgreSQL tool, and says how it failed.
fn try_run(command: &mut Command) -> Result<(), String> {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{command:?}: {error}"));
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "{command:?} failed with {}:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// A throwaway cluster, stopped and deleted along with its directory when
/// dropped.
struct Postgres {
    dir: PathBuf,
    /// The port it started on.
    port: u16,
    /// Only one cluster runs at a time: every PostgreSQL takes a System V
    /// shared memory segment, and macOS allows 32 of them in all. Declared
    /// last, to be released after the cluster is gone.
    _exclusive: MutexGuard<'static, ()>,
}

impl Postgres {
    fn start(dir: &Path) -> Postgres {
        static EXCLUSIVE: Mutex<()> = Mutex::new(());
        let exclusive = EXCLUSIVE.lock().unwrap_or_else(PoisonError::into_inner);
        fs::create_dir_all(dir).unwrap();
        // Made before the cluster, so that a failed start is cleaned up too.
        let mut postgres = Postgres {
            dir: dir.to_owned(),
            port: 0,
            _exclusive: exclusive,
        };
        run(Command::new("initdb")
            .env("LC_ALL", "C")
            .arg("-D")
            .arg(postgres.data())
            .args([
                "-U",
                "postgres",
                "--auth=trust",
                "--no-locale",
                "-E",
                "UTF8",
            ]));
        // No Unix socket: macOS limits the path of one to 103 bytes. The log
        // keeps the server from holding the pipes of `try_run` open.
        let mut started = Ok(());
        for _ in 0..5 {
            postgres.port = free_port();
            started = try_run(
                Command::new("pg_ctl")
                    .args(["-w", "-D"])
                    .arg(postgres.data())
                    .arg("-l")
                    .arg(dir.join("postgres.log"))
                    .arg("-o")
                    .arg(format!(
                        "-p {} -c listen_addresses=127.0.0.1 -c unix_socket_directories=",
                        postgres.port
                    ))
                    .arg("start"),
            );
            if started.is_ok() {
                break;
            }
        }
        if let Err(error) = started {
            panic!("{error}");
        }
        postgres
    }

    fn data(&self) -> PathBuf {
        self.dir.join("postgres")
    }

    fn createdb(&self, name: &str) {
        run(Command::new("createdb")
            .args(self.connection_arguments())
            .arg(name));
    }

    fn psql(&self, database: &str, sql: &str) {
        run(Command::new("psql")
            .args([
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-d",
                database,
                "-c",
                sql,
            ])
            .args(self.connection_arguments()));
    }

    fn connection_arguments(&self) -> [String; 6] {
        [
            "-h".to_owned(),
            "127.0.0.1".to_owned(),
            "-p".to_owned(),
            self.port.to_string(),
            "-U".to_owned(),
            "postgres".to_owned(),
        ]
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        // Nothing in the cluster is worth a clean shutdown. Failing is fine
        // when it never started.
        let _ = Command::new("pg_ctl")
            .args(["-m", "immediate", "-D"])
            .arg(self.data())
            .arg("stop")
            .output();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// The address pseudogress announced on its stderr for `what`, once bound.
fn announced(stderr: &str, what: &str) -> Option<SocketAddr> {
    let prefix = format!("pseudogress: {what} on ");
    stderr
        .lines()
        .find_map(|line| line.strip_prefix(&prefix)?.parse().ok())
}

/// A running pseudogress, killed when dropped, so that a failing test leaves
/// none behind.
struct Pseudogress {
    child: Child,
    stderr: PathBuf,
}

impl Pseudogress {
    fn spawn(mode: &str, config: &Path, flags: &[&str], stderr: &Path) -> Pseudogress {
        let child = Command::new(env!("CARGO_BIN_EXE_pseudogress"))
            .args([mode])
            .arg(config)
            .args(flags)
            .stdout(Stdio::null())
            .stderr(fs::File::create(stderr).unwrap())
            .spawn()
            .unwrap();
        Pseudogress {
            child,
            stderr: stderr.to_owned(),
        }
    }

    /// What it printed to stderr, for the message of a failure.
    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    /// Waits for the PostgreSQL listener and the admin API to be bound, and
    /// returns their addresses, which are the ones of this process.
    fn wait_until_listening(&mut self) -> (SocketAddr, SocketAddr) {
        let deadline = Instant::now() + STARTUP;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("pseudogress exited with {status}:\n{}", self.stderr());
            }
            let stderr = self.stderr();
            if let (Some(listen), Some(admin)) = (
                announced(&stderr, "PostgreSQL"),
                announced(&stderr, "admin API"),
            ) {
                return (listen, admin);
            }
            assert!(
                Instant::now() < deadline,
                "pseudogress does not announce its addresses:\n{stderr}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Waits for it to exit by itself.
    fn exited(&mut self) -> ExitStatus {
        let deadline = Instant::now() + STARTUP;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "pseudogress did not exit:\n{}",
                self.stderr()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Sends SIGTERM, which has record mode write its recordings, and waits
    /// for the exit.
    fn terminate(&mut self) -> ExitStatus {
        let sent = unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        assert_eq!(sent, 0, "{}", std::io::Error::last_os_error());
        let deadline = Instant::now() + STARTUP;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "pseudogress did not exit after SIGTERM:\n{}",
                self.stderr()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Pseudogress {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
