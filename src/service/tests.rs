use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use schemars::{Schema, transform::RecursiveTransform};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::{Lookup, NAME_PATTERN, TIMEOUT, Template, lookup, parse_class};
use crate::config::{self, ColumnOverride, Execution, Faults, Settings, Upstream};

// The URL template.

fn rejected(template: &str) -> String {
    Template::parse(template).unwrap_err()
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn parses_a_template_with_a_host_a_port_a_path_and_a_query() {
    let template =
        Template::parse("http://classes.internal:8080/v1/classes/{database}?user={user}").unwrap();
    let url = template.fill("tenant-7", "authenticator");
    assert_eq!(url.host, "classes.internal");
    assert_eq!(url.port, 8080);
    assert_eq!(url.target, "/v1/classes/tenant-7?user=authenticator");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_port_is_80_by_default_and_the_path_is_the_root() {
    let template = Template::parse("http://localhost?database={database}").unwrap();
    let url = template.fill("a", "b");
    assert_eq!((url.host.as_str(), url.port), ("localhost", 80));
    assert_eq!(url.target, "/?database=a");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn a_template_may_name_an_ip_address() {
    let url = Template::parse("http://127.0.0.1:9/{database}")
        .unwrap()
        .fill("a", "b");
    assert_eq!((url.host.as_str(), url.port), ("127.0.0.1", 9));
    let url = Template::parse("http://[::1]:9/{database}")
        .unwrap()
        .fill("a", "b");
    assert_eq!((url.host.as_str(), url.port), ("::1", 9));
    let url = Template::parse("http://[::1]/{database}")
        .unwrap()
        .fill("a", "b");
    assert_eq!((url.host.as_str(), url.port), ("::1", 80));
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_user_is_optional_and_the_database_may_be_repeated() {
    let url = Template::parse("http://h/{database}/{database}")
        .unwrap()
        .fill("a", "b");
    assert_eq!(url.target, "/a/a");
    let url = Template::parse("http://h/{user}@{database}")
        .unwrap()
        .fill("a", "b");
    assert_eq!(url.target, "/b@a");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn rejects_a_template_that_is_not_http() {
    for template in [
        "https://h/{database}",
        "ftp://h/{database}",
        "h/{database}",
        "//h/{database}",
        "",
    ] {
        let error = rejected(template);
        assert!(error.contains("http://"), "{template}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn rejects_a_template_without_the_database() {
    for template in ["http://h", "http://h/classes", "http://h/{user}?x=y"] {
        let error = rejected(template);
        assert!(error.contains("{database}"), "{template}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn rejects_a_bad_host() {
    for template in [
        "http:///{database}",
        "http://:80/{database}",
        "http://a b/{database}",
        "http://user@h/{database}",
        "http://{database}.h/x",
        "http://h%41/{database}",
        "http://[::1/{database}",
        "http://[::1]x/{database}",
        "http://[h]/{database}",
        "http://ho\u{15b}t/{database}",
    ] {
        let error = rejected(template);
        assert!(error.contains("host"), "{template}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn rejects_a_bad_port() {
    for template in [
        "http://h:/{database}",
        "http://h:http/{database}",
        "http://h:0/{database}",
        "http://h:65536/{database}",
        "http://h:-1/{database}",
        "http://[::1]:x/{database}",
    ] {
        let error = rejected(template);
        assert!(error.contains("port"), "{template}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn rejects_unknown_and_unbalanced_placeholders() {
    for (template, expected) in [
        ("http://h/{databse}", "placeholder"),
        ("http://h/{database}/{}", "placeholder"),
        ("http://h/{database}/{USER}", "placeholder"),
        ("http://h/{database}/{user", "unclosed"),
        ("http://h/{database}/}", "unmatched"),
    ] {
        let error = rejected(template);
        assert!(error.contains(expected), "{template}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn rejects_characters_that_cannot_be_in_a_request_target() {
    for template in [
        "http://h/a b/{database}",
        "http://h/{database}#fragment",
        "http://h/{database}\r\nHost: x",
        "http://h/\u{142}/{database}",
    ] {
        let error = rejected(template);
        assert!(error.contains("character"), "{template:?}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn fills_in_percent_encoded_values() {
    let template = Template::parse("http://h/{database}?user={user}").unwrap();
    for (value, encoded) in [
        ("tenant-7", "tenant-7"),
        ("AZaz09-._~", "AZaz09-._~"),
        ("a/b", "a%2Fb"),
        ("100%", "100%25"),
        ("what?", "what%3F"),
        ("a#b", "a%23b"),
        ("a b", "a%20b"),
        ("a+b&c=d", "a%2Bb%26c%3Dd"),
        ("\u{17c}\u{f3}\u{142}w", "%C5%BC%C3%B3%C5%82w"),
        ("a\r\nHost: x", "a%0D%0AHost%3A%20x"),
        ("", ""),
    ] {
        let url = template.fill(value, value);
        assert_eq!(
            url.target,
            format!("/{encoded}?user={encoded}"),
            "{value:?}"
        );
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn text_of_the_template_is_not_encoded() {
    let template = Template::parse("http://h/a%20b/~x/{database}?k=v&l=w").unwrap();
    assert_eq!(template.fill("d", "u").target, "/a%20b/~x/d?k=v&l=w");
}

// The client, against a server in this process.

/// What the stub server does after it has read a request's head.
enum Behaviour {
    /// Writes the bytes and closes the connection.
    Reply(Vec<u8>),
    /// Never answers.
    Silence,
    /// Sends the head of an answer, then a byte every 20 ms, for ever.
    Trickle,
}

struct Stub {
    template: Template,
    port: u16,
    /// The heads of the requests that arrived.
    requests: Arc<Mutex<Vec<String>>>,
}

async fn stub(behaviour: Behaviour) -> Stub {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let head = read_head(&mut stream).await;
            seen.lock().unwrap().push(head);
            match &behaviour {
                Behaviour::Reply(bytes) => {
                    let bytes = bytes.clone();
                    tokio::spawn(async move {
                        // The client may close before it has read everything.
                        let _ = stream.write_all(&bytes).await;
                        let _ = stream.shutdown().await;
                    });
                }
                Behaviour::Silence => {
                    tokio::spawn(async move {
                        let _stream = stream;
                        std::future::pending::<()>().await;
                    });
                }
                Behaviour::Trickle => {
                    tokio::spawn(async move {
                        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n";
                        if stream.write_all(head).await.is_err() {
                            return;
                        }
                        loop {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            if stream.write_all(b" ").await.is_err() {
                                return;
                            }
                        }
                    });
                }
            }
        }
    });
    let template = format!("http://127.0.0.1:{port}/classes/{{database}}?user={{user}}");
    Stub {
        template: Template::parse(&template).unwrap(),
        port,
        requests,
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

/// An answer with the headers and the body as given.
fn reply(status: &str, headers: &[&str], body: &[u8]) -> Vec<u8> {
    let mut bytes = format!("HTTP/1.1 {status}\r\n").into_bytes();
    for header in headers {
        bytes.extend(header.as_bytes());
        bytes.extend(b"\r\n");
    }
    bytes.extend(b"\r\n");
    bytes.extend(body);
    bytes
}

/// An answer with a Content-Length.
fn sized(status: &str, body: &[u8]) -> Vec<u8> {
    reply(status, &[&format!("Content-Length: {}", body.len())], body)
}

/// A chunked body with the chunks given, and a trailer.
fn chunked(chunks: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    for chunk in chunks {
        body.extend(format!("{:X};extension=1\r\n", chunk.len()).into_bytes());
        body.extend(*chunk);
        body.extend(b"\r\n");
    }
    body.extend(b"0\r\nTrailer: value\r\n\r\n");
    body
}

async fn ask(stub: &Stub) -> Lookup {
    lookup(&stub.template, "tenant-7", "authenticator", TIMEOUT).await
}

async fn failure(stub: &Stub) -> String {
    match ask(stub).await {
        Lookup::Failed(reason) => reason,
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn finds_a_class_in_a_body_with_a_content_length() {
    let body = MINIMAL.as_bytes();
    let stub = stub(Behaviour::Reply(reply(
        "200 OK",
        &[
            "Content-Type: application/json",
            &format!("Content-Length: {}", body.len()),
        ],
        body,
    )))
    .await;
    assert_eq!(ask(&stub).await, Lookup::Found(Box::new(minimal())));
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn finds_a_class_in_a_chunked_body() {
    let (first, second) = MINIMAL.as_bytes().split_at(40);
    let (second, third) = second.split_at(30);
    let stub = stub(Behaviour::Reply(reply(
        "200 OK",
        &["Transfer-Encoding: chunked"],
        &chunked(&[first, second, third]),
    )))
    .await;
    assert_eq!(ask(&stub).await, Lookup::Found(Box::new(minimal())));
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_chunked_body_wins_over_a_content_length() {
    let stub = stub(Behaviour::Reply(reply(
        "200 OK",
        &["content-length: 5", "TRANSFER-ENCODING: Chunked"],
        &chunked(&[MINIMAL.as_bytes()]),
    )))
    .await;
    assert_eq!(ask(&stub).await, Lookup::Found(Box::new(minimal())));
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn finds_a_class_in_a_body_that_ends_with_the_connection() {
    let stub = stub(Behaviour::Reply(reply("200 OK", &[], MINIMAL.as_bytes()))).await;
    assert_eq!(ask(&stub).await, Lookup::Found(Box::new(minimal())));
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn requests_with_get_and_asks_for_the_connection_to_close() {
    let stub = stub(Behaviour::Reply(sized("404 Not Found", b""))).await;
    lookup(&stub.template, "a/b", "x y", TIMEOUT).await;
    let requests = stub.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let head = &requests[0];
    assert!(
        head.starts_with("GET /classes/a%2Fb?user=x%20y HTTP/1.1\r\n"),
        "{head:?}"
    );
    assert!(
        head.contains(&format!("\r\nHost: 127.0.0.1:{}\r\n", stub.port)),
        "{head:?}"
    );
    assert!(head.contains("\r\nConnection: close\r\n"), "{head:?}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_404_is_not_found() {
    let stub = stub(Behaviour::Reply(sized("404 Not Found", b"no such class"))).await;
    assert_eq!(ask(&stub).await, Lookup::NotFound);
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_500_fails_and_its_body_is_in_the_reason() {
    let stub = stub(Behaviour::Reply(sized(
        "500 Internal Server Error",
        b"the registry is down\nretry later",
    )))
    .await;
    let reason = failure(&stub).await;
    assert!(reason.contains("500"), "{reason}");
    assert!(reason.contains("the registry is down"), "{reason}");
    assert!(!reason.contains('\n'), "{reason:?}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn other_statuses_fail_even_with_a_class_in_the_body() {
    for status in [
        "201 Created",
        "204 No Content",
        "301 Moved Permanently",
        "403 Forbidden",
    ] {
        let stub = stub(Behaviour::Reply(sized(status, MINIMAL.as_bytes()))).await;
        let reason = failure(&stub).await;
        assert!(reason.contains(&status[..3]), "{status}: {reason}");
    }
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn only_the_start_of_a_long_error_body_is_in_the_reason() {
    let body = "x".repeat(10_000);
    let stub = stub(Behaviour::Reply(sized("502 Bad Gateway", body.as_bytes()))).await;
    let reason = failure(&stub).await;
    assert!(reason.contains("502"), "{reason}");
    assert!(reason.len() < 500, "{} bytes", reason.len());
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_refused_connection_fails() {
    // The port is free once the listener is gone.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let template = Template::parse(&format!("http://127.0.0.1:{port}/{{database}}")).unwrap();
    match lookup(&template, "tenant-7", "authenticator", TIMEOUT).await {
        Lookup::Failed(reason) => assert!(reason.contains(&port.to_string()), "{reason}"),
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_server_that_never_answers_times_out() {
    let stub = stub(Behaviour::Silence).await;
    let started = Instant::now();
    let result = lookup(
        &stub.template,
        "tenant-7",
        "authenticator",
        Duration::from_millis(200),
    )
    .await;
    assert!(started.elapsed() < Duration::from_secs(3));
    let Lookup::Failed(reason) = result else {
        panic!("expected a failure, got {result:?}");
    };
    assert!(reason.contains("timed out"), "{reason}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn the_timeout_is_for_the_whole_request() {
    // Every byte arrives in time, but the answer never ends.
    let stub = stub(Behaviour::Trickle).await;
    let started = Instant::now();
    let result = lookup(
        &stub.template,
        "tenant-7",
        "authenticator",
        Duration::from_millis(300),
    )
    .await;
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(matches!(&result, Lookup::Failed(reason) if reason.contains("timed out")));
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_body_of_1_mib_is_read_and_a_larger_one_fails() {
    const MIB: usize = 1024 * 1024;
    let padded = |size: usize| {
        let mut body = MINIMAL.as_bytes().to_vec();
        body.resize(size, b' ');
        body
    };
    let stub_of = |body: Vec<u8>| stub(Behaviour::Reply(sized("200 OK", &body)));
    assert_eq!(
        ask(&stub_of(padded(MIB)).await).await,
        Lookup::Found(Box::new(minimal()))
    );
    let reason = failure(&stub_of(padded(MIB + 1)).await).await;
    assert!(reason.contains("1 MiB"), "{reason}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_body_without_a_length_of_1_mib_is_read_and_one_byte_more_fails() {
    const MIB: usize = 1024 * 1024;
    let padded = |size: usize| {
        let mut body = MINIMAL.as_bytes().to_vec();
        body.resize(size, b' ');
        body
    };
    let stub_of = |size: usize| stub(Behaviour::Reply(reply("200 OK", &[], &padded(size))));
    assert_eq!(
        ask(&stub_of(MIB).await).await,
        Lookup::Found(Box::new(minimal()))
    );
    let reason = failure(&stub_of(MIB + 1).await).await;
    assert!(reason.contains("1 MiB"), "{reason}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_chunked_body_or_one_without_a_length_over_1_mib_fails() {
    let big = vec![b' '; 600 * 1024];
    let chunked_body = reply(
        "200 OK",
        &["Transfer-Encoding: chunked"],
        &chunked(&[&big, &big]),
    );
    let unsized_body = reply("200 OK", &[], &[big.clone(), big.clone()].concat());
    for answer in [chunked_body, unsized_body] {
        let reason = failure(&stub(Behaviour::Reply(answer)).await).await;
        assert!(reason.contains("1 MiB"), "{reason}");
    }
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn invalid_json_fails() {
    let stub = stub(Behaviour::Reply(sized("200 OK", b"<html>sorry</html>"))).await;
    let reason = failure(&stub).await;
    assert!(reason.contains("invalid JSON"), "{reason}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn an_invalid_class_fails() {
    let stub = stub(Behaviour::Reply(sized("200 OK", b"{\"name\": \"x\"}"))).await;
    let reason = failure(&stub).await;
    assert!(reason.contains("invalid class"), "{reason}");
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn a_class_for_another_database_fails() {
    let stub = stub(Behaviour::Reply(sized("200 OK", MINIMAL.as_bytes()))).await;
    let result = lookup(&stub.template, "other-7", "authenticator", TIMEOUT).await;
    assert!(matches!(&result, Lookup::Failed(reason) if reason.contains("databases")));
}

#[tokio::test]
#[ignore = "xfail: the class service client is not implemented yet"]
async fn malformed_or_incomplete_answers_fail() {
    for answer in [
        &b""[..],
        b"not http\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\n{}",
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{}",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n{}",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n{}",
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}",
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain",
    ] {
        let stub = stub(Behaviour::Reply(answer.to_vec())).await;
        let reason = failure(&stub).await;
        assert!(!reason.is_empty(), "{answer:?}");
    }
}

// Parsing and checking the class.

const MINIMAL: &str = r#"{
    "name": "tenants",
    "databases": "tenant-*",
    "users": ["authenticator"],
    "upstream": {"host": "127.0.0.1", "port": 5432, "user": "postgres", "database": "{database}"}
}"#;

fn minimal() -> config::Class {
    config::Class {
        name: "tenants".into(),
        databases: "tenant-*".into(),
        users: vec!["authenticator".into()],
        password: None,
        recording: PathBuf::new(),
        upstream: Some(Upstream {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: None,
            database: "{database}".into(),
        }),
        ignore_parameters: vec![],
        max_connections: None,
        column_override: None,
        settings: Settings::default(),
    }
}

/// The minimal class with some of its fields replaced.
fn minimal_with(changes: Value) -> Vec<u8> {
    let mut class: Value = serde_json::from_str(MINIMAL).unwrap();
    class
        .as_object_mut()
        .unwrap()
        .extend(changes.as_object().unwrap().clone());
    class.to_string().into_bytes()
}

fn parse_error(body: &[u8], database: &str) -> String {
    parse_class(body, database).unwrap_err()
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn parses_a_minimal_class() {
    assert_eq!(
        parse_class(MINIMAL.as_bytes(), "tenant-7").unwrap(),
        minimal()
    );
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn parses_a_class_with_every_field() {
    let body = json!({
        "name": "big_tenants-2_v1",
        "databases": "big-*-tenant",
        "users": ["authenticator", "anon"],
        "password": "secret",
        "upstream": {
            "host": "db.internal",
            "port": 6432,
            "user": "postgres",
            "password": "upstream secret",
            "database": "{database}"
        },
        "ignore_parameters": ["select set_config(", "select pg_sleep("],
        "max_connections": 60,
        "override": {"column": "payload", "bytes": 4096},
        "settings": {
            "round_trip": "500us",
            "execution_scale": 0.5,
            "execution": {"median": "1ms", "p99": "20ms"},
            "execution_per_mib": "250us",
            "faults": {
                "refuse": 0.01,
                "hang": 0.02,
                "reject_password": 0.03,
                "reset": 0.04,
                "stall": 0.05,
                "stall_for": "2s",
                "statement_timeout": "30s"
            }
        }
    });
    let expected = config::Class {
        name: "big_tenants-2_v1".into(),
        databases: "big-*-tenant".into(),
        users: vec!["authenticator".into(), "anon".into()],
        password: Some("secret".into()),
        recording: PathBuf::new(),
        upstream: Some(Upstream {
            host: "db.internal".into(),
            port: 6432,
            user: "postgres".into(),
            password: Some("upstream secret".into()),
            database: "{database}".into(),
        }),
        ignore_parameters: vec!["select set_config(".into(), "select pg_sleep(".into()],
        max_connections: Some(60),
        column_override: Some(ColumnOverride {
            column: "payload".into(),
            bytes: 4096,
        }),
        settings: Settings {
            round_trip: Duration::from_micros(500),
            execution_scale: 0.5,
            execution: Some(Execution {
                median: Duration::from_millis(1),
                p99: Duration::from_millis(20),
            }),
            execution_per_mib: Duration::from_micros(250),
            faults: Faults {
                refuse: 0.01,
                hang: 0.02,
                reject_password: 0.03,
                reset: 0.04,
                stall: 0.05,
                stall_for: Duration::from_secs(2),
                statement_timeout: Some(Duration::from_secs(30)),
            },
        },
    };
    assert_eq!(
        parse_class(body.to_string().as_bytes(), "big-1-tenant").unwrap(),
        expected
    );
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_what_is_not_json() {
    for body in ["", "{", "not json", "<html></html>"] {
        let error = parse_error(body.as_bytes(), "tenant-7");
        assert!(error.contains("invalid JSON"), "{body:?}: {error}");
    }
    let error = parse_error(&[0xff, 0xfe], "tenant-7");
    assert!(error.contains("invalid JSON"), "{error}");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_json_that_is_not_a_class() {
    for body in ["[]", "null", "\"tenants\"", "{}", "{\"name\": 1}"] {
        let error = parse_error(body.as_bytes(), "tenant-7");
        assert!(error.contains("invalid class"), "{body}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_unknown_fields() {
    for changes in [
        json!({"colour": "blue"}),
        json!({"upstream": {"host": "h", "port": 1, "user": "u", "database": "d", "tls": true}}),
        json!({"settings": {"round_trp": "1ms"}}),
        json!({"settings": {"faults": {"explode": 1.0}}}),
    ] {
        let error = parse_error(&minimal_with(changes.clone()), "tenant-7");
        assert!(error.contains("unknown field"), "{changes}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_a_recording() {
    for recording in [
        json!("tenants.jsonl"),
        json!("/etc/passwd"),
        json!(""),
        json!(null),
    ] {
        let body = minimal_with(json!({ "recording": recording }));
        let error = parse_error(&body, "tenant-7");
        assert!(error.contains("recording"), "{recording}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn accepts_names_of_letters_digits_and_a_few_signs() {
    for name in [
        "a",
        "_",
        "0",
        "tenants",
        "Tenants_2",
        "a-b",
        "_-",
        "v1-2-rc_3",
    ] {
        let body = minimal_with(json!({ "name": name }));
        assert_eq!(parse_class(&body, "tenant-7").unwrap().name, name);
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_a_dot_in_a_name_so_that_no_class_has_the_files_of_another() {
    // `a` has its times in `a.times.jsonl`, which would be the recording of a
    // class named `a.times`.
    for name in ["a.times", "a.b", "v1.2", "a.", "tenants.jsonl"] {
        let body = minimal_with(json!({ "name": name }));
        let error = parse_error(&body, "tenant-7");
        assert!(error.contains("name"), "{name:?}: {error}");
        assert!(error.contains(&format!("{name:?}")), "{name:?}: {error}");
        assert!(error.contains(NAME_PATTERN), "{name:?}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_names_that_cannot_name_a_file() {
    for name in [
        "",
        ".",
        "..",
        ".hidden",
        "-x",
        "a/b",
        "../x",
        "a\\b",
        "a b",
        "a\n",
        "a:b",
        "tenants.jsonl/",
        "\u{e9}",
        "a\u{0}",
    ] {
        let body = minimal_with(json!({ "name": name }));
        let error = parse_error(&body, "tenant-7");
        assert!(error.contains("name"), "{name:?}: {error}");
        assert!(error.contains(NAME_PATTERN), "{name:?}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn a_name_is_at_most_200_bytes_so_that_its_files_fit_a_file_name() {
    let name = "n".repeat(200);
    let body = minimal_with(json!({ "name": name }));
    assert_eq!(parse_class(&body, "tenant-7").unwrap().name, name);

    let body = minimal_with(json!({ "name": "n".repeat(201) }));
    let error = parse_error(&body, "tenant-7");
    assert!(error.contains("name"), "{error}");
    assert!(error.contains("200"), "{error}");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_databases_that_do_not_match_the_one_asked_about() {
    for (pattern, database, matches) in [
        ("tenant-*", "tenant-7", true),
        ("tenant-*", "tenant-", true),
        ("tenant-*", "tenants", false),
        ("tenant-7", "tenant-7", true),
        ("tenant-7", "tenant-8", false),
        ("*", "anything", true),
        ("*-prod-*", "a-prod-b", true),
        ("*-prod-*", "a-dev-b", false),
        ("a*b*c", "abc", true),
        ("a*b*c", "ac", false),
        ("a*a", "a", false),
        ("", "", true),
        ("", "a", false),
    ] {
        let body = minimal_with(json!({ "databases": pattern }));
        let result = parse_class(&body, database);
        assert_eq!(
            result.is_ok(),
            matches,
            "{pattern} for {database}: {result:?}"
        );
        if let Err(error) = result {
            assert!(error.contains("databases"), "{error}");
            assert!(error.contains(database), "{error}");
        }
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_a_class_without_an_upstream() {
    let mut class: Value = serde_json::from_str(MINIMAL).unwrap();
    class.as_object_mut().unwrap().remove("upstream");
    let error = parse_error(class.to_string().as_bytes(), "tenant-7");
    assert!(error.contains("upstream"), "{error}");
    let error = parse_error(&minimal_with(json!({ "upstream": null })), "tenant-7");
    assert!(error.contains("upstream"), "{error}");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_invalid_settings() {
    for (settings, field) in [
        (json!({"execution_scale": -1.0}), "execution_scale"),
        (json!({"faults": {"reset": 2.0}}), "reset"),
        (
            json!({"execution": {"median": "20ms", "p99": "1ms"}}),
            "p99",
        ),
        (
            json!({"execution": {"median": "0s", "p99": "1ms"}}),
            "median",
        ),
    ] {
        let error = parse_error(&minimal_with(json!({ "settings": settings })), "tenant-7");
        assert!(error.contains(field), "{settings}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_durations_that_are_not_humantime_strings() {
    for round_trip in [json!(500), json!("soon"), json!("-1s")] {
        let body = minimal_with(json!({ "settings": { "round_trip": round_trip } }));
        let error = parse_error(&body, "tenant-7");
        assert!(error.contains("invalid class"), "{round_trip}: {error}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn refuses_an_empty_prefix_of_statements_that_ignore_parameters() {
    let body = minimal_with(json!({ "ignore_parameters": ["select set_config(", ""] }));
    let error = parse_error(&body, "tenant-7");
    assert!(error.contains("ignore_parameters"), "{error}");
}

// The JSON Schema of the class.

const SCHEMA_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/docs/class.schema.json");

fn schema() -> Value {
    let schema = schemars::generate::SchemaSettings::draft2020_12()
        .with_transform(RecursiveTransform(join_lines))
        .into_generator()
        .into_root_schema_for::<config::Class>();
    serde_json::to_value(schema).unwrap()
}

/// Doc comments are wrapped at 80 columns; a description keeps its paragraphs
/// and loses the line breaks within them.
fn join_lines(schema: &mut Schema) {
    if let Some(Value::String(description)) = schema.get_mut("description") {
        *description = description
            .split("\n\n")
            .map(|paragraph| paragraph.replace('\n', " "))
            .collect::<Vec<_>>()
            .join("\n\n");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_committed_schema_is_the_current_one() {
    let current = format!("{}\n", serde_json::to_string_pretty(&schema()).unwrap());
    if std::env::var("UPDATE_SCHEMA").is_ok_and(|value| value == "1") {
        std::fs::write(SCHEMA_PATH, &current).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(SCHEMA_PATH).unwrap_or_default();
    assert!(
        committed == current,
        "docs/class.schema.json is out of date: UPDATE_SCHEMA=1 cargo test \
         the_committed_schema_is_the_current_one rewrites it"
    );
}

/// The schema of one of the types that the class refers to.
fn definition<'a>(schema: &'a Value, name: &str) -> &'a Value {
    &schema["$defs"][name]
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_schema_is_draft_2020_12_with_a_title() {
    let schema = schema();
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert_eq!(schema["title"], "Pseudogress class");
    assert_eq!(schema["type"], "object");
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_schema_has_no_recording() {
    let schema = schema();
    assert!(schema["properties"].get("name").is_some());
    let definitions = schema["$defs"].as_object().unwrap().values();
    for object in std::iter::once(&schema).chain(definitions) {
        assert!(object["properties"].get("recording").is_none(), "{object}");
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_schema_refuses_unknown_properties_everywhere() {
    let schema = schema();
    assert_eq!(schema["additionalProperties"], false);
    for name in [
        "Upstream",
        "ColumnOverride",
        "Settings",
        "Execution",
        "Faults",
    ] {
        assert_eq!(
            definition(&schema, name)["additionalProperties"],
            false,
            "{name}"
        );
    }
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_schema_has_the_pattern_of_names_and_requires_the_upstream() {
    let schema = schema();
    assert_eq!(schema["properties"]["name"]["pattern"], NAME_PATTERN);
    assert_eq!(schema["properties"]["name"]["maxLength"], 200);
    let required = schema["required"].as_array().unwrap();
    for field in ["name", "databases", "users", "upstream"] {
        assert!(required.contains(&json!(field)), "{field}: {required:?}");
    }
    assert!(!required.contains(&json!("recording")), "{required:?}");
    assert!(
        schema["properties"]["upstream"].get("anyOf").is_none(),
        "the upstream may not be null: {}",
        schema["properties"]["upstream"]
    );
}

#[test]
#[ignore = "xfail: the class service client is not implemented yet"]
fn the_schema_has_durations_as_strings_and_says_so() {
    let schema = schema();
    for (definition_name, field, nullable) in [
        ("Settings", "round_trip", false),
        ("Settings", "execution_per_mib", false),
        ("Execution", "median", false),
        ("Execution", "p99", false),
        ("Faults", "stall_for", false),
        ("Faults", "statement_timeout", true),
    ] {
        let property = &definition(&schema, definition_name)["properties"][field];
        let expected = if nullable {
            json!(["string", "null"])
        } else {
            json!("string")
        };
        assert_eq!(property["type"], expected, "{definition_name}.{field}");
        let description = property["description"].as_str().unwrap_or_default();
        assert!(
            description.contains("humantime"),
            "{definition_name}.{field}: {description:?}"
        );
    }
}
