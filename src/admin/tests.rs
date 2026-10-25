use std::{net::SocketAddr, sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::serve;
use crate::{
    class::{Class, Classes},
    config::{self, Config},
    databases::{Connection, Databases, Event},
};

const CONFIG: &str = r#"
listen = ["127.0.0.1:0"]

[[class]]
name = "small"
databases = "small-*"
users = ["authenticator"]
recording = "recordings/small.jsonl"
"#;

struct Admin {
    address: SocketAddr,
    classes: Classes,
    databases: Arc<Databases>,
}

/// Starts the admin API on a free port.
async fn start() -> Admin {
    let config: Config = toml::from_str(CONFIG).unwrap();
    let classes: Classes = config.classes.iter().map(Class::new).collect();
    let databases = Arc::new(Databases::new(&config.classes));
    // Bound before `serve` gets it, so that no other test can take the port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, classes.clone(), databases.clone()));
    Admin {
        address,
        classes,
        databases,
    }
}

/// Sends raw bytes and returns the response's head and body.
async fn exchange(address: SocketAddr, request: &[u8]) -> std::io::Result<(String, String)> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(request).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response = String::from_utf8_lossy(&response).into_owned();
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or((response.as_str(), ""));
    Ok((head.to_owned(), body.to_owned()))
}

/// An HTTP/1.1 request whose `Content-Length` is the length of `body`.
fn request(method: &str, path: &str, body: &str) -> Vec<u8> {
    format!(
        "{method} {path} HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

async fn send(admin: &Admin, method: &str, path: &str, body: &str) -> (String, String) {
    exchange(admin.address, &request(method, path, body))
        .await
        .unwrap()
}

fn assert_status(head: &str, code: u16) {
    assert!(head.starts_with(&format!("HTTP/1.1 {code} ")), "{head}");
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap()
}

/// Waits for the connection's next event, failing the test if none comes.
async fn next_event(connection: &mut Connection) -> Event {
    tokio::time::timeout(Duration::from_secs(5), connection.event())
        .await
        .expect("the connection gets an event")
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn get_settings_returns_the_class_settings() {
    let admin = start().await;
    let (head, body) = send(&admin, "GET", "/classes/small/settings", "").await;
    assert_status(&head, 200);
    let settings: config::Settings = serde_json::from_str(&body).unwrap();
    assert_eq!(settings, *admin.classes[0].settings.load_full());
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn put_settings_replaces_the_class_settings() {
    let admin = start().await;
    let (head, body) = send(
        &admin,
        "PUT",
        "/classes/small/settings",
        r#"{"round_trip":"2ms","execution_scale":2.5,
            "execution":{"median":"1ms","p99":"20ms"},"execution_per_mib":"250us",
            "faults":{"stall":0.5,"stall_for":"1s"}}"#,
    )
    .await;
    assert_status(&head, 200);
    let stored = config::Settings {
        round_trip: Duration::from_millis(2),
        execution_scale: 2.5,
        execution: Some(config::Execution {
            median: Duration::from_millis(1),
            p99: Duration::from_millis(20),
        }),
        execution_per_mib: Duration::from_micros(250),
        faults: config::Faults {
            stall: 0.5,
            stall_for: Duration::from_secs(1),
            ..Default::default()
        },
    };
    assert_eq!(
        serde_json::from_str::<config::Settings>(&body).unwrap(),
        stored
    );
    assert_eq!(*admin.classes[0].settings.load_full(), stored);
    let (_, body) = send(&admin, "GET", "/classes/small/settings", "").await;
    assert_eq!(
        serde_json::from_str::<config::Settings>(&body).unwrap(),
        stored
    );
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn put_settings_defaults_missing_fields() {
    let admin = start().await;
    send(
        &admin,
        "PUT",
        "/classes/small/settings",
        r#"{"round_trip":"2ms","faults":{"reset":0.1}}"#,
    )
    .await;
    let (head, body) = send(
        &admin,
        "PUT",
        "/classes/small/settings",
        r#"{"execution_scale":3.0}"#,
    )
    .await;
    assert_status(&head, 200);
    let expected = config::Settings {
        execution_scale: 3.0,
        ..Default::default()
    };
    assert_eq!(
        serde_json::from_str::<config::Settings>(&body).unwrap(),
        expected
    );
    assert_eq!(*admin.classes[0].settings.load_full(), expected);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn put_settings_refuses_an_invalid_latency_model() {
    let admin = start().await;
    let before = admin.classes[0].settings.load_full();
    for (body, complaint) in [
        (
            r#"{"execution":{"median":"0s","p99":"20ms"}}"#,
            "median must be above 0",
        ),
        (
            r#"{"execution":{"median":"20ms","p99":"1ms"}}"#,
            "p99 (1ms) must not be below the median (20ms)",
        ),
    ] {
        let (head, response) = send(&admin, "PUT", "/classes/small/settings", body).await;
        assert_status(&head, 400);
        assert!(response.contains(complaint), "{response}");
    }
    assert_eq!(admin.classes[0].settings.load_full(), before);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn unknown_class_is_404() {
    let admin = start().await;
    let (head, _) = send(&admin, "GET", "/classes/nope/settings", "").await;
    assert_status(&head, 404);
    let (head, _) = send(&admin, "PUT", "/classes/nope/settings", "{}").await;
    assert_status(&head, 404);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn invalid_json_is_400() {
    let admin = start().await;
    for (method, path, body) in [
        ("PUT", "/classes/small/settings", r#"{"round_trp":"1ms"}"#),
        ("PUT", "/classes/small/settings", "not json"),
        ("POST", "/notify", r#"{"channel":"news"}"#),
        ("POST", "/notify", r#"{"database":1,"channel":"news"}"#),
        ("POST", "/terminate", "{}"),
    ] {
        let (head, _) = send(&admin, method, path, body).await;
        assert_status(&head, 400);
    }
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn malformed_request_is_400() {
    let admin = start().await;
    let (head, _) = exchange(admin.address, b"GARBAGE\r\n\r\n").await.unwrap();
    assert_status(&head, 400);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn oversized_head_is_431() {
    let admin = start().await;
    let mut request = b"GET /metrics HTTP/1.1\r\nX-Padding: ".to_vec();
    request.resize(16 * 1024 + 1, b'a');
    let (head, _) = exchange(admin.address, &request).await.unwrap();
    assert_status(&head, 431);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn oversized_body_is_413() {
    let admin = start().await;
    // Only the head is sent: the body is refused before it is read.
    let request = b"PUT /classes/small/settings HTTP/1.1\r\nContent-Length: 1048577\r\n\r\n";
    let (head, _) = exchange(admin.address, request).await.unwrap();
    assert_status(&head, 413);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn wrong_method_is_405() {
    let admin = start().await;
    for (method, path) in [
        ("DELETE", "/metrics"),
        ("POST", "/metrics"),
        ("POST", "/databases/small-1"),
        ("GET", "/notify"),
        ("GET", "/terminate"),
        ("POST", "/classes/small/settings"),
    ] {
        let (head, _) = send(&admin, method, path, "").await;
        assert_status(&head, 405);
    }
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn unknown_path_is_404() {
    let admin = start().await;
    let (head, _) = send(&admin, "GET", "/", "").await;
    assert_status(&head, 404);
    let (head, _) = send(&admin, "GET", "/nope", "").await;
    assert_status(&head, 404);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn metrics_names_the_classes() {
    let admin = start().await;
    let (head, body) = send(&admin, "GET", "/metrics", "").await;
    assert_status(&head, 200);
    assert!(
        head.contains("Content-Type: text/plain; version=0.0.4"),
        "{head}"
    );
    assert!(body.contains("small"), "{body}");
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn notify_reaches_listening_connections() {
    let admin = start().await;
    let mut connection = admin.databases.connect("small-1").unwrap();
    connection.listen("news");

    let (head, body) = send(
        &admin,
        "POST",
        "/notify",
        r#"{"database":"small-1","channel":"news","payload":"hello"}"#,
    )
    .await;
    assert_status(&head, 200);
    assert_eq!(parse(&body), json!({"connections": 1}));
    assert_eq!(
        next_event(&mut connection).await,
        Event::Notification {
            channel: "news".into(),
            payload: "hello".into(),
        }
    );

    // A class target with the default fraction, and no payload.
    let (head, body) = send(
        &admin,
        "POST",
        "/notify",
        r#"{"class":"small","channel":"news"}"#,
    )
    .await;
    assert_status(&head, 200);
    assert_eq!(parse(&body), json!({"connections": 1}));
    assert_eq!(
        next_event(&mut connection).await,
        Event::Notification {
            channel: "news".into(),
            payload: String::new(),
        }
    );
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn terminate_ends_connections() {
    let admin = start().await;
    let mut connection = admin.databases.connect("small-1").unwrap();
    let (head, body) = send(&admin, "POST", "/terminate", r#"{"database":"small-1"}"#).await;
    assert_status(&head, 200);
    assert_eq!(parse(&body), json!({"connections": 1}));
    assert_eq!(next_event(&mut connection).await, Event::Terminate);
}

#[tokio::test]
#[ignore = "xfail: admin is not implemented yet"]
async fn database_info_follows_open_connections() {
    let admin = start().await;
    let (head, _) = send(&admin, "GET", "/databases/small-1", "").await;
    assert_status(&head, 404);

    {
        let connection = admin.databases.connect("small-1").unwrap();
        connection.listen("news");
        let (head, body) = send(&admin, "GET", "/databases/small-1", "").await;
        assert_status(&head, 200);
        assert_eq!(
            parse(&body),
            json!({"class": "small", "connections": 1, "listening": {"news": 1}})
        );
    }

    // The connection gave up its place when its block ended.
    let (head, _) = send(&admin, "GET", "/databases/small-1", "").await;
    assert_status(&head, 404);
}
