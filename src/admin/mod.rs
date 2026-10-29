//! The admin HTTP API, for scripts that drive a test: metrics, notifications,
//! terminations, the open databases and the classes' settings.
//!
//! - `GET /metrics`
//! - `GET /classes`: the classes in matching order, with `"source"`
//!   `"config"` or `"service"`
//! - `GET /databases/{name}`
//! - `POST /notify` with `{"channel", "payload"}` and a target
//! - `POST /terminate` with a target
//! - `GET` and `PUT /classes/{name}/settings`
//!
//! A target is `{"database": name}` or `{"class": name}`, optionally with a
//! `"fraction"` of the class's open databases.

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    class::{Class, Classes, Source},
    config,
    databases::{Databases, Target},
    metrics::{self, LookupMetrics},
};

#[cfg(test)]
mod tests;

const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 1024 * 1024;
/// How long a client may take to send its request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a closed connection waits for a client that is still sending.
const LINGER: Duration = Duration::from_secs(1);

/// `lookups` are the metrics of a class service, if there is one.
pub async fn serve(
    listener: TcpListener,
    classes: Classes,
    databases: Arc<Databases>,
    lookups: Option<Arc<LookupMetrics>>,
) -> std::io::Result<()> {
    let api = Arc::new(Api {
        classes,
        databases,
        lookups,
    });
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            // Usually out of file descriptors: back off rather than stop the API.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let api = api.clone();
        tokio::spawn(async move { handle(&api, stream).await });
    }
}

struct Api {
    classes: Classes,
    databases: Arc<Databases>,
    lookups: Option<Arc<LookupMetrics>>,
}

/// Answers one request, then closes the connection.
async fn handle(api: &Api, mut stream: TcpStream) {
    let Ok(request) = tokio::time::timeout(REQUEST_TIMEOUT, read_request(&mut stream)).await else {
        return;
    };
    let response = match request {
        Ok(request) => api.route(&request),
        Err(response) => response,
    };
    if stream.write_all(&response.to_bytes()).await.is_err() {
        return;
    }
    // Closing with unread input resets the connection, which can discard the
    // response before the client reads it. Drain the input instead.
    let _ = stream.shutdown().await;
    let _ =
        tokio::time::timeout(LINGER, tokio::io::copy(&mut stream, &mut tokio::io::sink())).await;
}

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

/// Reads one request. The error is the response that refuses it.
async fn read_request(stream: &mut TcpStream) -> Result<Request, Response> {
    let mut buf = Vec::new();
    let head_end = loop {
        match buf.windows(4).position(|window| window == b"\r\n\r\n") {
            Some(end) if end + 4 <= MAX_HEAD => break end + 4,
            Some(_) => return Err(head_too_large()),
            None if buf.len() >= MAX_HEAD => return Err(head_too_large()),
            None => read_more(stream, &mut buf).await?,
        }
    };
    let head = parse_head(&buf[..head_end])?;
    if head.content_length > MAX_BODY {
        return Err(Response::text(
            "413 Payload Too Large",
            "the body is over 1 MiB",
        ));
    }
    // Bytes read past the head are the start of the body.
    let mut body = buf.split_off(head_end);
    while body.len() < head.content_length {
        read_more(stream, &mut body).await?;
    }
    body.truncate(head.content_length);
    Ok(Request {
        method: head.method,
        path: head.path,
        body,
    })
}

/// Appends what the client sends next. Closing or failing is an incomplete
/// request.
async fn read_more(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<(), Response> {
    match stream.read_buf(buf).await {
        Ok(0) | Err(_) => Err(bad_request("the request is incomplete")),
        Ok(_) => Ok(()),
    }
}

struct Head {
    method: String,
    path: String,
    content_length: usize,
}

fn parse_head(head: &[u8]) -> Result<Head, Response> {
    let head = std::str::from_utf8(head).map_err(|_| bad_request("the head is not UTF-8"))?;
    let mut lines = head.split("\r\n");
    let parts: Vec<&str> = lines.next().unwrap_or_default().split(' ').collect();
    let &[method, target, version] = parts.as_slice() else {
        return Err(bad_request("malformed request line"));
    };
    if !version.starts_with("HTTP/1.") {
        return Err(bad_request("unsupported HTTP version"));
    }
    let mut content_length = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            let length: usize = value
                .trim()
                .parse()
                .map_err(|_| bad_request("invalid Content-Length"))?;
            if content_length.replace(length).is_some() {
                return Err(bad_request("repeated Content-Length"));
            }
        }
    }
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    Ok(Head {
        method: method.to_owned(),
        path: path.to_owned(),
        content_length: content_length.unwrap_or(0),
    })
}

impl Api {
    fn route(&self, request: &Request) -> Response {
        let path: Vec<&str> = request.path.trim_start_matches('/').split('/').collect();
        match (request.method.as_str(), path.as_slice()) {
            ("GET", ["metrics"]) => self.metrics(),
            ("GET", ["classes"]) => self.list_classes(),
            ("GET", ["databases", name]) => self.database(name),
            ("POST", ["notify"]) => self.notify(&request.body),
            ("POST", ["terminate"]) => self.terminate(&request.body),
            ("GET", ["classes", name, "settings"]) => self.settings(name),
            ("PUT", ["classes", name, "settings"]) => self.set_settings(name, &request.body),
            (
                _,
                ["metrics"]
                | ["databases", _]
                | ["notify"]
                | ["terminate"]
                | ["classes"]
                | ["classes", _, "settings"],
            ) => Response::text("405 Method Not Allowed", "method not allowed"),
            _ => Response::text("404 Not Found", "not found"),
        }
    }

    fn metrics(&self) -> Response {
        let mut body = String::new();
        metrics::render(&self.classes, &mut body);
        if let Some(lookups) = &self.lookups {
            metrics::render_lookups(lookups, &mut body);
        }
        Response {
            status: "200 OK",
            content_type: "text/plain; version=0.0.4",
            body,
        }
    }

    fn list_classes(&self) -> Response {
        let classes: Vec<_> = self
            .classes
            .iter()
            .map(|class| ClassInfo {
                name: &class.name,
                databases: &class.databases,
                source: class.source,
            })
            .collect();
        json_response(&classes)
    }

    fn database(&self, name: &str) -> Response {
        match self.databases.info(name) {
            Some(info) => json_response(&info),
            None => Response::text("404 Not Found", "no open database"),
        }
    }

    fn notify(&self, body: &[u8]) -> Response {
        match serde_json::from_slice::<NotifyRequest>(body) {
            Ok(request) => {
                let connections =
                    self.databases
                        .notify(&request.target, &request.channel, &request.payload);
                json_response(&json!({ "connections": connections }))
            }
            Err(error) => bad_request(format!("invalid request body: {error}")),
        }
    }

    fn terminate(&self, body: &[u8]) -> Response {
        match serde_json::from_slice::<Target>(body) {
            Ok(target) => {
                json_response(&json!({ "connections": self.databases.terminate(&target) }))
            }
            Err(error) => bad_request(format!("invalid request body: {error}")),
        }
    }

    fn settings(&self, name: &str) -> Response {
        match self.class(name) {
            Some(class) => json_response(&**class.settings.load()),
            None => unknown_class(),
        }
    }

    fn set_settings(&self, name: &str, body: &[u8]) -> Response {
        let Some(class) = self.class(name) else {
            return unknown_class();
        };
        let settings = serde_json::from_slice::<config::Settings>(body)
            .map_err(|error| error.to_string())
            .and_then(|settings| settings.validate().map(|()| settings));
        match settings {
            Ok(settings) => {
                // The response is the stored value; serializing it before the
                // store keeps a concurrent PUT from changing what it reports.
                let response = json_response(&settings);
                class.settings.store(Arc::new(settings));
                response
            }
            Err(error) => bad_request(format!("invalid settings: {error}")),
        }
    }

    fn class(&self, name: &str) -> Option<&Class> {
        self.classes.iter().find(|class| class.name == name)
    }
}

/// What `GET /classes` shows of a class.
#[derive(Serialize)]
struct ClassInfo<'a> {
    name: &'a str,
    databases: &'a str,
    source: Source,
}

/// A notification: its target, and the channel and payload to send.
#[derive(Deserialize)]
struct NotifyRequest {
    #[serde(flatten)]
    target: Target,
    channel: String,
    #[serde(default)]
    payload: String,
}

struct Response {
    status: &'static str,
    content_type: &'static str,
    body: String,
}

impl Response {
    fn text(status: &'static str, body: impl Into<String>) -> Self {
        Response {
            status,
            content_type: "text/plain",
            body: body.into(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.status,
            self.content_type,
            self.body.len(),
            self.body
        )
        .into_bytes()
    }
}

fn json_response<T: Serialize>(value: &T) -> Response {
    Response {
        status: "200 OK",
        content_type: "application/json",
        // The values serialized here cannot fail to serialize.
        body: serde_json::to_string(value).expect("JSON values serialize"),
    }
}

fn bad_request(reason: impl Into<String>) -> Response {
    Response::text("400 Bad Request", reason)
}

fn head_too_large() -> Response {
    Response::text(
        "431 Request Header Fields Too Large",
        "the request head is over 16 KiB",
    )
}

fn unknown_class() -> Response {
    Response::text("404 Not Found", "unknown class")
}
