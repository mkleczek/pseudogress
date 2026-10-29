//! A minimal HTTP/1.1 client: one `GET` per connection.

use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::Url;

/// The longest body that is read.
const MAX_BODY: usize = 1024 * 1024;
const MAX_HEAD: usize = 16 * 1024;
/// The longest line of a chunked body's framing: a size and extensions.
const MAX_CHUNK_LINE: usize = 1024;
/// How much of the body of a failure goes into its reason.
const REASON_BODY: usize = 200;

/// Requests `url` with `Connection: close`. The result is the body of a 200,
/// `None` for a 404, and the reason for any other answer or failure, within
/// `timeout` as a whole.
pub(super) async fn get(url: &Url, timeout: Duration) -> Result<Option<Vec<u8>>, String> {
    tokio::time::timeout(timeout, request(url))
        .await
        .unwrap_or_else(|_| Err(format!("timed out after {timeout:?}")))
}

async fn request(url: &Url) -> Result<Option<Vec<u8>>, String> {
    let authority = url.authority();
    let mut stream = TcpStream::connect((url.host.as_str(), url.port))
        .await
        .map_err(|e| format!("connecting to {authority}: {e}"))?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        url.target
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("sending the request to {authority}: {e}"))?;
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(end) = find(&buf, b"\r\n\r\n") {
            break end + 4;
        }
        if buf.len() >= MAX_HEAD {
            return Err("the answer's head is over 16 KiB".into());
        }
        read_more(&mut stream, &mut buf).await?;
    };
    let head = parse_head(&buf[..head_end])?;
    if head.status == 404 {
        return Ok(None);
    }
    // Bytes read past the head are the start of the body.
    let body = read_body(&mut stream, buf.split_off(head_end), &head).await;
    if head.status == 200 {
        return body.map(Some);
    }
    Err(reason(&head.status_line, body.ok().as_deref()))
}

/// The position of the first `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Appends what the server sends next. Closing before the answer is complete
/// is an error.
async fn read_more(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<(), String> {
    match stream.read_buf(buf).await {
        Ok(0) => Err("the connection closed before the answer was complete".into()),
        Ok(_) => Ok(()),
        Err(e) => Err(format!("reading the answer: {e}")),
    }
}

struct Head {
    status: u16,
    /// The status and its reason phrase, as in `500 Internal Server Error`.
    status_line: String,
    content_length: Option<usize>,
    chunked: bool,
}

fn parse_head(head: &[u8]) -> Result<Head, String> {
    let head = std::str::from_utf8(head).map_err(|_| "the answer's head is not UTF-8")?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status_line = status_line
        .strip_prefix("HTTP/1.")
        .and_then(|rest| rest.split_once(' '))
        .map(|(_, status_line)| status_line.trim())
        .ok_or_else(|| format!("not an HTTP/1 answer: {}", excerpt(status_line.as_bytes())))?;
    let status = status_line
        .split(' ')
        .next()
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| format!("invalid status line: {}", excerpt(status_line.as_bytes())))?;
    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let length: usize = value.parse().map_err(|_| "invalid Content-Length")?;
            if content_length
                .replace(length)
                .is_some_and(|before| before != length)
            {
                return Err("conflicting Content-Length headers".into());
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if !value.eq_ignore_ascii_case("chunked") {
                return Err(format!("unsupported Transfer-Encoding: {value}"));
            }
            chunked = true;
        }
    }
    Ok(Head {
        status,
        status_line: status_line.to_owned(),
        content_length,
        chunked,
    })
}

/// Reads the body, of which `body` is what came with the head. A Transfer-Encoding
/// goes before a Content-Length; without either the body ends with the
/// connection.
async fn read_body(
    stream: &mut TcpStream,
    mut body: Vec<u8>,
    head: &Head,
) -> Result<Vec<u8>, String> {
    if head.chunked {
        return read_chunked(stream, body).await;
    }
    let Some(length) = head.content_length else {
        loop {
            if body.len() > MAX_BODY {
                return Err(too_large());
            }
            match stream.read_buf(&mut body).await {
                Ok(0) => return Ok(body),
                Ok(_) => {}
                Err(e) => return Err(format!("reading the answer: {e}")),
            }
        }
    };
    if length > MAX_BODY {
        return Err(too_large());
    }
    while body.len() < length {
        read_more(stream, &mut body).await?;
    }
    body.truncate(length);
    Ok(body)
}

/// Decodes a chunked body, of which `raw` is the start. Trailers are not read.
async fn read_chunked(stream: &mut TcpStream, mut raw: Vec<u8>) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let line_end = loop {
            if let Some(end) = find(&raw, b"\r\n") {
                break end;
            }
            if raw.len() > MAX_CHUNK_LINE {
                return Err("invalid chunked body: a chunk size is too long".into());
            }
            read_more(stream, &mut raw).await?;
        };
        let line = String::from_utf8_lossy(&raw[..line_end]);
        // What follows a semicolon is an extension.
        let size = line.split(';').next().unwrap_or_default().trim();
        if size.is_empty() || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!(
                "invalid chunked body: {size:?} is not a chunk size"
            ));
        }
        let size = usize::from_str_radix(size, 16).map_err(|_| too_large())?;
        raw.drain(..line_end + 2);
        if size == 0 {
            return Ok(body);
        }
        if size > MAX_BODY - body.len() {
            return Err(too_large());
        }
        // The data, and the CRLF that ends it.
        while raw.len() < size + 2 {
            read_more(stream, &mut raw).await?;
        }
        if &raw[size..size + 2] != b"\r\n" {
            return Err("invalid chunked body: a chunk is longer than its size".into());
        }
        body.extend_from_slice(&raw[..size]);
        raw.drain(..size + 2);
    }
}

fn too_large() -> String {
    "the body is over 1 MiB".into()
}

/// Why an answer failed: its status, and the start of its body, which often
/// says more.
fn reason(status_line: &str, body: Option<&[u8]>) -> String {
    match body.map(excerpt) {
        Some(body) if !body.is_empty() => format!("status {status_line}: {body}"),
        _ => format!("status {status_line}"),
    }
}

/// The start of some bytes, on one line.
fn excerpt(bytes: &[u8]) -> String {
    let start = &bytes[..bytes.len().min(REASON_BODY)];
    let text: String = String::from_utf8_lossy(start)
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let ellipsis = if bytes.len() > REASON_BODY { "..." } else { "" };
    format!("{}{ellipsis}", text.trim())
}
