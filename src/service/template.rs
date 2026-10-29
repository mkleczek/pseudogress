//! The URL template of a class service.

use std::{fmt::Write, net::Ipv6Addr};

/// `http://host[:port]/path[?query]`, where `{database}` and `{user}` in the
/// path and the query stand for the database and the user of a connection,
/// and `{database}` must appear.
#[derive(Debug)]
pub struct Template {
    /// Without the brackets of an IPv6 address.
    host: String,
    port: u16,
    target: Vec<Piece>,
}

#[derive(Debug)]
enum Piece {
    Text(String),
    Database,
    User,
}

/// A template filled in: where to connect and what to request.
#[derive(Debug)]
pub struct Url {
    pub(super) host: String,
    pub(super) port: u16,
    /// The path and the query, as in the request line.
    pub(super) target: String,
}

impl Url {
    /// As in a Host header: the brackets of an IPv6 address are back.
    pub(super) fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

impl Template {
    /// Checks a template at startup, so that a mistake fails before any
    /// connection does.
    pub fn parse(template: &str) -> Result<Template, String> {
        let rest = match template.split_at_checked("http://".len()) {
            Some((scheme, rest)) if scheme.eq_ignore_ascii_case("http://") => rest,
            _ => return Err("only http:// URLs are supported".into()),
        };
        let (authority, target) = rest.split_at(rest.find(['/', '?']).unwrap_or(rest.len()));
        let (host, port) = parse_authority(authority)?;
        let mut pieces = Vec::new();
        // Without a path, the request is for the root.
        if !target.starts_with('/') {
            pieces.push(Piece::Text("/".into()));
        }
        parse_target(target, &mut pieces)?;
        if !pieces.iter().any(|piece| matches!(piece, Piece::Database)) {
            return Err("{database} is missing from the path and the query".into());
        }
        Ok(Template {
            host,
            port,
            target: pieces,
        })
    }

    /// Replaces the placeholders with the values, percent-encoded: everything
    /// but `A-Z a-z 0-9 - . _ ~` is.
    pub fn fill(&self, database: &str, user: &str) -> Url {
        let mut target = String::new();
        for piece in &self.target {
            match piece {
                Piece::Text(text) => target.push_str(text),
                Piece::Database => percent_encode(database, &mut target),
                Piece::User => percent_encode(user, &mut target),
            }
        }
        Url {
            host: self.host.clone(),
            port: self.port,
            target,
        }
    }
}

/// The host and the port of `host`, `host:port` or `[ipv6]:port`. The port is
/// 80 by default.
fn parse_authority(authority: &str) -> Result<(String, u16), String> {
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (address, after) = rest
            .split_once(']')
            .ok_or("invalid host: an IPv6 address needs its closing ]")?;
        address
            .parse::<Ipv6Addr>()
            .map_err(|_| format!("invalid host: {address:?} is not an IPv6 address"))?;
        let port = match after {
            "" => None,
            _ => Some(
                after
                    .strip_prefix(':')
                    .ok_or("invalid host: text follows the IPv6 address")?,
            ),
        };
        (address, port)
    } else {
        let (host, port) = match authority.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        };
        // What a DNS name has, and underscores. It leaves out user info,
        // placeholders and percent-encoding.
        let valid = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_');
        if host.is_empty() || !host.bytes().all(valid) {
            return Err(format!("invalid host {host:?}"));
        }
        (host, port)
    };
    let port = match port {
        None => 80,
        Some(port) => match port.parse::<u16>() {
            Ok(port) if port > 0 => port,
            _ => return Err(format!("invalid port {port:?}")),
        },
    };
    Ok((host.to_owned(), port))
}

/// Splits a path and a query into text and placeholders.
fn parse_target(target: &str, pieces: &mut Vec<Piece>) -> Result<(), String> {
    let mut rest = target;
    while let Some(at) = rest.find(['{', '}']) {
        let (text, after) = rest.split_at(at);
        push_text(pieces, text)?;
        let Some(after) = after.strip_prefix('{') else {
            return Err("unmatched } in the path or the query".into());
        };
        let (name, after) = after
            .split_once('}')
            .ok_or("unclosed { in the path or the query")?;
        pieces.push(match name {
            "database" => Piece::Database,
            "user" => Piece::User,
            _ => {
                return Err(format!(
                    "unknown placeholder {{{name}}}: only {{database}} and {{user}} are replaced"
                ));
            }
        });
        rest = after;
    }
    push_text(pieces, rest)
}

/// Text goes into the request line as it is, so it must not break the line.
fn push_text(pieces: &mut Vec<Piece>, text: &str) -> Result<(), String> {
    if let Some(c) = text.chars().find(|c| !c.is_ascii_graphic() || *c == '#') {
        return Err(format!(
            "invalid character {c:?} in the path or the query: percent-encode it"
        ));
    }
    if !text.is_empty() {
        pieces.push(Piece::Text(text.to_owned()));
    }
    Ok(())
}

fn percent_encode(value: &str, out: &mut String) {
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            // Writing to a String doesn't fail.
            let _ = write!(out, "%{byte:02X}");
        }
    }
}
