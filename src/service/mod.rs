//! A client for a class service: an HTTP service that answers with the class
//! of a database that no class of the configuration matches.
//!
//! The service is asked with a `GET` of its URL template, filled in with the
//! database and the user of the connection; the answer is the class as JSON,
//! the fields of a `[[class]]` table except `recording`. `docs/class.schema.json`
//! describes it.

use std::time::Duration;

use crate::{config, databases::Pattern};

mod http;
mod template;
#[cfg(test)]
mod tests;

pub use template::{Template, Url};

/// How long a lookup may take, from connecting to the last byte of the answer.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// The names of classes: they name the files of recordings. A dot is not
/// allowed: the times of `a` are in `a.times.jsonl`, which would be the
/// recording of a class named `a.times`.
pub const NAME_PATTERN: &str = "^[A-Za-z0-9_][A-Za-z0-9_-]*$";

/// The most bytes of a name, so that `<name>.times.jsonl` fits the 255 bytes
/// of a file name.
pub const MAX_NAME_LENGTH: usize = 200;

/// What the service said about a database.
#[derive(Debug, PartialEq)]
pub enum Lookup {
    Found(Box<config::Class>),
    /// The service has no class for the database.
    NotFound,
    /// Why no class was found: the service or the network failed, or its answer
    /// is not a valid class. Fit for a log line and an error message.
    Failed(String),
}

/// Asks the service for the class of `database`, to which `user` connects.
pub async fn lookup(template: &Template, database: &str, user: &str, timeout: Duration) -> Lookup {
    match http::get(&template.fill(database, user), timeout).await {
        Ok(Some(body)) => match parse_class(&body, database) {
            Ok(class) => Lookup::Found(Box::new(class)),
            Err(reason) => Lookup::Failed(reason),
        },
        Ok(None) => Lookup::NotFound,
        Err(reason) => Lookup::Failed(reason),
    }
}

/// Parses and checks the service's answer to a question about `database`: the
/// class must be one that its configuration could hold, except for the
/// recording, and its `databases` must match `database`.
pub fn parse_class(body: &[u8], database: &str) -> Result<config::Class, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    // Pseudogress chooses where a class is recorded. Deserializing would take
    // an empty recording for none.
    if value.get("recording").is_some() {
        return Err("invalid class: recording: Pseudogress chooses it, not the service".into());
    }
    let class: config::Class =
        serde_json::from_value(value).map_err(|e| format!("invalid class: {e}"))?;
    if !valid_name(&class.name) {
        return Err(format!(
            "invalid class: name: {:?} must match {NAME_PATTERN} and be at most \
             {MAX_NAME_LENGTH} bytes",
            class.name
        ));
    }
    if !Pattern::new(&class.databases).matches(database) {
        return Err(format!(
            "invalid class: databases: {:?} does not match {database:?}, which was asked about",
            class.databases
        ));
    }
    // Only record mode takes classes from a service.
    if class.upstream.is_none() {
        return Err("invalid class: upstream: required".into());
    }
    class
        .validate()
        .map_err(|e| format!("invalid class: {e}"))?;
    Ok(class)
}

/// `NAME_PATTERN`, within `MAX_NAME_LENGTH`.
fn valid_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= MAX_NAME_LENGTH
        && bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
