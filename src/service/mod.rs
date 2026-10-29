//! A client for a class service: an HTTP service that answers with the class
//! of a database that no class of the configuration matches.
//!
//! The service is asked with a `GET` of its URL template, filled in with the
//! database and the user of the connection; the answer is the class as JSON,
//! the fields of a `[[class]]` table except `recording`. `docs/class.schema.json`
//! describes it.

use std::time::Duration;

use crate::config;

mod http;
mod template;
#[cfg(test)]
mod tests;

pub use template::{Template, Url};

/// How long a lookup may take, from connecting to the last byte of the answer.
#[allow(dead_code, reason = "nothing looks classes up yet")]
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// The names of classes: they name the files of recordings.
#[allow(dead_code, reason = "nothing looks classes up yet")]
pub const NAME_PATTERN: &str = "^[A-Za-z0-9_][A-Za-z0-9_.-]*$";

/// What the service said about a database.
#[allow(dead_code, reason = "nothing looks classes up yet")]
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
#[allow(dead_code, reason = "nothing looks classes up yet")]
pub async fn lookup(template: &Template, database: &str, user: &str, timeout: Duration) -> Lookup {
    let _ = (template, database, user, timeout);
    todo!()
}

/// Parses and checks the service's answer to a question about `database`: the
/// class must be one that its configuration could hold, except for the
/// recording, and its `databases` must match `database`.
#[allow(dead_code, reason = "nothing looks classes up yet")]
pub fn parse_class(body: &[u8], database: &str) -> Result<config::Class, String> {
    let _ = (body, database);
    todo!()
}
