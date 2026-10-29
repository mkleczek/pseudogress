//! The URL template of a class service.

/// `http://host[:port]/path[?query]`, where `{database}` and `{user}` in the
/// path and the query stand for the database and the user of a connection,
/// and `{database}` must appear.
#[allow(dead_code, reason = "nothing looks classes up yet")]
#[derive(Debug)]
pub struct Template {
    host: String,
    port: u16,
    target: Vec<Piece>,
}

#[allow(dead_code, reason = "nothing looks classes up yet")]
#[derive(Debug)]
enum Piece {
    Text(String),
    Database,
    User,
}

/// A template filled in: where to connect and what to request.
#[allow(dead_code, reason = "nothing looks classes up yet")]
#[derive(Debug)]
pub struct Url {
    pub(super) host: String,
    pub(super) port: u16,
    /// The path and the query, as in the request line.
    pub(super) target: String,
}

impl Template {
    /// Checks a template at startup, so that a mistake fails before any
    /// connection does.
    #[allow(dead_code, reason = "nothing looks classes up yet")]
    pub fn parse(template: &str) -> Result<Template, String> {
        let _ = template;
        todo!()
    }

    /// Replaces the placeholders with the values, percent-encoded: everything
    /// but `A-Z a-z 0-9 - . _ ~` is.
    #[allow(dead_code, reason = "nothing looks classes up yet")]
    pub fn fill(&self, database: &str, user: &str) -> Url {
        let _ = (database, user);
        todo!()
    }
}
