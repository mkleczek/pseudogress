//! LISTEN and UNLISTEN in simple queries. The backend answers them like any
//! statement, but the connection's channels decide which notifications it
//! gets.

use crate::databases::Connection;

/// Applies the LISTEN and UNLISTEN statements in `sql`, which holds any number
/// of statements separated by `;`. Almost every query has neither, so the
/// first word is all that is looked at, without allocating.
pub(super) fn apply(sql: &str, connection: &Connection) {
    for statement in sql.split(';') {
        let statement = statement.trim_start();
        if let Some(channel) = after_keyword(statement, "listen") {
            let channel = identifier(channel);
            if !channel.is_empty() {
                connection.listen(&channel);
            }
        } else if let Some(channel) = after_keyword(statement, "unlisten") {
            let channel = channel.trim();
            if channel == "*" {
                connection.unlisten("*");
            } else {
                let channel = identifier(channel);
                if !channel.is_empty() {
                    connection.unlisten(&channel);
                }
            }
        }
    }
}

/// What follows the keyword, if `statement` starts with it in any case.
fn after_keyword<'a>(statement: &'a str, keyword: &str) -> Option<&'a str> {
    let (head, rest) = statement.split_at_checked(keyword.len())?;
    (head.eq_ignore_ascii_case(keyword) && rest.starts_with(char::is_whitespace)).then_some(rest)
}

/// The identifier at the start of `text`, as PostgreSQL reads it: quoted
/// names keep their case and unquoted ones are folded to lower case.
fn identifier(text: &str) -> String {
    let text = text.trim_start();
    let Some(quoted) = text.strip_prefix('"') else {
        return text
            .split(char::is_whitespace)
            .next()
            .unwrap_or_default()
            .to_lowercase();
    };
    let mut name = String::new();
    let mut chars = quoted.chars().peekable();
    while let Some(c) = chars.next() {
        // A doubled quote is a quote in the name.
        if c == '"' && chars.next_if_eq(&'"').is_none() {
            break;
        }
        name.push(c);
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_keywords_in_any_case() {
        assert_eq!(after_keyword("LISTEN foo", "listen"), Some(" foo"));
        assert_eq!(after_keyword("lIsTeN\tfoo", "listen"), Some("\tfoo"));
        assert_eq!(after_keyword("listen", "listen"), None);
        assert_eq!(after_keyword("listening foo", "listen"), None);
        assert_eq!(after_keyword("select 1", "listen"), None);
        assert_eq!(after_keyword("é", "listen"), None);
    }

    #[test]
    fn reads_identifiers_like_postgresql() {
        assert_eq!(identifier("Foo"), "foo");
        assert_eq!(identifier(" foo bar"), "foo");
        assert_eq!(identifier("\"Foo Bar\""), "Foo Bar");
        assert_eq!(identifier("\"a\"\"b\" x"), "a\"b");
        assert_eq!(identifier(""), "");
    }
}
