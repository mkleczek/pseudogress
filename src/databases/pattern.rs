//! Database name patterns, where `*` matches any characters.

/// The text between the stars, so that matching a name allocates nothing.
pub(crate) struct Pattern {
    /// Before the first star; the whole pattern when it has none.
    first: Box<str>,
    /// After each star. Empty when the pattern has no star.
    rest: Box<[Box<str>]>,
}

impl Pattern {
    pub(crate) fn new(pattern: &str) -> Self {
        let mut parts = pattern.split('*');
        Pattern {
            first: parts.next().unwrap_or_default().into(),
            rest: parts.map(Into::into).collect(),
        }
    }

    pub(crate) fn matches(&self, name: &str) -> bool {
        let Some((last, middle)) = self.rest.split_last() else {
            return *name == *self.first;
        };
        // The ends are cut off first so that no part overlaps another.
        let Some(mut name) = name
            .strip_prefix(&*self.first)
            .and_then(|name| name.strip_suffix(&**last))
        else {
            return false;
        };
        // The leftmost match of a part leaves the most name for the next.
        middle.iter().all(|part| match name.find(&**part) {
            Some(at) => {
                name = &name[at + part.len()..];
                true
            }
            None => false,
        })
    }
}
