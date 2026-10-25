//! Splitting the backend messages of a recording.

/// The type and body of every message in `bytes`. A truncated message is the
/// last item, as an error.
pub fn frames(mut rest: &[u8]) -> impl Iterator<Item = Result<(u8, &[u8]), &'static str>> {
    std::iter::from_fn(move || {
        let (&kind, tail) = rest.split_first()?;
        let frame = tail.split_first_chunk::<4>().and_then(|(length, tail)| {
            let body = (u32::from_be_bytes(*length) as usize).checked_sub(4)?;
            tail.split_at_checked(body)
        });
        Some(match frame {
            Some((body, after)) => {
                rest = after;
                Ok((kind, body))
            }
            None => {
                rest = &[];
                Err("truncated message")
            }
        })
    })
}
