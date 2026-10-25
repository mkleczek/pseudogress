//! Sleeping with microsecond precision. tokio's timer has millisecond
//! granularity, while the delays pseudogress simulates are often well under a
//! millisecond.

use std::time::Instant;

#[cfg(test)]
mod tests;

/// Completes at `deadline`, or at once if it has passed.
pub async fn sleep_until(deadline: Instant) {
    let _ = deadline;
    todo!()
}
