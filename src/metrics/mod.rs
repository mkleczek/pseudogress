//! Counters of a class, and their rendering in Prometheus' text format.
//! Counters are relaxed atomics, updated at most a few times per batch.

use std::{
    sync::atomic::{AtomicI64, AtomicU64},
    time::Duration,
};

use crate::class::Class;

#[cfg(test)]
mod tests;

#[derive(Default)]
pub struct ClassMetrics {
    /// Open connections.
    pub connections: AtomicI64,
    /// Accepted connections.
    pub connects: AtomicU64,
    pub auth_failures: AtomicU64,
    /// Executes and simple queries answered.
    pub statements: AtomicU64,
    /// Batches whose responses wait for their simulated time.
    pub in_flight: AtomicI64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    /// Injected faults.
    pub faults: AtomicU64,
    /// Statements without a recording. Always 0 in a valid run.
    pub misses: AtomicU64,
    /// Executes answered by the recording of other parameters.
    pub fallbacks: AtomicU64,
    /// pseudogress' own time per batch, without the simulated delays.
    pub processing: Histogram,
    /// How late answers were sent after their simulated time.
    pub lateness: Histogram,
}

/// Durations in power-of-two buckets of nanoseconds.
#[derive(Default)]
pub struct Histogram {
    buckets: [AtomicU64; 32],
    sum_nanos: AtomicU64,
}

impl Histogram {
    pub fn record(&self, elapsed: Duration) {
        let _ = (elapsed, &self.buckets, &self.sum_nanos);
        todo!()
    }
}

/// Appends the metrics of every class to `out`.
pub fn render(classes: &[Class], out: &mut String) {
    let _ = (classes, out);
    todo!()
}
