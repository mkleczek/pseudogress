//! Counters of a class, and their rendering in Prometheus' text format.
//! Counters are relaxed atomics, updated at most a few times per batch.

use std::{
    fmt::{Display, Write},
    sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    time::Duration,
};

use crate::{class::Class, registry::Registry};

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
    /// Durations of 2^31 ns (about 2.1 s) and more count in the last bucket,
    /// which only `+Inf` reports.
    pub fn record(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        // Or-ing in 1 keeps 0 ns from having 64 leading zeros.
        let bucket = (63 - (nanos | 1).leading_zeros()).min(31);
        self.buckets[bucket as usize].fetch_add(1, Relaxed);
        self.sum_nanos.fetch_add(nanos, Relaxed);
    }
}

/// Appends the metrics of every class to `out`.
pub fn render(classes: &Registry<Class>, out: &mut String) {
    family(
        out,
        classes,
        "pseudogress_connections",
        "gauge",
        "Open connections.",
        |m| m.connections.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_connects_total",
        "counter",
        "Accepted connections.",
        |m| m.connects.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_auth_failures_total",
        "counter",
        "Failed authentications.",
        |m| m.auth_failures.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_statements_total",
        "counter",
        "Executes and simple queries answered.",
        |m| m.statements.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_in_flight",
        "gauge",
        "Batches waiting for their simulated time.",
        |m| m.in_flight.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_bytes_in_total",
        "counter",
        "Bytes received from clients.",
        |m| m.bytes_in.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_bytes_out_total",
        "counter",
        "Bytes sent to clients.",
        |m| m.bytes_out.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_faults_total",
        "counter",
        "Injected faults.",
        |m| m.faults.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_misses_total",
        "counter",
        "Statements without a recording.",
        |m| m.misses.load(Relaxed),
    );
    family(
        out,
        classes,
        "pseudogress_fallbacks_total",
        "counter",
        "Executes answered by the recording of other parameters.",
        |m| m.fallbacks.load(Relaxed),
    );
    histogram(
        out,
        classes,
        "pseudogress_processing_seconds",
        "pseudogress' own time per batch, without the simulated delays.",
        |m| &m.processing,
    );
    histogram(
        out,
        classes,
        "pseudogress_answer_lateness_seconds",
        "How late answers were sent after their simulated time.",
        |m| &m.lateness,
    );
}

/// A counter or gauge, with a sample per class.
fn family<T: Display>(
    out: &mut String,
    classes: &Registry<Class>,
    name: &str,
    kind: &str,
    help: &str,
    value: impl Fn(&ClassMetrics) -> T,
) {
    header(out, name, kind, help);
    for class in classes.iter() {
        sample_start(out, name, "", &class.name);
        let _ = writeln!(out, "}} {}", value(&class.metrics));
    }
}

fn histogram(
    out: &mut String,
    classes: &Registry<Class>,
    name: &str,
    help: &str,
    value: impl Fn(&ClassMetrics) -> &Histogram,
) {
    header(out, name, "histogram", help);
    for class in classes.iter() {
        let histogram = value(&class.metrics);
        let (finite, [overflow]) = histogram.buckets.split_at(31) else {
            unreachable!("32 buckets")
        };
        let mut cumulative = 0;
        for (i, bucket) in finite.iter().enumerate() {
            cumulative += bucket.load(Relaxed);
            let le = Duration::from_nanos(1 << (i + 1)).as_secs_f64();
            sample_start(out, name, "_bucket", &class.name);
            let _ = writeln!(out, ",le=\"{le}\"}} {cumulative}");
        }
        cumulative += overflow.load(Relaxed);
        sample_start(out, name, "_bucket", &class.name);
        let _ = writeln!(out, ",le=\"+Inf\"}} {cumulative}");
        let sum = Duration::from_nanos(histogram.sum_nanos.load(Relaxed));
        sample_start(out, name, "_sum", &class.name);
        let _ = writeln!(out, "}} {}", sum.as_secs_f64());
        sample_start(out, name, "_count", &class.name);
        let _ = writeln!(out, "}} {cumulative}");
    }
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// Writes up to the class label's closing quote; the caller writes the rest.
fn sample_start(out: &mut String, name: &str, suffix: &str, class: &str) {
    let _ = write!(out, "{name}{suffix}{{class=\"");
    for c in class.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
}
