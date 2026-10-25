use std::{sync::atomic::Ordering::Relaxed, time::Duration};

use super::*;
use crate::{class::Class, config::Config};

const CONFIG: &str = r#"
listen = ["127.0.0.1:5432"]

[[class]]
name = "small"
databases = "small-*"
users = ["authenticator"]
recording = "recordings/small.jsonl"

[[class]]
name = "large"
databases = "large-*"
users = ["authenticator"]
recording = "recordings/large.jsonl"
"#;

const NAMES: [&str; 11] = [
    "pseudogress_connections",
    "pseudogress_connects_total",
    "pseudogress_auth_failures_total",
    "pseudogress_statements_total",
    "pseudogress_in_flight",
    "pseudogress_bytes_in_total",
    "pseudogress_bytes_out_total",
    "pseudogress_faults_total",
    "pseudogress_misses_total",
    "pseudogress_fallbacks_total",
    "pseudogress_processing_seconds",
];

fn load_classes(config: &str) -> Vec<Class> {
    let config: Config = toml::from_str(config).unwrap();
    config.classes.iter().map(Class::new).collect()
}

/// The index of the one bucket that counts `elapsed`.
fn bucket_of(elapsed: Duration) -> usize {
    let histogram = Histogram::default();
    histogram.record(elapsed);
    histogram
        .buckets
        .iter()
        .position(|bucket| bucket.load(Relaxed) == 1)
        .expect("one bucket counts the duration")
}

/// The value of the sample whose line starts with `prefix`.
fn value(out: &str, prefix: &str) -> u64 {
    let line = out
        .lines()
        .find(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("no line starts with {prefix:?} in:\n{out}"));
    line.rsplit_once(' ').unwrap().1.parse().unwrap()
}

#[test]
fn buckets_are_powers_of_two_of_nanoseconds() {
    assert_eq!(bucket_of(Duration::ZERO), 0);
    assert_eq!(bucket_of(Duration::from_nanos(1)), 0);
    assert_eq!(bucket_of(Duration::from_nanos(2)), 1);
    assert_eq!(bucket_of(Duration::from_nanos(3)), 1);
    assert_eq!(bucket_of(Duration::from_nanos(4)), 2);
    assert_eq!(bucket_of(Duration::from_micros(1)), 9);
    assert_eq!(bucket_of(Duration::from_secs(5)), 31);
    assert_eq!(bucket_of(Duration::from_nanos((1 << 31) - 1)), 30);
    assert_eq!(bucket_of(Duration::from_nanos(1 << 31)), 31);
    assert_eq!(bucket_of(Duration::from_nanos(1 << 32)), 31);
    assert_eq!(bucket_of(Duration::from_secs(3600)), 31);
}

#[test]
fn sums_recorded_durations() {
    let histogram = Histogram::default();
    histogram.record(Duration::from_micros(1));
    histogram.record(Duration::from_secs(5));
    assert_eq!(histogram.sum_nanos.load(Relaxed), 5_000_001_000);
}

#[test]
fn renders_each_metric_once_with_a_sample_per_class() {
    let classes = load_classes(CONFIG);
    let (small, large) = (&classes[0].metrics, &classes[1].metrics);
    small.connections.store(3, Relaxed);
    small.connects.store(7, Relaxed);
    small.auth_failures.store(1, Relaxed);
    small.statements.store(42, Relaxed);
    small.in_flight.store(2, Relaxed);
    small.bytes_in.store(100, Relaxed);
    small.bytes_out.store(2000, Relaxed);
    small.faults.store(4, Relaxed);
    small.fallbacks.store(6, Relaxed);
    small.processing.record(Duration::from_micros(1));
    large.connections.store(5, Relaxed);
    large.statements.store(9, Relaxed);
    large.misses.store(1, Relaxed);

    let mut out = String::new();
    render(&classes, &mut out);

    for name in NAMES {
        assert_eq!(out.matches(&format!("# HELP {name} ")).count(), 1, "{name}");
        assert_eq!(out.matches(&format!("# TYPE {name} ")).count(), 1, "{name}");
    }
    let lines: Vec<&str> = out.lines().collect();
    for line in [
        "# TYPE pseudogress_connections gauge",
        "# TYPE pseudogress_connects_total counter",
        "# TYPE pseudogress_in_flight gauge",
        "# TYPE pseudogress_processing_seconds histogram",
        r#"pseudogress_connections{class="small"} 3"#,
        r#"pseudogress_connections{class="large"} 5"#,
        r#"pseudogress_connects_total{class="small"} 7"#,
        r#"pseudogress_connects_total{class="large"} 0"#,
        r#"pseudogress_auth_failures_total{class="small"} 1"#,
        r#"pseudogress_statements_total{class="small"} 42"#,
        r#"pseudogress_statements_total{class="large"} 9"#,
        r#"pseudogress_in_flight{class="small"} 2"#,
        r#"pseudogress_bytes_in_total{class="small"} 100"#,
        r#"pseudogress_bytes_out_total{class="small"} 2000"#,
        r#"pseudogress_faults_total{class="small"} 4"#,
        r#"pseudogress_misses_total{class="large"} 1"#,
        r#"pseudogress_fallbacks_total{class="small"} 6"#,
        r#"pseudogress_fallbacks_total{class="large"} 0"#,
        r#"pseudogress_processing_seconds_sum{class="small"} 0.000001"#,
        r#"pseudogress_processing_seconds_count{class="small"} 1"#,
        r#"pseudogress_processing_seconds_count{class="large"} 0"#,
    ] {
        assert!(lines.contains(&line), "missing {line:?} in:\n{out}");
    }
}

#[test]
fn renders_the_lateness_histogram_apart_from_processing() {
    let classes = load_classes(CONFIG);
    classes[0]
        .metrics
        .lateness
        .record(Duration::from_nanos(3_000));

    let mut out = String::new();
    render(&classes, &mut out);

    let name = "pseudogress_answer_lateness_seconds";
    assert_eq!(out.matches(&format!("# HELP {name} ")).count(), 1);
    assert_eq!(out.matches(&format!("# TYPE {name} histogram")).count(), 1);
    let lines: Vec<&str> = out.lines().collect();
    for line in [
        r#"pseudogress_answer_lateness_seconds_bucket{class="small",le="0.000002048"} 0"#,
        r#"pseudogress_answer_lateness_seconds_bucket{class="small",le="0.000004096"} 1"#,
        r#"pseudogress_answer_lateness_seconds_bucket{class="small",le="+Inf"} 1"#,
        r#"pseudogress_answer_lateness_seconds_sum{class="small"} 0.000003"#,
        r#"pseudogress_answer_lateness_seconds_count{class="small"} 1"#,
        r#"pseudogress_answer_lateness_seconds_count{class="large"} 0"#,
        r#"pseudogress_processing_seconds_count{class="small"} 0"#,
    ] {
        assert!(lines.contains(&line), "missing {line:?} in:\n{out}");
    }
}

#[test]
fn histogram_buckets_are_cumulative_and_end_at_the_count() {
    let classes = load_classes(CONFIG);
    let histogram = &classes[0].metrics.processing;
    for nanos in [1, 3, 1_000, 1_000_000, 5_000_000_000] {
        histogram.record(Duration::from_nanos(nanos));
    }

    let mut out = String::new();
    render(&classes, &mut out);

    let prefix = r#"pseudogress_processing_seconds_bucket{class="small","#;
    let buckets: Vec<u64> = out
        .lines()
        .filter(|line| line.starts_with(prefix))
        .map(|line| line.rsplit_once(' ').unwrap().1.parse().unwrap())
        .collect();
    assert_eq!(buckets.len(), 32, "31 finite buckets and +Inf");
    for line in [
        r#"pseudogress_processing_seconds_bucket{class="small",le="0.000000002"} 1"#,
        r#"pseudogress_processing_seconds_bucket{class="small",le="2.147483648"} 4"#,
    ] {
        assert!(out.lines().any(|l| l == line), "{line} in:\n{out}");
    }
    assert!(
        buckets.windows(2).all(|pair| pair[0] <= pair[1]),
        "{buckets:?}"
    );
    let count = value(
        &out,
        r#"pseudogress_processing_seconds_count{class="small"}"#,
    );
    assert_eq!(count, 5);
    assert_eq!(value(&out, &format!("{prefix}le=\"+Inf\"}}")), count);
    assert_eq!(buckets.last(), Some(&count));
}

#[test]
fn escapes_label_values() {
    let classes = load_classes(
        r#"
listen = []

[[class]]
name = "a\"b\\c\nd"
databases = "x"
users = []
recording = "x.jsonl"
"#,
    );
    let mut out = String::new();
    render(&classes, &mut out);
    assert!(
        out.lines()
            .any(|line| line == r#"pseudogress_connections{class="a\"b\\c\nd"} 0"#),
        "{out}"
    );
}
