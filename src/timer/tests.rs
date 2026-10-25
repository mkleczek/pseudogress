use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use tokio::sync::Barrier;

use super::sleep_until;

/// How late `sleep_until(deadline)` returned; fails if it returned early.
fn lateness(deadline: Instant) -> Duration {
    Instant::now()
        .checked_duration_since(deadline)
        .expect("woke before the deadline")
}

/// Sorts `late` and returns its median and maximum.
fn median_and_max(late: &mut [Duration]) -> (Duration, Duration) {
    late.sort();
    (late[late.len() / 2], late[late.len() - 1])
}

fn poll_once(sleep: impl Future<Output = ()>) -> Poll<()> {
    pin!(sleep).poll(&mut Context::from_waker(Waker::noop()))
}

/// No runtime here: a past deadline must not need tokio's timer.
#[test]
#[ignore = "xfail: timer is not implemented yet"]
fn a_past_deadline_completes_on_the_first_poll() {
    let now = Instant::now();
    assert!(poll_once(sleep_until(now - Duration::from_millis(1))).is_ready());
    assert!(poll_once(sleep_until(now)).is_ready());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "xfail: timer is not implemented yet"]
async fn a_sleep_never_wakes_early_and_wakes_promptly() {
    for delay in [200, 500, 2_000, 20_000].map(Duration::from_micros) {
        let mut late = Vec::new();
        for _ in 0..10 {
            let deadline = Instant::now() + delay;
            sleep_until(deadline).await;
            late.push(lateness(deadline));
        }
        let (median, max) = median_and_max(&mut late);
        assert!(median < Duration::from_micros(500), "{delay:?}: {late:?}");
        assert!(max < Duration::from_millis(5), "{delay:?}: {late:?}");
    }
}

/// The timer thread sleeps until the earliest deadline it knows, so a
/// deadline before it must wake the thread.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "xfail: timer is not implemented yet"]
async fn an_earlier_deadline_overtakes_a_registered_one() {
    let later = Instant::now() + Duration::from_millis(2);
    let mut registered = pin!(sleep_until(later));
    let first_poll = registered
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(first_poll.is_pending());

    let earlier = Instant::now() + Duration::from_micros(200);
    sleep_until(earlier).await;
    assert!(lateness(earlier) < Duration::from_millis(1));

    registered.await;
    assert!(lateness(later) < Duration::from_millis(2));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "xfail: timer is not implemented yet"]
async fn thousands_of_concurrent_sleepers_wake_on_time() {
    const SLEEPERS: usize = 10_000;
    // Deadlines are chosen once every task is running, so they all overlap.
    let start = Arc::new(Barrier::new(SLEEPERS));
    let tasks: Vec<_> = (0..SLEEPERS)
        .map(|_| {
            let start = start.clone();
            tokio::spawn(async move {
                start.wait().await;
                let deadline =
                    Instant::now() + Duration::from_micros(rand::random_range(0..20_000));
                sleep_until(deadline).await;
                Instant::now().checked_duration_since(deadline)
            })
        })
        .collect();
    let mut late = Vec::with_capacity(SLEEPERS);
    for task in tasks {
        late.push(task.await.unwrap().expect("woke before the deadline"));
    }
    let (median, max) = median_and_max(&mut late);
    assert!(median < Duration::from_micros(500), "median {median:?}");
    assert!(max < Duration::from_millis(10), "max {max:?}");
}
