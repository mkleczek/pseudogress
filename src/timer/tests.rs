use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker, ready},
    time::{Duration, Instant},
};

use tokio::time::timeout;

use super::sleep_until;

/// Long enough for any sleep here, so that only a lost wake-up exceeds it.
const PATIENCE: Duration = Duration::from_secs(5);

/// The timing tests would disturb each other, as they share the timer thread
/// and the cores.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Records when it is woken: that is the timer's doing, while when the task
/// is polled next depends on how busy the workers are.
struct Stamp {
    task: Waker,
    woken: Mutex<Instant>,
}

impl Wake for Stamp {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().unwrap() = Instant::now();
        self.task.wake_by_ref();
    }
}

/// Sleeps until `deadline` and returns how late the timer woke the task.
async fn woken_late_by(deadline: Instant) -> Duration {
    let mut sleep = pin!(sleep_until(deadline));
    let mut stamp = None;
    let woken = poll_fn(|cx| {
        let stamp = stamp.get_or_insert_with(|| {
            Arc::new(Stamp {
                task: cx.waker().clone(),
                woken: Mutex::new(Instant::now()),
            })
        });
        let waker = Waker::from(stamp.clone());
        ready!(sleep.as_mut().poll(&mut Context::from_waker(&waker)));
        assert!(Instant::now() >= deadline, "woke before the deadline");
        // Polled only after its deadline, the task was last woken before it,
        // and the timer is not to blame.
        Poll::Ready(
            stamp
                .woken
                .lock()
                .unwrap()
                .saturating_duration_since(deadline),
        )
    });
    timeout(PATIENCE, woken).await.expect("the wake-up is lost")
}

fn median(late: &mut [Duration]) -> Duration {
    late.sort();
    late[late.len() / 2]
}

/// Passes at the first of up to `attempts` runs of `measure` that meets its
/// bounds, and panics with the last failure otherwise. Another process's load
/// can make one batch late, while a timer that is too late is late in every
/// attempt. Panics in `measure` are correctness failures, not retried.
async fn within_bounds_in_some_attempt<F, Fut>(attempts: usize, mut measure: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut failure = String::new();
    for _ in 0..attempts {
        match measure().await {
            Ok(()) => return,
            Err(last) if !lateness_is_checked() => {
                eprintln!("lateness not checked on a hosted macOS runner: {last}");
                return;
            }
            Err(last) => failure = last,
        }
    }
    panic!("{failure}");
}

/// GitHub's macOS runners are VMs that fire every timer about half its delay
/// late, whatever the timer: relative or absolute kqueue timers with
/// NOTE_CRITICAL and strict dispatch timers alike. There only early and lost
/// wake-ups are checked.
fn lateness_is_checked() -> bool {
    !(cfg!(target_os = "macos")
        && std::env::var_os("RUNNER_ENVIRONMENT").is_some_and(|runner| runner == "github-hosted"))
}

fn poll_once(sleep: impl Future<Output = ()>) -> Poll<()> {
    poll_once_pinned(pin!(sleep))
}

fn poll_once_pinned(sleep: Pin<&mut impl Future<Output = ()>>) -> Poll<()> {
    sleep.poll(&mut Context::from_waker(Waker::noop()))
}

/// No runtime here: a past deadline must not need tokio's timer.
#[test]
fn a_past_deadline_completes_on_the_first_poll() {
    let now = Instant::now();
    assert!(poll_once(sleep_until(now - Duration::from_millis(1))).is_ready());
    assert!(poll_once(sleep_until(now)).is_ready());
}

/// Medians, as a loaded machine delays single wake-ups by milliseconds.
#[tokio::test(flavor = "multi_thread")]
async fn a_sleep_never_wakes_early_and_wakes_promptly() {
    let _alone = ONE_AT_A_TIME.lock().await;
    for delay in [200, 500, 2_000, 20_000].map(Duration::from_micros) {
        within_bounds_in_some_attempt(3, || async move {
            let mut late = Vec::new();
            for _ in 0..10 {
                late.push(woken_late_by(Instant::now() + delay).await);
            }
            let median = median(&mut late);
            if median < Duration::from_micros(500) {
                Ok(())
            } else {
                Err(format!("{delay:?}: {late:?}"))
            }
        })
        .await;
    }
}

/// The timer thread sleeps until the earliest deadline it knows, so a
/// deadline before it must wake the thread.
#[tokio::test(flavor = "multi_thread")]
async fn an_earlier_deadline_overtakes_a_registered_one() {
    let _alone = ONE_AT_A_TIME.lock().await;
    within_bounds_in_some_attempt(3, || async {
        let mut late = Vec::new();
        for _ in 0..10 {
            let later = Instant::now() + Duration::from_millis(4);
            let mut registered = pin!(sleep_until(later));
            assert!(poll_once_pinned(registered.as_mut()).is_pending());
            // Gives the thread time to sleep until `later`.
            std::thread::sleep(Duration::from_micros(300));

            late.push(woken_late_by(Instant::now() + Duration::from_micros(200)).await);
            timeout(PATIENCE, registered)
                .await
                .expect("the wake-up is lost");
        }
        let median = median(&mut late);
        if median < Duration::from_micros(500) {
            Ok(())
        } else {
            Err(format!("{late:?}"))
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn thousands_of_concurrent_sleepers_wake_on_time() {
    let _alone = ONE_AT_A_TIME.lock().await;
    const SLEEPERS: usize = 10_000;
    within_bounds_in_some_attempt(3, || async {
        // Deadlines are chosen once every task is running, so they all overlap.
        let start = Arc::new(tokio::sync::Barrier::new(SLEEPERS));
        let tasks: Vec<_> = (0..SLEEPERS)
            .map(|_| {
                let start = start.clone();
                tokio::spawn(async move {
                    start.wait().await;
                    let delay = Duration::from_micros(rand::random_range(0..20_000));
                    woken_late_by(Instant::now() + delay).await
                })
            })
            .collect();
        let mut late = Vec::with_capacity(SLEEPERS);
        for task in tasks {
            late.push(task.await.unwrap());
        }
        late.sort();
        let (median, max) = (late[SLEEPERS / 2], late[SLEEPERS - 1]);
        if median < Duration::from_micros(500) && max < Duration::from_millis(50) {
            Ok(())
        } else {
            Err(format!("median {median:?}, max {max:?}"))
        }
    })
    .await;
}
