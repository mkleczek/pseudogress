//! Sleeping with microsecond precision. tokio's timer has millisecond
//! granularity, while the delays pseudogress simulates are often well under a
//! millisecond.
//!
//! A sleeper first waits in tokio's timer, which is cheap, until shortly
//! before its deadline. Only then does it join the one timer thread. That
//! keeps the thread's heap small, however long the delays are, and drops the
//! entries of cancelled sleeps soon.
//!
//! The thread blocks on an OS timer that always holds the earliest deadline in
//! the heap. Parking until a deadline is too coarse (macOS stretches it by up
//! to half) and spinning for its last stretch keeps a core busy whenever a
//! deadline is near, which with delays this short is nearly always. A thread
//! blocked on an OS timer uses no CPU, and the kernel wakes it when the timer
//! expires.
//!
//! The heap's lock is held only to change the heap, never for setting the OS
//! timer: with sleepers registering hundreds of thousands of times a second,
//! a system call under the lock makes them queue on it.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{
        Mutex, OnceLock,
        atomic::{
            AtomicU64,
            Ordering::{AcqRel, Acquire, Release},
        },
    },
    thread,
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

// An `OsTimer` is armed by `set` for an instant, or disarmed by `None`, and
// `wait` blocks until the instant it was last set to has passed. `set` can
// be called from any thread while another is in `wait`, which then waits for
// the new instant. A `wait` may return with nothing due, so callers check.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::OsTimer;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos::OsTimer;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("the timer needs an OS timer, which is implemented for Linux and macOS only");

#[cfg(test)]
mod tests;

/// How long before a deadline tokio's timer wakes a sleeper. It rounds up to
/// a millisecond and parks in timeouts that the OS may stretch: macOS fires
/// up to 3 ms late.
const COARSE_MARGIN: Duration = Duration::from_millis(5);

/// Completes at `deadline`, or at once if it has passed.
pub async fn sleep_until(deadline: Instant) {
    if deadline > Instant::now() + COARSE_MARGIN {
        tokio::time::sleep_until((deadline - COARSE_MARGIN).into()).await;
    }
    if deadline <= Instant::now() {
        return;
    }
    let (wake, woken) = oneshot::channel();
    Timer::get().register(deadline, wake);
    // The timer thread never drops a sender without sending.
    let _ = woken.await;
}

struct Timer {
    origin: Instant,
    os: OsTimer,
    /// The deadline that `os` is set to, or is about to be set to by the
    /// thread that recorded it: nanoseconds since `origin`, `NONE` for none.
    /// It is recorded before the OS timer is set, so that whoever lowers it
    /// knows that it has to set the timer.
    armed: AtomicU64,
    heap: Mutex<BinaryHeap<Sleeper>>,
}

const NONE: u64 = u64::MAX;

/// Ordered by deadline, earliest first.
struct Sleeper {
    deadline: Instant,
    wake: oneshot::Sender<()>,
}

impl Timer {
    /// Starts the thread on first use.
    fn get() -> &'static Timer {
        static TIMER: OnceLock<Timer> = OnceLock::new();
        TIMER.get_or_init(|| {
            // The thread's own `get` waits until this initialisation is done,
            // and would run it again if it panicked, so nothing after the
            // spawn may.
            let os = OsTimer::new().expect("the OS timer can be created");
            thread::Builder::new()
                .name("pseudogress-timer".to_owned())
                .spawn(|| Timer::get().run())
                .expect("the timer thread can be spawned");
            Timer {
                origin: Instant::now(),
                os,
                armed: AtomicU64::new(NONE),
                heap: Mutex::default(),
            }
        })
    }

    fn nanos(&self, deadline: Option<Instant>) -> u64 {
        deadline.map_or(NONE, |deadline| {
            deadline.saturating_duration_since(self.origin).as_nanos() as u64
        })
    }

    fn register(&self, deadline: Instant, wake: oneshot::Sender<()>) {
        self.heap.lock().unwrap().push(Sleeper { deadline, wake });
        // Only an earlier deadline changes what the thread waits for.
        let nanos = self.nanos(Some(deadline));
        if self.armed.fetch_min(nanos, AcqRel) > nanos {
            self.set(nanos);
        }
    }

    /// Sets the OS timer to `nanos`, then to any earlier deadline recorded
    /// meanwhile: the thread that recorded it may have set the timer before
    /// this one did, and been overwritten.
    fn set(&self, mut nanos: u64) {
        loop {
            let deadline = (nanos != NONE).then(|| self.origin + Duration::from_nanos(nanos));
            self.os.set(deadline);
            let armed = self.armed.load(Acquire);
            if armed >= nanos {
                return;
            }
            nanos = armed;
        }
    }

    fn run(&self) -> ! {
        let mut due = Vec::new();
        loop {
            self.os.wait();
            let next = {
                let mut heap = self.heap.lock().unwrap();
                let now = Instant::now();
                while heap.peek().is_some_and(|sleeper| sleeper.deadline <= now) {
                    due.extend(heap.pop().map(|sleeper| sleeper.wake));
                }
                // Under the lock, so that a sleeper that is not in the heap
                // yet records its deadline after this.
                let next = self.nanos(heap.peek().map(|sleeper| sleeper.deadline));
                self.armed.store(next, Release);
                next
            };
            // Also when nothing was due, as the deadline may have moved or the
            // wake-up been spurious.
            self.set(next);
            // Waking takes a while, and registrations must not wait for it.
            for wake in due.drain(..) {
                let _ = wake.send(());
            }
        }
    }
}

impl PartialEq for Sleeper {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Eq for Sleeper {}

impl PartialOrd for Sleeper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Sleeper {
    /// Reversed, as `BinaryHeap` is a max-heap.
    fn cmp(&self, other: &Self) -> Ordering {
        other.deadline.cmp(&self.deadline)
    }
}
