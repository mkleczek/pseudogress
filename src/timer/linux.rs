//! A `timerfd`: a file descriptor that becomes readable when its timer
//! expires. It counts `CLOCK_MONOTONIC`, which is what Rust's `Instant` is on
//! Linux.

use std::{
    fs::File,
    io::{self, Read},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr,
    time::{Duration, Instant},
};

pub(super) struct OsTimer(File);

impl OsTimer {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: `timerfd_create` takes plain integers only.
        let fd = unsafe { libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is open and nothing else owns it.
        Ok(Self(File::from(unsafe { OwnedFd::from_raw_fd(fd) })))
    }

    pub(super) fn set(&self, deadline: Option<Instant>) {
        let left = match deadline {
            // All zeros would disarm the timer, so a deadline that has passed
            // is set to the nearest instant there is.
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_nanos(1)),
            None => Duration::ZERO,
        };
        let spec = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: libc::time_t::try_from(left.as_secs()).unwrap_or(libc::time_t::MAX),
                tv_nsec: left.subsec_nanos() as _,
            },
        };
        // SAFETY: `spec` is valid for the call, and a null pointer says that
        // the previous setting is not wanted.
        let result =
            unsafe { libc::timerfd_settime(self.0.as_raw_fd(), 0, &spec, ptr::null_mut()) };
        assert_eq!(result, 0, "timerfd_settime: {}", io::Error::last_os_error());
    }

    pub(super) fn wait(&self) {
        // The count of expirations is of no use. `read_exact` retries when a
        // signal interrupts the read.
        (&self.0)
            .read_exact(&mut [0; 8])
            .expect("the timerfd can be read");
    }
}
