//! A `kqueue` holding one timer event. `NOTE_CRITICAL` keeps the kernel from
//! coalescing it with other timers, which would fire it late.

use std::{
    io::{self, Error},
    mem::MaybeUninit,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr,
    time::Instant,
};

pub(super) struct OsTimer(OwnedFd);

impl OsTimer {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: `kqueue` takes no arguments.
        let fd = unsafe { libc::kqueue() };
        if fd < 0 {
            return Err(Error::last_os_error());
        }
        // SAFETY: `fd` is open and nothing else owns it.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: `fcntl` with `F_SETFD` takes plain integers only.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(Error::last_os_error());
        }
        Ok(Self(fd))
    }

    pub(super) fn set(&self, deadline: Option<Instant>) {
        let (flags, data) = match deadline {
            // Adding an event that exists replaces it. At least 1 ns, as 0
            // is no timer.
            Some(deadline) => {
                let left = deadline.saturating_duration_since(Instant::now());
                (
                    libc::EV_ADD | libc::EV_ONESHOT,
                    isize::try_from(left.as_nanos().max(1)).unwrap_or(isize::MAX),
                )
            }
            None => (libc::EV_DELETE, 0),
        };
        let change = libc::kevent {
            ident: 0,
            filter: libc::EVFILT_TIMER,
            flags,
            fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
            data,
            udata: ptr::null_mut(),
        };
        // SAFETY: `change` is valid for the call, and with no room for events
        // an error comes back as the result.
        let result = unsafe {
            libc::kevent(
                self.0.as_raw_fd(),
                &change,
                1,
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        };
        if result < 0 {
            let error = Error::last_os_error();
            // A one-shot timer that has fired is gone already.
            assert!(
                flags == libc::EV_DELETE && error.raw_os_error() == Some(libc::ENOENT),
                "kevent: {error}"
            );
        }
    }

    pub(super) fn wait(&self) {
        let mut event = MaybeUninit::<libc::kevent>::uninit();
        loop {
            // SAFETY: `event` has room for the one event asked for, and a null
            // timeout blocks until there is one.
            let result = unsafe {
                libc::kevent(
                    self.0.as_raw_fd(),
                    ptr::null(),
                    0,
                    event.as_mut_ptr(),
                    1,
                    ptr::null(),
                )
            };
            if result >= 0 {
                return;
            }
            let error = Error::last_os_error();
            assert!(
                error.kind() == io::ErrorKind::Interrupted,
                "kevent: {error}"
            );
        }
    }
}
