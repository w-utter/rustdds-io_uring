// TODO: this needs to be moved to multishot timeouts
//  - similar to what was used for ecat

pub struct Timer<T, S> {
  state: S,
  t: T,
}

pub mod timer_state {
  pub struct Uninit {
    pub(crate) duration: core::time::Duration,
    pub(crate) multishot: bool,
  }

  pub struct Init;
}

use core::time::Duration;

use timer_state::*;
use encoding::user_data;

use crate::io_uring::encoding;

impl<T> Timer<T, Uninit> {
  pub(crate) fn new(t: T, duration: Duration) -> Self {
    Self::new_(t, duration, false)
  }

  pub(crate) fn new_(t: T, duration: Duration, multishot: bool) -> Self {
    let state = Uninit {
      duration,
      multishot,
    };

    Self { state, t }
  }

  pub(crate) fn new_periodic(t: T, duration: Duration) -> Self {
    Self::new_(t, duration, true)
  }

  pub(crate) fn register(
    self,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    kind: user_data::Timer,
    user: u8,
  ) -> std::io::Result<Timer<T, Init>> {
    let Self {
      state: Uninit {
        duration,
        multishot,
      },
      t,
    } = self;

    let timespec = io_uring::types::Timespec::new()
      .sec(duration.as_secs())
      .nsec(duration.subsec_nanos());

    let timeout = io_uring::opcode::Timeout::new(&timespec);

    use io_uring::types::TimeoutFlags;
    let timeout = if multishot {
      timeout.flags(TimeoutFlags::MULTISHOT)
    } else {
      timeout
    };

    let timeout = timeout
      .build()
      .user_data(encoding::UserData::new(domain_id, user_data::Variant::Timer(kind), user).into());

    /*
    let flags = flags.unwrap_or(SetTimeFlags::Default);

    let mut tfd = TimerFd::new()?;

    tfd.set_state(state.clone(), flags);

    use std::os::fd::AsRawFd;

    use io_uring::{opcode, types::Fd};
    let poll = opcode::PollAdd::new(Fd(tfd.as_raw_fd()), libc::POLLIN as _)
      .multi(matches!(state, TimerState::Periodic { .. }))
      .build()
      .user_data(encoding::UserData::new(domain_id, user_data::Variant::Timer(kind)).into());
      */

    unsafe {
      ring
        .submission()
        .push(&timeout)
        .expect("could not push sqe entry");
    }

    ring.submit()?;

    Ok(Timer { state: Init, t })
  }
}

impl<T> Timer<T, Init> {
  pub(crate) fn reset(&mut self) {
    //self.state.prev_state = self.state.tfd.get_state();
  }

  pub(crate) fn take_inner(self) -> T {
    self.t
  }

  pub(crate) fn new_immediate_oneshot(
    t: T,
    duration: Duration,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    kind: user_data::Timer,
    user: u8,
  ) -> std::io::Result<Self> {
    let timer = Timer::new(t, duration);
    timer.register(ring, domain_id, kind, user)
  }
}
