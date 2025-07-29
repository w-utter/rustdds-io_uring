use timerfd::{TimerFd, TimerState, SetTimeFlags};
pub struct Timer<T, S> {
  state: S,
  t: T,
}

pub mod timer_state {
  use timerfd::{TimerFd, TimerState, SetTimeFlags};
  pub struct Uninit {
    pub(crate) state: TimerState,
    pub(crate) flags: Option<SetTimeFlags>,
  }

  pub struct Init {
    pub(crate) tfd: TimerFd,
    pub(super) prev_state: TimerState,
  }
}

use timer_state::*;
use core::time::Duration;

use crate::io_uring::encoding;
use encoding::user_data;

impl<T> Timer<T, Uninit> {
  pub(crate) fn new(t: T, state: TimerState) -> Self {
    let state = Uninit { state, flags: None };

    Self { state, t }
  }

  pub(crate) fn new_oneshot(t: T, duration: Duration) -> Self {
    Self::new(t, TimerState::Oneshot(duration))
  }

  pub(crate) fn new_periodic(t: T, duration: Duration) -> Self {
    let state = TimerState::Periodic {
      current: duration,
      interval: duration,
    };
    Self::new(t, state)
  }

  pub(crate) fn with_flags(mut self, flags: SetTimeFlags) -> Self {
    self.state.flags = Some(flags);
    self
  }

  pub(crate) fn register(
    self,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    kind: user_data::Timer,
  ) -> std::io::Result<Timer<T, Init>> {
    let Self {
      state: Uninit { state, flags },
      t,
    } = self;

    let flags = flags.unwrap_or(SetTimeFlags::Default);

    let mut tfd = TimerFd::new()?;

    tfd.set_state(state.clone(), flags);

    use std::os::fd::AsRawFd;

    use io_uring::{opcode, types::Fd};
    let poll = opcode::PollAdd::new(Fd(tfd.as_raw_fd()), libc::POLLIN as _)
      .multi(matches!(state, TimerState::Periodic { .. }))
      .build()
      .user_data(encoding::UserData::new(domain_id, user_data::Variant::Timer(kind)).into());

    unsafe {
      ring
        .submission()
        .push(&poll)
        .expect("could not push sqe entry");
    }

    ring.submit()?;

    Ok(Timer {
      state: Init {
        tfd,
        prev_state: state,
      },
      t,
    })
  }
}

impl<T> Timer<T, Init> {
  pub(crate) fn reset(&mut self) {
    self.state.prev_state = self.state.tfd.get_state();
  }

  pub(crate) fn take_inner(self) -> T {
    self.t
  }

  pub(crate) fn inner_mut(&mut self) -> &mut T {
    &mut self.t
  }

  pub(crate) fn try_update_duration(&mut self, duration: Duration, flags: Option<SetTimeFlags>) {
    match self.state.prev_state {
      TimerState::Periodic { interval, .. } if duration != interval => {
        let flags = flags.unwrap_or(SetTimeFlags::Default);
        self.state.prev_state = self.state.tfd.set_state(
          TimerState::Periodic {
            interval: duration,
            current: duration,
          },
          flags,
        );
      }
      _ => (),
    }
  }

  pub(crate) fn new_immediate(
    t: T,
    state: TimerState,
    flags: Option<SetTimeFlags>,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    kind: user_data::Timer,
  ) -> std::io::Result<Self> {
    let mut timer = Timer::new(t, state);
    timer.state.flags = flags;

    timer.register(ring, domain_id, kind)
  }

  pub(crate) fn new_immediate_periodic(
    t: T,
    duration: Duration,
    flags: Option<SetTimeFlags>,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    kind: user_data::Timer,
  ) -> std::io::Result<Self> {
    Self::new_immediate(
      t,
      TimerState::Periodic {
        current: duration,
        interval: duration,
      },
      flags,
      ring,
      domain_id,
      kind,
    )
  }

  pub(crate) fn new_immediate_oneshot(
    t: T,
    duration: Duration,
    flags: Option<SetTimeFlags>,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    kind: user_data::Timer,
  ) -> std::io::Result<Self> {
    Self::new_immediate(
      t,
      TimerState::Oneshot(duration),
      flags,
      ring,
      domain_id,
      kind,
    )
  }
}
