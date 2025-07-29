use std::net::UdpSocket;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use io_uring_buf_ring::{buf_ring_state, BufRing};
use crate::network;

const MAX_MESSAGE_SIZE: usize = 64 * 1024; // This is max we can get from UDP.
const MESSAGE_BUFFER_ALLOCATION_CHUNK: usize = 256 * 1024; // must be >= MAX_MESSAGE_SIZE
static_assertions::const_assert!(MESSAGE_BUFFER_ALLOCATION_CHUNK > MAX_MESSAGE_SIZE);

pub struct UDPListener<S> {
  socket: UdpSocket,
  buf: BufRing<S>,
  multicast_group: Option<Ipv4Addr>,
}

impl<S> Drop for UDPListener<S> {
  fn drop(&mut self) {
    if let Some(mcg) = &self.multicast_group {
      let _ = self.leave_multicast(mcg);
    }
  }
}

use crate::io_uring::encoding;

const BUFFER_ENTRIES: u16 = 16;

impl UDPListener<buf_ring_state::Uninit> {
  fn new_listening_socket(host: IpAddr, port: u16, reuse_addr: bool) -> std::io::Result<UdpSocket> {
    use socket2::{Socket, Domain, Type, Protocol, SockAddr};

    let domain = match &host {
      IpAddr::V6(_) => Domain::IPV6,
      IpAddr::V4(_) => Domain::IPV4,
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    if reuse_addr {
      socket.set_reuse_address(true)?;
    }

    #[cfg(not(any(target_os = "solaris", target_os = "illumos", windows)))]
    {
      if reuse_addr {
        socket.set_reuse_port(true)?;
      }
    }

    let addr = SocketAddr::new(host, port);
    socket.bind(&SockAddr::from(addr))?;

    let socket: UdpSocket = socket.into();

    socket.set_nonblocking(true)?;
    Ok(socket)
  }

  pub fn new_unicast(addr: IpAddr, port: u16) -> std::io::Result<Self> {
    let socket = Self::new_listening_socket(addr, port, false)?;
    const ENTRIES: u16 = 16;

    Ok(Self {
      socket,
      buf: BufRing::new(BUFFER_ENTRIES, MESSAGE_BUFFER_ALLOCATION_CHUNK as _, 0)?,
      multicast_group: None,
    })
  }

  pub fn new_multicast(
    addr: IpAddr,
    port: u16,
    multicast_group: Ipv4Addr,
  ) -> std::io::Result<Self> {
    if !multicast_group.is_multicast() {
      return std::io::Result::Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Not a multicast address",
      ));
    }

    let socket = Self::new_listening_socket(addr, port, true)?;

    for multicast_if_ipaddr in network::util::get_local_multicast_ip_addrs()? {
      match multicast_if_ipaddr {
        IpAddr::V4(v4) => {
          socket.join_multicast_v4(&multicast_group, &v4)?;
        }
        IpAddr::V6(v6) => {
          socket.join_multicast_v6(&v6, 0)?;
        }
      }
    }

    Ok(Self {
      socket,
      buf: BufRing::new(BUFFER_ENTRIES, MESSAGE_BUFFER_ALLOCATION_CHUNK as _, 0)?,
      multicast_group: Some(multicast_group),
    })
  }

  pub fn register(
    self,
    ring: &mut io_uring::IoUring,
    buf_id: &mut u16,
    domain_id: u16,
    kind: encoding::user_data::UdpDataRecv,
  ) -> std::io::Result<UDPListener<buf_ring_state::Init>> {
    use core::ptr;
    // SAFETY: were never using `self` here again
    let socket = unsafe { ptr::read(&self.socket) };
    let buf = unsafe { ptr::read(&self.buf) };
    let multicast_group = unsafe { ptr::read(&self.multicast_group) };
    core::mem::forget(self);

    let mut res = buf.register(&ring.submitter());

    while let Err((e, mut buf)) = res {
      if let std::io::ErrorKind::AlreadyExists = e.kind() {
        *buf_id += 1;
        buf.set_bgid(*buf_id);
        res = buf.register(&ring.submitter());
      } else {
        return Err(e);
      }
    }

    let buf = unsafe { res.unwrap_unchecked() };
    let buf = buf.init();

    use io_uring::{opcode, types::Fd};
    use std::os::fd::AsRawFd;

    let recv_multi = opcode::RecvMulti::new(Fd(socket.as_raw_fd()), buf.bgid())
      .build()
      .user_data(
        encoding::UserData::new(domain_id, encoding::user_data::Variant::DataRecv(kind)).into(),
      );

    unsafe {
      ring
        .submission()
        .push(&recv_multi)
        .expect("submission queue full");
    }
    ring.submit()?;

    Ok(UDPListener {
      socket,
      buf,
      multicast_group,
    })
  }
}

impl<S> UDPListener<S> {
  #[cfg(test)]
  pub fn port(&self) -> u16 {
    match self.socket.local_addr() {
      Ok(addr) => addr.port(),
      _ => 0,
    }
  }

  fn leave_multicast(&self, addr: &Ipv4Addr) -> std::io::Result<()> {
    if addr.is_multicast() {
      return self.socket.leave_multicast_v4(addr, &Ipv4Addr::UNSPECIFIED);
    }
    std::io::Result::Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      "Not a multicast address",
    ))
  }
}

use crate::structure::locator::Locator;

impl UDPListener<buf_ring_state::Init> {
  pub(crate) fn buf_ring(&mut self) -> &mut BufRing<buf_ring_state::Init> {
    &mut self.buf
  }

  pub fn to_locator_address(&self) -> std::io::Result<Vec<Locator>> {
    let local_port = self.socket.local_addr()?.port();

    Ok(if self.multicast_group.is_some() {
      network::util::get_local_multicast_locators(local_port)
    } else {
      network::util::get_local_unicast_locators(local_port)
    })
  }
}

#[cfg(test)]
mod tests {
  // use std::os::unix::io::AsRawFd;
  // use nix::sys::socket::setsockopt;
  // use nix::sys::socket::sockopt::IpMulticastLoop;
  use std::{thread, time};

  use super::*;
  use crate::network::udp_sender::*;

  use crate::io_uring::encoding::user_data::UdpDataRecv;
  const DUMMY_ID: UdpDataRecv = UdpDataRecv::MulticastDiscovery;

  #[test]
  fn udpl_single_address() {
    let mut ring = io_uring::IoUring::new(8).unwrap();
    let mut id = 1;

    let mut listener = UDPListener::new_unicast("127.0.0.1".parse().unwrap(), 10001)
      .unwrap()
      .register(&mut ring, &mut id, 0, DUMMY_ID)
      .unwrap();

    let sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");

    let data: Vec<u8> = vec![0, 1, 2, 3, 4];

    let addrs = vec![SocketAddr::new("127.0.0.1".parse().unwrap(), 10001)];
    sender.send_to_all(&data, &addrs);

    let mut count = 0;

    while let Some(cqe) = ring.completion().next() {
      assert_eq!(cqe.result(), 5);
      let buf = listener.buf_ring();
      let bid = buf.buffer_id_from_cqe(&cqe).unwrap().unwrap();
      let recv = bid.buffer();
      assert_eq!(&data, recv);
      count += 1;
    }
    assert_eq!(count, 1);
  }

  #[test]
  fn udpl_multicast_address() {
    let mut ring = io_uring::IoUring::new(8).unwrap();
    let mut id = 1;

    let mut listener = UDPListener::new_multicast(
      "0.0.0.0".parse().unwrap(),
      10003,
      Ipv4Addr::new(239, 255, 0, 1),
    )
    .unwrap()
    .register(&mut ring, &mut id, 0, DUMMY_ID)
    .unwrap();

    let sender = UDPSender::new_with_random_port().unwrap();

    // setsockopt(sender.socket.as_raw_fd(), IpMulticastLoop, &true)
    //  .expect("Unable set IpMulticastLoop option on socket");

    let data: Vec<u8> = vec![2, 4, 6];

    sender
      .send_multicast(&data, Ipv4Addr::new(239, 255, 0, 1), 10003)
      .expect("Failed to send multicast");

    let mut count = 0;

    while let Some(cqe) = ring.completion().next() {
      assert_eq!(cqe.result(), 3);
      let buf = listener.buf_ring();
      let bid = buf.buffer_id_from_cqe(&cqe).unwrap().unwrap();
      let recv = bid.buffer();
      assert_eq!(&data, recv);
      count += 1;
    }
    listener
      .leave_multicast(&Ipv4Addr::new(239, 255, 0, 1))
      .unwrap();
    assert_eq!(count, 1);
  }
}
