use std::net::UdpSocket;
use std::net::SocketAddr;

use crate::structure::locator::Locator;

pub struct UDPSender {
  unicast_socket: UdpSocket,
  multicast_sockets: Vec<UdpSocket>,
}

impl UDPSender {
  pub fn new(sender_port: u16) -> std::io::Result<Self> {
    use std::net::IpAddr;

    let unicast_socket = {
      use std::net::Ipv6Addr;
      let sock_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), sender_port);
      let socket = UdpSocket::bind(sock_addr)?;
      socket.set_multicast_loop_v4(true)?;
      socket
    };

    let mut multicast_sockets = Vec::with_capacity(1);

    for multicast_if_ipaddr in crate::network::util::get_local_multicast_ip_addrs()? {
      use socket2::{Socket, Domain, Type, Protocol, SockAddr};
      let mc_socket = match multicast_if_ipaddr {
        IpAddr::V4(v4) => {
          let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
          socket.set_multicast_if_v4(&v4)?;

          if cfg!(windows) {
            socket.set_reuse_address(true)?;
          }

          socket.bind(&SockAddr::from(SocketAddr::new(multicast_if_ipaddr, 0)))?;

          let mc_socket = UdpSocket::from(socket);
          mc_socket.set_multicast_loop_v4(true)?;
          mc_socket
        }
        IpAddr::V6(_v6) => {
          let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

          socket.bind(&SockAddr::from(SocketAddr::new(multicast_if_ipaddr, 0)))?;

          let mc_socket = UdpSocket::from(socket);
          mc_socket.set_multicast_loop_v6(true)?;

          mc_socket
        }
      };
      multicast_sockets.push(mc_socket);
    }

    Ok(Self {
      unicast_socket,
      multicast_sockets,
    })
  }

  #[cfg(test)]
  pub fn new_with_random_port() -> std::io::Result<Self> {
    Self::new(0)
  }

  unsafe fn queue_multicast(
    &self,
    buf: &[u8],
    addr: SocketAddr,
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    use socket2::SockAddr;
    let raw_addr: SockAddr = addr.into();
    let addr_ptr = raw_addr.as_ptr();
    let addr_len = raw_addr.len();

    use io_uring::{opcode, types::Fd};
    use std::os::fd::AsRawFd;
    for mc_sock in &self.multicast_sockets {
      let send = opcode::Send::new(Fd(mc_sock.as_raw_fd()), buf.as_ptr(), buf.len() as _)
        .dest_addr(addr_ptr)
        .dest_addr_len(addr_len)
        .build();

      unsafe {
        ring.submission().push(&send).unwrap();
      }
    }
    ring.submitter().submit()?;
    Ok(())
  }

  unsafe fn queue_unicast(
    &self,
    buf: &[u8],
    addr: SocketAddr,
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    use socket2::SockAddr;
    let raw_addr: SockAddr = addr.into();
    let addr_ptr = raw_addr.as_ptr();
    let addr_len = raw_addr.len();

    let uni_sock = &self.unicast_socket;

    use io_uring::{opcode, types::Fd};
    use std::os::fd::AsRawFd;

    let send = opcode::Send::new(Fd(uni_sock.as_raw_fd()), buf.as_ptr(), buf.len() as _)
      .dest_addr(addr_ptr)
      .dest_addr_len(addr_len)
      .build();

    unsafe {
      ring.submission().push(&send).unwrap();
    }
    ring.submitter().submit()?;
    Ok(())
  }

  #[cfg(test)]
  fn queue_all(
    &self,
    buf: &[u8],
    addrs: &[SocketAddr],
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    for &addr in addrs {
      unsafe {
        self.queue_unicast(buf, addr.clone(), ring)?;
        self.queue_multicast(buf, addr, ring)?;
      }
    }
    Ok(())
  }

  pub(crate) fn send_to_locator_list(
    &self,
    buf: &[u8],
    locators: &[Locator],
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    for loc in locators {
      self.send_to_locator(buf, loc, ring)?;
    }
    Ok(())
  }

  pub(crate) fn send_to_locator(
    &self,
    buf: &[u8],
    locator: &Locator,
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    match locator {
      Locator::UdpV4(addr) => self.send_to_socket(buf, SocketAddr::from(*addr), ring),
      Locator::UdpV6(addr) => self.send_to_socket(buf, SocketAddr::from(*addr), ring),
      _ => todo!(),
    }
  }

  fn send_to_socket(
    &self,
    buf: &[u8],
    addr: SocketAddr,
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    if addr.ip().is_multicast() {
      unsafe { self.queue_multicast(buf, addr, ring) }
    } else {
      unsafe { self.queue_unicast(buf, addr, ring) }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::network::udp_listener::*;

  #[test]
  fn udps_single_send() {
    let listener = UDPListener::new_unicast("127.0.0.1", 10201).unwrap();
    let sender = UDPSender::new(11201).expect("failed to create UDPSender");

    let mut ring = io_uring::IoUring::new(16).expect("failed to create io uring");

    let data: Vec<u8> = vec![0, 1, 2, 3, 4];

    let addrs = vec![SocketAddr::new("127.0.0.1".parse().unwrap(), 10201)];

    sender
      .queue_all(&data, &addrs, &mut ring)
      .expect("could not queue send");

    let rec_data = listener.get_message();

    assert_eq!(rec_data.len(), 5);
    assert_eq!(rec_data, data);
  }

  #[test]
  fn udps_multi_send() {
    let listener_1 = UDPListener::new_unicast("127.0.0.1", 10301).unwrap();
    let listener_2 = UDPListener::new_unicast("127.0.0.1", 10302).unwrap();
    let sender = UDPSender::new(11301).expect("failed to create UDPSender");

    let mut ring = io_uring::IoUring::new(16).expect("failed to create io uring");

    let data: Vec<u8> = vec![5, 4, 3, 2, 1, 0];

    let addrs = vec![
      SocketAddr::new("127.0.0.1".parse().unwrap(), 10301),
      SocketAddr::new("127.0.0.1".parse().unwrap(), 10302),
    ];

    sender
      .queue_all(&data, &addrs, &mut ring)
      .expect("could not queue send");

    let rec_data_1 = listener_1.get_message();
    let rec_data_2 = listener_2.get_message();

    assert_eq!(rec_data_1.len(), 6);
    assert_eq!(rec_data_1, data);
    assert_eq!(rec_data_2.len(), 6);
    assert_eq!(rec_data_2, data);
  }
}
