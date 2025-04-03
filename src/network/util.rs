use std::{
  io,
  net::{IpAddr, SocketAddr},
};

use log::error;
use pnet::datalink::NetworkInterface;

use crate::structure::locator::Locator;

pub fn get_local_multicast_locators(port: u16) -> Vec<Locator> {
  let saddr = SocketAddr::new("239.255.0.1".parse().unwrap(), port);
  vec![Locator::from(saddr)]
}

pub fn get_local_unicast_locators(port: u16) -> Vec<Locator> {
  match if_addrs::get_if_addrs() {
    Ok(ifaces) => ifaces
      .iter()
      .filter(|ip| !ip.is_loopback())
      .map(|ip| Locator::from(SocketAddr::new(ip.ip(), port)))
      .collect(),
    Err(e) => {
      error!(
        "Cannot get local network interfaces: get_if_addrs() : {:?}",
        e
      );
      vec![]
    }
  }
}

/// Enumerates local interfaces that we may use for multicasting.
///
/// The result of this function is used to set up senders and listeners.
pub fn get_local_multicast_ip_addrs() -> io::Result<Vec<IpAddr>> {
  // grab a list of system intefaces.
  //
  // note: `pnet` works on mac, windows, and linux, potentially alongside
  // other systems.
  let interfaces = pnet::datalink::interfaces();

  // grab all the ips from each interface
  Ok(get_local_multicast_ip_addrs_inner(interfaces))
}

/// Inner implementation of [`get_local_multicast_ip_addrs`], for testing
/// purposes.
fn get_local_multicast_ip_addrs_inner(interfaces: Vec<NetworkInterface>) -> Vec<IpAddr> {
  interfaces
    .into_iter()
    .filter(|ifaddr| !ifaddr.is_loopback()) // don't use lo interface
    .filter(|ifaddr| ifaddr.is_multicast()) // require support for multicast
    .flat_map(|ifaddr| ifaddr.ips)
    .map(|ip_net| ip_net.ip()) // get the ip from each ip network
    .filter(|ip| ip.is_ipv4()) // use ipv4 only :(
    .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
  use std::{
    ffi::c_int,
    net::{Ipv4Addr, Ipv6Addr},
  };

  use pnet::{
    datalink::{InterfaceType, NetworkInterface},
    ipnetwork::{IpNetwork, Ipv4Network, Ipv6Network},
  };

  /// Mocks the `get_local_multicast_ip_addrs` function.
  #[test]
  fn test_get_local_multicast_ip_addrs() {
    let eth0 = interface(
      "eth0",
      1,
      &[
        IpNetwork::V4(Ipv4Network::new(Ipv4Addr::new(192, 168, 0, 137), 24).unwrap()),
        IpNetwork::V6(
          Ipv6Network::new(Ipv6Addr::new(0xfd73, 0x40a2, 0x1c3e, 0, 0, 0, 0, 0), 64).unwrap(),
        ),
      ],
      &[pnet_sys::IFF_MULTICAST],
    );

    let interfaces = vec![loopback(), eth0];

    let ips = super::get_local_multicast_ip_addrs_inner(interfaces);

    // TODO: uncomment if IPv6 becomes supported :(
    // assert_eq!(ips.len(), 2, "should only contain the non-loopback iface");
    assert_eq!(ips.len(), 1, "should only contain the non-loopback iface");
    assert!(ips.contains(&Ipv4Addr::new(192, 168, 0, 137).into()));

    // TODO: uncomment if ipv6
    // assert!(ips.contains(&Ipv6Addr::new(0xfd73, 0x40a2, 0x1c3e, 0, 0, 0, 0,
    // 0).into()))
  }

  /// Tries a number of interfaces, none of which support multicast.
  ///
  /// This should result in an empty list of IP addresses.
  #[test]
  fn no_multicast() {
    let mut interfaces = Vec::new();

    for index in 0..10 {
      interfaces.push(interface(
        &format!("eth{}", index),
        index,
        &[IpNetwork::V4(
          Ipv4Network::new(Ipv4Addr::new(192, 168, 0, rand::random()), 24).unwrap(),
        )],
        &[],
      ));
    }

    let ips = super::get_local_multicast_ip_addrs_inner(interfaces);

    assert!(
      ips.is_empty(),
      "we only want interfaces w/ multicast support"
    );
  }

  #[test]
  fn empty_interfaces() {
    let ips = super::get_local_multicast_ip_addrs_inner(Vec::new());
    assert!(
      ips.is_empty(),
      "blank iface list should result in empty list of ips"
    );
  }

  fn loopback() -> NetworkInterface {
    NetworkInterface {
      name: "lo".to_string(),
      description: "loopback".to_string(),
      index: 0,
      mac: None,
      ips: vec![
        IpNetwork::V4(Ipv4Network::new(Ipv4Addr::LOCALHOST, 24).unwrap()),
        IpNetwork::V6(Ipv6Network::new(Ipv6Addr::LOCALHOST, 64).unwrap()),
      ],
      flags: pnet_sys::IFF_MULTICAST as InterfaceType & pnet_sys::IFF_LOOPBACK as InterfaceType,
    }
  }

  fn interface(
    name: impl AsRef<str>,
    index: u32,
    ips: &[IpNetwork],
    flags: &[c_int],
  ) -> NetworkInterface {
    NetworkInterface {
      name: name.as_ref().into(),
      description: String::new(),
      index,
      mac: None,
      ips: ips.to_vec(),
      flags: flags
        .iter()
        .fold(0, |acc, &flag| acc | flag as InterfaceType),
    }
  }
}
