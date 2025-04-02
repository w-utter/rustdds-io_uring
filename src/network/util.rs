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

/// Inner implementation of [`get_local_multicast_ip_addrs`], for testing purposes.
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
