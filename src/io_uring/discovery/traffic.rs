use std::net::{IpAddr, Ipv4Addr};

use io_uring_buf_ring::{buf_ring_state, BufferId};
use log::{info, warn};

use crate::{
  create_error_out_of_resources,
  dds::result::{CreateError, CreateResult},
  io_uring::{encoding::user_data::UdpDataRecv, network::udp_listener::UDPListener},
  network::constant as network,
};

// section 9.6.1.1 discovery traffic
pub struct UdpListeners<S> {
  multicast_discovery: Option<UDPListener<S>>,
  unicast_discovery: UDPListener<S>,
  pub(crate) multicast_user_traffic: Option<UDPListener<S>>,
  pub(crate) unicast_user_traffic: UDPListener<S>,
}

impl UdpListeners<buf_ring_state::Uninit> {
  // returns the participant id
  pub fn try_new(domain_id: u16) -> CreateResult<(Self, u16)> {
    const UNSPECIFIED_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

    let multicast_discovery = match UDPListener::new_multicast(
      UNSPECIFIED_ADDR,
      network::spdp_well_known_multicast_port(domain_id),
      Ipv4Addr::new(239, 255, 0, 1),
    ) {
      Ok(l) => Some(l),
      Err(e) => {
        warn!("Cannot get multicast discovery listener: {e:?}");
        None
      }
    };

    let mut participant_id = 0;

    let mut discovery_listener = None;

    // Magic value 120 below is from RTPS spec 2.5 Section "9.6.2.3 Default Port
    // Numbers"
    while discovery_listener.is_none() && participant_id < 120 {
      discovery_listener = UDPListener::new_unicast(
        UNSPECIFIED_ADDR,
        network::spdp_well_known_unicast_port(domain_id, participant_id),
      )
      .ok();
      if discovery_listener.is_none() {
        participant_id += 1;
      }
    }

    info!("ParticipantId {} selected.", participant_id);

    // here discovery_listener is redefined (shadowed)
    let unicast_discovery = match discovery_listener {
      Some(dl) => dl,
      None => return create_error_out_of_resources!("Could not find free ParticipantId"),
    };

    // Now the user traffic listeners

    let multicast_user_traffic = match UDPListener::new_multicast(
      UNSPECIFIED_ADDR,
      network::user_traffic_multicast_port(domain_id),
      Ipv4Addr::new(239, 255, 0, 1),
    ) {
      Ok(l) => Some(l),
      Err(e) => {
        warn!("Cannot get multicast user traffic listener: {e:?}");
        None
      }
    };

    let unicast_user_traffic = UDPListener::new_unicast(
      UNSPECIFIED_ADDR,
      network::user_traffic_unicast_port(domain_id, participant_id),
    )
    .or_else(|e| {
      use std::io::ErrorKind;
      if matches!(e.kind(), ErrorKind::AddrInUse) {
        // If we do not get the preferred listening port,
        // try again, with "any" port number.
        UDPListener::new_unicast(UNSPECIFIED_ADDR, 0).or_else(|e| {
          create_error_out_of_resources!(
            "Could not open unicast user traffic listener, any port number: {:?}",
            e
          )
        })
      } else {
        create_error_out_of_resources!("Could not open unicast user traffic listener: {e:?}")
      }
    })?;

    Ok((
      Self {
        multicast_discovery,
        unicast_discovery,
        multicast_user_traffic,
        unicast_user_traffic,
      },
      participant_id,
    ))
  }

  pub fn register(
    self,
    ring: &mut io_uring::IoUring,
    buf_id: &mut u16,
    domain_id: u16,
    user: u8,
  ) -> std::io::Result<UdpListeners<buf_ring_state::Init>> {
    let Self {
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
    } = self;

    let multicast_discovery = multicast_discovery
      .map(|disc| {
        disc.register(
          ring,
          buf_id,
          domain_id,
          UdpDataRecv::MulticastDiscovery,
          user,
        )
      })
      .transpose()?;

    let unicast_discovery =
      unicast_discovery.register(ring, buf_id, domain_id, UdpDataRecv::UnicastDiscovery, user)?;

    let multicast_user_traffic = multicast_user_traffic
      .map(|disc| {
        disc.register(
          ring,
          buf_id,
          domain_id,
          UdpDataRecv::MulticastUserTraffic,
          user,
        )
      })
      .transpose()?;

    let unicast_user_traffic = unicast_user_traffic.register(
      ring,
      buf_id,
      domain_id,
      UdpDataRecv::UnicastUserTraffic,
      user,
    )?;

    Ok(UdpListeners {
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
    })
  }
}

impl UdpListeners<buf_ring_state::Init> {
  pub fn buffer_from_cqe<'a, 'b>(
    &'a mut self,
    kind: UdpDataRecv,
    entry: &'b io_uring::cqueue::Entry,
  ) -> std::io::Result<Option<BufferId<'a, 'b, io_uring::cqueue::Entry>>> {
    let buffer = match kind {
      UdpDataRecv::UnicastUserTraffic => &mut self.unicast_user_traffic,
      UdpDataRecv::MulticastUserTraffic => {
        let Some(traffic) = self.multicast_user_traffic.as_mut() else {
          return Ok(None);
        };
        traffic
      }
      UdpDataRecv::MulticastDiscovery => {
        let Some(discovery) = self.multicast_discovery.as_mut() else {
          return Ok(None);
        };
        discovery
      }
      UdpDataRecv::UnicastDiscovery => &mut self.unicast_discovery,
    };
    buffer.buf_ring().buffer_id_from_cqe(entry)
  }

  pub(crate) fn self_locators(&self) -> TrafficLocators {
    let DiscoveryTrafficLocators {
      multicast_discovery,
      unicast_discovery,
    } = self.discovery_traffic_locators();

    let UserTrafficLocators {
      multicast_user_traffic,
      unicast_user_traffic,
    } = self.user_traffic_locators();

    TrafficLocators {
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
    }
  }

  pub(crate) fn user_traffic_locators(&self) -> UserTrafficLocators {
    let multicast_user_traffic = self
      .multicast_user_traffic
      .as_ref()
      .map(|l| l.to_locator_address().ok())
      .flatten()
      .unwrap_or_default();
    let unicast_user_traffic = self
      .unicast_user_traffic
      .to_locator_address()
      .unwrap_or_default();

    UserTrafficLocators {
      multicast_user_traffic,
      unicast_user_traffic,
    }
  }

  pub(crate) fn discovery_traffic_locators(&self) -> DiscoveryTrafficLocators {
    let multicast_discovery = self
      .multicast_discovery
      .as_ref()
      .map(|l| l.to_locator_address().ok())
      .flatten()
      .unwrap_or_default();
    let unicast_discovery = self
      .unicast_discovery
      .to_locator_address()
      .unwrap_or_default();

    DiscoveryTrafficLocators {
      multicast_discovery,
      unicast_discovery,
    }
  }

  pub(crate) fn try_fix_err(
    &mut self,
    err: Option<i32>,
    variant: UdpDataRecv,
    domain_id: u16,
    user: u8,
    ring: &mut io_uring::IoUring,
  ) -> std::io::Result<()> {
    if let Some(105) = err {
      match variant {
        UdpDataRecv::UnicastUserTraffic => self
          .unicast_user_traffic
          .setup_recv_multi(domain_id, variant, user, ring),
        UdpDataRecv::MulticastUserTraffic => {
          let Some(traffic) = self.multicast_user_traffic.as_mut() else {
            return Err(std::io::Error::new(
              std::io::ErrorKind::NotFound,
              "multicast user traffic not found",
            ));
          };
          traffic.setup_recv_multi(domain_id, variant, user, ring)
        }
        UdpDataRecv::MulticastDiscovery => {
          let Some(discovery) = self.multicast_discovery.as_mut() else {
            return Err(std::io::Error::new(
              std::io::ErrorKind::NotFound,
              "multicast discovery not found",
            ));
          };
          discovery.setup_recv_multi(domain_id, variant, user, ring)
        }
        UdpDataRecv::UnicastDiscovery => self
          .unicast_discovery
          .setup_recv_multi(domain_id, variant, user, ring),
      }
    } else {
      Err(std::io::Error::from_raw_os_error(err.unwrap()))
    }
  }
}

use crate::structure::locator::Locator;

pub struct TrafficLocators {
  pub multicast_discovery: Vec<Locator>,
  pub unicast_discovery: Vec<Locator>,
  pub multicast_user_traffic: Vec<Locator>,
  pub unicast_user_traffic: Vec<Locator>,
}

pub struct DiscoveryTrafficLocators {
  pub multicast_discovery: Vec<Locator>,
  pub unicast_discovery: Vec<Locator>,
}

pub struct UserTrafficLocators {
  pub multicast_user_traffic: Vec<Locator>,
  pub unicast_user_traffic: Vec<Locator>,
}
