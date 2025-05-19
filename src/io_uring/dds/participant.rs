// do basic participant
// then do discovery

use crate::dds::result::{CreateResult, CreateError};
use crate::{create_error_out_of_resources, create_error_poisoned};

use io_uring_buf_ring::buf_ring_state;
use crate::io_uring::network::udp_listener::UDPListener;
use crate::network::constant as network;
use std::net::{IpAddr, Ipv4Addr};

use log::{debug, error, info, trace, warn};

struct UdpListeners<S> {
    multicast_discovery: Option<UDPListener<S>>,
    unicast_discovery: UDPListener<S>,
    multicast_user_traffic: Option<UDPListener<S>>,
    unicast_user_traffic: UDPListener<S>,
}

impl UdpListeners<buf_ring_state::Uninit> {
    fn try_new(domain_id: u16) -> CreateResult<Self> {

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

        Ok(Self {
            multicast_discovery,
            unicast_discovery,
            multicast_user_traffic,
            unicast_user_traffic,
        })
    }

    pub fn register(self, ring: &mut io_uring::IoUring, buf_id: &mut u16, udata: u64) -> std::io::Result<UdpListeners<buf_ring_state::Init>> {
        let Self {
            multicast_discovery,
            unicast_discovery,
            multicast_user_traffic,
            unicast_user_traffic,
        } = self;

        let multicast_discovery = multicast_discovery.map(|disc| disc.register(ring, buf_id, udata)).transpose()?;

        let unicast_discovery = unicast_discovery.register(ring, buf_id, udata)?;

        let multicast_user_traffic = multicast_user_traffic.map(|disc| disc.register(ring, buf_id, udata)).transpose()?;

        let unicast_user_traffic = unicast_user_traffic.register(ring, buf_id, udata)?;

        Ok(UdpListeners {
            multicast_discovery,
            unicast_discovery,
            multicast_user_traffic,
            unicast_user_traffic,
        })
    }
}

