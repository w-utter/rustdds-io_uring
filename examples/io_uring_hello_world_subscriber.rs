use rustdds::{
  io_uring::{
    dds::DDSCache,
    discovery::{Discovery2, UdpListeners},
    encoding::UserData,
    participant::Participant,
  },
  QosPolicies, TopicCache, TypeDesc, GUID,
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct HelloWorldData {
  pub user_id: i32,
  pub message: String,
}

use rustdds::io_uring::encoding;

impl rustdds::Keyed for HelloWorldData {
  type K = i32;
  fn key(&self) -> Self::K {
    self.user_id
  }
}

fn main() {
  let mut ring = io_uring::IoUring::new(64).unwrap();
  let mut buf_id = 0;

  let mut participant = Participant::new();
  let guid = participant.guid;
  let domain_id = 0;

  const USERDATA: u8 = 1;

  use io_uring::opcode;

  debug_assert!(
    {
      let mut probe = io_uring::register::Probe::new();
      ring
        .submitter()
        .register_probe(&mut probe)
        .expect("could not register io_uring probe");
      probe.is_supported(opcode::Read::CODE) && probe.is_supported(opcode::Write::CODE)
    },
    "io_uring does not support read and write opcodes"
  );

  use rustdds::io_uring::discovery::DiscoveryDB;
  let mut discovery_db = DiscoveryDB::new(guid);

  let mut discovery = Discovery2::new(None, guid.prefix, domain_id)
    .register(&mut ring, USERDATA)
    .unwrap();

  use rustdds::EntityId;
  let udp_sender = rustdds::io_uring::network::UDPSender::new(0).unwrap();

  use std::collections::HashMap;

  use rustdds::io_uring::rtps::Domain;

  let mut domain = Domain::new(domain_id, guid, HashMap::new())
    .unwrap()
    .register(&mut ring, &mut buf_id, USERDATA)
    .unwrap();

  use rustdds::{policy::Reliability, QosPolicyBuilder};

  let qos = QosPolicyBuilder::new()
    .reliability(Reliability::Reliable {
      max_blocking_time: rustdds::Duration::from_secs(1),
    })
    .build();

  let mut dds_cache = DDSCache::new();

  use rustdds::TopicKind;
  let mut topic = participant.create_topic(
    "HelloWorldData_Msg".into(),
    "HelloWorldData::Msg".into(),
    &qos,
    TopicKind::WithKey,
    &mut dds_cache,
  );

  let mut subscriber = topic.subscriber(&qos);
  let mut reader = subscriber
    .create_datareader_cdr::<HelloWorldData>(
      None,
      &mut dds_cache,
      &mut discovery_db,
      &mut domain,
      &mut ring,
      &mut discovery,
      &udp_sender,
      USERDATA,
    )
    .unwrap();

  discovery.initialize(&mut discovery_db, &udp_sender, &mut ring, &domain);
  println!("\nstarting \n\n\n\n\n");

  use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
  let mut init = AtomicBool::from(false);

  loop {
    let entry = ring.completion().next();
    if let Some(entry) = entry {
      let udata = (entry.user_data() >> encoding::USER_OFFSET) & 0xFF;
      if udata == 1 {
        domain.handle_event(
          entry,
          &mut ring,
          &mut discovery,
          &mut discovery_db,
          &udp_sender,
          &mut dds_cache,
          |ev, guid, mut dref| {
            use rustdds::{io_uring::rtps::DataStatus, DataReaderStatus};
            match ev {
              DataStatus::Reader(status) => match status {
                DataReaderStatus::SubscriptionMatched { .. } => {
                  init.store(true, Ordering::SeqCst);
                }
                _ => (),
              },
              _ => (),
            }
          },
          |participant, dref| (),
          |topic, cache, dref| {
            if init.load(Ordering::Relaxed) {
              match topic {
                "HelloWorldData_Msg" => {
                  use rustdds::ReadCondition;
                  for sample in reader
                    .read(usize::MAX, ReadCondition::not_read(), cache)
                    .unwrap()
                  {
                    println!("received: {:?}", sample.value());
                  }
                }
                t => println!("unknown topic: {t:?}"),
              }
            }
          },
          USERDATA,
        );
      } else {
        println!("{entry:?}");
      }
    }
  }
}
