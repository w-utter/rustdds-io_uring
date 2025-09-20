use rustdds::{
  io_uring::{
    dds::DDSCache,
    discovery::{Discovery2, UdpListeners},
    encoding::UserData,
    participant::Participant,
    rtps::MessageReceiver,
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
  let mut ring = io_uring::IoUring::new(16).unwrap();
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

  let mut publisher = topic.publisher(&qos);
  let writer = publisher.create_datawriter_cdr::<HelloWorldData>(
    None,
    &mut discovery_db,
    &mut domain,
    &mut ring,
    &mut discovery,
    &udp_sender,
    USERDATA,
  );

  discovery.initialize(&mut discovery_db, &udp_sender, &mut ring, &domain);
  println!("\nstarting \n\n\n\n\n");

  let timespec = io_uring::types::Timespec::new().sec(1)/*.nsec(200000)*/;
  let timeout = io_uring::opcode::Timeout::new(&timespec)
    .flags(io_uring::types::TimeoutFlags::MULTISHOT)
    .build()
    .user_data(u64::MAX);

  unsafe {
    ring.submission().push(&timeout).unwrap();
  }
  ring.submit().unwrap();

  use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
  let mut init = AtomicBool::from(false);
  let mut count = AtomicI32::from(0);

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
            use rustdds::{io_uring::rtps::DataStatus, DataWriterStatus};
            match ev {
              DataStatus::Writer(status) => match status {
                DataWriterStatus::PublicationMatched { current, .. } => {
                  if current.count_change() > 0 {
                    println!("\n\npublication matched\n\n\n\n\n\n");
                  } else {
                    panic!("lost")
                  }

                  init.store(true, Ordering::SeqCst);
                }
                _ => (),
              },
              _ => (),
            }
          },
          |participant, dref| {},
          |topic, cache, dref| {},
          USERDATA,
        );
      } else if udata == 255 {
        if init.load(Ordering::Relaxed) {
          let user_id = count.fetch_add(1, Ordering::SeqCst);

          let msg = HelloWorldData {
            user_id,
            message: "Hello".to_string(),
          };
          println!("\n!!!sending\n");

          let data = writer.write(msg, None).unwrap();
          let mut dref = domain.domain_ref(&mut ring, &mut discovery, &udp_sender);

          if data.write_to(&mut dref) {
            println!("lost sub");
          }
        }
      } else {
        println!("{entry:?}");
      }
    }
  }
}
