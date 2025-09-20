use std::collections::BTreeMap;

use crate::{
  io_uring::{
    dds::DDSCache,
    timer::{timer_state, Timer},
  },
  TopicCache,
};

struct Timers<T> {
  acknack: Timer<(), T>,
  cache_gc: Timer<(), T>,
}

impl Timers<timer_state::Uninit> {
  fn new() -> Self {
    let acknack = Timer::new_periodic((), PREEMPTIVE_ACKNACK_PERIOD);
    let cache_gc = Timer::new_periodic((), CACHE_CLEAN_PERIOD);

    Self { acknack, cache_gc }
  }

  fn register(
    self,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    user: u8,
  ) -> std::io::Result<Timers<timer_state::Init>> {
    let Self { acknack, cache_gc } = self;

    use crate::io_uring::encoding::user_data::{BuiltinTimerVariant, Timer};

    let acknack = acknack.register(
      ring,
      domain_id,
      Timer::Builtin(BuiltinTimerVariant::AckNack),
      user,
    )?;
    let cache_gc = cache_gc.register(
      ring,
      domain_id,
      Timer::Builtin(BuiltinTimerVariant::CacheCleaning),
      user,
    )?;

    Ok(Timers { acknack, cache_gc })
  }
}

use crate::io_uring::discovery::{traffic::UdpListeners, Discovery2, DiscoveryDB};

pub struct Domain<TS, BS> {
  pub(crate) domain_info: DomainInfo,
  //dds_cache: Arc<RwLock<DDSCache>>,
  //discovery_db: Arc<RwLock<DiscoveryDB>>,
  message_receiver: MessageReceiver, // This contains our Readers

  timers: Timers<TS>,
  pub(crate) listeners: UdpListeners<BS>,

  // If security is enabled, this contains the security plugins
  #[cfg(feature = "security")]
  security_plugins_opt: Option<SecurityPluginsHandle>,

  pub(crate) writers: HashMap<EntityId, Writer<TS>>,
}

use io_uring_buf_ring::buf_ring_state;

impl Domain<timer_state::Uninit, buf_ring_state::Uninit> {
  pub fn new(
    domain_id: u16,
    domain_participant_guid: GUID,
    //dds_cache: Arc<RwLock<DDSCache>>,
    //discovery_db: Arc<RwLock<DiscoveryDB>>,
    #[cfg(feature = "security")] security_plugins_opt: Option<SecurityPluginsHandle>,

    writers: HashMap<EntityId, Writer<timer_state::Uninit>>,
  ) -> crate::dds::CreateResult<Self> {
    let (listeners, participant_id) = UdpListeners::try_new(domain_id)?;
    let timers = Timers::new();

    let domain_info = DomainInfo {
      domain_id,
      participant_id,
      domain_participant_guid,
    };

    let message_receiver = MessageReceiver::new(
      domain_participant_guid.prefix,
      #[cfg(feature = "security")]
      security_plugins_opt.clone(),
      #[cfg(not(feature = "security"))]
      None,
    );

    Ok(Self {
      domain_info,
      //dds_cache,
      //discovery_db,
      message_receiver,

      timers,
      listeners,

      // If security is enabled, this contains the security plugins
      #[cfg(feature = "security")]
      security_plugins_opt,

      writers,
    })
  }

  pub fn register(
    self,
    ring: &mut io_uring::IoUring,
    id: &mut u16,
    user: u8,
  ) -> std::io::Result<Domain<timer_state::Init, buf_ring_state::Init>> {
    let Self {
      domain_info,
      message_receiver,

      timers,
      listeners,

      // If security is enabled, this contains the security plugins
      #[cfg(feature = "security")]
      security_plugins_opt,

      writers,
    } = self;

    let writers = writers
      .into_iter()
      .map(|(id, w)| Ok((id, w.register(ring, domain_info.domain_id, user)?)))
      .collect::<Result<_, std::io::Error>>()?;

    let listeners = listeners.register(ring, id, domain_info.domain_id, user)?;
    let timers = timers.register(ring, domain_info.domain_id, user)?;

    Ok(Domain {
      domain_info,
      //dds_cache,
      //discovery_db,
      message_receiver,

      timers,
      listeners,

      // If security is enabled, this contains the security plugins
      #[cfg(feature = "security")]
      security_plugins_opt,

      writers,
    })
  }
}

use crate::io_uring::rtps::PassedSubmessage;

impl Domain<timer_state::Init, buf_ring_state::Init> {
  pub fn handle_event(
    &mut self,
    entry: io_uring::cqueue::Entry,
    ring: &mut io_uring::IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    discovery_db: &mut DiscoveryDB,
    udp_sender: &UDPSender,
    dds_cache: &mut DDSCache,
    mut on_data_status: impl for<'a> FnMut(DataStatus, GUID, DomainRef<'a, 'a>),
    mut on_participant_status: impl for<'a> FnMut(DomainParticipantStatusEvent, DomainRef<'a, 'a>),
    mut on_read: impl for<'a> FnMut(&str, &mut TopicCache, DomainRef<'a, 'a>),
    user: u8,
  ) -> Option<()> {
    use crate::io_uring::encoding::{user_data::*, UserData};

    let res = entry.result();

    let userdata = UserData::try_from(entry.user_data()).ok();

    if res < 0 {
      println!("{}: {:?}", entry.result(), userdata);
    }

    let userdata = userdata?;

    match userdata.variant() {
      Variant::DataRecv(udp_variant) => {
        let buf_id = self.listeners.buffer_from_cqe(udp_variant, &entry);
        let buf_id = match buf_id {
          Err(ref e) => {
            let kind = e.raw_os_error();
            drop(buf_id);
            self
              .listeners
              .try_fix_err(kind, udp_variant, self.domain_info.domain_id, user, ring)
              .ok()?;
            return Some(());
          }
          Ok(o) => o?,
        };

        let buffer = buf_id.buffer();
        let bytes = bytes::Bytes::copy_from_slice(buffer);

        let state = self.message_receiver.clone_partial_message_receiver_state();
        drop(buf_id);

        let mut submsgs = self.message_receiver.handle_received_packet_2(&bytes)?;
        /*
        for submsg in submsgs {
            println!("{submsg:?}");
        }
        */

        // let f = |hb| hb.send_initial_cache_change(udp_sender, ring);
        // move this (&F: Fn(HbMsg) -> ()) to the

        while let Some(submsg) = submsgs.next() {
          let msg_recv = &mut *submsgs.msg_recv;

          match submsg {
            PassedSubmessage::Writer(msg) => {
              //println!("writer msg: {msg:?}");
              match discovery.handle_writer_msg(msg, &state, discovery_db, udp_sender, ring) {
                // builtin
                Ok(Some(mut changes)) => {
                  //let mut state = None;

                  let readers = &mut msg_recv.available_readers;

                  //let caches: &mut Caches<timer_state::Init> = unsafe {&mut*(&mut
                  // discovery.caches as *mut _)};

                  iterate_events(
                    discovery,
                    discovery_db,
                    readers,
                    &mut self.writers,
                    &mut changes,
                    ring,
                    udp_sender,
                    |ev, dref| match ev {
                      DomainStatusEvent::Participant(p) => on_participant_status(p, dref),
                      DomainStatusEvent::Data(d, guid) => {
                        if guid.entity_id.entity_kind.is_user_defined() {
                          on_data_status(d, guid, dref);
                        }
                      }
                      DomainStatusEvent::Mixed(p, (d, guid)) => {
                        let dref2 =
                          DomainRef::new(dref.writers, dref.ring, dref.discovery, dref.udp_sender);

                        on_participant_status(p, dref2);
                        if guid.entity_id.entity_kind.is_user_defined() {
                          on_data_status(d, guid, dref);
                        }
                      }
                    },
                  );
                }
                Ok(None) => return None,
                //not a builtin.
                Err(msg) => {
                  let recv_state = msg_recv.clone_partial_message_receiver_state();
                  use crate::messages::submessages::submessage::HasEntityIds;
                  let writer_entity_id = msg.sender_entity_id();
                  //let source_guid = GUID::new(recv_state.source_guid_prefix, writer_entity_id);

                  let target_readers = msg_recv
                    .available_readers
                    .values_mut()
                    .filter(|target_reader| target_reader.contains_writer(writer_entity_id));

                  use crate::messages::submessages::submessages::WriterSubmessage;
                  for reader in target_readers {
                    let topic_cache = dds_cache
                      .get_existing_topic_cache(&reader.topic_name)
                      .expect("no topic cache for reader?");

                    match &msg {
                      WriterSubmessage::Data(data, flags) => {
                        let changed = reader.handle_data_msg(
                          data.clone(),
                          flags.clone(),
                          &recv_state,
                          topic_cache,
                        );
                        if !changed {
                          continue;
                        }
                        let topic = &reader.topic_name;
                        on_read(
                          topic,
                          topic_cache,
                          DomainRef::new(&mut self.writers, ring, discovery, udp_sender),
                        );
                      }
                      WriterSubmessage::Heartbeat(heartbeat, flags) => {
                        use crate::messages::submessages::submessages::HEARTBEAT_Flags;
                        reader.handle_heartbeat_msg(
                          heartbeat,
                          flags.contains(HEARTBEAT_Flags::Final),
                          &recv_state,
                          udp_sender,
                          ring,
                          topic_cache,
                        );
                      }
                      WriterSubmessage::Gap(gap, _) => {
                        reader.handle_gap_msg(gap, &recv_state, topic_cache);
                      }
                      WriterSubmessage::DataFrag(datafrag, flags) => {
                        reader.handle_datafrag_msg(
                          datafrag,
                          flags.clone(),
                          &recv_state,
                          topic_cache,
                        );
                      }
                      WriterSubmessage::HeartbeatFrag(heartbeatfrag, _) => {
                        reader.handle_heartbeatfrag_msg(heartbeatfrag, &recv_state)
                      }
                    }
                  }
                }
              }
            }
            PassedSubmessage::Reader(m, source_guid_prefix) => {
              //println!("reader msg: {m:?}");
              match discovery.handle_reader_submsg(m, source_guid_prefix, ring, user) {
                // builtin
                Ok(_) => {
                  //println!("builtin reader");
                }
                // not a builtin
                Err((msg, prefix)) => {
                  use crate::messages::submessages::submessage::HasEntityIds;
                  let writer_entity_id = msg.receiver_entity_id();

                  if let Some(writer) = self.writers.get_mut(&writer_entity_id) {
                    use crate::messages::submessages::submessages::{
                      AckSubmessage, ReaderSubmessage,
                    };
                    let ack_submsg = match msg {
                      ReaderSubmessage::AckNack(acknack, _) => AckSubmessage::AckNack(acknack),
                      ReaderSubmessage::NackFrag(nack_frag, _) => {
                        AckSubmessage::NackFrag(nack_frag)
                      }
                    };

                    writer.handle_ack_nack(
                      prefix,
                      &ack_submsg,
                      ring,
                      udp_sender,
                      self.domain_info.domain_id,
                      user,
                    );
                  } else {
                    //println!("missed reader: {msg:?} with id
                    // {writer_entity_id:?}");
                  }
                }
              }
            }
          }
        }
      }
      Variant::Timer(timer) => match timer {
        Timer::Builtin(b) => match b {
          // domain
          BuiltinTimerVariant::AckNack => {
            self.timers.acknack.reset();
            self
              .message_receiver
              .send_preemptive_acknacks(udp_sender, ring);
            discovery
              .caches
              .send_preemptive_acknacks(udp_sender, ring)
              .unwrap();
          }
          BuiltinTimerVariant::CacheCleaning => {
            self.timers.cache_gc.reset();
            discovery.caches.garbage_collect_cache();
            dds_cache.garbage_collect();
          }
          // discovery
          BuiltinTimerVariant::ParticipantCleaning => {
            let mut cleanup = discovery.participant_cleanup(discovery_db);
            let readers = &mut self.message_receiver.available_readers;

            let mut state = None;

            let mut lost_cleanup = CleanupLostParticipant::new(
              discovery,
              readers,
              &mut self.writers,
              &mut cleanup,
              &mut state,
            );

            while let Some(lost_participant) = lost_cleanup.next() {
              match lost_participant {
                DomainStatusEvent::Participant(p) => on_participant_status(
                  p,
                  DomainRef::new(
                    &mut lost_cleanup.writers,
                    ring,
                    lost_cleanup.discovery,
                    udp_sender,
                  ),
                ),
                DomainStatusEvent::Data(d, guid) => {
                  if guid.entity_id.entity_kind.is_user_defined() {
                    on_data_status(
                      d,
                      guid,
                      DomainRef::new(
                        lost_cleanup.writers,
                        ring,
                        lost_cleanup.discovery,
                        udp_sender,
                      ),
                    );
                  }
                }
                DomainStatusEvent::Mixed(p, (d, guid)) => {
                  on_participant_status(
                    p,
                    DomainRef::new(
                      lost_cleanup.writers,
                      ring,
                      lost_cleanup.discovery,
                      udp_sender,
                    ),
                  );
                  if guid.entity_id.entity_kind.is_user_defined() {
                    on_data_status(
                      d,
                      guid,
                      DomainRef::new(
                        lost_cleanup.writers,
                        ring,
                        lost_cleanup.discovery,
                        udp_sender,
                      ),
                    );
                  }
                }
              }
            }
          }
          BuiltinTimerVariant::TopicCleaning => {
            discovery.topic_cleanup(discovery_db);
          }
          BuiltinTimerVariant::SpdpPublish => {
            discovery.spdp_publish(
              self.domain_info.domain_participant_guid,
              &self.listeners,
              udp_sender,
              ring,
            );
          }
          BuiltinTimerVariant::ParticipantMessages => {
            discovery.publish_participant_message(discovery_db, udp_sender, ring);
          }
        },
        t => {
          println!("timed event");
          match discovery.caches.handle_timed_event(t, udp_sender, ring) {
            // builtin
            Ok(_) => (),
            // non builtin
            Err(t) => {
              use crate::io_uring::encoding::user_data::Timer;
              match t {
                Timer::Write(entity_id, kind) => {
                  if let Some(writer) = self.writers.get_mut(&entity_id) {
                    writer.handle_timed_event(
                      udp_sender,
                      ring,
                      kind,
                      self.domain_info.domain_id,
                      user,
                    );
                  }
                }
                Timer::Read(entity_id, kind) => {
                  if let Some(reader) = self.message_receiver.available_readers.get_mut(&entity_id)
                  {
                    for _ev in reader.handle_timed_event(kind) {}
                  }
                }
                _ => unreachable!(),
              }
            }
          }
        }
      },
    }
    None
  }

  pub(crate) fn add_local_reader(
    &mut self,
    reader_ing: ReaderIngredients,
    ring: &mut io_uring::IoUring,
    user: u8,
  ) -> std::io::Result<()> {
    let domain_id = self.domain_info.domain_id;

    let new_reader = Reader::new(reader_ing).register(ring, domain_id, user)?;

    self.message_receiver.add_reader(new_reader);
    Ok(())
  }

  pub(crate) fn add_local_writer(
    &mut self,
    writer_ing: WriterIngredients,
    udp_sender: &UDPSender,
    ring: &mut io_uring::IoUring,
    initialized: bool,
    user: u8,
  ) -> std::io::Result<()> {
    let entity_id = writer_ing.guid.entity_id;
    let domain_id = self.domain_info.domain_id;

    let mut new_writer = Writer::new(writer_ing).register(ring, domain_id, user)?;

    if initialized {
      new_writer.handle_heartbeat_tick(false, udp_sender, ring);
    }

    self.writers.insert(entity_id, new_writer);
    Ok(())
  }

  #[cfg(feature = "security")]
  fn on_remote_participant_authentication_status_changed(&mut self, remote_guidp: GuidPrefix) {
    //TODO
    let auth_status = discovery_db_read(&self.discovery_db).get_authentication_status(remote_guidp);

    auth_status.map(|status| {
      self.send_participant_status(DomainParticipantStatusEvent::Authentication {
        participant: remote_guidp,
        status,
      });
    });

    match auth_status {
      Some(AuthenticationStatus::Authenticated) => {
        // The participant has been authenticated
        // First connect the built-in endpoints
        self.update_participant(remote_guidp, |_, _| (), |_, _| ());
        // Then start the key exchange
        if let Err(e) = self.discovery_command_sender.send(
          DiscoveryCommand::StartKeyExchangeWithRemoteParticipant {
            participant_guid_prefix: remote_guidp,
          },
        ) {
          error!(
            "Could not signal Discovery to start the key exchange with remote. Reason: {}. \
             Remote: {:?}",
            e, remote_guidp
          );
        }
      }
      Some(AuthenticationStatus::Authenticating) => {
        // The following call should connect the endpoints used for authentication
        self.update_participant(remote_guidp, |_, _| (), |_, _| ());
      }
      Some(AuthenticationStatus::Rejected) => {
        // TODO: disconnect endpoints from the participant?
        info!(
          "Status Rejected in on_remote_participant_authentication_status_changed with {:?}. TODO!",
          remote_guidp
        );
      }
      other => {
        info!(
          "Status {:?}, in on_remote_participant_authentication_status_changed. What to do?",
          other
        );
      }
    }
  }

  pub fn domain_ref<'a, 'b>(
    &'a mut self,
    ring: &'b mut IoUring,
    discovery: &'b mut Discovery2<timer_state::Init>,
    udp_sender: &'b UDPSender,
  ) -> DomainRef<'a, 'b> {
    DomainRef::new(&mut self.writers, ring, discovery, udp_sender)
  }
}

pub struct DomainRef<'a, 'b> {
  pub(crate) writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
  pub(crate) ring: &'b mut IoUring,
  pub(crate) discovery: &'b mut Discovery2<timer_state::Init>,
  pub(crate) udp_sender: &'b UDPSender,
}

impl<'a, 'b> DomainRef<'a, 'b> {
  fn new(
    writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    ring: &'b mut IoUring,
    discovery: &'b mut Discovery2<timer_state::Init>,
    udp_sender: &'b UDPSender,
  ) -> Self {
    Self {
      writers,
      ring,
      discovery,
      udp_sender,
    }
  }
}

use crate::{
  discovery::{DiscoveredTopicData, ParticipantMessageData, SpdpDiscoveredParticipantData},
  io_uring::discovery::{Cache, Caches, DiscoveredKind},
};

struct Builtins<'a> {
  publications: Option<&'a mut Cache<DiscoveredWriterData, timer_state::Init>>,
  topics: Option<&'a mut Cache<DiscoveredTopicData, timer_state::Init>>,
  participants: Option<&'a mut Cache<SpdpDiscoveredParticipantData, timer_state::Init>>,
  subscriptions: Option<&'a mut Cache<DiscoveredReaderData, timer_state::Init>>,
  participant_messages: Option<&'a mut Cache<ParticipantMessageData, timer_state::Init>>,
}

impl<'a> Builtins<'a> {
  fn new(caches: &'a mut Caches<timer_state::Init>) -> Self {
    Self {
      publications: Some(&mut caches.publications),
      topics: Some(&mut caches.topics),
      participants: Some(&mut caches.participants),
      subscriptions: Some(&mut caches.subscriptions),
      participant_messages: Some(&mut caches.participant_messages),
    }
  }
}

pub(crate) struct UpdatedParticipant2<'a> {
  builtins: Builtins<'a>,
  writers_list: vec::IntoIter<(EntityId, EntityId, u32)>,
  readers_list: vec::IntoIter<(EntityId, EntityId, u32)>,
  discovered_participant: crate::discovery::SpdpDiscoveredParticipantData,
}

impl<'a> UpdatedParticipant2<'a> {
  pub(crate) fn new(
    caches: &'a mut Caches<timer_state::Init>,
    discovered_participant: crate::discovery::SpdpDiscoveredParticipantData,
  ) -> Self {
    let builtins = Builtins::new(caches);
    let writers_list = crate::rtps::constant::STANDARD_BUILTIN_WRITERS_INIT_LIST
      .to_vec()
      .into_iter();
    let readers_list = crate::rtps::constant::STANDARD_BUILTIN_READERS_INIT_LIST
      .to_vec()
      .into_iter();

    Self {
      builtins,
      writers_list,
      readers_list,
      discovered_participant,
    }
  }
}

impl<'a> Iterator for UpdatedParticipant2<'a> {
  type Item = DomainStatusEvent;

  fn next(&mut self) -> Option<Self::Item> {
    while let Some((writer_eid, reader_eid, endpoint)) = self.readers_list.next() {
      println!("reader list: {writer_eid:?}");
      if !self
        .discovered_participant
        .available_builtin_endpoints
        .contains(endpoint)
      {
        continue;
      }

      let reader_proxy = self
        .discovered_participant
        .as_reader_proxy(true, Some(reader_eid));

      let (upd, guid) = match (writer_eid, &mut self.builtins) {
        (
          EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
          Builtins {
            publications: Some(cache),
            ..
          },
        ) => {
          let ret = cache.update_reader_proxy(&reader_proxy);
          //(self.f)(cache.minimal_hb());
          (ret, cache.writer_guid)
        }
        (
          EntityId::SEDP_BUILTIN_TOPIC_WRITER,
          Builtins {
            topics: Some(cache),
            ..
          },
        ) => {
          let ret = cache.update_reader_proxy(&reader_proxy);
          //(self.f)(cache.minimal_hb());
          (ret, cache.writer_guid)
        }
        (
          EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER,
          Builtins {
            participants: Some(cache),
            ..
          },
        ) => {
          let ret = cache.update_reader_proxy(&reader_proxy);
          (ret, cache.writer_guid)
        }
        (
          EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER,
          Builtins {
            subscriptions: Some(cache),
            ..
          },
        ) => {
          let ret = cache.update_reader_proxy(&reader_proxy);
          (ret, cache.writer_guid)
        }
        (
          EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER,
          Builtins {
            participant_messages: Some(cache),
            ..
          },
        ) => {
          let qos = &cache.qos;
          let upd = if self
            .discovered_participant
            .builtin_endpoint_qos
            .is_some_and(|epq| epq.is_best_effort())
          {
            let mut qos = qos.clone();
            qos.reliability = Some(policy::Reliability::BestEffort);

            let ret = cache.update_reader_proxy_with_qos(&reader_proxy, &qos);
            ret
          } else {
            let ret = cache.update_reader_proxy(&reader_proxy);
            ret
          };
          (upd, cache.writer_guid)
        }
        _ => continue,
      };

      if let Some((writer, domain)) = upd {
        return Some(DomainStatusEvent::Mixed(
          domain,
          (DataStatus::Writer(writer), guid),
        ));
      } else {
        continue;
      }
    }

    while let Some((writer_eid, reader_eid, endpoint)) = self.writers_list.next() {
      println!("writer list: {writer_eid:?}");
      if !self
        .discovered_participant
        .available_builtin_endpoints
        .contains(endpoint)
      {
        continue;
      }

      let writer_proxy = self
        .discovered_participant
        .as_writer_proxy(true, Some(writer_eid));

      let (upd, guid) = match (reader_eid, &mut self.builtins) {
        (
          EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
          Builtins {
            publications: Some(cache),
            ..
          },
        ) => (cache.update_writer_proxy(writer_proxy), cache.reader_guid),
        (
          EntityId::SEDP_BUILTIN_TOPIC_READER,
          Builtins {
            topics: Some(cache),
            ..
          },
        ) => (cache.update_writer_proxy(writer_proxy), cache.reader_guid),
        (
          EntityId::SPDP_BUILTIN_PARTICIPANT_READER,
          Builtins {
            participants: Some(cache),
            ..
          },
        ) => (cache.update_writer_proxy(writer_proxy), cache.reader_guid),
        (
          EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER,
          Builtins {
            subscriptions: Some(cache),
            ..
          },
        ) => (cache.update_writer_proxy(writer_proxy), cache.reader_guid),
        (
          EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER,
          Builtins {
            participant_messages: Some(cache),
            ..
          },
        ) => (cache.update_writer_proxy(writer_proxy), cache.reader_guid),
        _ => continue,
      };

      if let Some((writer, domain)) = upd {
        return Some(DomainStatusEvent::Mixed(
          domain,
          (DataStatus::Reader(writer), guid),
        ));
      } else {
        continue;
      }
    }
    None
  }
}

struct BuiltinLostParticipant {
  builtin: BuiltinParticipantLost,
  state: BuiltinLostParticipantState,
}

impl BuiltinLostParticipant {
  fn next(
    &mut self,
    builtins: &mut Builtins<'_>,
    guid_prefix: GuidPrefix,
  ) -> Option<(DataStatus, GUID)> {
    if let Some(r) = {
      use BuiltinLostParticipantState as BPS;
      match self.state {
        BPS::Publications => self
          .builtin
          .inner(builtins.publications.as_mut().unwrap())
          .next(),
        BPS::Topics => self.builtin.inner(builtins.topics.as_mut().unwrap()).next(),
        BPS::Participants => self
          .builtin
          .inner(builtins.participants.as_mut().unwrap())
          .next(),
        BPS::Subscriptions => self
          .builtin
          .inner(builtins.subscriptions.as_mut().unwrap())
          .next(),
        BPS::ParticipantMessages => self
          .builtin
          .inner(builtins.participant_messages.as_mut().unwrap())
          .next(),
      }
    } {
      return Some(r);
    } else {
      use BuiltinLostParticipantState as BPS;
      match self.state {
        BPS::Publications => {
          core::mem::take(&mut builtins.publications);
        }
        BPS::Topics => {
          core::mem::take(&mut builtins.topics);
        }
        BPS::Participants => {
          core::mem::take(&mut builtins.participants);
        }
        BPS::Subscriptions => {
          core::mem::take(&mut builtins.subscriptions);
        }
        BPS::ParticipantMessages => {
          core::mem::take(&mut builtins.participant_messages);
        }
      }
    }

    if let Some(s) = Self::new(builtins, guid_prefix) {
      *self = s;
      self.next(builtins, guid_prefix)
    } else {
      None
    }
  }

  fn new(builtins: &mut Builtins<'_>, guid_prefix: GuidPrefix) -> Option<Self> {
    use BuiltinLostParticipantState as BPS;
    Some(match builtins {
      Builtins {
        publications: Some(b),
        ..
      } => Self {
        state: BPS::Publications,
        builtin: b.participant_lost(guid_prefix),
      },
      Builtins {
        topics: Some(b), ..
      } => Self {
        state: BPS::Topics,
        builtin: b.participant_lost(guid_prefix),
      },
      Builtins {
        participants: Some(b),
        ..
      } => Self {
        state: BPS::Participants,
        builtin: b.participant_lost(guid_prefix),
      },
      Builtins {
        subscriptions: Some(b),
        ..
      } => Self {
        state: BPS::Subscriptions,
        builtin: b.participant_lost(guid_prefix),
      },
      Builtins {
        participant_messages: Some(b),
        ..
      } => Self {
        state: BPS::ParticipantMessages,
        builtin: b.participant_lost(guid_prefix),
      },
      _ => return None,
    })
  }
}

struct CleanupLostParticipant<'a, 'c, 'f> {
  pub(crate) discovery: &'c mut Discovery2<timer_state::Init>,
  readers: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
  writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
  cleanup: &'f mut ParticipantCleanup,
  state: &'f mut Option<(
    LostParticipant2<'a, 'c>,
    Option<DomainParticipantStatusEvent>,
  )>,
}

impl<'a, 'c, 'f> CleanupLostParticipant<'a, 'c, 'f> {
  fn new(
    discovery: &'c mut Discovery2<timer_state::Init>,
    readers: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
    writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    cleanup: &'f mut ParticipantCleanup,
    state: &'f mut Option<(
      LostParticipant2<'a, 'c>,
      Option<DomainParticipantStatusEvent>,
    )>,
  ) -> Self {
    Self {
      discovery,
      readers,
      writers,
      cleanup,
      state,
    }
  }
}

impl<'a, 'c, 'f> Iterator for CleanupLostParticipant<'a, 'c, 'f> {
  type Item = DomainStatusEvent;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some((iter_state, state)) = self.state.as_mut() {
      match (core::mem::take(state), iter_state.next()) {
        (Some(participant), Some(data)) => {
          return Some(DomainStatusEvent::Mixed(participant, data))
        }
        (Some(participant), _) => return Some(DomainStatusEvent::Participant(participant)),
        (_, Some((data, guid))) => return Some(DomainStatusEvent::Data(data, guid)),
        _ => (),
      }
    }

    let (guid_prefix, reason) = self.cleanup.next()?;

    let participant_status = DomainParticipantStatusEvent::ParticipantLost {
      id: guid_prefix,
      reason,
    };

    //SAFETY: this is not mutably borrowed more than once at a time.
    let readers = unsafe { &mut *(self.readers as *mut _) };
    let writers = unsafe { &mut *(self.writers as *mut _) };
    let caches = unsafe { &mut *(&mut self.discovery.caches as *mut _) };

    let iter_state = LostParticipant2::new(guid_prefix, readers, writers, caches);

    *self.state = Some((iter_state, Some(participant_status)));
    self.next()
  }
}

use crate::io_uring::discovery::ParticipantCleanup;
use super::{reader::LostWriters, writer::LostReaders};

struct LostParticipant2<'a, 'b> {
  guid_prefix: GuidPrefix,
  readers: std::collections::btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>,
  writers: std::collections::hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
  stored_reader: Option<LostReaders<'a>>,
  stored_writer: Option<LostWriters<'a>>,

  lost_builtins: Option<BuiltinLostParticipant>,
  builtins: Builtins<'b>,
}

impl<'a, 'b> LostParticipant2<'a, 'b> {
  fn new(
    guid_prefix: GuidPrefix,
    readers: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
    writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    caches: &'b mut Caches<timer_state::Init>,
  ) -> Self {
    let readers = readers.values_mut();
    let writers = writers.values_mut();
    let mut builtins = Builtins::new(caches);

    let lost_builtins = BuiltinLostParticipant::new(&mut builtins, guid_prefix);

    Self {
      readers,
      writers,
      stored_reader: None,
      stored_writer: None,
      guid_prefix,
      builtins,
      lost_builtins,
    }
  }
}

use crate::io_uring::discovery::BuiltinParticipantLost;

enum BuiltinLostParticipantState {
  Publications,
  Topics,
  Participants,
  Subscriptions,
  ParticipantMessages,
}

impl Iterator for LostParticipant2<'_, '_> {
  type Item = (DataStatus, GUID);
  fn next(&mut self) -> Option<Self::Item> {
    if let Some(reader) = self.stored_reader.as_mut() {
      if let Some((status, guid)) = reader.next() {
        return Some((DataStatus::Writer(status), guid));
      }
    }

    if let Some(writer) = self.stored_writer.as_mut() {
      if let Some((status, guid)) = writer.next() {
        return Some((DataStatus::Reader(status), guid));
      }
    }

    if let Some(builtin) = self.lost_builtins.as_mut() {
      if let Some(r) = builtin.next(&mut self.builtins, self.guid_prefix) {
        return Some(r);
      }
    }

    if let Some(writer) = self.writers.next() {
      self.stored_reader = Some(writer.participant_lost(self.guid_prefix));
    }

    if let Some(reader) = self.readers.next() {
      self.stored_writer = Some(reader.participant_lost(self.guid_prefix))
    }
    None
  }
}

use std::collections::HashMap;

use crate::{
  dds::{qos::policy, statusevents::DomainParticipantStatusEvent},
  discovery::sedp_messages::{DiscoveredReaderData, DiscoveredWriterData},
  io_uring::{
    network::udp_sender::UDPSender,
    rtps::{
      message_receiver::MessageReceiver,
      reader::{Reader, ReaderIngredients},
      writer::{Writer, WriterIngredients},
    },
  },
  rtps::{
    constant::*, dp_event_loop::DomainInfo, rtps_reader_proxy::RtpsReaderProxy,
    rtps_writer_proxy::RtpsWriterProxy,
  },
  structure::guid::{EntityId, GuidPrefix, GUID},
};
#[cfg(feature = "security")]
use crate::{
  discovery::secure_discovery::AuthenticationStatus,
  security::{security_plugins::SecurityPluginsHandle, EndpointSecurityInfo},
  security_warn,
};

struct DiscoveredUpdates2<C, U> {
  remote: C,
  remote_discovered: U,
}

type DiscoveredReaderUpdates2<'a> = DiscoveredUpdates2<
  hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
  DiscoveredReaderData,
>;

impl<'a>
  DiscoveredUpdates2<
    hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
    DiscoveredReaderData,
  >
{
  fn new_reader_data(
    map: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    reader_data: DiscoveredReaderData,
  ) -> Self {
    Self {
      remote: map.values_mut(),
      remote_discovered: reader_data,
    }
  }
}

type DiscoveredWriterUpdates2<'a> = DiscoveredUpdates2<
  btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>,
  DiscoveredWriterData,
>;

impl<'a>
  DiscoveredUpdates2<
    btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>,
    DiscoveredWriterData,
  >
{
  fn new_writer_data(
    map: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
    writer_data: DiscoveredWriterData,
  ) -> Self {
    Self {
      remote: map.values_mut(),
      remote_discovered: writer_data,
    }
  }
}

use std::collections::hash_map;

impl<'a> Iterator
  for DiscoveredUpdates2<
    hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
    DiscoveredReaderData,
  >
{
  type Item = (DataWriterStatus, DomainParticipantStatusEvent);

  fn next(&mut self) -> Option<Self::Item> {
    while let Some(writer) = self.remote.next() {
      if self.remote_discovered.subscription_topic_data.topic_name() != writer.topic_name() {
        continue;
      }

      let match_to_reader = true;

      if !match_to_reader {
        continue;
      }

      let requested_qos = self.remote_discovered.subscription_topic_data.qos();

      let Some(r) = writer.update_reader_proxy(
        &RtpsReaderProxy::from_discovered_reader_data(&self.remote_discovered, &[], &[]),
        &requested_qos,
      ) else {
        continue;
      };
      println!("updated proxy: {r:?}");

      return Some(r);
    }
    None
  }
}

use std::collections::btree_map;

impl<'a> Iterator
  for DiscoveredUpdates2<
    btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>,
    DiscoveredWriterData,
  >
{
  type Item = (DataReaderStatus, DomainParticipantStatusEvent);

  fn next(&mut self) -> Option<Self::Item> {
    while let Some(reader) = self.remote.next() {
      if self.remote_discovered.publication_topic_data.topic_name() != reader.topic_name() {
        continue;
      }

      let match_to_reader = true;

      if !match_to_reader {
        continue;
      }

      let offered_qos = self.remote_discovered.publication_topic_data.qos();

      let Some(r) = reader.update_writer_proxy(
        RtpsWriterProxy::from_discovered_writer_data(&self.remote_discovered, &[], &[]),
        &offered_qos,
      ) else {
        continue;
      };

      return Some(r);
    }
    None
  }
}

use crate::io_uring::discovery::Discovered;

#[derive(Debug)]
pub enum DataStatus {
  Reader(DataReaderStatus),
  Writer(DataWriterStatus),
}

/*
struct DDIState<'a, 'c, 'd, 'e, 'f> {
  pub(crate) discovery: &'c mut Discovery2<timer_state::Init>,
  pub(crate) discovery_db: &'d mut DiscoveryDB,
  readers: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
  writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
  discovered: &'f mut DiscoveredKind,
  state: &'f mut Option<(DState<'a, 'c>, Option<DomainParticipantStatusEvent>)>,
  ring: &'e mut IoUring,
  udp_sender: &'e UDPSender,
}

impl<'a, 'c, 'd, 'e, 'f> DDIState<'a, 'c, 'd, 'e, 'f> {
  fn new(
    discovery: &'c mut Discovery2<timer_state::Init>,
    discovery_db: &'d mut DiscoveryDB,
    readers: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
    writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    discovered: &'f mut DiscoveredKind,
    state: &'f mut Option<(DState<'a, 'c>, Option<DomainParticipantStatusEvent>)>,
    ring: &'e mut IoUring,
    udp_sender: &'e UDPSender,
  ) -> Self {
    Self {
      discovery,
      discovery_db,
      readers,
      writers,
      discovered,
      state,
      ring,
      udp_sender,
    }
  }
}

impl<'a, 'c, 'd, 'e, 'f> Iterator for DDIState<'a, 'c, 'd, 'e, 'f> {
  type Item = DomainStatusEvent;

  fn next(&mut self) -> Option<Self::Item> {
    /*
    macro_rules! check_matched_publication {
        ($ev:expr) => {
            match &$ev {
              DomainStatusEvent::Data(status, guid) | DomainStatusEvent::Mixed(_, (status, guid)) => match status {
                  DataStatus::Writer(status) => match status {
                      DataWriterStatus::PublicationMatched{..} => {
                          if let Some(writer) = self.writers.get_mut(&guid.entity_id) {
                              writer.handle_heartbeat_tick(false, self.udp_sender, self.ring);
                              println!("sending inital heartbeat");
                          }
                          println!("added: {guid:?}");
                      }
                      _ => (),
                  }
                  _ => (),
              }

              /*
              DomainStatusEvent::Data(DataStatus::Writer(DataWriterStatus::PublicationMatched {..}), guid) | DomainStatusEvent::Mixed(_, (DataStatus::Writer(DataWriterStatus::PublicationMatched {..}), guid)) => {
                  if let Some(writer) = self.writers.get_mut(&guid.entity_id) {
                      writer.handle_heartbeat_tick(false, self.udp_sender, self.ring);
                      println!("sending inital heartbeat");
                  }
              }
              */
              _ => (),
            }
        }
    }
    */

    macro_rules! try_take_state {
      ($state:expr) => {
        if let Some((state, stored)) = $state.as_mut() {
          if let Some(participant_status) = core::mem::take(stored) {
            Some(DomainStatusEvent::Participant(participant_status))
          } else if let Some(r) = state.next() {
            //check_matched_publication!(r);
            Some(r)
          } else {
            None
          }
        } else {
          None
        }
      };
    }

    if let Some(r) = try_take_state!(self.state) {
      //check_matched_publication!(r);
      return Some(r);
    }

    let mut discovered = Discovered::new(self.discovery, self.discovery_db, &mut self.discovered);
    while let Some((discovery, status)) = discovered.next() {
      //SAFETY: this is not mutably borrowed more than once at a time.
      let readers = unsafe { &mut *(self.readers as *mut _) };
      let writers = unsafe { &mut *(self.writers as *mut _) };
      let caches = unsafe { &mut *(&mut discovered.discovery.caches as *mut _) };

      let Some(state) = DState::new(
        discovery,
        readers,
        writers,
        caches,
        self.ring,
        discovered.discovery_db,
        self.udp_sender,
      ) else {
        continue;
      };

      let mut state = Some(state);

      if let Some(participant_status) = status {
        *self.state = state;
        return Some(DomainStatusEvent::Participant(participant_status));
      } else if let Some(r) = try_take_state!(state) {
        //check_matched_publication!(r);
        *self.state = state;
        return Some(r);
      }
    }
    None
  }
}
*/

fn iterate_events(
  discovery: &mut Discovery2<timer_state::Init>,
  discovery_db: &mut DiscoveryDB,
  readers: &mut BTreeMap<EntityId, Reader<timer_state::Init>>,
  writers: &mut HashMap<EntityId, Writer<timer_state::Init>>,
  discovered: &mut DiscoveredKind,
  ring: &mut IoUring,
  udp_sender: &UDPSender,
  mut f: impl for<'a> FnMut(DomainStatusEvent, DomainRef<'a, 'a>),
) {
  let mut discovered = Discovered::new(discovery, discovery_db, discovered);
  while let Some((discovery_status, mut status)) = discovered.next() {
    if let Some(participant_status) = core::mem::take(&mut status) {
      f(
        DomainStatusEvent::Participant(participant_status),
        DomainRef::new(writers, ring, discovered.discovery, udp_sender),
      );
    }

    //SAFETY: there should not be any overlap.
    // move the ring into each of the fns?
    //let ring_2 = unsafe {&mut *(ring as *mut _)};
    //it happens regardless???
    // SAFETY: this is only used between iterations
    // which will not effect the underlying collection
    //
    // and is only used outisde of processing elements in the iteration
    let (discovery_reborrow, writers_reborrow) = unsafe {
      (
        &mut *(discovered.discovery as *mut _),
        &mut *(writers as *mut _),
      )
    };

    let Some((mut dstate, mut status)) = DState::new(
      discovery_status,
      readers,
      writers,
      &mut discovered.discovery.caches,
      ring,
      discovered.discovery_db,
      udp_sender,
    ) else {
      continue;
    };

    if let Some(participant_status) = core::mem::take(&mut status) {
      f(
        DomainStatusEvent::Participant(participant_status),
        DomainRef::new(writers_reborrow, ring, discovery_reborrow, udp_sender),
      );
    }

    while let Some(status) = dstate.next() {
      f(
        status,
        DomainRef::new(writers_reborrow, ring, discovery_reborrow, udp_sender),
      );
    }
  }
}

enum DState<'a, 'b> {
  ReaderLost(LostReaderUpdates<'a>),
  ReaderUpdates(DiscoveredReaderUpdates2<'a>),
  WriterLost(LostWriterUpdates<'a>),
  WriterUpdates(DiscoveredWriterUpdates2<'a>),
  ParticipantLost(LostParticipant2<'a, 'b>),
  ParticipantUpdates(UpdatedParticipant2<'b>),
}

impl<'a, 'b> DState<'a, 'b> {
  fn new(
    notif: DiscoveryNotificationType,
    readers: &'a mut BTreeMap<EntityId, Reader<timer_state::Init>>,
    writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    caches: &'b mut Caches<timer_state::Init>,
    ring: &mut IoUring,
    discovery_db: &mut DiscoveryDB,
    udp_sender: &UDPSender,
  ) -> Option<(Self, Option<DomainParticipantStatusEvent>)> {
    use DiscoveryNotificationType as DDT;

    match notif {
      DDT::WriterUpdated {
        discovered_writer_data,
      } => {
        use chrono::Utc;

        use crate::EndpointDescription;
        let domain_ev = DomainParticipantStatusEvent::WriterDetected {
          writer: EndpointDescription {
            updated_time: Utc::now(),
            guid: discovered_writer_data.writer_proxy.remote_writer_guid,
            topic_name: discovered_writer_data
              .publication_topic_data
              .topic_name
              .clone(),
            type_name: discovered_writer_data
              .publication_topic_data
              .type_name
              .clone(),
            qos: discovered_writer_data.publication_topic_data.qos(),
          },
        };

        let state = Self::WriterUpdates(DiscoveredWriterUpdates2::new_writer_data(
          readers,
          discovered_writer_data,
        ));

        Some((state, Some(domain_ev)))
      }
      DDT::WriterLost { writer_guid } => Some((
        Self::WriterLost(LostWriterUpdates::new(readers.values_mut(), writer_guid)),
        None,
      )),
      DDT::ReaderUpdated {
        discovered_reader_data,
      } => {
        use chrono::Utc;

        use crate::EndpointDescription;
        let domain_ev = DomainParticipantStatusEvent::ReaderDetected {
          reader: EndpointDescription {
            updated_time: Utc::now(),
            guid: discovered_reader_data.reader_proxy.remote_reader_guid,
            topic_name: discovered_reader_data
              .subscription_topic_data
              .topic_name
              .clone(),
            type_name: discovered_reader_data
              .subscription_topic_data
              .type_name()
              .clone(),
            qos: discovered_reader_data.subscription_topic_data.qos(),
          },
        };

        let state = Self::ReaderUpdates(DiscoveredReaderUpdates2::new_reader_data(
          writers,
          discovered_reader_data,
        ));

        Some((state, Some(domain_ev)))
      }
      DDT::ReaderLost { reader_guid } => Some((
        Self::ReaderLost(LostReaderUpdates::new(writers.values_mut(), reader_guid)),
        None,
      )),
      DDT::ParticipantUpdated { guid_prefix } => {
        let participant_proxy = discovery_db.find_participant_proxy(guid_prefix)?;

        //FIXME: this may be able to avoid a clone now.
        Some((
          Self::ParticipantUpdates(UpdatedParticipant2::new(caches, participant_proxy.clone())),
          None,
        ))
      }
      DDT::ParticipantLost { guid_prefix } => Some((
        Self::ParticipantLost(LostParticipant2::new(guid_prefix, readers, writers, caches)),
        None,
      )),
      DDT::AssertTopicLiveliness {
        writer_guid,
        manual_assertion,
      } => {
        writers
          .get_mut(&writer_guid.entity_id)
          .map(|w| w.handle_heartbeat_tick(manual_assertion, udp_sender, ring));
        None
      }
      DDT::TopicDiscovered => None,
    }
  }
}

impl<'a, 'b> Iterator for DState<'a, 'b> {
  type Item = DomainStatusEvent;

  fn next(&mut self) -> Option<Self::Item> {
    match self {
      Self::ReaderLost(updates) => updates
        .next()
        .map(|u| DomainStatusEvent::Data(DataStatus::Writer(u), updates.reader_guid)),
      Self::ReaderUpdates(updates) => updates.next().map(|(writer, domain)| {
        DomainStatusEvent::Mixed(
          domain,
          (
            DataStatus::Writer(writer),
            updates.remote_discovered.reader_proxy.remote_reader_guid,
          ),
        )
      }),
      Self::WriterLost(updates) => updates
        .next()
        .map(|u| DomainStatusEvent::Data(DataStatus::Reader(u), updates.writer_guid)),
      Self::WriterUpdates(updates) => updates.next().map(|(reader, domain)| {
        DomainStatusEvent::Mixed(
          domain,
          (
            DataStatus::Reader(reader),
            updates.remote_discovered.writer_proxy.remote_writer_guid,
          ),
        )
      }),
      Self::ParticipantLost(updates) => updates
        .next()
        .map(|(data, guid)| DomainStatusEvent::Data(data, guid)),
      Self::ParticipantUpdates(updates) => updates.next(),
    }
  }
}

use io_uring::IoUring;

#[derive(Debug)]
pub enum DomainStatusEvent {
  Participant(DomainParticipantStatusEvent),
  Data(DataStatus, GUID),
  Mixed(DomainParticipantStatusEvent, (DataStatus, GUID)),
}

use std::vec;

use crate::{DataReaderStatus, DataWriterStatus};

struct LostReaderUpdates<'a> {
  remote_writers: std::collections::hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
  reader_guid: GUID,
}

impl<'a> LostReaderUpdates<'a> {
  fn new(
    remote_writers: std::collections::hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
    reader_guid: GUID,
  ) -> Self {
    Self {
      remote_writers,
      reader_guid,
    }
  }
}

impl Iterator for LostReaderUpdates<'_> {
  type Item = DataWriterStatus;

  fn next(&mut self) -> Option<Self::Item> {
    while let Some(writer) = self.remote_writers.next() {
      if let Some(ret) = writer.reader_lost(self.reader_guid) {
        return Some(ret);
      } else {
        continue;
      }
    }
    None
  }
}

struct LostWriterUpdates<'a> {
  remote_readers: std::collections::btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>,
  writer_guid: GUID,
}

impl<'a> LostWriterUpdates<'a> {
  fn new(
    remote_readers: std::collections::btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>,
    writer_guid: GUID,
  ) -> Self {
    Self {
      remote_readers,
      writer_guid,
    }
  }
}

impl Iterator for LostWriterUpdates<'_> {
  type Item = DataReaderStatus;

  fn next(&mut self) -> Option<Self::Item> {
    while let Some(reader) = self.remote_readers.next() {
      if let Some(ret) = reader.remove_writer_proxy(self.writer_guid) {
        return Some(ret);
      } else {
        continue;
      }
    }
    None
  }
}

#[cfg(feature = "security")]
fn check_are_endpoints_securities_compatible(
  local_info_opt: Option<EndpointSecurityInfo>,
  remote_info_opt: Option<EndpointSecurityInfo>,
) -> bool {
  let (local_info, remote_info) = match (local_info_opt, remote_info_opt) {
    (None, None) => {
      // Neither has security info. Pass?
      return true;
    }
    (Some(_info), None) | (None, Some(_info)) => {
      // Only one of the endpoints has security info. Reject.
    }
    (Some(local_info), Some(remote_info)) => (local_info, remote_info),
  };

  // See Security specification section 7.2.8 EndpointSecurityInfo
  if local_info.endpoint_security_attributes.is_valid()
    && local_info.plugin_endpoint_security_attributes.is_valid()
    && remote_info.endpoint_security_attributes.is_valid()
    && remote_info.plugin_endpoint_security_attributes.is_valid()
  {
    // When all masks are valid, values need to be equal
    local_info == remote_info
  } else {
    // From the spec:
    // "If the is_valid is set to zero on either of the masks, the comparison
    // between the local and remote setting for the EndpointSecurityInfo shall
    // ignore the attribute"

    // TODO: Does it actually make sense to ignore the masks if they're not valid?
    // Seems a bit strange. Currently we require that all masks are valid
    false
  }
}

// -----------------------------------------------------------
// -----------------------------------------------------------
// -----------------------------------------------------------

/*
#[cfg(test)]
mod tests {
  use std::{sync::Mutex, thread};

  use mio_extras::channel as mio_channel;

  use super::*;
  use crate::{
    dds::{
      qos::QosPolicies,
      statusevents::{sync_status_channel, DataReaderStatus},
      typedesc::TypeDesc,
      with_key::simpledatareader::ReaderCommand,
    },
    mio_source,
  };

  //#[test]
  // TODO: Investigate why this fails in the github CI pipeline
  // Then re-enable this test.
  #[allow(dead_code)]
  fn dpew_add_and_remove_readers() {
    // Test sending 'add reader' and 'remove reader' commands to DP event loop
    // TODO: There are no assertions in this test case. Does in actually test
    // anything?

    // Create DP communication channels
    let (sender_add_reader, receiver_add) = mio_channel::channel::<ReaderIngredients>();
    let (sender_remove_reader, receiver_remove) = mio_channel::channel::<GUID>();

    let (_add_writer_sender, add_writer_receiver) = mio_channel::channel();
    let (_remove_writer_sender, remove_writer_receiver) = mio_channel::channel();

    let (_stop_poll_sender, stop_poll_receiver) = mio_channel::channel();

    let (_discovery_update_notification_sender, discovery_update_notification_receiver) =
      mio_channel::channel();
    let (discovery_command_sender, _discovery_command_receiver) =
      mio_channel::sync_channel::<DiscoveryCommand>(64);
    let (spdp_liveness_sender, _spdp_liveness_receiver) = mio_channel::sync_channel(8);
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let dds_cache_clone = Arc::clone(&dds_cache);
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      mio_channel::sync_channel::<()>(4);

    let discovery_db = Arc::new(RwLock::new(DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      participant_status_sender.clone(),
    )));

    let domain_info = DomainInfo {
      domain_participant_guid: GUID::default(),
      domain_id: 0,
      participant_id: 0,
    };

    let (sender_stop, receiver_stop) = mio_channel::channel::<i32>();

    // Start event loop
    let child = thread::spawn(move || {
      let dp_event_loop = DPEventLoop::new(
        domain_info,
        dds_cache_clone,
        HashMap::new(),
        discovery_db,
        GuidPrefix::default(),
        TokenReceiverPair {
          token: ADD_READER_TOKEN,
          receiver: receiver_add,
        },
        TokenReceiverPair {
          token: REMOVE_READER_TOKEN,
          receiver: receiver_remove,
        },
        TokenReceiverPair {
          token: ADD_WRITER_TOKEN,
          receiver: add_writer_receiver,
        },
        TokenReceiverPair {
          token: REMOVE_WRITER_TOKEN,
          receiver: remove_writer_receiver,
        },
        stop_poll_receiver,
        discovery_update_notification_receiver,
        discovery_command_sender,
        spdp_liveness_sender,
        participant_status_sender,
        None,
      );
      dp_event_loop
        .poll
        .register(
          &receiver_stop,
          STOP_POLL_TOKEN,
          Ready::readable(),
          PollOpt::edge(),
        )
        .expect("Failed to register receivers.");
      dp_event_loop.event_loop();
    });

    // Create a topic cache
    let topic_cache = dds_cache.write().unwrap().add_new_topic(
      "test".to_string(),
      TypeDesc::new("test_type".to_string()),
      &QosPolicies::qos_none(),
    );

    let num_of_readers = 3;

    // Send some 'add reader' commands
    let mut reader_guids = Vec::new();
    for i in 0..num_of_readers {
      let new_guid = GUID::default();

      // Create mechanisms for notifications, statuses & commands
      let (notification_sender, _notification_receiver) = mio_channel::sync_channel::<()>(100);
      let (_notification_event_source, notification_event_sender) =
        mio_source::make_poll_channel().unwrap();
      let data_reader_waker = Arc::new(Mutex::new(None));

      let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();

      let (_reader_command_sender, reader_command_receiver) =
        mio_channel::sync_channel::<ReaderCommand>(10);

      let new_reader_ing = ReaderIngredients {
        guid: new_guid,
        notification_sender,
        status_sender,
        topic_cache_handle: topic_cache.clone(),
        topic_name: "test".to_string(),
        like_stateless: false,
        qos_policy: QosPolicies::qos_none(),
        data_reader_command_receiver: reader_command_receiver,
        data_reader_waker: data_reader_waker.clone(),
        poll_event_sender: notification_event_sender,
        security_plugins: None,
      };

      reader_guids.push(new_reader_ing.guid);
      info!("\nSent reader number {}: {:?}\n", i, &new_reader_ing);
      sender_add_reader.send(new_reader_ing).unwrap();
      std::thread::sleep(Duration::new(0, 100));
    }

    // Send a command to remove the second reader
    info!("\nremoving the second\n");
    let some_guid = reader_guids[1];
    sender_remove_reader.send(some_guid).unwrap();
    std::thread::sleep(Duration::new(0, 100));

    info!("\nsending end token\n");
    sender_stop.send(0).unwrap();
    child.join().unwrap();
  }

  // TODO: Rewrite / remove this test - all asserts in it use
  // DataReader::get_requested_deadline_missed_status which is
  // currently commented out

  // #[test]
  // fn dpew_test_reader_commands() {
  //   let somePolicies = QosPolicies {
  //     durability: None,
  //     presentation: None,
  //     deadline: Some(Deadline(DurationDDS::from_millis(500))),
  //     latency_budget: None,
  //     ownership: None,
  //     liveliness: None,
  //     time_based_filter: None,
  //     reliability: None,
  //     destination_order: None,
  //     history: None,
  //     resource_limits: None,
  //     lifespan: None,
  //   };
  //   let dp = DomainParticipant::new(0).expect("Failed to create
  // participant");   let sub = dp.create_subscriber(&somePolicies).unwrap();

  //   let topic_1 = dp
  //     .create_topic("TOPIC_1", "something", &somePolicies,
  // TopicKind::WithKey)     .unwrap();
  //   let _topic_2 = dp
  //     .create_topic("TOPIC_2", "something", &somePolicies,
  // TopicKind::WithKey)     .unwrap();
  //   let _topic_3 = dp
  //     .create_topic("TOPIC_3", "something", &somePolicies,
  // TopicKind::WithKey)     .unwrap();

  //   // Adding readers
  //   let (sender_add_reader, receiver_add) = mio_channel::channel::<Reader>();
  //   let (_sender_remove_reader, receiver_remove) =
  // mio_channel::channel::<GUID>();

  //   let (_add_writer_sender, add_writer_receiver) = mio_channel::channel();
  //   let (_remove_writer_sender, remove_writer_receiver) =
  // mio_channel::channel();

  //   let (_stop_poll_sender, stop_poll_receiver) = mio_channel::channel();

  //   let (_discovery_update_notification_sender,
  // discovery_update_notification_receiver) =     mio_channel::channel();

  //   let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
  //   let discovery_db = Arc::new(RwLock::new(DiscoveryDB::new()));

  //   let domain_info = DomainInfo {
  //     domain_participant_guid: GUID::default(),
  //     domain_id: 0,
  //     participant_id: 0,
  //   };

  //   let dp_event_loop = DPEventLoop::new(
  //     domain_info,
  //     HashMap::new(),
  //     dds_cache,
  //     discovery_db,
  //     GuidPrefix::default(),
  //     TokenReceiverPair {
  //       token: ADD_READER_TOKEN,
  //       receiver: receiver_add,
  //     },
  //     TokenReceiverPair {
  //       token: REMOVE_READER_TOKEN,
  //       receiver: receiver_remove,
  //     },
  //     TokenReceiverPair {
  //       token: ADD_WRITER_TOKEN,
  //       receiver: add_writer_receiver,
  //     },
  //     TokenReceiverPair {
  //       token: REMOVE_WRITER_TOKEN,
  //       receiver: remove_writer_receiver,
  //     },
  //     stop_poll_receiver,
  //     discovery_update_notification_receiver,
  //   );

  //   let (sender_stop, receiver_stop) = mio_channel::channel::<i32>();
  //   dp_event_loop
  //     .poll
  //     .register(
  //       &receiver_stop,
  //       STOP_POLL_TOKEN,
  //       Ready::readable(),
  //       PollOpt::edge(),
  //     )
  //     .expect("Failed to register receivers.");

  //   let child = thread::spawn(move ||
  // DPEventLoop::event_loop(dp_event_loop));

  //   //TODO IF THIS IS SET TO 1 TEST SUCCEEDS
  //   let n = 1;

  //   let mut reader_guids = Vec::new();
  //   let mut data_readers: Vec<DataReader<RandomData,
  // CDRDeserializerAdapter<RandomData>>> = vec![];   let _topics: Vec<Topic>
  // = vec![];   for i in 0..n {
  //     //topics.push(topic);
  //     let new_guid = GUID::default();

  //     let (send, _rec) = mio_channel::sync_channel::<()>(100);
  //     let (status_sender, status_receiver_DataReader) =
  //       mio_extras::channel::sync_channel::<DataReaderStatus>(1000);
  //     let (reader_commander, reader_command_receiver) =
  //       mio_extras::channel::sync_channel::<ReaderCommand>(1000);

  //     let mut new_reader = Reader::new(
  //       new_guid,
  //       send,
  //       status_sender,
  //       Arc::new(RwLock::new(DDSCache::new())),
  //       "test".to_string(),
  //       QosPolicies::qos_none(),
  //       reader_command_receiver,
  //     );

  //     let somePolicies = QosPolicies {
  //       durability: None,
  //       presentation: None,
  //       deadline: Some(Deadline(DurationDDS::from_millis(50))),
  //       latency_budget: None,
  //       ownership: None,
  //       liveliness: None,
  //       time_based_filter: None,
  //       reliability: None,
  //       destination_order: None,
  //       history: None,
  //       resource_limits: None,
  //       lifespan: None,
  //     };

  //     let mut datareader = sub
  //       .create_datareader::<RandomData, CDRDeserializerAdapter<RandomData>>(
  //         topic_1.clone(),
  //         Some(somePolicies.clone()),
  //       )
  //       .unwrap();

  //     datareader.set_status_change_receiver(status_receiver_DataReader);
  //     datareader.set_reader_commander(reader_commander);
  //     data_readers.push(datareader);

  //     //new_reader.set_qos(&somePolicies).unwrap();
  //     new_reader.matched_writer_add(GUID::default(),
  // EntityId::UNKNOWN, vec![], vec![]);     reader_guids.
  // push(new_reader.guid().clone());     info!("\nSent reader number {}:
  // {:?}\n", i, &new_reader);     sender_add_reader.send(new_reader).
  // unwrap();     std::thread::sleep(Duration::from_millis(100));
  //   }
  //   thread::sleep(Duration::from_millis(100));

  //   let status = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status);
  //   assert_eq!(
  //     status.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(3, 3)
  //     )),
  //   );
  //   thread::sleep(Duration::from_millis(150));

  //   let status2 = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status2);
  //   assert_eq!(
  //     status2.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(6, 3)
  //     ))
  //   );

  //   let status3 = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status3);
  //   assert_eq!(
  //     status3.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(6, 0)
  //     ))
  //   );

  //   thread::sleep(Duration::from_millis(50));

  //   let status4 = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status4);
  //   assert_eq!(
  //     status4.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(7, 1)
  //     ))
  //   );

  //   info!("\nsending end token\n");
  //   sender_stop.send(0).unwrap();
  //   child.join().unwrap();
  // }
}
*/
