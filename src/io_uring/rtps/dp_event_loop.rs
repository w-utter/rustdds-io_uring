
//this can probably be moved in with discovery.

use crate::io_uring::timer::{timer_state, Timer};

struct Timers<T> {
    acknack: Timer<(), T>,
    cache_gc: Timer<(), T>,
}

impl Timers<timer_state::Uninit> {
    fn new() -> Self {
        let acknack = Timer::new_periodic((), PREEMPTIVE_ACKNACK_PERIOD);
        let cache_gc = Timer::new_periodic((), CACHE_CLEAN_PERIOD);

        Self {
            acknack,
            cache_gc,
        }
    }

    fn register(self, ring: &mut io_uring::IoUring, domain_id: u16) -> std::io::Result<Timers<timer_state::Init>> {
        let Self {
            acknack,
            cache_gc,
        } = self;

        use crate::io_uring::encoding::user_data::{Timer, BuiltinTimerVariant};

        let acknack = acknack.register(ring, domain_id, Timer::Builtin(BuiltinTimerVariant::AckNack))?;
        let cache_gc = cache_gc.register(ring, domain_id, Timer::Builtin(BuiltinTimerVariant::CacheCleaning))?;

        Ok(Timers {
            acknack,
            cache_gc,
        })
    }
}

use crate::io_uring::discovery::traffic::UdpListeners;

//TODO: a better word for this struct?
struct DPEventLoop2<TS, BS> {
    domain_info: DomainInfo,
    dds_cache: Arc<RwLock<DDSCache>>,
    discovery_db: Arc<RwLock<DiscoveryDB>>,

    message_receiver: MessageReceiver, // This contains our Readers

    timers: Timers<TS>,
    listeners: UdpListeners<BS>,

    // If security is enabled, this contains the security plugins
    #[cfg(feature = "security")]
    security_plugins_opt: Option<SecurityPluginsHandle>,

    writers: HashMap<EntityId, Writer<TS>>,
    udp_sender: Rc<UDPSender>,
}

use io_uring_buf_ring::buf_ring_state;

impl DPEventLoop2<timer_state::Uninit, buf_ring_state::Uninit> {
    fn new(
        domain_id: u16,
        domain_participant_guid: GUID,
        participant_guid_prefix: GuidPrefix,
        dds_cache: Arc<RwLock<DDSCache>>,
        discovery_db: Arc<RwLock<DiscoveryDB>>,

        #[cfg(feature = "security")]
        security_plugins_opt: Option<SecurityPluginsHandle>,

        writers: HashMap<EntityId, Writer<timer_state::Uninit>>,
        udp_sender: Rc<UDPSender>,
    ) -> Self {
        let (listeners, participant_id) = UdpListeners::try_new(domain_id).unwrap();
        let timers = Timers::new();

        let domain_info = DomainInfo {
            domain_id,
            participant_id,
            domain_participant_guid,
        };

        let message_receiver = MessageReceiver::new(
            participant_guid_prefix,
            #[cfg(feature = "security")]
            security_plugins_opt.clone(),
            #[cfg(not(feature = "security"))]
            None,
        );

        Self {
            domain_info,
            dds_cache,
            discovery_db,

            message_receiver,

            timers,
            listeners,

            // If security is enabled, this contains the security plugins
            #[cfg(feature = "security")]
            security_plugins_opt,

            writers,
            udp_sender,
        }
    }

    fn register(self, ring: &mut io_uring::IoUring) -> std::io::Result<DPEventLoop2<timer_state::Init, buf_ring_state::Init>> {
        // NOTE: this could be changed to Domain?

        //TODO: proper type -> encoding enum.


        todo!()
    }
}



impl DPEventLoop2<timer_state::Init, buf_ring_state::Init> {
    // returns if readers are pending
  fn handle_writer_command(&mut self, entity_id: EntityId, event: crate::io_uring::rtps::writer::WriterCommand, ring: &mut io_uring::IoUring) -> Option<impl Iterator<Item = EntityId> + use<'_>> {
    if let Some(writer) = self.writers.get_mut(&entity_id) {
        if matches!(writer.process_command(ring, event), None | Some(false)) {
            Some(writer.local_readers())
        } else {
            // waiting for acknowledgements
            // theres no change to update
            None
        }
    } else {
        None
    }
  }

  fn handle_reader_command(&mut self, entity_id: EntityId, event: crate::dds::with_key::ReaderCommand) {
      if let Some(reader) = self.message_receiver.reader_mut(entity_id) {
          reader.process_command(event)
      } else {
        error!("Event for unknown reader {entity_id:?}");
      }
  }

  fn handle_writer_timed_event(&mut self, entity_id: EntityId, event: crate::rtps::writer::TimedEvent, ring: &mut io_uring::IoUring) {
    if let Some(writer) = self.writers.get_mut(&entity_id) {
      writer.handle_timed_event(ring, event, self.domain_info.domain_id);
    } else {
      error!("Writer was not found with {:?}", entity_id);
    }
  }

  fn handle_reader_timed_event(&mut self, entity_id: EntityId, event: crate::rtps::reader::TimedEvent) {
    if let Some(reader) = self.message_receiver.reader_mut(entity_id) {
      reader.handle_timed_event(event);
    } else {
      error!("Reader was not found with {:?}", entity_id);
    }
  }

  fn handle_writer_acknack_action(&mut self, acknack_sender_prefix: GuidPrefix, acknack_submessage: &AckSubmessage, ring: &mut io_uring::IoUring) {
      let writer_guid = GUID::new_with_prefix_and_id(
        self.domain_info.domain_participant_guid.prefix,
        acknack_submessage.writer_id(),
      );

      if let Some(found_writer) = self.writers.get_mut(&writer_guid.entity_id) {
        if found_writer.is_reliable() {
          found_writer.handle_ack_nack(acknack_sender_prefix, &acknack_submessage, ring, self.domain_info.domain_id);
        }
      } else {
        // Note: when testing against FastDDS Shapes demo, this else branch is
        // repeatedly triggered. The resulting log entry contains the following
        // EntityId: {[0, 3, 0] EntityKind::WRITER_NO_KEY_BUILT_IN}.
        // In this case a writer cannot be found, because FastDDS sends
        // pre-emptive acknacks about a built-in topic defined in DDS Xtypes
        // specification, which RustDDS does not implement. So even though the acknack
        // cannot be handled, it is not a problem in this case.
        debug!(
          "Couldn't handle acknack/nackfrag! Did not find local RTPS writer with GUID: {:x?}",
          writer_guid
        );
      }
  }

  fn update_participant<'a, 'b>(&'a mut self, participant_guid_prefix: GuidPrefix, discovered_participant: &'b crate::discovery::SpdpDiscoveredParticipantData) -> (impl Iterator<Item = (DataReaderStatus, DomainParticipantStatusEvent)> + use<'a, 'b>, impl Iterator<Item = (DataWriterStatus, DomainParticipantStatusEvent)> + use<'a, 'b>) {
    debug!(
      "update_participant {:?} myself={}",
      participant_guid_prefix,
      participant_guid_prefix == self.domain_info.domain_participant_guid.prefix
    );

    /*
    let db = discovery_db_read(&self.discovery_db);
    // new Remote Participant discovered
    let discovered_participant =
      if let Some(dpd) = db.find_participant_proxy(participant_guid_prefix) {
        dpd
      } else {
        error!("Participant was updated, but DB does not have it. Strange.");
        return;
      };
    */

    // Select which builtin endpoints of the remote participant are updated to local
    // readers & writers
    #[cfg(not(feature = "security"))]
    let (readers_init_list, writers_init_list) = (
      STANDARD_BUILTIN_READERS_INIT_LIST.to_vec(),
      STANDARD_BUILTIN_WRITERS_INIT_LIST.to_vec(),
    );

    #[cfg(feature = "security")]
    let (readers_init_list, writers_init_list) = match &self.security_plugins_opt {
      None => {
        // No security enabled, just the standard endpoints
        let readers_init_list = STANDARD_BUILTIN_READERS_INIT_LIST.to_vec();
        let writers_init_list = STANDARD_BUILTIN_WRITERS_INIT_LIST.to_vec();

        (readers_init_list, writers_init_list)
      }
      Some(_handle) => {
        // Security enabled. The endpoints are selected based on the authentication
        // status of the remote participant
        let mut readers_init_list = vec![];
        let mut writers_init_list = vec![];

        match db.get_authentication_status(participant_guid_prefix) {
          Some(AuthenticationStatus::Authenticating) => {
            // Add just the stateless endpoint used for authentication
            readers_init_list.extend_from_slice(AUTHENTICATION_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(AUTHENTICATION_BUILTIN_WRITERS_INIT_LIST);
          }
          Some(AuthenticationStatus::Authenticated) => {
            // Match all builtin endpoints
            readers_init_list.extend_from_slice(STANDARD_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(STANDARD_BUILTIN_WRITERS_INIT_LIST);
            readers_init_list.extend_from_slice(SECURE_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(SECURE_BUILTIN_WRITERS_INIT_LIST);
          }
          Some(AuthenticationStatus::Unauthenticated) => {
            // Match only the regular builtin endpoints (see Security spec section 8.8.2.1)
            readers_init_list.extend_from_slice(STANDARD_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(STANDARD_BUILTIN_WRITERS_INIT_LIST);
          }
          _ => {
            // Not adding any endpoints when authentication status is Rejected
            // or None
          }
        }
        (readers_init_list, writers_init_list)
      }
    };

    let reader_updates = UpdatedReadersUpdates::new(&mut self.writers, readers_init_list.into_iter(), discovered_participant);

    let writer_updates = UpdatedWritersUpdates::new(&mut self.message_receiver.available_readers, writers_init_list.into_iter(), discovered_participant);

    debug!(
      "update_participant - finished for {:?}",
      participant_guid_prefix
    );
    (writer_updates, reader_updates)
  }



  fn remote_participant_lost(&mut self, participant_guid_prefix: GuidPrefix) -> (impl Iterator<Item = DataReaderStatus> + use<'_>, impl Iterator<Item = DataWriterStatus> + use<'_>) {
    // TODO: this has iterators (nested) as well.
    info!(
      "remote_participant_lost guid_prefix={:?}",
      &participant_guid_prefix
    );
    // Discovery has already removed Participant from Discovery DB
    // Now we have to remove any ReaderProxies and WriterProxies belonging
    // to that participant, so that we do not send messages to them anymore.

    let reader_status = self.writers.values_mut().flat_map(move |writer| writer.participant_lost(participant_guid_prefix));

    let writer_status = self.message_receiver.available_readers.values_mut().flat_map(move |reader| reader.participant_lost(participant_guid_prefix));

    #[cfg(feature = "security")]
    if let Some(security_plugins_handle) = &self.security_plugins_opt {
      security_plugins_handle
        .get_plugins()
        .unregister_remote_participant(&participant_guid_prefix)
        .unwrap_or_else(|e| error!("{e}"));
    }
    (writer_status, reader_status)
  }

  fn remote_reader_discovered<'a, 'b>(&'a mut self, remote_reader: &'b DiscoveredReaderData) -> impl Iterator<Item = (DataWriterStatus, DomainParticipantStatusEvent)> + use<'a, 'b> {
        DiscoveredReaderUpdates::new(
            self.writers.values_mut(),
            |writer| remote_reader.subscription_topic_data.topic_name() == writer.topic_name(),
            remote_reader,
        )
  }

  fn remote_reader_lost(&mut self, reader_guid: GUID) -> impl Iterator<Item = DataWriterStatus> + use<'_> {
    LostReaderUpdates::new(self.writers.values_mut(), reader_guid)
  }

  fn remote_writer_discovered<'a, 'b>(&'a mut self, remote_writer: &'b DiscoveredWriterData) -> impl Iterator<Item = (DataReaderStatus, DomainParticipantStatusEvent)> + use<'a, 'b> {
    // update writer proxies in local readers
    DiscoveredWriterUpdates::new(
        self.message_receiver.available_readers.values_mut(),
        |reader| &remote_writer.publication_topic_data.topic_name == reader.topic_name(),
        remote_writer,
    )
  }




  fn remote_writer_lost(&mut self, writer_guid: GUID) -> impl Iterator<Item = DataReaderStatus> + use<'_> {
    LostWriterUpdates::new(self.message_receiver.available_readers.values_mut(), writer_guid)
  }


  fn add_local_reader(&mut self, reader_ing: ReaderIngredients, ring: &mut io_uring::IoUring) -> std::io::Result<()> {
    let domain_id = self.domain_info.domain_id;

    let new_reader = Reader::new(reader_ing, self.udp_sender.clone()).register(ring, domain_id)?;

    self.message_receiver.add_reader(new_reader);
    Ok(())
  }

  fn remove_local_reader(&mut self, reader_guid: GUID) {
    if let Some(old_reader) = self.message_receiver.remove_reader(reader_guid) {
      #[cfg(feature = "security")]
      if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
        // Security is enabled. Unregister the reader with the crypto plugin.
        // Currently the unregister method is called for every reader, and errors are
        // ignored. If this is inconvenient, add a check if the reader has been
        // registered/is secure, and unregister only if it is so
        let _ = plugins_handle
          .get_plugins()
          .unregister_local_reader(&reader_guid);
      }
    } else {
      warn!("Tried to remove nonexistent Reader {reader_guid:?}");
    }
  }

  fn add_local_writer(&mut self, writer_ing: WriterIngredients, ring: &mut io_uring::IoUring) -> std::io::Result<()> {
    let entity_id = writer_ing.guid.entity_id;
    let domain_id = self.domain_info.domain_id;

    let new_writer = Writer::new(writer_ing, self.udp_sender.clone()).register(ring, domain_id)?;

    self.writers.insert(entity_id, new_writer);
    Ok(())
  }

  fn remove_local_writer(&mut self, writer_guid: &GUID) {
    if let Some(w) = self.writers.remove(&writer_guid.entity_id) {
      #[cfg(feature = "security")]
      if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
        // Security is enabled. Unregister the writer with the crypto plugin.
        // Currently the unregister method is called for every writer, and errors are
        // ignored. If this is inconvenient, add a check if the writer has been
        // registered/is secure, and unregister only if it is so
        let _ = plugins_handle
          .get_plugins()
          .unregister_local_writer(writer_guid);
      }

    }
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
        self.update_participant(remote_guidp, |_,_| (), |_,_| ());
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
        self.update_participant(remote_guidp, |_,_| (), |_,_| ());
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
}

use std::{
  collections::HashMap,
  rc::Rc,
  sync::{Arc, RwLock},
  time::{Duration, Instant},
};

use log::{debug, error, info, trace, warn};
use mio_06::{Event, Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel as mio_channel;

use crate::{
  dds::{
    qos::policy,
    statusevents::{DomainParticipantStatusEvent, StatusChannelSender},
  },
  discovery::{
    discovery::DiscoveryCommand,
    discovery_db::{discovery_db_read, DiscoveryDB},
    sedp_messages::{DiscoveredReaderData, DiscoveredWriterData},
  },
  messages::submessages::submessages::AckSubmessage,
  //network::{udp_listener::UDPListener, udp_sender::UDPSender},
  polling::new_simple_timer,
  qos::HasQoSPolicy,
  rtps::{
    constant::*,
    rtps_reader_proxy::RtpsReaderProxy,
    rtps_writer_proxy::RtpsWriterProxy,
  },
  structure::{
    dds_cache::DDSCache,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, TokenDecode, GUID},
  },
};

use crate::io_uring::rtps::writer::{Writer, WriterIngredients};
use crate::io_uring::rtps::message_receiver::MessageReceiver;
use crate::io_uring::rtps::reader::{Reader, ReaderIngredients};
use crate::io_uring::network::udp_sender::UDPSender;
use crate::io_uring::network::udp_listener::UDPListener;

#[cfg(feature = "security")]
use crate::{
  discovery::secure_discovery::AuthenticationStatus,
  security::{security_plugins::SecurityPluginsHandle, EndpointSecurityInfo},
  security_warn,
};
#[cfg(not(feature = "security"))]
use crate::no_security::security_plugins::SecurityPluginsHandle;

use crate::rtps::dp_event_loop::{DomainInfo, EventLoopCommand};

pub struct DPEventLoop<S> {
  domain_info: DomainInfo,
  poll: Poll,
  dds_cache: Arc<RwLock<DDSCache>>,
  discovery_db: Arc<RwLock<DiscoveryDB>>,
  udp_listeners: HashMap<Token, UDPListener<buf_ring_state::Init>>,
  message_receiver: MessageReceiver, // This contains our Readers

  // If security is enabled, this contains the security plugins
  #[cfg(feature = "security")]
  security_plugins_opt: Option<SecurityPluginsHandle>,

  // Adding readers
  add_reader_receiver: TokenReceiverPair<ReaderIngredients>,
  remove_reader_receiver: TokenReceiverPair<GUID>,

  // Writers
  add_writer_receiver: TokenReceiverPair<WriterIngredients>,
  remove_writer_receiver: TokenReceiverPair<GUID>,
  stop_poll_receiver: mio_channel::Receiver<EventLoopCommand>,
  // GuidPrefix sent in this channel needs to be RTPSMessage source_guid_prefix. Writer needs this
  // to locate RTPSReaderProxy if negative acknack.
  ack_nack_receiver: mio_channel::Receiver<(GuidPrefix, AckSubmessage)>,

  writers: HashMap<EntityId, Writer<S>>,
  udp_sender: Rc<UDPSender>,

  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,

  discovery_update_notification_receiver: mio_channel::Receiver<DiscoveryNotificationType>,
  #[cfg(feature = "security")]
  discovery_command_sender: mio_channel::SyncSender<DiscoveryCommand>,
}

impl DPEventLoop<timer_state::Uninit> {
  // This pub(crate) , because it should be constructed only by DomainParticipant.
  #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
  pub(crate) fn new(
    domain_info: DomainInfo,
    dds_cache: Arc<RwLock<DDSCache>>,
    udp_listeners: HashMap<Token, UDPListener<buf_ring_state::Init>>,
    discovery_db: Arc<RwLock<DiscoveryDB>>,
    participant_guid_prefix: GuidPrefix,
    add_reader_receiver: TokenReceiverPair<ReaderIngredients>,
    remove_reader_receiver: TokenReceiverPair<GUID>,
    add_writer_receiver: TokenReceiverPair<WriterIngredients>,
    remove_writer_receiver: TokenReceiverPair<GUID>,
    stop_poll_receiver: mio_channel::Receiver<EventLoopCommand>,
    discovery_update_notification_receiver: mio_channel::Receiver<DiscoveryNotificationType>,
    _discovery_command_sender: mio_channel::SyncSender<DiscoveryCommand>,
    spdp_liveness_sender: mio_channel::SyncSender<GuidPrefix>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
    security_plugins_opt: Option<SecurityPluginsHandle>,
  ) -> Self {
    #[cfg(not(feature = "security"))]
    let _dummy = _discovery_command_sender;

    let poll = Poll::new().expect("Unable to create new poll.");
    let (acknack_sender, acknack_receiver) =
      mio_channel::sync_channel::<(GuidPrefix, AckSubmessage)>(100);
    let mut udp_listeners = udp_listeners;
    /*
    for (token, listener) in &mut udp_listeners {
      poll
        .register(
          listener.mio_socket(),
          *token,
          Ready::readable(),
          PollOpt::edge(),
        )
        .expect("Failed to register listener.");
    }
    */

    poll
      .register(
        &add_reader_receiver.receiver,
        add_reader_receiver.token,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register reader adder.");

    poll
      .register(
        &remove_reader_receiver.receiver,
        remove_reader_receiver.token,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register reader remover.");
    poll
      .register(
        &add_writer_receiver.receiver,
        add_writer_receiver.token,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register add writer channel");

    poll
      .register(
        &remove_writer_receiver.receiver,
        remove_writer_receiver.token,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register remove writer channel");

    poll
      .register(
        &stop_poll_receiver,
        STOP_POLL_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register stop poll channel");

    poll
      .register(
        &acknack_receiver,
        ACKNACK_MESSAGE_TO_LOCAL_WRITER_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register AckNack submessage sending from MessageReceiver to DPEventLoop");

    poll
      .register(
        &discovery_update_notification_receiver,
        DISCOVERY_UPDATE_NOTIFICATION_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Failed to register reader update notification.");

    // port number 0 means OS chooses an available port number.
    let udp_sender = UDPSender::new(0).expect("UDPSender construction fail"); // TODO

    #[cfg(not(feature = "security"))]
    let security_plugins_opt = security_plugins_opt.and(None); // make sure it is None an consume value

    Self {
      domain_info,
      poll,
      dds_cache,
      discovery_db,
      udp_listeners,
      udp_sender: Rc::new(udp_sender),
      message_receiver: MessageReceiver::new(
        participant_guid_prefix,
        security_plugins_opt.clone(),
      ),
      #[cfg(feature = "security")]
      security_plugins_opt,
      add_reader_receiver,
      remove_reader_receiver,
      add_writer_receiver,
      remove_writer_receiver,
      stop_poll_receiver,
      writers: HashMap::new(),
      ack_nack_receiver: acknack_receiver,
      discovery_update_notification_receiver,
      participant_status_sender,
      #[cfg(feature = "security")]
      discovery_command_sender: _discovery_command_sender,
    }
  }
}

impl DPEventLoop<timer_state::Init> {
    /*
  pub fn event_loop(self, ring: &mut io_uring::IoUring) {
    let mut events = Events::with_capacity(16); // too small capacity just delays events to next poll

    let mut acknack_timer = new_simple_timer();
    acknack_timer.set_timeout(PREEMPTIVE_ACKNACK_PERIOD, ());

    let mut cache_gc_timer = new_simple_timer();
    cache_gc_timer.set_timeout(CACHE_CLEAN_PERIOD, ());

    self
      .poll
      .register(
        &acknack_timer,
        DPEV_ACKNACK_TIMER_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();
    self
      .poll
      .register(
        &cache_gc_timer,
        DPEV_CACHE_CLEAN_TIMER_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();
    let mut poll_alive = Instant::now();
    let mut ev_wrapper = self;
    let mut preparing_to_stop = false;

    //TODO: add timers? and structure this better.

    // loop starts here
    loop {
      ev_wrapper
        .poll
        .poll(&mut events, Some(Duration::from_millis(2000)))
        .expect("Failed in waiting of poll.");

      // liveness watchdog
      let now = Instant::now();
      if now > poll_alive + Duration::from_secs(2) {
        debug!("Poll loop alive");
        poll_alive = now;
      }

      if events.is_empty() {
        debug!("dp_event_loop idling.");
      } else {
        for event in events.iter() {
          match EntityId::from_token(event.token()) {
            TokenDecode::FixedToken(fixed_token) => match fixed_token {
              STOP_POLL_TOKEN => {
                use std::sync::mpsc::TryRecvError;
                // Read commands from the stop receiver until none left or quitting
                // It would be nice turn the receiver into an iterator and avoid using the
                // boolean..
                let mut try_recv_more = true;
                while try_recv_more {
                  match ev_wrapper.stop_poll_receiver.try_recv() {
                    Ok(EventLoopCommand::PrepareStop) => {
                      info!("dp_event_loop preparing to stop.");
                      preparing_to_stop = true;
                      // There could still be an EventLoopCommand::Stop coming. Keep on receiving.
                      try_recv_more = true;
                    }
                    Ok(EventLoopCommand::Stop) => {
                      info!("Stopping dp_event_loop");
                      return;
                    }
                    Err(err) => match err {
                      TryRecvError::Empty => {
                        try_recv_more = false;
                      }
                      TryRecvError::Disconnected => {
                        error!(
                          "Application thread has exited abnormally. Stopping RustDDS event loop."
                        );
                        return;
                      }
                    },
                  }
                }
              }
              DISCOVERY_LISTENER_TOKEN
              | DISCOVERY_MUL_LISTENER_TOKEN
              | USER_TRAFFIC_LISTENER_TOKEN
              | USER_TRAFFIC_MUL_LISTENER_TOKEN => {
                let udp_messages = ev_wrapper
                  .udp_listeners
                  .get_mut(&event.token())
                  .map_or_else(
                    || {
                      error!("No listener with token {:?}", &event.token());
                      vec![]
                    },
                    UDPListener::messages,
                  );
                for packet in udp_messages {
                  ev_wrapper.message_receiver.handle_received_packet(&packet, ring);
                }
              }
              ADD_READER_TOKEN | REMOVE_READER_TOKEN => {
                ev_wrapper.handle_reader_action(&event);
              }
              ADD_WRITER_TOKEN | REMOVE_WRITER_TOKEN => {
                ev_wrapper.handle_writer_action(&event);
              }
              ACKNACK_MESSAGE_TO_LOCAL_WRITER_TOKEN => {
                ev_wrapper.handle_writer_acknack_action(&event, ring);
              }
              DISCOVERY_UPDATE_NOTIFICATION_TOKEN => {
                while let Ok(dnt) = ev_wrapper.discovery_update_notification_receiver.try_recv() {
                  use DiscoveryNotificationType::*;
                  match dnt {
                    WriterUpdated {
                      discovered_writer_data,
                    } => {
                        // TODO: propagate errors.
                        for _ in ev_wrapper.remote_writer_discovered(&discovered_writer_data) {
                        }
                    }

                    WriterLost { writer_guid } => {
                        // TODO: propagate errors.
                        for _ in ev_wrapper.remote_writer_lost(writer_guid) {
                        }
                    }

                    ReaderUpdated {
                      discovered_reader_data,
                    } => {
                        // TODO: propagate errors.
                        for _ in ev_wrapper.remote_reader_discovered(&discovered_reader_data) {
                        }
                    }

                    ReaderLost { reader_guid } => {
                        // TODO: propagate errors.
                        for _ in ev_wrapper.remote_reader_lost(reader_guid) {

                        }
                    }

                    ParticipantUpdated { guid_prefix } => {
                        //TODO: propagate errors
                      ev_wrapper.update_participant(guid_prefix, |_,_| (), |_,_| ());
                    }

                    ParticipantLost { guid_prefix } => {
                        let (readers, writers) = ev_wrapper.remote_participant_lost(guid_prefix);
                        //TODO: propagate this.
                        for _ in readers {
                        }

                        for _ in writers {
                        }
                    }

                    AssertTopicLiveliness {
                      writer_guid,
                      manual_assertion,
                    } => {
                      ev_wrapper
                        .writers
                        .get_mut(&writer_guid.entity_id)
                        .map(|w| w.handle_heartbeat_tick(manual_assertion, ring));
                    }

                    #[cfg(feature = "security")]
                    ParticipantAuthenticationStatusChanged { guid_prefix } => {
                      ev_wrapper.on_remote_participant_authentication_status_changed(guid_prefix);
                    }
                  }
                }
              }
              DPEV_ACKNACK_TIMER_TOKEN => {
                ev_wrapper.message_receiver.send_preemptive_acknacks(ring);
                acknack_timer.set_timeout(PREEMPTIVE_ACKNACK_PERIOD, ());
              }
              DPEV_CACHE_CLEAN_TIMER_TOKEN => {
                debug!("Clean DDSCache on timer");
                ev_wrapper.dds_cache.write().unwrap().garbage_collect();
                cache_gc_timer.set_timeout(CACHE_CLEAN_PERIOD, ());
              }

              fixed_unknown => {
                error!(
                  "Unknown event.token {:?} = 0x{:x?} , decoded as {:?}",
                  event.token(),
                  event.token().0,
                  fixed_unknown
                );
              }
            },

            // Commands/actions
            TokenDecode::Entity(eid) => {
              if eid.kind().is_reader() {
                  /* TODO:
                ev_wrapper.message_receiver.reader_mut(eid).map_or_else(
                  || {
                    if !preparing_to_stop {
                      error!("Event for unknown reader {eid:?}");
                    }
                  },
                  Reader::process_command,
                );
                */
              } else if eid.kind().is_writer() {
                  /* TODO:
                let local_readers = match ev_wrapper.writers.get_mut(&eid) {
                  None => {
                    if !preparing_to_stop {
                      error!("Event for unknown writer {eid:?}");
                    };
                    vec![]
                  }
                  Some(writer) => {
                    // Writer will record data to DDSCache and send it out.
                    writer.process_writer_command();
                    writer.local_readers()
                  }
                };
                // Notify local (same participant) readers that new data is available in the
                // cache.
                ev_wrapper
                  .message_receiver
                  .notify_data_to_readers(local_readers);
                */
              } else {
                error!("Entity Event for unknown EntityKind {eid:?}");
              }
            }

            // Timed Actions
            TokenDecode::AltEntity(eid) => {
              if eid.kind().is_reader() {
                ev_wrapper.handle_reader_timed_event(eid);
              } else if eid.kind().is_writer() {
                ev_wrapper.handle_writer_timed_event(eid);
              } else {
                error!("AltEntity Event for unknown EntityKind {eid:?}");
              }
            }
          }
        } // for
      } // if
    } // loop
  } // fn
  */

  #[cfg(feature = "security")] // Currently used only with security.
                               // Just remove attribute if used also without.
  fn send_participant_status(&self, event: DomainParticipantStatusEvent) {
    self
      .participant_status_sender
      .try_send(event)
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));
  }

  /*
  fn handle_reader_action(&mut self, event: &Event) {
    match event.token() {
      ADD_READER_TOKEN => {
        trace!("add reader(s)");
        while let Ok(new_reader_ing) = self.add_reader_receiver.receiver.try_recv() {
          self.add_local_reader(new_reader_ing);
        }
      }
      REMOVE_READER_TOKEN => {
        while let Ok(old_reader_guid) = self.remove_reader_receiver.receiver.try_recv() {
          self.remove_local_reader(old_reader_guid);
        }
      }
      _ => {}
    }
  }

  fn handle_writer_action(&mut self, event: &Event) {
    match event.token() {
      ADD_WRITER_TOKEN => {
        while let Ok(new_writer_ingredients) = self.add_writer_receiver.receiver.try_recv() {
          self.add_local_writer(new_writer_ingredients);
        }
      }
      REMOVE_WRITER_TOKEN => {
        while let Ok(writer_guid) = &self.remove_writer_receiver.receiver.try_recv() {
          self.remove_local_writer(writer_guid);
        }
      }
      other => error!("Expected writer action token, got {:?}", other),
    }
  }
  */

  /*
  /// Writer timed events can be heartbeats or cache cleaning events.
  /// events are distinguished by TimerMessageType which is send via mio
  /// channel. Channel token in
  fn handle_writer_timed_event(&mut self, entity_id: EntityId) {
      /* TODO:
    if let Some(writer) = self.writers.get_mut(&entity_id) {
      writer.handle_timed_event();
    } else {
      error!("Writer was not found with {:?}", entity_id);
    }
    */
  }

  fn handle_reader_timed_event(&mut self, entity_id: EntityId) {
      /* TODO:
    if let Some(reader) = self.message_receiver.reader_mut(entity_id) {
      reader.handle_timed_event();
    } else {
      error!("Reader was not found with {:?}", entity_id);
    }
    */
  }

  fn handle_writer_acknack_action(&mut self, _event: &Event, ring: &mut io_uring::IoUring) {
    while let Ok((acknack_sender_prefix, acknack_submessage)) = self.ack_nack_receiver.try_recv() {
      let writer_guid = GUID::new_with_prefix_and_id(
        self.domain_info.domain_participant_guid.prefix,
        acknack_submessage.writer_id(),
      );
      if let Some(found_writer) = self.writers.get_mut(&writer_guid.entity_id) {
        if found_writer.is_reliable() {
          found_writer.handle_ack_nack(acknack_sender_prefix, &acknack_submessage, ring);
        }
      } else {
        // Note: when testing against FastDDS Shapes demo, this else branch is
        // repeatedly triggered. The resulting log entry contains the following
        // EntityId: {[0, 3, 0] EntityKind::WRITER_NO_KEY_BUILT_IN}.
        // In this case a writer cannot be found, because FastDDS sends
        // pre-emptive acknacks about a built-in topic defined in DDS Xtypes
        // specification, which RustDDS does not implement. So even though the acknack
        // cannot be handled, it is not a problem in this case.
        debug!(
          "Couldn't handle acknack/nackfrag! Did not find local RTPS writer with GUID: {:x?}",
          writer_guid
        );
        continue;
      }
    }
  }

  fn update_participant(&mut self, participant_guid_prefix: GuidPrefix, mut on_reader: impl FnMut(DataReaderStatus, DomainParticipantStatusEvent), mut on_writer: impl FnMut(DataWriterStatus, DomainParticipantStatusEvent)) {
      // TODO: return (impl Iterator<..>, impl Iterator<..>)
      // for the readers and writers, respectively.
      // (its the update_*_proxy thats giving the iterations.)
    debug!(
      "update_participant {:?} myself={}",
      participant_guid_prefix,
      participant_guid_prefix == self.domain_info.domain_participant_guid.prefix
    );

    let db = discovery_db_read(&self.discovery_db);
    // new Remote Participant discovered
    let discovered_participant =
      if let Some(dpd) = db.find_participant_proxy(participant_guid_prefix) {
        dpd
      } else {
        error!("Participant was updated, but DB does not have it. Strange.");
        return;
      };

    // Select which builtin endpoints of the remote participant are updated to local
    // readers & writers
    #[cfg(not(feature = "security"))]
    let (readers_init_list, writers_init_list) = (
      STANDARD_BUILTIN_READERS_INIT_LIST.to_vec(),
      STANDARD_BUILTIN_WRITERS_INIT_LIST.to_vec(),
    );

    #[cfg(feature = "security")]
    let (readers_init_list, writers_init_list) = match &self.security_plugins_opt {
      None => {
        // No security enabled, just the standard endpoints
        let readers_init_list = STANDARD_BUILTIN_READERS_INIT_LIST.to_vec();
        let writers_init_list = STANDARD_BUILTIN_WRITERS_INIT_LIST.to_vec();

        (readers_init_list, writers_init_list)
      }
      Some(_handle) => {
        // Security enabled. The endpoints are selected based on the authentication
        // status of the remote participant
        let mut readers_init_list = vec![];
        let mut writers_init_list = vec![];

        match db.get_authentication_status(participant_guid_prefix) {
          Some(AuthenticationStatus::Authenticating) => {
            // Add just the stateless endpoint used for authentication
            readers_init_list.extend_from_slice(AUTHENTICATION_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(AUTHENTICATION_BUILTIN_WRITERS_INIT_LIST);
          }
          Some(AuthenticationStatus::Authenticated) => {
            // Match all builtin endpoints
            readers_init_list.extend_from_slice(STANDARD_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(STANDARD_BUILTIN_WRITERS_INIT_LIST);
            readers_init_list.extend_from_slice(SECURE_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(SECURE_BUILTIN_WRITERS_INIT_LIST);
          }
          Some(AuthenticationStatus::Unauthenticated) => {
            // Match only the regular builtin endpoints (see Security spec section 8.8.2.1)
            readers_init_list.extend_from_slice(STANDARD_BUILTIN_READERS_INIT_LIST);
            writers_init_list.extend_from_slice(STANDARD_BUILTIN_WRITERS_INIT_LIST);
          }
          _ => {
            // Not adding any endpoints when authentication status is Rejected
            // or None
          }
        }
        (readers_init_list, writers_init_list)
      }
    };

    // Update local writers


    let reader_updates = UpdatedReadersUpdates::new(&mut self.writers, readers_init_list.into_iter(), discovered_participant);

    for (writer_status, participant_status) in reader_updates {
        on_writer(writer_status, participant_status)
    }

    let writer_updates = UpdatedWritersUpdates::new(&mut self.message_receiver.available_readers, writers_init_list.into_iter(), discovered_participant);

    for (reader_status, participant_status) in writer_updates {
        on_reader(reader_status, participant_status)
    }

    debug!(
      "update_participant - finished for {:?}",
      participant_guid_prefix
    );

    //Some((reader_updates, writer_updates))
  }

  fn remote_participant_lost(&mut self, participant_guid_prefix: GuidPrefix) -> (impl Iterator<Item = DataReaderStatus> + use<'_>, impl Iterator<Item = DataWriterStatus> + use<'_>) {
    // TODO: this has iterators (nested) as well.
    info!(
      "remote_participant_lost guid_prefix={:?}",
      &participant_guid_prefix
    );
    // Discovery has already removed Participant from Discovery DB
    // Now we have to remove any ReaderProxies and WriterProxies belonging
    // to that participant, so that we do not send messages to them anymore.

    let reader_status = self.writers.values_mut().flat_map(move |writer| writer.participant_lost(participant_guid_prefix));

    let writer_status = self.message_receiver.available_readers.values_mut().flat_map(move |reader| reader.participant_lost(participant_guid_prefix));

    #[cfg(feature = "security")]
    if let Some(security_plugins_handle) = &self.security_plugins_opt {
      security_plugins_handle
        .get_plugins()
        .unregister_remote_participant(&participant_guid_prefix)
        .unwrap_or_else(|e| error!("{e}"));
    }
    (writer_status, reader_status)
  }

  fn remote_reader_discovered<'a, 'b>(&'a mut self, remote_reader: &'b DiscoveredReaderData) -> impl Iterator<Item = (DataWriterStatus, DomainParticipantStatusEvent)> + use<'a, 'b> {
        DiscoveredReaderUpdates::new(
            self.writers.values_mut(),
            |writer| remote_reader.subscription_topic_data.topic_name() == writer.topic_name(),
            remote_reader,
        )
  }

  fn remote_reader_lost(&mut self, reader_guid: GUID) -> impl Iterator<Item = DataWriterStatus> + use<'_> {
    LostReaderUpdates::new(self.writers.values_mut(), reader_guid)
  }

  fn remote_writer_discovered<'a, 'b>(&'a mut self, remote_writer: &'b DiscoveredWriterData) -> impl Iterator<Item = (DataReaderStatus, DomainParticipantStatusEvent)> + use<'a, 'b> {
    // update writer proxies in local readers
    DiscoveredWriterUpdates::new(
        self.message_receiver.available_readers.values_mut(),
        |reader| &remote_writer.publication_topic_data.topic_name == reader.topic_name(),
        remote_writer,
    )
  }

  fn remote_writer_lost(&mut self, writer_guid: GUID) -> impl Iterator<Item = DataReaderStatus> + use<'_> {
    LostWriterUpdates::new(self.message_receiver.available_readers.values_mut(), writer_guid)
  }

  fn add_local_reader(&mut self, reader_ing: ReaderIngredients) {
      /* TODO:
    let timer = new_simple_timer();
    self
      .poll
      .register(
        &timer,
        reader_ing.alt_entity_token(),
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Reader timer channel registration failed!");

    let mut new_reader = Reader::new(
      reader_ing,
      self.udp_sender.clone(),
      timer,
      self.participant_status_sender.clone(),
    );

    // Non-timed action polling
    self
      .poll
      .register(
        &new_reader.data_reader_command_receiver,
        new_reader.entity_token(),
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Reader command channel registration failed!!!");

    new_reader.set_requested_deadline_check_timer();
    trace!("Add reader: {:?}", new_reader);
    self.message_receiver.add_reader(new_reader);
    */
  }

  fn remove_local_reader(&mut self, reader_guid: GUID) {
    if let Some(old_reader) = self.message_receiver.remove_reader(reader_guid) {
        /* TODO:
      self
        .poll
        .deregister(&old_reader.timed_event_timer)
        .unwrap_or_else(|e| error!("Cannot deregister Reader timed_event_timer: {e:?}"));
      self
        .poll
        .deregister(&old_reader.data_reader_command_receiver)
        .unwrap_or_else(|e| {
          error!("Cannot deregister data_reader_command_receiver: {e:?}");
        });
        */

      #[cfg(feature = "security")]
      if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
        // Security is enabled. Unregister the reader with the crypto plugin.
        // Currently the unregister method is called for every reader, and errors are
        // ignored. If this is inconvenient, add a check if the reader has been
        // registered/is secure, and unregister only if it is so
        let _ = plugins_handle
          .get_plugins()
          .unregister_local_reader(&reader_guid);
      }
    } else {
      warn!("Tried to remove nonexistent Reader {reader_guid:?}");
    }
  }

  fn add_local_writer(&mut self, writer_ing: WriterIngredients) {
      /*
    let timer = new_simple_timer();
    self
      .poll
      .register(
        &timer,
        writer_ing.alt_entity_token(),
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Writer heartbeat timer channel registration failed!!");

    TODO: might need to bring back the Rc around the sender
    let new_writer = Writer::new(
      writer_ing,
      self.udp_sender.clone(),
      timer,
      self.participant_status_sender.clone(),
    );

    self
      .poll
      .register(
        &new_writer.writer_command_receiver,
        new_writer.entity_token(),
        Ready::readable(),
        PollOpt::edge(),
      )
      .expect("Writer command channel registration failed!!");

    self.writers.insert(new_writer.guid().entity_id, new_writer);
    */
  }

  fn remove_local_writer(&mut self, writer_guid: &GUID) {
    if let Some(w) = self.writers.remove(&writer_guid.entity_id) {
        /*
      self
        .poll
        .deregister(&w.writer_command_receiver)
        .unwrap_or_else(|e| error!("Deregister fail (writer command rec) {e:?}"));
      self
        .poll
        .deregister(&w.timed_event_timer)
        .unwrap_or_else(|e| error!("Deregister fail (writer timer) {e:?}"));
        */

      #[cfg(feature = "security")]
      if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
        // Security is enabled. Unregister the writer with the crypto plugin.
        // Currently the unregister method is called for every writer, and errors are
        // ignored. If this is inconvenient, add a check if the writer has been
        // registered/is secure, and unregister only if it is so
        let _ = plugins_handle
          .get_plugins()
          .unregister_local_writer(writer_guid);
      }
    }
  }

  #[cfg(feature = "security")]
  fn on_remote_participant_authentication_status_changed(&mut self, remote_guidp: GuidPrefix) {
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
        self.update_participant(remote_guidp, |_,_| (), |_,_| ());
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
        self.update_participant(remote_guidp, |_,_| (), |_,_| ());
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
  */
}

struct DiscoveredReaderUpdates<'a, 'b, F> {
    remote_writers: std::iter::Filter<std::collections::hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>, F>,
    remote_reader: &'b DiscoveredReaderData,
    #[cfg(feature = "security")]
    security_plugins: Option<&'a SecurityPluginsHandle>,
}

impl <'a, 'b, F: FnMut(&&'a mut Writer<timer_state::Init>) -> bool> DiscoveredReaderUpdates<'a, 'b, F> {
    fn new(remote_writers: std::collections::hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>, filter: F, remote_reader: &'b DiscoveredReaderData,
    #[cfg(feature = "security")]
    security_plugins: Option<&'a SecurityPluginsHandle>,
           ) -> Self {
        Self {
            remote_writers: remote_writers.filter(filter),
            remote_reader,
            #[cfg(feature = "security")]
            security_plugins,
        }
    }
}

use crate::{DataWriterStatus, DataReaderStatus};

impl <'a, F> Iterator for DiscoveredReaderUpdates<'a, '_, F>
where
    F: FnMut(&&'a mut Writer<timer_state::Init>) -> bool
{
    type Item = (DataWriterStatus, DomainParticipantStatusEvent);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(writer) = self.remote_writers.next() {
            #[cfg(not(feature = "security"))]
            let match_to_reader = true;
            #[cfg(feature = "security")]
            let match_to_reader = if let Some(plugins_handle) = self.security_plugins {
                todo!()
            } else {
                true
            };

            if !match_to_reader {
                continue;
            }

            let requested_qos = self.remote_reader.subscription_topic_data.qos();

            if let Some(ret) = writer.update_reader_proxy(&RtpsReaderProxy::from_discovered_reader_data(self.remote_reader, &[], &[]), &requested_qos) {
                return Some(ret);
            } else {
                continue;
            }
        }
        None
    }
}


struct LostReaderUpdates<'a> {
    remote_writers: std::collections::hash_map::ValuesMut<'a, EntityId, Writer<timer_state::Init>>,
    reader_guid: GUID,
}

impl <'a> LostReaderUpdates<'a> {
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
                return Some(ret)
            } else {
                continue;
            }
        }
        None
    }
}



struct DiscoveredWriterUpdates<'a, 'b, F> {
    remote_readers: std::iter::Filter<std::collections::btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>, F>,
    remote_writer: &'b DiscoveredWriterData,
    #[cfg(feature = "security")]
    security_plugins: Option<&'a SecurityPluginsHandle>,
}

impl <'a, 'b, F: FnMut(&&'a mut Reader<timer_state::Init>) -> bool> DiscoveredWriterUpdates<'a, 'b, F> {
    fn new(remote_readers: std::collections::btree_map::ValuesMut<'a, EntityId, Reader<timer_state::Init>>, filter: F, remote_writer: &'b DiscoveredWriterData,
    #[cfg(feature = "security")]
    security_plugins: Option<&'a SecurityPluginsHandle>,
           ) -> Self {
        Self {
            remote_readers: remote_readers.filter(filter),
            remote_writer,
            #[cfg(feature = "security")]
            security_plugins,
        }
    }
}

impl <'a, F> Iterator for DiscoveredWriterUpdates<'a, '_, F>
where
    F: FnMut(&&'a mut Reader<timer_state::Init>) -> bool
{
    type Item = (DataReaderStatus, DomainParticipantStatusEvent);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(reader) = self.remote_readers.next() {
            #[cfg(not(feature = "security"))]
            let match_to_writer = true;
            #[cfg(feature = "security")]
            let match_to_writer = if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
                todo!()
            } else {
                true
            };

            if !match_to_writer {
                continue;
            }

            let offered_qos = self.remote_writer.publication_topic_data.qos();

            if let Some(ret) = reader.update_writer_proxy(
                RtpsWriterProxy::from_discovered_writer_data(self.remote_writer, &[], &[]),
                &offered_qos,
              ) {
                return Some(ret)
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

impl <'a> LostWriterUpdates<'a> {
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

struct UpdatedWritersUpdates<'a, 'b> {
    available_readers: &'a mut std::collections::BTreeMap<EntityId, Reader<timer_state::Init>>,
    writers_init_list: std::vec::IntoIter<(EntityId, EntityId, u32)>,
    discovered_participant: &'b crate::discovery::SpdpDiscoveredParticipantData,
}

impl <'a, 'b> UpdatedWritersUpdates<'a, 'b> {
    fn new(
        available_readers: &'a mut std::collections::BTreeMap<EntityId, Reader<timer_state::Init>>,
        writers_init_list: std::vec::IntoIter<(EntityId, EntityId, u32)>,
        discovered_participant: &'b crate::discovery::SpdpDiscoveredParticipantData,
        ) -> Self {
        Self {
            available_readers,
            writers_init_list,
            discovered_participant,
        }
    }
}

impl Iterator for UpdatedWritersUpdates<'_, '_> {
    type Item = (DataReaderStatus, DomainParticipantStatusEvent);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((writer_eid, reader_eid, endpoint)) = self.writers_init_list.next() {
            let Some(reader) = self.available_readers.get_mut(&reader_eid) else {
                continue;
            };

            debug!("try update_discovery_reader - {:?}", reader.topic_name());

            if !self.discovered_participant
              .available_builtin_endpoints
              .contains(endpoint)
            {
                continue;
            }

            let wp = self.discovered_participant.as_writer_proxy(true, Some(writer_eid));

            let qos = reader.qos();

            let ret = reader.update_writer_proxy(wp, &qos);
            debug!(
                "update_discovery_reader - endpoint {:?} - {:?}",
                endpoint, self.discovered_participant.participant_guid
            );

            if let Some(ret) = ret {
                return Some(ret);
            } else {
                continue;
            }
        }
        None
    }
}

struct UpdatedReadersUpdates<'a, 'b> {
    writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
    readers_init_list: std::vec::IntoIter<(EntityId, EntityId, u32)>,
    discovered_participant: &'b crate::discovery::SpdpDiscoveredParticipantData,
}

impl <'a, 'b> UpdatedReadersUpdates<'a, 'b> {
    fn new(
        writers: &'a mut HashMap<EntityId, Writer<timer_state::Init>>,
        readers_init_list: std::vec::IntoIter<(EntityId, EntityId, u32)>,
        discovered_participant: &'b crate::discovery::SpdpDiscoveredParticipantData,
        ) -> Self {

        Self {
            writers,
            readers_init_list,
            discovered_participant,
        }
    }
}

impl Iterator for UpdatedReadersUpdates<'_, '_> {
    type Item = (DataWriterStatus, DomainParticipantStatusEvent);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((writer_eid, reader_eid, endpoint)) = self.readers_init_list.next() {
            let Some(writer) = self.writers.get_mut(&writer_eid) else {
                continue;
            };
            if !self.discovered_participant
              .available_builtin_endpoints
              .contains(endpoint) {
                  continue;
            }

            let reader_proxy = self.discovered_participant.as_reader_proxy(true, Some(reader_eid));

            // Get the QoS for the built-in topic from the local writer
            let mut qos = writer.qos();
            // special case by RTPS 2.3 spec Section
            // "8.4.13.3 BuiltinParticipantMessageWriter and
            // BuiltinParticipantMessageReader QoS"
            if reader_eid == EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER
                && self.discovered_participant
                .builtin_endpoint_qos
                .is_some_and(|beq| beq.is_best_effort())
            {
                qos.reliability = Some(policy::Reliability::BestEffort);
            };

            let ret = writer.update_reader_proxy(&reader_proxy, &qos);
            debug!(
                "update_discovery writer - endpoint {:?} - {:?}",
                endpoint, self.discovered_participant.participant_guid
            );
            if let Some(ret) = ret {
                return Some(ret);
            } else {
                continue;
            }
        }
        None
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
