// TODO: this whole file.
// (that might also mean that datareader/datawriter cdr
use std::time::Duration as StdDuration;

use io_uring_buf_ring::buf_ring_state;
use io_uring::IoUring;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    ddsdata::DDSData,
    qos::{
      policy::{
        Deadline, DestinationOrder, Durability, History, Liveliness, Ownership, Presentation,
        PresentationAccessScope, Reliability, TimeBasedFilter,
      },
      QosPolicies, QosPolicyBuilder,
    },
    readcondition::ReadCondition,
    result::{WriteError, WriteResult},
    statusevents::{DomainParticipantStatusEvent, LostReason},
    ReadError, ReadResult,
  },
  discovery::{
    discovery::LivelinessState,
    sedp_messages::{
      DiscoveredReaderData, DiscoveredTopicData, DiscoveredWriterData, ParticipantMessageData,
      ParticipantMessageDataKind,
    },
    spdp_participant_data::SpdpDiscoveredParticipantData,
  },
  io_uring::{
    discovery::{
      discovery_db::{DiscoveredVia, DiscoveryDB},
      traffic::{TrafficLocators, UdpListeners},
    },
    network::UDPSender,
    rtps::Domain,
  },
  rtps::{constant::*, message_receiver::MessageReceiverState},
  serialization::{pl_cdr_adapters::*, CDRSerializerAdapter},
  structure::{
    cache_change::CacheChange,
    duration::Duration,
    guid::{EntityId, GuidPrefix, GUID},
    time::Timestamp,
  },
  with_key::{DataSample, DeserializedCacheChange, Sample},
  SequenceNumber, TopicCache, WriteOptions,
};
// This module implements the control logic of the Discovery process.
//
// The built-in discovery consists of
// Simple Participant Discovery Protocol (SPDP), (RTPS spec v2.5
// Section "8.5.3 The Simple Participant Discovery Protocol") and
// Simple Endpoint Discovery Protocol (SEDP) (RTPS spec v2.5
// Section "8.5.4 The Simple Endpoint Discovery Protocol")
//
// Notes:
//
// SPDP is periodically broadcast from each DomainParticipant to announce presence to others.
// It is essentially stateless and best-effort. It also announces built-in endpoints.
// See module spdp_participant_data for details.
//
// SEDP is Reliable transfer of user-defined endpoints (Readers, Writers and Topics).
// Many DDS implementations do not communicate Topics at all, and seem to work just fine.
//
// "Participant Message" protocol is NOT the same as SPDP. See RTPS spec v2.5 Section
// "8.4.13 Writer Liveliness Protocol". It can be used as liveliness (hearbeat)
// signalling of individual Writers.
//
// "Participant Stateless Message" and "Participant Volatile Message" only appear
// in Secure RTPS. Please see the RTPS Security specification for explanation.
#[cfg(feature = "security")]
use crate::{
  discovery::secure_discovery::SecureDiscovery,
  security::{security_plugins::SecurityPluginsHandle, types::*},
};
#[cfg(not(feature = "security"))]
use crate::no_security::*;
//use crate::discovery::discovery::NormalDiscoveryPermission;
use crate::io_uring::timer::{timer_state, Timer};

struct Timers<T> {
  participant_cleanup: Timer<(), T>,
  topic_cleanup: Timer<(), T>,
  spdp_publish: Timer<(), T>,
  participant_messages: Timer<(), T>,
}

use crate::{
  dds::key::Key,
  io_uring::rtps::{reader, writer},
  rtps::{
    fragment_assembler::FragmentAssembler, rtps_reader_proxy::RtpsReaderProxy,
    rtps_writer_proxy::RtpsWriterProxy, writer::HistoryBuffer,
  },
  with_key::{datasample_cache::DataSampleCache, ReadState},
};

pub(crate) struct Cache<D: Keyed, S> {
  pub(crate) qos: QosPolicies,
  // read section
  pub(crate) reader_guid: GUID,
  topic: TopicCache,
  read: ReadState<<D as Keyed>::K>,
  datasample: DataSampleCache<D>,
  writer_proxies: BTreeMap<GUID, RtpsWriterProxy>,
  writer_match_count_total: i32,
  reader_timers: reader::Timers<S>,
  fragment_assemblers: BTreeMap<GUID, FragmentAssembler>,

  // write section
  pub(crate) writer_guid: GUID,
  sequence_number: i64,
  history_buffer: HistoryBuffer,
  reader_proxies: BTreeMap<GUID, RtpsReaderProxy>,
  matched_readers_count_total: i32,
  requested_incompatible_qos_count: i32,
  heartbeat_message_counter: i32,
  writer_timers: writer::Timers<S>,
}

use std::collections::BTreeMap;

use crate::{
  dds::{key::KeyHash, statusevents::CountWithChange},
  with_key::{DefaultDecoder, DeserializerAdapter, SerializerAdapter},
  DataReaderStatus, DataWriterStatus,
};

impl<D: Keyed> Cache<D, timer_state::Uninit> {
  fn new(
    topic_name: &str,
    topic_type: &str,
    qos: QosPolicies,
    writer_guid: GUID,
    reader_guid: GUID,
  ) -> Self {
    use crate::TypeDesc;
    let qos_2 = qos.clone();

    use crate::io_uring::discovery::discovery::Liveliness;

    let writer_cache_cleaning_period = Duration::from_secs(6);

    let qos_3 = qos.clone();

    let writer_heartbeat_period = qos_3
      .reliability
      .and_then(|reliability| {
        if matches!(reliability, Reliability::Reliable { .. }) {
          Some(Duration::from_secs(1))
        } else {
          None
        }
      })
      .map(|hbp| {
        // What is the logic here? Which spec section?
        if let Some(Liveliness::ManualByTopic { lease_duration }) = qos_3.liveliness {
          let std_dur = lease_duration;
          std_dur / 3
        } else {
          hbp
        }
      });

    Self {
      topic: TopicCache::new(
        topic_name.to_string(),
        TypeDesc::new(topic_type.to_string()),
        &qos,
      ),
      read: ReadState::new(),
      datasample: DataSampleCache::new(qos_2),
      qos,
      reader_guid,
      writer_proxies: BTreeMap::new(),
      writer_match_count_total: 0,

      writer_guid,
      sequence_number: 1,
      history_buffer: HistoryBuffer::new(topic_name.to_string()),
      reader_proxies: BTreeMap::new(),
      matched_readers_count_total: 0,
      requested_incompatible_qos_count: 0,
      heartbeat_message_counter: 1,

      reader_timers: reader::Timers::new(qos_3.deadline),
      writer_timers: writer::Timers::new(writer_cache_cleaning_period, writer_heartbeat_period),

      fragment_assemblers: BTreeMap::new(),
    }
  }

  fn register(
    self,
    ring: &mut IoUring,
    domain_id: u16,
    user: u8,
  ) -> std::io::Result<Cache<D, timer_state::Init>> {
    let Self {
      topic,
      read,
      datasample,
      qos,
      reader_guid,
      writer_proxies,
      writer_match_count_total,

      writer_guid,
      sequence_number,
      history_buffer,
      reader_proxies,
      matched_readers_count_total,
      requested_incompatible_qos_count,
      heartbeat_message_counter,

      reader_timers,
      writer_timers,
      fragment_assemblers,
    } = self;

    let reader_timers = reader_timers.register(ring, reader_guid.entity_id, domain_id, user)?;
    let writer_timers = writer_timers.register(ring, domain_id, writer_guid.entity_id, user)?;

    Ok(Cache {
      topic,
      read,
      datasample,
      qos,
      reader_guid,
      writer_proxies,
      writer_match_count_total,

      writer_guid,
      sequence_number,
      history_buffer,
      reader_proxies,
      matched_readers_count_total,
      requested_incompatible_qos_count,
      heartbeat_message_counter,

      reader_timers,
      writer_timers,

      fragment_assemblers,
    })
  }
}

impl<D: Keyed> Cache<D, timer_state::Init> {
  fn deserialize<DA: DeserializerAdapter<D> + DefaultDecoder<D>>(
    timestamp: Timestamp,
    cc: &CacheChange,
    hash_to_key_map: &mut BTreeMap<KeyHash, <D as Keyed>::K>,
  ) -> ReadResult<DeserializedCacheChange<D>> {
    let decoder = DA::DECODER;

    match &cc.data_value {
      DDSData::Data { serialized_payload } => {
        // what is our data serialization format (representation identifier) ?
        if let Some(recognized_rep_id) = DA::supported_encodings()
          .iter()
          .find(|r| **r == serialized_payload.representation_identifier)
        {
          match DA::from_bytes_with(&serialized_payload.value, *recognized_rep_id, decoder) {
            // Data update, decoded ok
            Ok(payload) => {
              let p = Sample::Value(payload);
              Self::update_hash_to_key_map(hash_to_key_map, &p);
              Ok(DeserializedCacheChange::new(timestamp, cc, p))
            }
            Err(e) => Err(ReadError::Deserialization {
              reason: format!(
                "Failed to deserialize sample bytes: {},",
                e,
                /*
                self.my_topic.name(),
                self.my_topic.get_type()
                */
              ),
            }),
          }
        } else {
          info!(
            "Unknown representation id: {:?}, data = {:02x?}",
            serialized_payload.representation_identifier,
            /*
            self.my_topic.name(),
            self.my_topic.get_type(),
            */
            serialized_payload.value,
          );
          Err(ReadError::Deserialization {
            reason: format!(
              "Unknown representation id {:?}",
              serialized_payload.representation_identifier,
              /*
              self.my_topic.name(),
              self.my_topic.get_type()
              */
            ),
          })
        }
      }

      DDSData::DisposeByKey {
        key: ref serialized_key,
        ..
      } => {
        match DA::key_from_bytes_with(
          &serialized_key.value,
          serialized_key.representation_identifier,
          decoder,
        ) {
          Ok(key) => {
            let k = Sample::Dispose(key);
            Self::update_hash_to_key_map(hash_to_key_map, &k);
            Ok(DeserializedCacheChange::new(timestamp, cc, k))
          }
          Err(e) => Err(ReadError::Deserialization {
            reason: format!(
              "Failed to deserialize key {}",
              e,
              /*
              self.my_topic.name(),
              self.my_topic.get_type()
              */
            ),
          }),
        }
      }

      DDSData::DisposeByKeyHash { key_hash, .. } => {
        // The cache should know hash -> key mapping even if the sample
        // has been disposed or .take()n
        if let Some(key) = hash_to_key_map.get(&key_hash) {
          Ok(DeserializedCacheChange::new(
            timestamp,
            cc,
            Sample::Dispose(key.clone()),
          ))
        } else {
          Err(ReadError::UnknownKey {
            details: format!("Received dispose with unknown key hash: {:x?}", key_hash,),
          })
        }
      }
    } // match
  }

  fn update_and_send(
    &mut self,
    data: DDSData,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
    initialized: bool,
  ) {
    let sequence_number = self.sequence_number;

    self.sequence_number += 1;

    // NOTE: manual liveliness assertions are not checked as all builtin
    // livelinesses are either automatic or none.
    //Ok((ddsdata, WriteOptions::from(None), sequence_number.into()))

    let cc = CacheChange::new(
      self.writer_guid,
      sequence_number.into(),
      WriteOptions::from(None),
      data,
    );
    let timestamp = Timestamp::now();

    self.history_buffer.add_change(timestamp, cc);

    if !initialized {
      return;
    }

    // assuming that all builtin writers are operating only in push mode
    // and that all builtin writers are broadcasting

    use crate::rtps::Message;
    if let Some(cc) = self.history_buffer.get_change(timestamp) {
      let mut messages_to_send: Vec<Message> = vec![];
      {
        // appending stuff to msgs_to_send
        // TODO: change this to the new impl with an iterator
        let reader_entity_id = EntityId::UNKNOWN;

        let endianness = speedy::Endianness::LittleEndian;

        use crate::{rtps::MessageBuilder, structure::sequence_number::FragmentNumber};

        let send_also_heartbeat = true;

        let data_size = cc.data_value.payload_size();
        let data_max_size_serialized = 1024;
        let fragmentation_needed = data_size > data_max_size_serialized;

        if !fragmentation_needed {
          // We can send DATA
          let mut message_builder = MessageBuilder::new();

          // If DataWriter sent us a source timestamp, then add that.
          // Timestamp has to go before Data to have effect on Data.
          if let Some(src_ts) = cc.write_options.source_timestamp() {
            message_builder = message_builder.ts_msg(endianness, Some(src_ts));
          }

          // Add the DATA submessage
          message_builder = message_builder.data_msg(
            cc,
            reader_entity_id,
            self.writer_guid, // writer
            endianness,
            None, /*TODO
                   *self.security_plugins.as_ref(), */
          );

          // Add HEARTBEAT if needed
          if send_also_heartbeat {
            let final_flag = false; // false = request that readers acknowledge with ACKNACK.
            let liveliness_flag = false; // This is not a manual liveliness assertion (DDS API call), but side-effect of
                                         // writing new data.
            message_builder = message_builder.heartbeat_msg(
              self.writer_guid.entity_id, // from Writer
              self.history_buffer.first_change_sequence_number(),
              self.history_buffer.last_change_sequence_number(),
              self.next_heartbeat_count(),
              endianness,
              reader_entity_id, // to Reader
              final_flag,
              liveliness_flag,
            );
          }

          let data_message = message_builder.add_header_and_build(self.writer_guid.prefix);

          messages_to_send.push(data_message);
        } else {
          // fragmentation_needed: We need to send DATAFRAGs

          // If sending to a single reader, add a GAP message with pending gaps if any

          let (num_frags, fragment_size) = self.num_frags_and_frag_size(data_size);

          for frag_num in
            FragmentNumber::range_inclusive(FragmentNumber::new(1), FragmentNumber::new(num_frags))
          {
            let mut message_builder = MessageBuilder::new(); // fresh builder

            if let Some(src_ts) = cc.write_options.source_timestamp() {
              // Add timestamp
              message_builder = message_builder.ts_msg(endianness, Some(src_ts));
            }

            message_builder = message_builder.data_frag_msg(
              cc,
              reader_entity_id, // reader
              self.writer_guid, // writer
              frag_num,
              fragment_size,
              data_size.try_into().unwrap(),
              endianness,
              None, /* TODO
                     *self.security_plugins.as_ref(), */
            );

            let datafrag_msg = message_builder.add_header_and_build(self.writer_guid.prefix);
            messages_to_send.push(datafrag_msg);
          } // end for

          // Add HEARTBEAT message if needed
          if send_also_heartbeat {
            let final_flag = false; // false = request that readers acknowledge with ACKNACK.
            let liveliness_flag = false; // This is not a manual liveliness assertion (DDS API call), but side-effect of
                                         // writing new data.
            let hb_msg = MessageBuilder::new()
              .heartbeat_msg(
                self.writer_guid.entity_id, // from Writer
                self.history_buffer.first_change_sequence_number(),
                self.history_buffer.last_change_sequence_number(),
                self.next_heartbeat_count(),
                endianness,
                reader_entity_id, // to Reader
                final_flag,
                liveliness_flag,
              )
              .add_header_and_build(self.writer_guid.prefix);
            messages_to_send.push(hb_msg);
          }
        }
      }

      for msg in messages_to_send {
        use speedy::Endianness;

        use crate::rtps::writer::DeliveryMode;
        crate::io_uring::rtps::Writer::send_message_to_readers(
          Endianness::LittleEndian,
          DeliveryMode::Multicast,
          msg,
          &mut self.reader_proxies.values(),
          udp_sender,
          ring,
        );
      }
    }
  }

  pub fn write<SA: SerializerAdapter<D>>(
    &mut self,
    data: D,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
    initialized: bool,
  ) -> WriteResult<(), D> {
    // serialize
    let send_buffer = match SA::to_bytes(&data) {
      Ok(b) => b,
      Err(e) => {
        return Err(WriteError::Serialization {
          reason: format!("{e}"),
          data,
        })
      }
    };

    use crate::messages::submessages::elements::serialized_payload::SerializedPayload;
    let ddsdata = DDSData::new(SerializedPayload::new_from_bytes(
      SA::output_encoding(),
      send_buffer,
    ));

    self.update_and_send(ddsdata, udp_sender, ring, initialized);
    Ok(())
  }

  fn dispose<SA: SerializerAdapter<D>>(
    &mut self,
    key: &<D as Keyed>::K,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
    initialized: bool,
  ) -> WriteResult<(), ()> {
    let send_buffer = match SA::key_to_bytes(key) {
      Ok(b) => b,
      Err(e) => {
        return Err(WriteError::Serialization {
          reason: format!("{e}"),
          data: (),
        })
      }
    };

    use crate::messages::submessages::elements::serialized_payload::SerializedPayload;
    let ddsdata = DDSData::new(SerializedPayload::new_from_bytes(
      SA::output_encoding(),
      send_buffer,
    ));

    self.update_and_send(ddsdata, udp_sender, ring, initialized);

    Ok(())
  }

  fn next_heartbeat_count(&mut self) -> i32 {
    let curr = self.heartbeat_message_counter;
    self.heartbeat_message_counter += 1;
    curr
  }

  fn num_frags_and_frag_size(&self, payload_size: usize) -> (u32, u16) {
    let fragment_size = 1024;

    let data_size = payload_size as u32; // TODO: overflow check
                                         // Formula from RTPS spec v2.5 Section "8.3.8.3.5 Logical Interpretation"
    let num_frags = (data_size / fragment_size) + u32::from(data_size % fragment_size != 0); // rounding up
    debug!("Fragmenting {data_size} to {num_frags} x {fragment_size}");
    // TODO: Check fragment_size overflow
    (num_frags, fragment_size as u16)
  }

  fn update_hash_to_key_map(
    hash_to_key_map: &mut BTreeMap<KeyHash, D::K>,
    deserialized: &Sample<D, D::K>,
  ) {
    let instance_key = match deserialized {
      Sample::Value(d) => d.key(),
      Sample::Dispose(k) => k.clone(),
    };
    hash_to_key_map.insert(instance_key.hash_key(false), instance_key);
  }

  // TODO: this is not called at the start
  // - therefore the default locators are never given
  // - => none of the stuff is discoverable until the thing is ready
  // - the stuff is there for it but its never called
  //    - an issue with how UpdatedParticipant works?
  //    - this is propagated by DState, so its just a matter of not iterating over
  //      it
  //      - might be better to have a minimum viable DState that only works with
  //        builtins to see whats going wrong
  pub(crate) fn update_reader_proxy(
    &mut self,
    reader_proxy: &RtpsReaderProxy,
  ) -> Option<(DataWriterStatus, DomainParticipantStatusEvent)> {
    let change = self.matched_reader_update(reader_proxy);

    if change > 0 {
      self.matched_readers_count_total += change;

      let writer_status = DataWriterStatus::PublicationMatched {
        total: CountWithChange::new(self.matched_readers_count_total, change),
        current: CountWithChange::new(self.reader_proxies.len() as i32, change),
        reader: reader_proxy.remote_reader_guid,
      };

      let participant_status = DomainParticipantStatusEvent::RemoteReaderMatched {
        local_writer: self.writer_guid,
        remote_reader: reader_proxy.remote_reader_guid,
      };
      Some((writer_status, participant_status))
    } else {
      None
    }
  }

  pub(crate) fn update_reader_proxy_with_qos(
    &mut self,
    reader_proxy: &RtpsReaderProxy,
    requested_qos: &QosPolicies,
  ) -> Option<(DataWriterStatus, DomainParticipantStatusEvent)> {
    if let Some(bad_policy_id) = self.qos.compliance_failure_wrt(requested_qos) {
      let writer_status = DataWriterStatus::OfferedIncompatibleQos {
        count: CountWithChange::new(self.requested_incompatible_qos_count, 1),
        last_policy_id: bad_policy_id,
        reader: reader_proxy.remote_reader_guid,
        requested_qos: Box::new(requested_qos.clone()),
        offered_qos: Box::new(self.qos.clone()),
      };

      let domain_status = DomainParticipantStatusEvent::RemoteReaderQosIncompatible {
        local_writer: self.writer_guid,
        remote_reader: reader_proxy.remote_reader_guid,
        requested_qos: Box::new(requested_qos.clone()),
        offered_qos: Box::new(self.qos.clone()),
      };

      Some((writer_status, domain_status))
    } else {
      self.update_reader_proxy(reader_proxy)
    }
  }

  pub(crate) fn update_writer_proxy(
    &mut self,
    writer_proxy: RtpsWriterProxy,
  ) -> Option<(DataReaderStatus, DomainParticipantStatusEvent)> {
    let writer = writer_proxy.remote_writer_guid;

    let count_change = if let Some(prox) = self
      .writer_proxies
      .get_mut(&writer_proxy.remote_writer_guid)
    {
      prox.update_contents(writer_proxy);
      0
    } else {
      self
        .writer_proxies
        .insert(writer_proxy.remote_writer_guid, writer_proxy);
      1
    };

    if count_change > 0 {
      self.writer_match_count_total += count_change;

      let reader_status = DataReaderStatus::SubscriptionMatched {
        total: CountWithChange::new(self.writer_match_count_total, count_change),
        current: CountWithChange::new(self.writer_proxies.len() as i32, count_change),
        writer,
      };

      let participant_status = DomainParticipantStatusEvent::RemoteWriterMatched {
        local_reader: self.reader_guid,
        remote_writer: writer,
      };

      Some((reader_status, participant_status))
    } else {
      None
    }
  }

  fn matched_reader_update(&mut self, updated_reader_proxy: &RtpsReaderProxy) -> i32 {
    let mut new = 0;
    let is_volatile = self.qos.is_volatile();

    self
      .reader_proxies
      .entry(updated_reader_proxy.remote_reader_guid)
      .and_modify(|rp| rp.update(updated_reader_proxy))
      .or_insert_with(|| {
        new = 1;
        let mut new_proxy = updated_reader_proxy.clone();
        if is_volatile {
          // With Durabilty::Volatile QoS we won't send the sequence numbers which existed
          // before matching with this reader. Therefore we set the reader as pending GAP
          // for all existing sequence numbers
          new_proxy.set_pending_gap_up_to(self.history_buffer.last_change_sequence_number());
        }
        new_proxy
      });
    new
  }

  pub(crate) fn participant_lost(&mut self, guid_prefix: GuidPrefix) -> BuiltinParticipantLost {
    let lost_readers = self
      .reader_proxies
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect::<Vec<_>>();

    let lost_writers = self
      .writer_proxies
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect::<Vec<_>>();

    BuiltinParticipantLost {
      readers: lost_readers.into_iter(),
      writers: lost_writers.into_iter(),
    }
  }

  fn reader_lost(&mut self, guid: GUID) -> Option<DataWriterStatus> {
    if self.reader_proxies.remove(&guid).is_some() {
      Some(DataWriterStatus::PublicationMatched {
        total: CountWithChange::new(self.matched_readers_count_total, 0),
        current: CountWithChange::new(self.reader_proxies.len() as i32, -1),
        reader: guid,
      })
    } else {
      None
    }
  }

  fn writer_lost(&mut self, guid: GUID) -> Option<DataReaderStatus> {
    if self.writer_proxies.remove(&guid).is_some() {
      Some(DataReaderStatus::SubscriptionMatched {
        total: CountWithChange::new(self.writer_match_count_total, 0),
        current: CountWithChange::new(self.writer_proxies.len() as i32, -1),
        writer: guid,
      })
    } else {
      None
    }
  }

  fn handle_heartbeat_msg(
    &mut self,
    writer_guid: GUID,
    heartbeat: &Heartbeat,
    final_flag_set: bool,
    mr_state: &MessageReceiverState,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) -> bool {
    let Some(proxy) = self.writer_proxies.get_mut(&writer_guid) else {
      return false;
    };

    if heartbeat.first_sn < SequenceNumber::default() {
      warn!(
        "Writer {:?} advertised SequenceNumbers from {:?} to {:?}!",
        writer_guid, heartbeat.first_sn, heartbeat.last_sn
      );
    }

    if heartbeat.count <= proxy.received_heartbeat_count {
      return false;
    }

    proxy.received_heartbeat_count = heartbeat.count;

    // remove changes until first sn
    proxy.irrelevant_changes_up_to(heartbeat.first_sn);

    self
      .topic
      .mark_reliably_received_before(writer_guid, proxy.all_ackable_before());

    let reader_id = self.reader_guid.entity_id;

    use enumflags2::BitFlags;

    use crate::{
      messages::submessages::submessages::{
        ACKNACK_Flags, AckNack, InfoDestination, NACKFRAG_Flags,
      },
      structure::{locator::Locator, sequence_number::SequenceNumberSet},
    };

    let missing_seqnums = proxy.missing_seqnums(heartbeat.first_sn, heartbeat.last_sn);

    // Interpretation of final flag in RTPS spec
    // 8.4.2.3.1 Readers must respond eventually after receiving a HEARTBEAT with
    // final flag not set
    //
    // Upon receiving a HEARTBEAT Message with final flag not set, the Reader must
    // respond with an ACKNACK Message. The ACKNACK Message may acknowledge
    // having received all the data samples or may indicate that some data
    // samples are missing. The response may be delayed to avoid message storms.

    if !missing_seqnums.is_empty() || !final_flag_set {
      let mut partially_received = Vec::new();
      // report of what we have.
      // We claim to have received all SNs before "base" and produce a set of missing
      // sequence numbers that are >= base.
      let reader_sn_state = match missing_seqnums.first() {
        Some(&first_missing) => {
          // Here we assume missing_seqnums are returned in order.
          // Limit the set to maximum that can be sent in acknack submessage.
          SequenceNumberSet::from_base_and_set(
            first_missing,
            &missing_seqnums
              .iter()
              .copied()
              .take_while(|sn| sn < &(first_missing + SequenceNumber::new(256)))
              .filter(|sn| {
                if self
                  .fragment_assemblers
                  .get(&writer_guid)
                  .is_some_and(|fa| fa.is_partially_received(*sn))
                {
                  partially_received.push(*sn);
                  false
                } else {
                  true
                }
              })
              .collect(),
          )
        }

        // Nothing missing. Report that we have all we have.
        None => SequenceNumberSet::new_empty(proxy.all_ackable_before()),
      };

      //panic!("{partially_received:?}, {missing_seqnums:?}");

      let response_ack_nack = AckNack {
        reader_id,
        writer_id: heartbeat.writer_id,
        reader_sn_state,
        count: proxy.next_ack_nack_sequence_number(),
      };

      // Sanity check
      //
      // Wrong. This sanity check is invalid. The condition
      // ack_base > heartbeat.last_sn + 1
      // May be legitimately true, if there are some changes available, and a GAP
      // after that. E.g. HEARTBEAT 1..8 and GAP 9..10. Then acknack_base == 11
      // and 11 > 8 + 1.
      //
      //
      // if response_ack_nack.reader_sn_state.base() > heartbeat.last_sn +
      // SequenceNumber::new(1) {   error!(
      //     "OOPS! AckNack sanity check tripped: HEARTBEAT = {:?} ACKNACK = {:?}
      // missing_seqnums = {:?} all_ackable_before = {:?} writer={:?}",
      //     &heartbeat, &response_ack_nack, missing_seqnums,
      // writer_proxy.all_ackable_before(), writer_guid,   );
      // }

      // The acknack can be sent now or later. The rest of the RTPS message
      // needs to be constructed. p. 48
      let acknack_flags = BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Endianness)
        | BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Final);

      let _nackfrag_flags = BitFlags::<NACKFRAG_Flags>::from_flag(NACKFRAG_Flags::Endianness);

      if !partially_received.is_empty() {
        todo!()
      }
      // send NackFrags, if any
      //let mut nackfrags = Vec::new();
      /* TODO:
      for sn in partially_received {
        let count = proxy.next_ack_nack_sequence_number();
        let mut missing_frags = vec![];
        //TODO
        //let mut missing_frags = this.missing_frags_for(writer_guid, sn);
        let first_missing = missing_frags.next();
        if let Some(first) = first_missing {
          let missing_frags_set = std::iter::once(first).chain(missing_frags).collect(); // "undo" the .next() above
          let nf = NackFrag {
            reader_id,
            writer_id: proxy.remote_writer_guid.entity_id,
            writer_sn: sn,
            fragment_number_state: FragmentNumberSet::from_base_and_set(
              first,
              &missing_frags_set,
            ),
            count,
          };
          nackfrags.push(nf);
        } else {
          error!("The dog ate my missing fragments.");
          // Really, this should not happen, as we are above checking
          // that this SN is really partially (and not fully) received.
        }
      }
      */

      // Decide where should we send a reply, i.e. ACKNACK
      let reply_locators = match &*mr_state.unicast_reply_locator_list {
        [] | [Locator::Invalid] => &proxy.unicast_locator_list,
        //TODO: What is writer_proxy has an empty list?
        others => others,
      };

      /*
      if !nackfrags.is_empty() {
        this.send_nackfrags_to(
          nackfrag_flags,
          nackfrags,
          InfoDestination {
            guid_prefix: mr_state.source_guid_prefix,
          },
          reply_locators,
          writer_guid,
        );
      }
      */

      let acknack_msg = Self::create_acknack(
        self.reader_guid.prefix,
        acknack_flags,
        response_ack_nack,
        InfoDestination {
          guid_prefix: mr_state.source_guid_prefix,
        },
      );

      use speedy::{Endianness, Writable};
      let bytes = acknack_msg
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap();

      println!("sending acknack to {reply_locators:?}");

      udp_sender
        .send_to_locator_list(&bytes, reply_locators, ring)
        .unwrap();

      return true;
    }

    false
  }

  fn create_acknack(
    guid_prefix: GuidPrefix,
    flags: BitFlags<ACKNACK_Flags>,
    acknack: AckNack,
    info_dst: InfoDestination,
  ) -> Message {
    use crate::messages::{
      header::Header, protocol_id::ProtocolId, protocol_version::ProtocolVersion,
      submessages::submessages::INFODESTINATION_Flags, vendor_id::VendorId,
    };

    let infodst_flags =
      BitFlags::<INFODESTINATION_Flags>::from_flag(INFODESTINATION_Flags::Endianness);

    let mut message = Message::new(Header {
      protocol_id: ProtocolId::default(),
      protocol_version: ProtocolVersion::THIS_IMPLEMENTATION,
      vendor_id: VendorId::THIS_IMPLEMENTATION,
      guid_prefix: guid_prefix,
    });

    message.add_submessage(info_dst.create_submessage(infodst_flags));

    message.add_submessage(acknack.create_submessage(flags));
    message
  }

  fn send_preemptive_acknacks(
    &mut self,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) -> std::io::Result<()> {
    let flags = BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Endianness);

    let reader_id = self.reader_guid.entity_id;

    for (_, writer_proxy) in self
      .writer_proxies
      .iter_mut()
      .filter(|(_, p)| p.no_changes_received())
    {
      let acknack_count = writer_proxy.next_ack_nack_sequence_number();
      let RtpsWriterProxy {
        remote_writer_guid,
        unicast_locator_list,
        ..
      } = writer_proxy;

      use crate::structure::sequence_number::SequenceNumberSet;

      let acknack = Self::create_acknack(
        self.reader_guid.prefix,
        flags,
        AckNack {
          reader_id,
          writer_id: remote_writer_guid.entity_id,
          reader_sn_state: SequenceNumberSet::new_empty(SequenceNumber::new(1)),
          count: acknack_count,
        },
        InfoDestination {
          guid_prefix: remote_writer_guid.prefix,
        },
      );

      use speedy::{Endianness, Writable};

      let bytes = acknack
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap();

      udp_sender.send_to_locator_list(&bytes, unicast_locator_list, ring)?;
    }
    Ok(())
  }

  fn handle_acknack(
    &mut self,
    ack_submsg: AckSubmessage,
    reader_guid_prefix: GuidPrefix,
    ring: &mut IoUring,
    domain_id: u16,
    user: u8,
  ) {
    if !self.qos.is_reliable() {
      return;
    }

    match ack_submsg {
      AckSubmessage::AckNack(ref acknack) => {
        let last_seq = self.history_buffer.last_change_sequence_number();

        if let Some(0) = acknack.reader_sn_state.iter().next().map(i64::from) {
          warn!("Request for SN zero! : {acknack:?}");
        }

        let reader_guid = GUID::new(reader_guid_prefix, acknack.reader_id);

        // sanity check
        if acknack.reader_sn_state.base() < SequenceNumber::from(1) {
          // This check is based on RTPS v2.5 Spec
          // Section "8.3.5.5 SequenceNumberSet" and
          // Section "8.3.8.1.3 Validity".
          // But apparently some RTPS implementations send ACKNACK with
          // reader_sn_state.base = 0 to indicate they have matched the writer,
          // so seeing these once per new writer should be ok.
          info!(
            "ACKNACK SequenceNumberSet minimum must be >= 1, got {:?} from {:?}",
            acknack.reader_sn_state.base(),
            reader_guid,
            //self.topic_name()
          );
        }

        if let Some(reader_proxy) = self.reader_proxies.get_mut(&reader_guid) {
          //TODO: We should drop SNs in "pending gap" from unsent changes
          reader_proxy.handle_ack_nack(&ack_submsg, last_seq);

          // if we cannot send more data, we are done.
          // This is to prevent empty "repair data" messages from being sent.

          /*
          if reader_guid.prefix == self.writer_guid.prefix {
            return;
          }
          */

          println!("{:?} > {last_seq:?}", reader_proxy.all_acked_before);
          if reader_proxy.all_acked_before > last_seq {
            reader_proxy.repair_mode = false;
          } else {
            reader_proxy.repair_mode = true;
            self.writer_timers.set_send_repair_data(
              self.writer_guid.entity_id,
              reader_guid,
              crate::rtps::constant::NACK_RESPONSE_DELAY.into(),
              domain_id,
              ring,
              user,
            );
          }

          if !reader_proxy.get_pending_gap().is_empty() {
            todo!("issue gap msg for builtins");
          }
        }
      }
      _ => todo!(),
    }
  }

  fn garbage_collect_cache(&mut self) {
    let topic = &mut self.topic;
    if let Some((last_timestamp, _)) = topic.changes.iter().next_back() {
      if *last_timestamp > topic.changes_reallocated_up_to {
        topic.remove_changes_before(Timestamp::ZERO);
      }
    }
  }

  fn handle_writer_timed_event(
    &mut self,
    ev: &user_data::WriteTimerVariant,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    println!("builtin: {ev:?}");
    use user_data::WriteTimerVariant;
    match ev {
      WriteTimerVariant::Heartbeat => {
        self.writer_timers.heartbeat.as_mut().map(|hb| hb.reset());

        self.handle_heartbeat_tick(false, udp_sender, ring)
      }
      WriteTimerVariant::CacheCleaning => {
        self.writer_timers.cache_cleaning.reset();

        let resource_limit = 32;

        let depth = match self.qos.history {
          None => Some(1),
          Some(History::KeepAll) => None,
          Some(History::KeepLast { depth }) => Some(depth as _),
        };

        self.remove_all_acked_changes_but_keep_depth(depth, resource_limit);
      }
      WriteTimerVariant::SendRepairData => {
        let Some(timer) = core::mem::take(&mut self.writer_timers.send_repair_data) else {
          return;
        };

        let guid = timer.take_inner();
        self.handle_repair_data_send(guid, udp_sender, ring);
      }
      v => todo!("builtin writer timer: {v:?}"),
    }
  }

  fn handle_repair_data_send(
    &mut self,
    reader_guid: GUID,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let Some(reader_proxy) = self.reader_proxies.get_mut(&reader_guid) else {
      return;
    };

    use std::collections::BTreeSet;

    use crate::rtps::{writer::DeliveryMode, MessageBuilder};

    println!(
      "unsent changes: {:?} to {reader_guid:?}",
      reader_proxy.unsent_changes_debug()
    );

    if let Some(unsent_sn) = reader_proxy.first_unsent_change() {
      println!("{unsent_sn:?} is unsent according to reader_proxy");
      // There are unsent changes.
      let mut no_longer_relevant: BTreeSet<SequenceNumber> = BTreeSet::new();
      let mut all_irrelevant_before = None;

      // If we have set the reader as pending GAP for the unsent sequence number,
      // just send a GAP message, not DATA.
      let pending_gaps = reader_proxy.get_pending_gap();

      // Check what we actually have in store
      let first_available = self.history_buffer.first_change_sequence_number();
      if unsent_sn < first_available {
        // Reader is requesting older than what we actually have. Notify that they are
        // gone.
        all_irrelevant_before = Some(first_available);
      }

      // If all_irrelevant_before is still None, then TopicCache has SNs that are
      // less than equal to the requested "unsent_sn". But might not have that exact
      // SN.
      if pending_gaps.contains(&unsent_sn) || all_irrelevant_before.is_some() {
        no_longer_relevant.extend(pending_gaps);
      } else {
        // Reader not pending gap on unsent_sn. Get the cache change from topic cache
        if let Some(cc) = self.history_buffer.get_by_sn(unsent_sn) {
          // // DEBUG
          // if self.my_guid.entity_id == EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER
          //   && reader_proxy.remote_reader_guid.prefix != self.my_guid.prefix
          // {
          //   info!("Publications Writer sends repair DATA {:?}", unsent_sn);
          // }
          // // DEBUG

          // The cache change was found. Send it to the reader

          let data_was_fragmented = {
            let mut messages_to_send = vec![];

            let reader_entity_id = reader_proxy.remote_reader_guid.entity_id;
            let data_size = cc.data_value.payload_size();
            let fragmentation_needed = data_size > 1024;

            if !fragmentation_needed {
              let mut message_builder = MessageBuilder::new();

              if let Some(src_ts) = cc.write_options.source_timestamp() {
                message_builder =
                  message_builder.ts_msg(speedy::Endianness::LittleEndian, Some(src_ts));
              }

              message_builder = message_builder.dst_submessage(
                speedy::Endianness::LittleEndian,
                reader_proxy.remote_reader_guid.prefix,
              );

              // If the reader is pending GAPs on any sequence numbers, add a GAP
              if !reader_proxy.get_pending_gap().is_empty() {
                message_builder = message_builder.gap_msg(
                  reader_proxy.get_pending_gap(),
                  self.writer_guid.entity_id,
                  speedy::Endianness::LittleEndian,
                  reader_proxy.remote_reader_guid,
                );
              }

              message_builder = message_builder.data_msg(
                cc,
                reader_entity_id,
                self.writer_guid, // writer
                speedy::Endianness::LittleEndian,
                None,
              );

              let data_msg = message_builder.add_header_and_build(self.writer_guid.prefix);
              messages_to_send.push(data_msg);

              println!("sending msgs from repairdata: {messages_to_send:?}");

              for msg in messages_to_send {
                crate::io_uring::rtps::Writer::send_message_to_readers(
                  speedy::Endianness::LittleEndian,
                  DeliveryMode::Unicast,
                  msg,
                  std::iter::once(&*reader_proxy),
                  udp_sender,
                  ring,
                );
              }
            } else {
              todo!("data was fragmented");
            }
            fragmentation_needed
          };

          if data_was_fragmented {
            todo!("data was fragmented");
            /*
            // Mark the reader as having requested all frags
            let (num_frags, _frag_size) =
              self.num_frags_and_frag_size(cc.data_value.payload_size());
            reader_proxy.mark_all_frags_requested(unsent_sn, num_frags);

            // Set a timer to send repair frags if needed
            self.timed_event_timer.set_timeout(
              self.repairfrags_continue_delay,
              TimedEvent::SendRepairFrags {
                to_reader: reader_guid,
              },
            );
            */
          }
          println!("marking {unsent_sn:?} as sent");
          // mark as sent
          reader_proxy.mark_change_sent(unsent_sn);
        } else {
          // Did not find a cache change for the sequence number. Mark for GAP.
          no_longer_relevant.insert(unsent_sn);
          // Try to find a reason why and log about it
          if unsent_sn < first_available {
            /*
            info!(
              "Reader {:?} requested too old data {:?}. I have only from {:?}. Topic {:?}",
              &reader_proxy, unsent_sn, first_available, &self.my_topic_name
            );
            */
          } else {
            // we are running out of excuses
            /*
            error!(
              "handle_repair_data_send_worker {:?} seq.number {:?} missing. first_change={:?}",
              self.my_guid, unsent_sn, first_available
            );
            */
          }
        }
      }

      // Send a GAP if we marked a sequence number as no longer relevant
      if !no_longer_relevant.is_empty() || all_irrelevant_before.is_some() {
        let mut gap_msg = MessageBuilder::new()
          .dst_submessage(speedy::Endianness::LittleEndian, reader_guid.prefix);
        if let Some(all_irrelevant_before) = all_irrelevant_before {
          gap_msg = gap_msg.gap_msg_before(
            all_irrelevant_before,
            self.writer_guid.entity_id,
            speedy::Endianness::LittleEndian,
            reader_guid,
          );
          reader_proxy.remove_from_unsent_set_all_before(all_irrelevant_before);
        }
        if !no_longer_relevant.is_empty() {
          gap_msg = gap_msg.gap_msg(
            &no_longer_relevant,
            self.writer_guid.entity_id,
            speedy::Endianness::LittleEndian,
            reader_guid,
          );
          no_longer_relevant
            .iter()
            .for_each(|sn| reader_proxy.mark_change_sent(*sn));
        }
        let gap_msg = gap_msg.add_header_and_build(self.writer_guid.prefix);

        crate::io_uring::rtps::Writer::send_message_to_readers(
          speedy::Endianness::LittleEndian,
          DeliveryMode::Unicast,
          gap_msg,
          std::iter::once(&*reader_proxy),
          udp_sender,
          ring,
        );
      } // if sending GAP
    } else {
      // Unsent list is empty. Switch off repair mode.
      reader_proxy.repair_mode = false;
    }
  }

  fn remove_all_acked_changes_but_keep_depth(
    &mut self,
    depth: Option<usize>,
    resource_limit: usize,
  ) {
    use std::cmp::max;
    let first_keeper = {
      // Regular stateful writer behavior
      // All readers have acked up to this point (SequenceNumber)
      let acked_by_all_readers = self
        .reader_proxies
        .values()
        .map(RtpsReaderProxy::acked_up_to_before)
        .min()
        .unwrap_or_else(SequenceNumber::zero);
      // If all readers have acked all up to before 5, and depth is 5, we need
      // to keep samples 0..4, i.e. from acked_up_to_before - depth .
      if let Some(depth) = depth {
        max(
          acked_by_all_readers - SequenceNumber::from(depth),
          self.history_buffer.first_change_sequence_number(),
        )
      } else {
        // try to keep all
        self.history_buffer.first_change_sequence_number()
      }
    };

    let first_keeper = max(
      max(
        first_keeper,
        self.history_buffer.last_change_sequence_number() - SequenceNumber::from(resource_limit),
      ),
      SequenceNumber::zero(),
    );

    // actual cleaning
    self.history_buffer.remove_changes_before(first_keeper);
  }

  pub(crate) fn handle_heartbeat_tick(
    &mut self,
    manual_assertion: bool,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let final_flag = false;
    let liveliness_flag = manual_assertion;

    let first_change = self.history_buffer.first_change_sequence_number();
    let last_change = self.history_buffer.last_change_sequence_number();

    if self
      .reader_proxies
      .values()
      .all(|rp| last_change < rp.all_acked_before)
    {
      println!("Readers have available data");
      trace!("heartbeat tick: all readers have all available data.");
      return;
    }

    use crate::rtps::{writer::DeliveryMode, MessageBuilder};

    let endianness = speedy::Endianness::LittleEndian;

    // the interface to .heartbeat_msg is silly: we give ref to ourself
    // and that function then queries us.
    let hb_message = MessageBuilder::new()
      .ts_msg(endianness, Some(Timestamp::now()))
      .heartbeat_msg(
        self.writer_guid.entity_id, // from Writer
        first_change,
        last_change,
        self.next_heartbeat_count(),
        endianness,
        EntityId::UNKNOWN, // to Reader
        final_flag,
        liveliness_flag,
      )
      .add_header_and_build(self.writer_guid.prefix);

    // In the volatile key exchange topic we cannot send to multiple readers by any
    // means, so we handle that separately.
    use speedy::Endianness;
    if self.writer_guid.entity_id == EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER {
      for rp in self.reader_proxies.values() {
        if last_change < rp.all_acked_before {
          // Everything we have has been acknowledged already. Do nothing.
        } else {
          crate::io_uring::rtps::Writer::send_message_to_readers(
            Endianness::LittleEndian,
            DeliveryMode::Unicast,
            hb_message.clone(),
            std::iter::once(rp),
            udp_sender,
            ring,
          );
        }
      }
    } else {
      // Normal case
      crate::io_uring::rtps::Writer::send_message_to_readers(
        Endianness::LittleEndian,
        DeliveryMode::Multicast,
        hb_message,
        self.reader_proxies.values(),
        udp_sender,
        ring,
      );
    }
  }

  fn handle_reader_timed_event(&mut self, _ev: &user_data::ReadTimerVariant) {
    todo!()
  }
}

use enumflags2::BitFlags;

use crate::{
  messages::submessages::submessages::{ACKNACK_Flags, AckNack, Heartbeat, InfoDestination},
  rtps::Message,
};

pub(crate) struct BuiltinParticipantLost {
  readers: vec::IntoIter<GUID>,
  writers: vec::IntoIter<GUID>,
}

impl BuiltinParticipantLost {
  pub fn inner<'a, 'b, D: Keyed>(
    &'b mut self,
    cache: &'a mut Cache<D, timer_state::Init>,
  ) -> BuiltinParticipantLostInner<'a, 'b, D> {
    BuiltinParticipantLostInner::new(cache, self)
  }
}

pub(crate) struct BuiltinParticipantLostInner<'a, 'b, D: Keyed> {
  cache: &'a mut Cache<D, timer_state::Init>,
  participant_lost: &'b mut BuiltinParticipantLost,
}

impl<'a, 'b, D: Keyed> BuiltinParticipantLostInner<'a, 'b, D> {
  fn new(
    cache: &'a mut Cache<D, timer_state::Init>,
    participant_lost: &'b mut BuiltinParticipantLost,
  ) -> Self {
    Self {
      cache,
      participant_lost,
    }
  }
}

impl<D: Keyed> Iterator for BuiltinParticipantLostInner<'_, '_, D> {
  type Item = (DataStatus, GUID);

  fn next(&mut self) -> Option<Self::Item> {
    while let Some(reader) = self.participant_lost.readers.next() {
      if let Some(lost) = self.cache.reader_lost(reader) {
        return Some((DataStatus::Writer(lost), reader));
      }
    }

    while let Some(writer) = self.participant_lost.writers.next() {
      if let Some(lost) = self.cache.writer_lost(writer) {
        return Some((DataStatus::Reader(lost), writer));
      }
    }
    None
  }
}

use crate::{io_uring::rtps::DataStatus, Keyed};

pub(crate) struct Caches<S> {
  pub(crate) publications: Cache<DiscoveredWriterData, S>,
  pub(crate) topics: Cache<DiscoveredTopicData, S>,
  pub(crate) participants: Cache<SpdpDiscoveredParticipantData, S>,
  pub(crate) subscriptions: Cache<DiscoveredReaderData, S>,
  pub(crate) participant_messages: Cache<ParticipantMessageData, S>,
}

impl Caches<timer_state::Uninit> {
  fn new(guid_prefix: GuidPrefix) -> Self {
    Self {
      publications: Cache::new(
        builtin_topic_names::DCPS_PUBLICATION,
        builtin_topic_type_names::DCPS_PUBLICATION,
        Self::subscriber_qos(),
        GUID::new(guid_prefix, EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER),
        GUID::new(guid_prefix, EntityId::SEDP_BUILTIN_PUBLICATIONS_READER),
      ),
      topics: Cache::new(
        builtin_topic_names::DCPS_TOPIC,
        builtin_topic_type_names::DCPS_TOPIC,
        Self::subscriber_qos(),
        GUID::new(guid_prefix, EntityId::SEDP_BUILTIN_TOPIC_WRITER),
        GUID::new(guid_prefix, EntityId::SEDP_BUILTIN_TOPIC_READER),
      ),
      participants: Cache::new(
        builtin_topic_names::DCPS_PARTICIPANT,
        builtin_topic_type_names::DCPS_PARTICIPANT,
        Self::spdp_participant_qos(),
        GUID::new(guid_prefix, EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER),
        GUID::new(guid_prefix, EntityId::SPDP_BUILTIN_PARTICIPANT_READER),
      ),
      subscriptions: Cache::new(
        builtin_topic_names::DCPS_SUBSCRIPTION,
        builtin_topic_type_names::DCPS_SUBSCRIPTION,
        Self::subscriber_qos(),
        GUID::new(guid_prefix, EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER),
        GUID::new(guid_prefix, EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER),
      ),
      participant_messages: Cache::new(
        builtin_topic_names::DCPS_PARTICIPANT_MESSAGE,
        builtin_topic_type_names::DCPS_PARTICIPANT_MESSAGE,
        Self::participant_message_qos(),
        GUID::new(
          guid_prefix,
          EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER,
        ),
        GUID::new(
          guid_prefix,
          EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER,
        ),
      ),
    }
  }

  fn register(
    self,
    ring: &mut IoUring,
    domain_id: u16,
    user: u8,
  ) -> std::io::Result<Caches<timer_state::Init>> {
    let Caches {
      publications,
      topics,
      participants,
      subscriptions,
      participant_messages,
    } = self;

    let publications = publications.register(ring, domain_id, user)?;
    let topics = topics.register(ring, domain_id, user)?;
    let participants = participants.register(ring, domain_id, user)?;
    let subscriptions = subscriptions.register(ring, domain_id, user)?;
    let participant_messages = participant_messages.register(ring, domain_id, user)?;
    Ok(Caches {
      publications,
      topics,
      participants,
      subscriptions,
      participant_messages,
    })
  }
}

impl<S> Caches<S> {
  // used if theres no
  fn subscriber_qos() -> QosPolicies {
    // The Subscriber QoS is specified in DDS Spec v1.4 Section
    // "2.2.5 Built-in Topics"
    QosPolicyBuilder::new()
      .durability(Durability::TransientLocal)
      .presentation(Presentation {
        access_scope: PresentationAccessScope::Topic,
        coherent_access: false,
        ordered_access: false,
      })
      .deadline(Deadline(Duration::INFINITE))
      .ownership(Ownership::Shared)
      .liveliness(Liveliness::Automatic {
        lease_duration: Duration::INFINITE,
      })
      .time_based_filter(TimeBasedFilter {
        minimum_separation: Duration::ZERO,
      })
      .reliability(Reliability::Reliable {
        max_blocking_time: Duration::from_std(StdDuration::from_millis(100)),
      })
      .destination_order(DestinationOrder::ByReceptionTimestamp)
      // Spec gives History KeepLast depth = 1, but we
      // use somewhat higher to avoid losing data at the receiver in case
      // it comes in bursts and there is some delay in Discovery processing.
      .history(History::KeepLast { depth: 4 })
      // TODO:
      // Spec says all resource limits should be "LENGTH_UNLIMITED",
      // but that may lead to memory exhaustion.
      //
      // .resource_limits(ResourceLimits { // TODO: Maybe lower limits would suffice?
      //   max_instances: std::i32::MAX,
      //   max_samples: std::i32::MAX,
      //   max_samples_per_instance: std::i32::MAX,
      // })
      .build()
  }

  fn participant_message_qos() -> QosPolicies {
    QosPolicies {
      durability: Some(Durability::TransientLocal),
      presentation: None,
      deadline: None,
      latency_budget: None,
      ownership: None,
      liveliness: None,
      time_based_filter: None,
      reliability: Some(Reliability::Reliable {
        max_blocking_time: Duration::ZERO,
      }),
      destination_order: None,
      history: Some(History::KeepLast { depth: 1 }),
      resource_limits: None,
      lifespan: None,
      #[cfg(feature = "security")]
      property: None,
    }
  }

  fn spdp_participant_qos() -> QosPolicies {
    QosPolicyBuilder::new()
      .reliability(Reliability::BestEffort)
      // Use depth=8 to avoid losing data when we receive notifications
      // faster than we can process.
      .history(History::KeepLast { depth: 8 })
      .build()
  }
}

impl Caches<timer_state::Init> {
  fn topic_cache_from_entity_id(
    &mut self,
    entity_id: EntityId,
    remote_writer: &GUID,
  ) -> (&mut TopicCache, Option<&mut RtpsWriterProxy>) {
    match entity_id {
      EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => (
        &mut self.publications.topic,
        self.publications.writer_proxies.get_mut(remote_writer),
      ),
      EntityId::SEDP_BUILTIN_TOPIC_WRITER => (
        &mut self.topics.topic,
        self.topics.writer_proxies.get_mut(remote_writer),
      ),
      EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => (
        &mut self.participants.topic,
        self.participants.writer_proxies.get_mut(remote_writer),
      ),
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => (
        &mut self.subscriptions.topic,
        self.subscriptions.writer_proxies.get_mut(remote_writer),
      ),
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => (
        &mut self.participant_messages.topic,
        self
          .participant_messages
          .writer_proxies
          .get_mut(remote_writer),
      ),
      _ => unreachable!(),
    }
  }

  fn update_reader_state(
    &mut self,
    entity_id: EntityId,
    instant: Timestamp,
    writer_guid: GUID,
    sequence_number: SequenceNumber,
  ) {
    match entity_id {
      EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => {
        let state = &mut self.publications.read;
        state.last_read_sn.insert(writer_guid, sequence_number);
        state.latest_instant = instant;
      }
      EntityId::SEDP_BUILTIN_TOPIC_WRITER => {
        let state = &mut self.topics.read;
        state.last_read_sn.insert(writer_guid, sequence_number);
        state.latest_instant = instant;
      }
      EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => {
        let state = &mut self.participants.read;
        state.last_read_sn.insert(writer_guid, sequence_number);
        state.latest_instant = instant;
      }
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => {
        let state = &mut self.subscriptions.read;
        state.last_read_sn.insert(writer_guid, sequence_number);
        state.latest_instant = instant;
      }
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => {
        let state = &mut self.participant_messages.read;
        state.last_read_sn.insert(writer_guid, sequence_number);
        state.latest_instant = instant;
      }
      _ => unreachable!(),
    }
  }

  fn populate_changes(&mut self, entity_id: EntityId, is_reliable: bool) {
    let (mut latest_instant, last_read_sn, topic_cache) = match entity_id {
      EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => {
        let ReadState {
          ref mut last_read_sn,
          latest_instant,
          ..
        } = self.publications.read;
        let topic = &mut self.publications.topic;
        (latest_instant, last_read_sn, topic)
      }
      EntityId::SEDP_BUILTIN_TOPIC_WRITER => {
        let ReadState {
          ref mut last_read_sn,
          latest_instant,
          ..
        } = self.topics.read;
        let topic = &mut self.topics.topic;
        (latest_instant, last_read_sn, topic)
      }
      EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => {
        let ReadState {
          ref mut last_read_sn,
          latest_instant,
          ..
        } = self.participants.read;
        let topic = &mut self.participants.topic;
        (latest_instant, last_read_sn, topic)
      }
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => {
        let ReadState {
          ref mut last_read_sn,
          latest_instant,
          ..
        } = self.subscriptions.read;
        let topic = &mut self.subscriptions.topic;
        (latest_instant, last_read_sn, topic)
      }
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => {
        let ReadState {
          ref mut last_read_sn,
          latest_instant,
          ..
        } = self.participant_messages.read;
        let topic = &mut self.participant_messages.topic;
        (latest_instant, last_read_sn, topic)
      }
      _ => unreachable!(),
    };

    let changes = if is_reliable {
      topic_cache.get_changes_in_range_reliable(last_read_sn)
    } else {
      topic_cache.get_changes_in_range_best_effort(latest_instant, Timestamp::now())
    };

    use crate::{serialization::pl_cdr_adapters::PlCdrDeserializerAdapter, CDRDeserializerAdapter};

    let mut reliable_reading = None;

    // handle changes
    for (timestamp, change) in changes {
      latest_instant = core::cmp::max(latest_instant, timestamp);
      reliable_reading = Some((change.writer_guid, change.sequence_number));

      macro_rules! deserialize_and_populate_cache {
        ($namespace:expr, $adapter:ty) => {
          let key_map = &mut $namespace.read.hash_to_key_map;

          let deserialized = Cache::deserialize::<$adapter>(timestamp, change, key_map);

          if matches!(deserialized, Err(ReadError::UnknownKey { .. })) {
            continue;
          }

          //TODO: could aggregate the error here.
          match deserialized {
            Ok(deserialized) => {
              $namespace
                .datasample
                .fill_from_deserialized_cache_change(deserialized);
            }
            Err(e) => warn!("{e:?}"),
          }
        };
      }

      match entity_id {
        EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => {
          deserialize_and_populate_cache!(
            self.publications,
            PlCdrDeserializerAdapter<DiscoveredWriterData>
          );
        }
        EntityId::SEDP_BUILTIN_TOPIC_WRITER => {
          deserialize_and_populate_cache!(
            self.topics,
            PlCdrDeserializerAdapter<DiscoveredTopicData>
          );
        }
        EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => {
          deserialize_and_populate_cache!(
            self.participants,
            PlCdrDeserializerAdapter<SpdpDiscoveredParticipantData>
          );
        }
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => {
          deserialize_and_populate_cache!(
            self.subscriptions,
            PlCdrDeserializerAdapter<DiscoveredReaderData>
          );
        }
        EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => {
          deserialize_and_populate_cache!(
            self.participant_messages,
            CDRDeserializerAdapter<ParticipantMessageData>
          );
        }
        _ => unreachable!(),
      }
    }

    if let Some((guid, sn)) = reliable_reading {
      self.update_reader_state(entity_id, latest_instant, guid, sn);
    }
  }

  fn distribute_changes<'a, 'b>(
    &'a mut self,
    entity_id: EntityId,
    discovery_db: &'b mut DiscoveryDB,
  ) -> Option<DiscoveredKind> {
    let read_condition = ReadCondition::not_read();
    Some(match entity_id {
      EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => {
        let cache = &mut self.publications.datasample;
        let keys = cache.select_keys_for_access(read_condition);
        let result = cache.take_by_keys(&keys);

        DiscoveredKind::Writer(DiscoveredIter::new(result))
      }
      EntityId::SEDP_BUILTIN_TOPIC_WRITER => {
        let cache = &mut self.topics.datasample;
        let keys = cache.select_keys_for_access(read_condition);
        let result = cache.take_by_keys(&keys);
        DiscoveredKind::Topic(TopicDiscoveryIter::new(result))
      }
      EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => {
        let cache = &mut self.participants.datasample;
        let keys = cache.select_keys_for_access(read_condition);
        let result = cache.take_by_keys(&keys);

        let domain_guid = discovery_db.my_guid.prefix;

        DiscoveredKind::Participant(ParticipantDiscoveryIter::new(domain_guid, result))
      }
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => {
        let cache = &mut self.subscriptions.datasample;
        let keys = cache.select_keys_for_access(read_condition);
        let result = cache.take_by_keys(&keys);

        DiscoveredKind::Reader(DiscoveredIter::new(result))
      }
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => {
        let cache = &mut self.participant_messages.datasample;
        let keys = cache.select_keys_for_access(read_condition);
        let result = cache.read_by_keys(&keys);

        //receive_participant_message()
        for sample in result {
          match sample.value {
            Sample::Value(sub_data) => {
              discovery_db.update_lease_duration(sub_data);
            }
            Sample::Dispose(reader_key) => {
              println!("lost: {reader_key:?}")
            }
          }
        }
        return None;
      }
      _ => unreachable!(),
    })
  }

  fn handle_heartbeat_msg(
    &mut self,
    writer_guid: GUID,
    heartbeat: &Heartbeat,
    final_flag_set: bool,
    mr_state: &MessageReceiverState,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) -> bool {
    //panic!("{writer_guid:?}");

    match writer_guid.entity_id {
      EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => self.publications.handle_heartbeat_msg(
        writer_guid,
        heartbeat,
        final_flag_set,
        mr_state,
        udp_sender,
        ring,
      ),
      EntityId::SEDP_BUILTIN_TOPIC_WRITER => self.topics.handle_heartbeat_msg(
        writer_guid,
        heartbeat,
        final_flag_set,
        mr_state,
        udp_sender,
        ring,
      ),
      EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => self.participants.handle_heartbeat_msg(
        writer_guid,
        heartbeat,
        final_flag_set,
        mr_state,
        udp_sender,
        ring,
      ),
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => self.subscriptions.handle_heartbeat_msg(
        writer_guid,
        heartbeat,
        final_flag_set,
        mr_state,
        udp_sender,
        ring,
      ),
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => {
        self.participant_messages.handle_heartbeat_msg(
          writer_guid,
          heartbeat,
          final_flag_set,
          mr_state,
          udp_sender,
          ring,
        )
      }
      _ => unreachable!(),
    }
  }

  pub(crate) fn send_preemptive_acknacks(
    &mut self,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) -> std::io::Result<()> {
    self
      .publications
      .send_preemptive_acknacks(udp_sender, ring)?;
    self.topics.send_preemptive_acknacks(udp_sender, ring)?;
    self
      .participants
      .send_preemptive_acknacks(udp_sender, ring)?;
    self
      .subscriptions
      .send_preemptive_acknacks(udp_sender, ring)?;
    self
      .participant_messages
      .send_preemptive_acknacks(udp_sender, ring)?;
    Ok(())
  }

  pub(crate) fn garbage_collect_cache(&mut self) {
    self.publications.garbage_collect_cache();
    self.topics.garbage_collect_cache();
    self.participants.garbage_collect_cache();
    self.subscriptions.garbage_collect_cache();
    self.participant_messages.garbage_collect_cache();
  }

  pub(crate) fn handle_timed_event(
    &mut self,
    ev: user_data::Timer,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) -> Result<(), user_data::Timer> {
    use user_data::Timer;
    Ok(match &ev {
      Timer::Write(entity_id, kind) => match *entity_id {
        EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER => self
          .publications
          .handle_writer_timed_event(kind, udp_sender, ring),
        EntityId::SEDP_BUILTIN_TOPIC_WRITER => self
          .topics
          .handle_writer_timed_event(kind, udp_sender, ring),
        EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER => self
          .participants
          .handle_writer_timed_event(kind, udp_sender, ring),
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER => self
          .subscriptions
          .handle_writer_timed_event(kind, udp_sender, ring),
        EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER => self
          .participant_messages
          .handle_writer_timed_event(kind, udp_sender, ring),
        _ => return Err(ev),
      },
      Timer::Read(entity_id, kind) => match *entity_id {
        EntityId::SEDP_BUILTIN_PUBLICATIONS_READER => {
          self.publications.handle_reader_timed_event(kind)
        }
        EntityId::SEDP_BUILTIN_TOPIC_READER => self.topics.handle_reader_timed_event(kind),
        EntityId::SPDP_BUILTIN_PARTICIPANT_READER => {
          self.participants.handle_reader_timed_event(kind)
        }
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER => {
          self.subscriptions.handle_reader_timed_event(kind)
        }
        EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER => {
          self.participant_messages.handle_reader_timed_event(kind)
        }
        _ => return Err(ev),
      },
      _ => unreachable!("all builtin timers should be checked by now."),
    })
  }

  fn initialize(&mut self, udp_sender: &UDPSender, ring: &mut io_uring::IoUring) {
    let Self {
      topics,
      participant_messages,
      ..
    } = self;

    topics.handle_heartbeat_tick(false, udp_sender, ring);
    participant_messages.handle_heartbeat_tick(false, udp_sender, ring);
  }
}

use crate::io_uring::encoding::user_data;

pub struct ParticipantCleanup {
  iter: vec::IntoIter<(GuidPrefix, LostReason)>,
}

impl Iterator for ParticipantCleanup {
  type Item = (GuidPrefix, LostReason);

  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next()
  }
}

pub(crate) struct DiscoveredData<'a, 'b, D: Keyed> {
  pub discovery_db: &'a mut DiscoveryDB,
  discovered: &'b mut std::vec::IntoIter<DataSample<D>>,
}

impl<'a, 'b, D: Keyed> DiscoveredData<'a, 'b, D> {
  fn new(
    discovery_db: &'a mut DiscoveryDB,
    discovered: &'b mut std::vec::IntoIter<DataSample<D>>,
  ) -> Self {
    Self {
      discovery_db,
      discovered,
    }
  }
}

#[derive(Debug)]
enum RediscoveredState {
  Topic(
    std::vec::IntoIter<DataSample<DiscoveredTopicData>>,
    Option<TopicDiscoveryState>,
  ),
  Reader(std::vec::IntoIter<DataSample<DiscoveredReaderData>>),
  Writer(std::vec::IntoIter<DataSample<DiscoveredWriterData>>),
}

#[derive(Debug)]
pub(crate) struct ParticipantDiscoveryIter {
  participant_guid_prefix: GuidPrefix,
  participant_data: vec::IntoIter<DataSample<SpdpDiscoveredParticipantData>>,
  current: Option<(RediscoveredState, GuidPrefix)>,
}

impl ParticipantDiscoveryIter {
  fn new(
    participant_guid_prefix: GuidPrefix,
    participant_data: Vec<DataSample<SpdpDiscoveredParticipantData>>,
  ) -> Self {
    Self {
      participant_guid_prefix,
      participant_data: participant_data.into_iter(),
      current: None,
    }
  }

  fn inner<'a, 'b>(
    &mut self,
    discovery_db: &'a mut DiscoveryDB,
    discovery: &'b mut Discovery2<timer_state::Init>,
  ) -> ParticipantDiscoveryIterInner<'a, 'b, '_> {
    ParticipantDiscoveryIterInner {
      participant_guid_prefix: &mut self.participant_guid_prefix,
      participant_data: &mut self.participant_data,
      topic_data: &mut discovery.caches.topics.datasample,
      reader_data: &mut discovery.caches.subscriptions.datasample,
      writer_data: &mut discovery.caches.publications.datasample,
      discovery_db,
      current: &mut self.current,
    }
  }
}

pub struct Discovered<'a, 'b, 'c> {
  pub(crate) discovery: &'a mut Discovery2<timer_state::Init>,
  pub(crate) discovery_db: &'b mut DiscoveryDB,
  kind: &'c mut DiscoveredKind,
}

impl<'a, 'b, 'c> Discovered<'a, 'b, 'c> {
  pub(crate) fn new(
    discovery: &'a mut Discovery2<timer_state::Init>,
    discovery_db: &'b mut DiscoveryDB,
    kind: &'c mut DiscoveredKind,
  ) -> Self {
    Self {
      discovery,
      discovery_db,
      kind,
    }
  }
}

#[derive(Debug)]
pub(crate) enum DiscoveredKind {
  Participant(ParticipantDiscoveryIter),
  Topic(TopicDiscoveryIter),
  Reader(DiscoveredIter<DiscoveredReaderData>),
  Writer(DiscoveredIter<DiscoveredWriterData>),
}

impl Iterator for Discovered<'_, '_, '_> {
  type Item = (
    DiscoveryNotificationType,
    Option<DomainParticipantStatusEvent>,
  );

  fn next(&mut self) -> Option<Self::Item> {
    match &mut self.kind {
      DiscoveredKind::Participant(p) => p.inner(&mut self.discovery_db, &mut self.discovery).next(),
      DiscoveredKind::Topic(t) => t.inner(&mut self.discovery_db).next(),
      DiscoveredKind::Reader(r) => r.inner(&mut self.discovery_db).next(),
      DiscoveredKind::Writer(w) => w.inner(&mut self.discovery_db).next(),
    }
  }
}

struct ParticipantDiscoveryIterInner<'a, 'b, 'c> {
  participant_guid_prefix: &'c GuidPrefix,
  discovery_db: &'a mut DiscoveryDB,
  participant_data: &'c mut vec::IntoIter<DataSample<SpdpDiscoveredParticipantData>>,
  topic_data: &'b mut DataSampleCache<DiscoveredTopicData>,
  reader_data: &'b mut DataSampleCache<DiscoveredReaderData>,
  writer_data: &'b mut DataSampleCache<DiscoveredWriterData>,
  current: &'c mut Option<(RediscoveredState, GuidPrefix)>,
}

impl<'a, 'b, 'c, 'd> ParticipantDiscoveryIterInner<'a, 'b, 'c> {
  fn advance_state<D: Keyed>(
    cache: &mut DataSampleCache<D>,
    f: impl FnMut(&DataSample<D>) -> bool,
  ) -> std::vec::IntoIter<DataSample<D>> {
    let read_condition = ReadCondition::any();
    let keys = cache.select_keys_for_access(read_condition);
    let result = cache.take_by_keys(&keys);

    let filtered = result.into_iter().filter(f).collect::<Vec<_>>();
    filtered.into_iter()
  }

  fn advance_to_readers(
    readers: &mut DataSampleCache<DiscoveredReaderData>,
    guid: GuidPrefix,
  ) -> std::vec::IntoIter<DataSample<DiscoveredReaderData>> {
    Self::advance_state(readers, |reader| match &reader.value {
      Sample::Value(r) => r.reader_proxy.remote_reader_guid.prefix == guid,
      Sample::Dispose(reader_key) => reader_key.0.prefix == guid,
    })
  }

  fn advance_to_writers(
    writers: &mut DataSampleCache<DiscoveredWriterData>,
    guid: GuidPrefix,
  ) -> std::vec::IntoIter<DataSample<DiscoveredWriterData>> {
    Self::advance_state(writers, |writer| match &writer.value {
      Sample::Value(r) => r.writer_proxy.remote_writer_guid.prefix == guid,
      Sample::Dispose(writer_key) => writer_key.0.prefix == guid,
    })
  }
}

impl Iterator for ParticipantDiscoveryIterInner<'_, '_, '_> {
  type Item = (
    DiscoveryNotificationType,
    Option<DomainParticipantStatusEvent>,
  );
  fn next(&mut self) -> Option<Self::Item> {
    // this is the only way to make the compiler happy.
    // something about the invariance of the lifetimes.
    match &mut self.current {
      None => (),
      Some((RediscoveredState::Topic(topics, state), guid)) => {
        if let Some(ret) = TopicDiscoveryIterInner::new(self.discovery_db, topics, state).next() {
          return Some(ret);
        }

        let mut readers = Self::advance_to_readers(self.reader_data, *guid);

        if let Some(ret) = DiscoveredData::new(self.discovery_db, &mut readers).next() {
          let state = RediscoveredState::Reader(readers);
          *self.current = Some((state, *guid));
          return Some(ret);
        }
        drop(readers);

        let mut writers = Self::advance_to_writers(self.writer_data, *guid);

        if let Some(ret) = DiscoveredData::new(self.discovery_db, &mut writers).next() {
          let state = RediscoveredState::Writer(writers);
          *self.current = Some((state, *guid));
          return Some(ret);
        }
        drop(writers);
      }
      Some((RediscoveredState::Reader(readers), guid)) => {
        if let Some(ret) = DiscoveredData::new(self.discovery_db, readers).next() {
          return Some(ret);
        }

        let mut writers = Self::advance_to_writers(self.writer_data, *guid);

        if let Some(ret) = DiscoveredData::new(self.discovery_db, &mut writers).next() {
          let state = RediscoveredState::Writer(writers);
          *self.current = Some((state, *guid));
          return Some(ret);
        }
        drop(writers);
      }
      Some((RediscoveredState::Writer(writers), _guid)) => {
        if let Some(ret) = DiscoveredData::new(self.discovery_db, writers).next() {
          return Some(ret);
        }
      }
    }

    let (guid, discovery_notification, status_event) =
      DiscoveredData::new(self.discovery_db, &mut self.participant_data).next()?;

    match guid {
      // remote participant that may have been discovered earlier,
      // re iterate over any existing caches.
      Some(guid) if guid != *self.participant_guid_prefix => {
        let topics = Self::advance_state(self.topic_data, |topic| {
          topic.sample_info.writer_guid().prefix == guid
        });

        let state = RediscoveredState::Topic(topics, None);
        *self.current = Some((state, guid));
      }
      _ => *self.current = None,
    }
    Some((discovery_notification, status_event))
  }
}

impl Iterator for DiscoveredData<'_, '_, SpdpDiscoveredParticipantData> {
  type Item = (
    Option<GuidPrefix>,
    DiscoveryNotificationType,
    Option<DomainParticipantStatusEvent>,
  );

  fn next(&mut self) -> Option<Self::Item> {
    let disc = self.discovered.next()?.value;

    match disc {
      Sample::Value(participant_data) => {
        let was_new = self.discovery_db.update_participant(&participant_data);
        let guid_prefix = participant_data.participant_guid.prefix;
        let discovery_notification = DiscoveryNotificationType::ParticipantUpdated { guid_prefix };

        let participant_status = if was_new {
          Some(DomainParticipantStatusEvent::ParticipantDiscovered {
            dpd: (&participant_data).into(),
          })
        } else {
          None
        };

        Some((
          Some(guid_prefix),
          discovery_notification,
          participant_status,
        ))
      }
      Sample::Dispose(participant_guid) => {
        let guid_prefix = participant_guid.0.prefix;
        self.discovery_db.remove_participant(guid_prefix, true);
        Some((
          None,
          DiscoveryNotificationType::ParticipantLost { guid_prefix },
          Some(DomainParticipantStatusEvent::ParticipantLost {
            id: guid_prefix,
            reason: LostReason::Disposed,
          }),
        ))
      }
    }
  }
}

pub(crate) struct DiscoveredIter<D: Keyed> {
  data: vec::IntoIter<DataSample<D>>,
}

use std::fmt::Debug;

impl<D: Keyed + Debug> Debug for DiscoveredIter<D>
where
  <D as Keyed>::K: Debug,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("DiscoveredIter")
      .field("data", &self.data)
      .finish()
  }
}

impl<D: Keyed> DiscoveredIter<D> {
  fn new(data: Vec<DataSample<D>>) -> Self {
    Self {
      data: data.into_iter(),
    }
  }

  fn inner<'a>(&mut self, discovery_db: &'a mut DiscoveryDB) -> DiscoveredData<'a, '_, D> {
    DiscoveredData::new(discovery_db, &mut self.data)
  }
}

impl Iterator for DiscoveredData<'_, '_, DiscoveredReaderData> {
  type Item = (
    DiscoveryNotificationType,
    Option<DomainParticipantStatusEvent>,
  );

  fn next(&mut self) -> Option<Self::Item> {
    let discovered = self.discovered.next()?.value;

    match discovered.map_dispose(|g| g.0) {
      Sample::Value(sub) => {
        let (discovered_reader_data, participant_status) =
          self.discovery_db.update_subscription(&sub);

        let discovery_notification = DiscoveryNotificationType::ReaderUpdated {
          discovered_reader_data,
        };

        Some((discovery_notification, participant_status))
      }
      Sample::Dispose(reader_key) => {
        self.discovery_db.remove_topic_reader(reader_key);

        let discovery_notification = DiscoveryNotificationType::ReaderLost {
          reader_guid: reader_key,
        };

        let participant_status = DomainParticipantStatusEvent::ReaderLost {
          guid: reader_key,
          reason: LostReason::Disposed,
        };
        Some((discovery_notification, Some(participant_status)))
      }
    }
  }
}

impl Iterator for DiscoveredData<'_, '_, DiscoveredWriterData> {
  type Item = (
    DiscoveryNotificationType,
    Option<DomainParticipantStatusEvent>,
  );

  fn next(&mut self) -> Option<Self::Item> {
    let discovered = self.discovered.next()?.value;

    match discovered.map_dispose(|g| g.0) {
      Sample::Value(discovered) => {
        let (discovered_writer_data, participant_status) =
          self.discovery_db.update_publication(&discovered);

        let discovery_notification = DiscoveryNotificationType::WriterUpdated {
          discovered_writer_data,
        };
        Some((discovery_notification, participant_status))
      }
      Sample::Dispose(writer_key) => {
        self.discovery_db.remove_topic_writer(writer_key);

        let discovery_notification = DiscoveryNotificationType::WriterLost {
          writer_guid: writer_key,
        };
        let participant_status = DomainParticipantStatusEvent::WriterLost {
          guid: writer_key,
          reason: LostReason::Disposed,
        };
        Some((discovery_notification, Some(participant_status)))
      }
    }
  }
}

//wrapper for DiscoveredData<'a, DiscoveredTopicData>
//to be able to iterate over the updated readers/writers associated with the
// topic. if there are no readers/writers for a topic then a status event will
// not be emitted.
use std::vec;

#[derive(Debug)]
pub(crate) struct TopicDiscoveryIter {
  topic_data: vec::IntoIter<DataSample<DiscoveredTopicData>>,
  state: Option<TopicDiscoveryState>,
}

impl TopicDiscoveryIter {
  fn new(topic_data: Vec<DataSample<DiscoveredTopicData>>) -> Self {
    Self {
      topic_data: topic_data.into_iter(),
      state: None,
    }
  }

  fn inner<'a>(&mut self, discovery_db: &'a mut DiscoveryDB) -> TopicDiscoveryIterInner<'a, '_> {
    TopicDiscoveryIterInner {
      current: &mut self.state,
      discovery_db,
      topic_data: &mut self.topic_data,
    }
  }
}

#[derive(Debug)]
struct TopicDiscoveryState {
  discovered_readers: vec::IntoIter<DiscoveredReaderData>,
  discovered_writers: vec::IntoIter<DiscoveredWriterData>,
}

impl TopicDiscoveryState {
  fn new(
    discovered_readers: vec::IntoIter<DiscoveredReaderData>,
    discovered_writers: vec::IntoIter<DiscoveredWriterData>,
  ) -> Self {
    Self {
      discovered_readers,
      discovered_writers,
    }
  }

  fn take_item(&mut self) -> Option<DiscoveryNotificationType> {
    if let Some(notification) = if let Some(discovered_reader_data) = self.discovered_readers.next()
    {
      Some(DiscoveryNotificationType::ReaderUpdated {
        discovered_reader_data,
      })
    } else if let Some(discovered_writer_data) = self.discovered_writers.next() {
      Some(DiscoveryNotificationType::WriterUpdated {
        discovered_writer_data,
      })
    } else {
      None
    } {
      Some(notification)
    } else {
      None
    }
  }
}

struct TopicDiscoveryIterInner<'a, 'b> {
  discovery_db: &'a mut DiscoveryDB,
  topic_data: &'b mut vec::IntoIter<DataSample<DiscoveredTopicData>>,
  current: &'b mut Option<TopicDiscoveryState>,
}

impl<'a, 'b> TopicDiscoveryIterInner<'a, 'b> {
  fn new(
    discovery_db: &'a mut DiscoveryDB,
    topic_data: &'b mut vec::IntoIter<DataSample<DiscoveredTopicData>>,
    current: &'b mut Option<TopicDiscoveryState>,
  ) -> Self {
    Self {
      discovery_db,
      topic_data,
      current,
    }
  }
}

impl Iterator for TopicDiscoveryIterInner<'_, '_> {
  type Item = (
    DiscoveryNotificationType,
    Option<DomainParticipantStatusEvent>,
  );

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(state) = self.current.as_mut() {
      if let Some(ret) = state.take_item() {
        return Some((ret, None));
      }
    }

    let (topic_data, writer, status_event) =
      DiscoveredData::new(self.discovery_db, &mut self.topic_data).next()?;

    let readers = self
      .discovery_db
      .readers_on_topic_and_participant(topic_data.topic_name(), writer);

    let writers = self
      .discovery_db
      .writers_on_topic_and_participant(topic_data.topic_name(), writer);

    *self.current = Some(TopicDiscoveryState::new(
      readers.into_iter(),
      writers.into_iter(),
    ));

    Some((DiscoveryNotificationType::TopicDiscovered, status_event))
  }
}

impl Iterator for DiscoveredData<'_, '_, DiscoveredTopicData> {
  type Item = (
    DiscoveredTopicData,
    GuidPrefix,
    Option<DomainParticipantStatusEvent>,
  );
  fn next(&mut self) -> Option<Self::Item> {
    let discovered = self.discovered.next()?;

    match discovered.value {
      Sample::Value(topic_data) => {
        let writer = discovered.sample_info.writer_guid();
        let status_event =
          self
            .discovery_db
            .update_topic_data(&topic_data, writer, DiscoveredVia::Topic);
        Some((topic_data, writer.prefix, status_event))
      }
      Sample::Dispose(_key) => {
        warn!("not implemented");
        None
      }
    }
  }
}

// DomainParticipantStatusEvent is for the user to listen in on the domain
// participant (wow) DiscoveryNotificationType is used in the DpEventLoop (now
// Domain) to update connections (and also send some
// DomainParticipantStatusEvent's)

impl Timers<timer_state::Uninit> {
  const PARTICIPANT_CLEANUP_PERIOD: StdDuration = StdDuration::from_secs(2);
  const TOPIC_CLEANUP_PERIOD: StdDuration = StdDuration::from_secs(30); // timer for cleaning up inactive topics
  const SPDP_PUBLISH_PERIOD: StdDuration = StdDuration::from_secs(10);
  const CHECK_PARTICIPANT_MESSAGES_PERIOD: StdDuration = StdDuration::from_secs(1);

  #[cfg(feature = "security")]
  const CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_PERIOD: StdDuration = StdDuration::from_secs(1);

  fn new() -> Self {
    let participant_cleanup = Timer::new_periodic((), Self::PARTICIPANT_CLEANUP_PERIOD);
    let topic_cleanup = Timer::new_periodic((), Self::TOPIC_CLEANUP_PERIOD);
    let spdp_publish = Timer::new_periodic((), Self::SPDP_PUBLISH_PERIOD);
    let participant_messages = Timer::new_periodic((), Self::CHECK_PARTICIPANT_MESSAGES_PERIOD);

    Self {
      participant_cleanup,
      topic_cleanup,
      spdp_publish,
      participant_messages,
    }
  }

  fn register(
    self,
    ring: &mut io_uring::IoUring,
    domain_id: u16,
    user: u8,
  ) -> std::io::Result<Timers<timer_state::Init>> {
    let Self {
      participant_cleanup,
      topic_cleanup,
      spdp_publish,
      participant_messages,
    } = self;

    use crate::io_uring::encoding::user_data::{BuiltinTimerVariant, Timer};

    let participant_cleanup = participant_cleanup.register(
      ring,
      domain_id,
      Timer::Builtin(BuiltinTimerVariant::ParticipantCleaning),
      user,
    )?;
    let topic_cleanup = topic_cleanup.register(
      ring,
      domain_id,
      Timer::Builtin(BuiltinTimerVariant::TopicCleaning),
      user,
    )?;
    let spdp_publish = spdp_publish.register(
      ring,
      domain_id,
      Timer::Builtin(BuiltinTimerVariant::SpdpPublish),
      user,
    )?;
    let participant_messages = participant_messages.register(
      ring,
      domain_id,
      Timer::Builtin(BuiltinTimerVariant::ParticipantMessages),
      user,
    )?;

    Ok(Timers {
      participant_cleanup,
      topic_cleanup,
      spdp_publish,
      participant_messages,
    })
  }
}
use crate::messages::submessages::submessage::WriterSubmessage;

pub struct Discovery2<T> {
  pub(crate) liveliness_state: LivelinessState,

  /*
  dcps_participant: with_key::DiscoveryTopicPlCdr<SpdpDiscoveredParticipantData>,

  // Topic "DCPSSubscription" - announcing and detecting Readers
  dcps_subscription: with_key::DiscoveryTopicPlCdr<DiscoveredReaderData>,

  // Topic "DCPSPublication" - announcing and detecting Writers
  dcps_publication: with_key::DiscoveryTopicPlCdr<DiscoveredWriterData>,

  // Topic "DCPSTopic" - announcing and detecting topics
  dcps_topic: with_key::DiscoveryTopicPlCdr<DiscoveredTopicData>,

  // DCPSParticipantMessage - used by participants to communicate liveness
  dcps_participant_message: with_key::DiscoveryTopicCDR<ParticipantMessageData>,
  */
  // If security is enabled, this field contains a SecureDiscovery struct, an appendix
  // which is used for Secure functionality
  security_opt: Option<SecureDiscovery>,
  domain_id: u16,

  timers: Timers<T>,
  pub(crate) caches: Caches<T>,
  initialized: bool,
}

impl Discovery2<timer_state::Uninit> {
  pub fn new(
    security_plugins_opt: Option<SecurityPluginsHandle>,
    guid_prefix: GuidPrefix,
    domain_id: u16,
  ) -> Self {
    #[cfg(not(feature = "security"))]
    let security_opt = security_plugins_opt.and(None); // = None, but avoid warning.

    Self {
      timers: Timers::new(),
      liveliness_state: LivelinessState::new(),
      security_opt,
      caches: Caches::new(guid_prefix),
      domain_id,
      initialized: false,
    }
  }

  pub fn register(
    self,
    ring: &mut io_uring::IoUring,
    user: u8,
  ) -> std::io::Result<Discovery2<timer_state::Init>> {
    let Self {
      timers,
      liveliness_state,
      security_opt,
      caches,
      domain_id,
      initialized,
    } = self;

    let timers = timers.register(ring, domain_id, user)?;
    let caches = caches.register(ring, domain_id, user)?;

    Ok(Discovery2 {
      timers,
      liveliness_state,
      security_opt,
      caches,
      domain_id,
      initialized,
    })
  }
}

impl Discovery2<timer_state::Init> {
  pub(crate) fn handle_writer_msg<'a, 'b>(
    &'a mut self,
    submsg: WriterSubmessage,
    mr_state: &MessageReceiverState,
    discovery_db: &'b mut DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) -> Result<Option<DiscoveredKind>, WriterSubmessage> {
    use crate::messages::submessages::submessage::HasEntityIds;

    let sender_id = submsg.sender_entity_id();

    //println!("({:?} / {sender_id:?})", submsg.receiver_entity_id());

    if !matches!(
      (submsg.receiver_entity_id(), sender_id),
      (
        EntityId::UNKNOWN,
        EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER
          | EntityId::SEDP_BUILTIN_TOPIC_WRITER
          | EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
          | EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER
          | EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER
      ) | (
        EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
        EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER
      ) | (
        EntityId::SEDP_BUILTIN_TOPIC_READER,
        EntityId::SEDP_BUILTIN_TOPIC_WRITER
      ) | (
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER,
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER
      )
    ) {
      return Err(submsg);
    }

    match submsg {
      WriterSubmessage::Data(data, flags) => {
        use crate::{
          dds::ddsdata::DDSData,
          messages::submessages::{
            elements::{inline_qos::InlineQos, serialized_payload::SerializedPayload},
            submessages::DATA_Flags,
          },
          structure::cache_change::ChangeKind,
          WriteOptionsBuilder,
        };

        let receive_timestamp = Timestamp::now();

        // parse write_options out of the message
        let mut write_options_b = WriteOptionsBuilder::new();
        // Check if we have s source timestamp
        if let Some(source_timestamp) = mr_state.source_timestamp {
          write_options_b = write_options_b.source_timestamp(source_timestamp);
        }
        // Check if the message specifies a related_sample_identity
        let representation_identifier = DATA_Flags::cdr_representation_identifier(flags);
        if let Some(related_sample_identity) =
          data.inline_qos.as_ref().and_then(|inline_qos_parameters| {
            InlineQos::related_sample_identity(inline_qos_parameters, representation_identifier)
              .unwrap_or_else(|e| {
                error!("Deserializing related_sample_identity: {:?}", &e);
                None
              })
          })
        {
          write_options_b = write_options_b.related_sample_identity(related_sample_identity);
        }

        let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, data.writer_id);
        let writer_seq_num = data.writer_sn; // for borrow checker

        let dds_data = match (
          data.serialized_payload,
          flags.contains(DATA_Flags::Data),
          flags.contains(DATA_Flags::Key),
        ) {
          (Some(serialized_payload), true, false) => {
            // data
            Ok(DDSData::new(
              SerializedPayload::from_bytes(&serialized_payload)
                .map_err(|e| format!("{e:?}"))
                .unwrap(),
            ))
          }

          (Some(serialized_payload), false, true) => {
            // key
            Ok(DDSData::new_disposed_by_key(
              match data.inline_qos.as_ref().and_then(|inline_qos_parameters| {
                InlineQos::status_info(inline_qos_parameters, representation_identifier)
                  .map_or_else(
                    |e| {
                      error!("Deserializing status_info: {:?}", &e);
                      None
                    },
                    Some,
                  )
              }) {
                Some(si) => si.change_kind(), // get from inline QoS
                // TODO: What if si.change_kind() gives ALIVE ??
                None => {
                  if false {
                    ChangeKind::NotAliveUnregistered
                  } else {
                    ChangeKind::NotAliveDisposed
                  } // TODO: Is this reasonable default?
                }
              },
              SerializedPayload::from_bytes(&serialized_payload)
                .map_err(|e| format!("{e:?}"))
                .unwrap(),
            ))
          }

          (None, false, false) => {
            // no data, no key. Maybe there is inline QoS?
            // At least we should find key hash, or we do not know WTF the writer is talking
            // about
            let key_hash = if let Some(h) =
              data.inline_qos.as_ref().and_then(|inline_qos_parameters| {
                InlineQos::key_hash(inline_qos_parameters).unwrap_or_else(|e| {
                  error!("Deserializing key_hash: {:?}", &e);
                  None
                })
              }) {
              Ok(h)
            } else {
              info!("Received DATA that has no payload and no key_hash inline QoS - discarding");
              // Note: This case is normal when handling coherent sets.
              // The coherent set end marker is sent as DATA with no payload and not key, only
              // Inline QoS.
              Err("DATA with no contents".to_string())
            }
            .unwrap();
            // now, let's try to determine what is the dispose reason

            let change_kind = match data.inline_qos.as_ref().and_then(|inline_qos_parameters| {
              InlineQos::status_info(inline_qos_parameters, representation_identifier).map_or_else(
                |e| {
                  error!("Deserializing status_info: {:?}", &e);
                  None
                },
                Some,
              )
            }) {
              Some(si) => si.change_kind(), // get from inline QoS
              // TODO: What if si.change_kind() gives ALIVE ??
              None => {
                if false {
                  ChangeKind::NotAliveUnregistered
                } else {
                  ChangeKind::NotAliveDisposed
                } // TODO: Is this reasonable default?
              }
            };
            /*
            info!(
              "status change by Inline QoS: topic={:?} change={:?}",
              self.topic_name, change_kind
            );
            */
            Ok(DDSData::new_disposed_by_key_hash(change_kind, key_hash))
          }

          (Some(_), true, true) => {
            // payload cannot be both key and data.
            // RTPS Spec 9.4.5.3.1 Flags in the Submessage Header says
            // "D=1 and K=1 is an invalid combination in this version of the protocol."
            warn!("Got DATA that claims to be both data and key - discarding.");
            Err("Ambiguous data/key received.".to_string())
          }

          (Some(_), false, false) => {
            // data but no data? - this should not be possible
            warn!("make_cache_change - Flags says no data or key, but got payload!");
            Err("DATA message has mystery contents".to_string())
          }
          (None, true, _) | (None, _, true) => {
            warn!("make_cache_change - Where is my SerializedPayload?");
            Err("DATA message contents missing".to_string())
          }
        };

        let Ok(data) = dds_data else {
          println!("could not parse data to DDSData");
          return Ok(None);
        };

        //println!("trying to add cache change");

        use crate::structure::cache_change::CacheChange;
        let cache_change =
          CacheChange::new(writer_guid, writer_seq_num, write_options_b.build(), data);

        let (topic_cache, writer) = self
          .caches
          .topic_cache_from_entity_id(sender_id, &writer_guid);
        topic_cache.add_change(&receive_timestamp, cache_change);

        use crate::io_uring::discovery::discovery::Reliability;

        let _is_reliable = matches!(
          topic_cache.topic_qos.reliability(),
          Some(Reliability::Reliable { .. })
        );

        if let Some(writer) = writer {
          if !writer.should_ignore_change(writer_seq_num) {
            writer.received_changes_add(writer_seq_num, receive_timestamp);
          } else {
            println!("ignoring cache change")
          }

          topic_cache.mark_reliably_received_before(writer_guid, writer.all_ackable_before());
        }

        // 1) populate changes to datasample_cache
        //TODO: replace 'false' with 'is_reliable'
        // - this currently makes msg delivery unreliable
        self.caches.populate_changes(sender_id, false);
        // 2) handle unread changes from the datasample_cache and bring them back here.

        if matches!(sender_id, EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER) {
          //TODO: send spdp_liveness for the given guid prefix
        }

        return Ok(self.caches.distribute_changes(sender_id, discovery_db));
      }
      WriterSubmessage::Heartbeat(heartbeat, flags) => {
        let writer_guid =
          GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, heartbeat.writer_id);
        use crate::messages::submessages::submessages::HEARTBEAT_Flags;

        self.caches.handle_heartbeat_msg(
          writer_guid,
          &heartbeat,
          flags.contains(HEARTBEAT_Flags::Final),
          mr_state,
          udp_sender,
          ring,
        );
      }
      //_ => (),
      o => println!("\nother missed {o:?}"),
    }
    Ok(None)
  }

  pub(crate) fn spdp_publish(
    &mut self,
    participant_guid: GUID,
    listeners: &UdpListeners<buf_ring_state::Init>,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    self.timers.spdp_publish.reset();

    let TrafficLocators {
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
    } = listeners.self_locators();

    let data = SpdpDiscoveredParticipantData::from_local(
      participant_guid,
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
      &self.security_opt,
      5.0 * Duration::from(Timers::SPDP_PUBLISH_PERIOD),
    );

    self
      .caches
      .participants
      .write::<PlCdrSerializerAdapter<SpdpDiscoveredParticipantData>>(data, udp_sender, ring, true)
      .unwrap();
  }

  pub fn handle_reader_submsg(
    &mut self,
    submsg: ReaderSubmessage,
    source_guid_prefix: GuidPrefix,
    ring: &mut IoUring,
    user: u8,
  ) -> Result<Option<()>, (ReaderSubmessage, GuidPrefix)> {
    use crate::messages::submessages::submessage::HasEntityIds;
    let sender_id = submsg.sender_entity_id();

    if !matches!(
      (submsg.receiver_entity_id(), sender_id),
      (
        EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
        EntityId::SEDP_BUILTIN_PUBLICATIONS_READER
      ) | (
        EntityId::SEDP_BUILTIN_TOPIC_WRITER,
        EntityId::SEDP_BUILTIN_TOPIC_READER
      ) | (
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER,
        EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER
      ) | (
        EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER,
        EntityId::SPDP_BUILTIN_PARTICIPANT_READER
      ) | (
        EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER,
        EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER
      )
    ) {
      return Err((submsg, source_guid_prefix));
    }

    let ack_submsg = match submsg {
      ReaderSubmessage::AckNack(acknack, _) => AckSubmessage::AckNack(acknack),
      ReaderSubmessage::NackFrag(nack_frag, _) => AckSubmessage::NackFrag(nack_frag),
    };

    let domain_id = self.domain_id;

    match sender_id {
      EntityId::SEDP_BUILTIN_PUBLICATIONS_READER => self.caches.publications.handle_acknack(
        ack_submsg,
        source_guid_prefix,
        ring,
        domain_id,
        user,
      ),
      EntityId::SEDP_BUILTIN_TOPIC_READER => {
        self
          .caches
          .topics
          .handle_acknack(ack_submsg, source_guid_prefix, ring, domain_id, user)
      }
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER => self.caches.subscriptions.handle_acknack(
        ack_submsg,
        source_guid_prefix,
        ring,
        domain_id,
        user,
      ),
      EntityId::SPDP_BUILTIN_PARTICIPANT_READER => self.caches.participants.handle_acknack(
        ack_submsg,
        source_guid_prefix,
        ring,
        domain_id,
        user,
      ),
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER => self
        .caches
        .participant_messages
        .handle_acknack(ack_submsg, source_guid_prefix, ring, domain_id, user),
      _ => unreachable!(),
    }
    Ok(Some(()))
  }

  // this is going to be another iterator.
  pub fn participant_cleanup(&mut self, discovery_db: &mut DiscoveryDB) -> ParticipantCleanup {
    self.timers.participant_cleanup.reset();
    ParticipantCleanup {
      iter: discovery_db.participant_cleanup().into_iter(),
    }
  }

  pub fn topic_cleanup(&mut self, discovery_db: &mut DiscoveryDB) {
    self.timers.topic_cleanup.reset();
    discovery_db.topic_cleanup()
  }

  pub fn publish_participant_message(
    &mut self,
    discovery_db: &DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    self.timers.participant_messages.reset();

    let min_automatic_lease_duration = discovery_db
      .get_all_local_topic_writers()
      .filter_map(|p| match p.publication_topic_data.liveliness? {
        Liveliness::Automatic { lease_duration } => Some(lease_duration),
        _ => None,
      })
      .min();

    let timenow = Timestamp::now();

    let mut messages_to_be_sent: Vec<ParticipantMessageData> = vec![];

    let guid = self.caches.participant_messages.writer_guid.prefix;

    // Send Automatic liveness update if needed
    if let Some(min_auto_duration) = min_automatic_lease_duration {
      let time_since_last_auto_update =
        timenow.duration_since(self.liveliness_state.last_auto_update);
      trace!(
        "time_since_last_auto_update: {time_since_last_auto_update:?}, min_auto_duration \
         {min_auto_duration:?}"
      );

      // We choose to send a new liveliness message if longer than half of the min
      // auto duration has elapsed since last message
      if time_since_last_auto_update > min_auto_duration / 2 {
        let msg = ParticipantMessageData {
          guid,
          kind: ParticipantMessageDataKind::AUTOMATIC_LIVELINESS_UPDATE,
          data: Vec::new(),
        };
        messages_to_be_sent.push(msg);
      }
    }

    // Send ManualByParticipant liveliness update if someone has requested us to do
    // so.
    // Note: According to the RTPS spec (8.7.2.2.3 LIVELINESS) the interval at which
    // we check if we need to send a manual liveness update should depend on the
    // lease durations of writers with ManualByParticipant liveness QoS.
    // Now we just check this at the same interval as with Automatic liveness.
    // So TODO if needed: comply with the spec.
    if self
      .liveliness_state
      .manual_participant_liveness_refresh_requested
    {
      let msg = ParticipantMessageData {
        guid,
        kind: ParticipantMessageDataKind::MANUAL_LIVELINESS_UPDATE,
        data: Vec::new(),
      };
      messages_to_be_sent.push(msg);
    }

    for msg in messages_to_be_sent {
      self
        .caches
        .participant_messages
        .write::<CDRSerializerAdapter<ParticipantMessageData>>(msg, udp_sender, ring, true)
        .unwrap();
    }
  }

  // NOTE: for all of these, only user-defined are allowed
  // eg, no builtin readers/writers/topics
  // TODO: could have a user facing api that takes a mut ref
  // of discoverydb, domain, discovery, udpsender, and io_uring
  // and you can create mut refs to subscribers/publishers
  // and those can create either create managed readers or user defined writers.
  // also, when starting discovery, `initalize_participant`
  // and `sedp_publish_writers/readers` is called
  pub fn publish_writer(
    &mut self,
    guid: GUID,
    discovery_db: &DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let Some(writer) = discovery_db.get_local_topic_writer(guid) else {
      warn!("Did not find a local writer {guid:?}");
      return;
    };

    //println!("publishing writer: {writer:?}");

    if !writer
      .writer_proxy
      .remote_writer_guid
      .entity_id
      .kind()
      .is_user_defined()
    {
      // Only writers of user-defined topics are published to discovery
      return;
    }

    self
      .caches
      .publications
      .write::<PlCdrSerializerAdapter<DiscoveredWriterData>>(
        writer.clone(),
        udp_sender,
        ring,
        self.initialized,
      )
      .unwrap();
  }

  pub fn publish_reader(
    &mut self,
    guid: GUID,
    discovery_db: &DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let Some(reader) = discovery_db.get_local_topic_reader(guid) else {
      warn!("Did not find a local reader {guid:?}");
      return;
    };

    //println!("publish reader: {reader:?}");

    if !reader
      .reader_proxy
      .remote_reader_guid
      .entity_id
      .kind()
      .is_user_defined()
    {
      // Only readers of user-defined topics are published to discovery
      return;
    }

    self
      .caches
      .subscriptions
      .write::<PlCdrSerializerAdapter<DiscoveredReaderData>>(
        reader.clone(),
        udp_sender,
        ring,
        self.initialized,
      )
      .unwrap();
  }

  pub fn initialize(
    &mut self,
    discovery_db: &mut DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
    domain: &Domain<timer_state::Init, buf_ring_state::Init>,
  ) {
    //println!("init participant");
    self.initialize_participant(
      discovery_db,
      domain.domain_info.domain_participant_guid,
      &domain.listeners,
    );

    self.initialized = true;

    //println!("publishing ext");

    self.spdp_publish(
      domain.domain_info.domain_participant_guid,
      &domain.listeners,
      udp_sender,
      ring,
    );

    self.publish_participant_message(discovery_db, udp_sender, ring);

    self.publish_writers(discovery_db, udp_sender, ring);
    self.publish_readers(discovery_db, udp_sender, ring);

    self.caches.initialize(udp_sender, ring);
  }

  fn initialize_participant(
    &mut self,
    discovery_db: &mut DiscoveryDB,
    participant_guid: GUID,
    listeners: &UdpListeners<buf_ring_state::Init>,
  ) {
    let TrafficLocators {
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
    } = listeners.self_locators();

    let participant_data = SpdpDiscoveredParticipantData::from_local(
      participant_guid,
      multicast_discovery,
      unicast_discovery,
      multicast_user_traffic,
      unicast_user_traffic,
      &self.security_opt,
      Duration::INFINITE,
    );
    discovery_db.update_participant(&participant_data);

    // dont care to send updates for discovering itself
    for _ in crate::io_uring::rtps::dp_event_loop::UpdatedParticipant2::new(
      &mut self.caches,
      participant_data,
    ) {}
  }

  pub fn initialized(&self) -> bool {
    self.initialized
  }

  fn publish_writers(
    &mut self,
    discovery_db: &DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let local_writer_guids = discovery_db
      .get_all_local_topic_writers()
      .filter(|p| {
        p.writer_proxy
          .remote_writer_guid
          .entity_id
          .kind()
          .is_user_defined()
      })
      .map(|writer| writer.writer_proxy.remote_writer_guid);

    for guid in local_writer_guids {
      self.publish_writer(guid, discovery_db, udp_sender, ring);
    }
  }

  fn publish_readers(
    &mut self,
    discovery_db: &DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let local_reader_guids = discovery_db
      .get_all_local_topic_readers()
      .filter(|p| {
        p.reader_proxy
          .remote_reader_guid
          .entity_id
          .kind()
          .is_user_defined()
      })
      .map(|reader| reader.reader_proxy.remote_reader_guid);

    for guid in local_reader_guids {
      self.publish_reader(guid, discovery_db, udp_sender, ring);
    }
  }

  pub fn remove_writer(
    &mut self,
    guid: GUID,
    discovery_db: &mut DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    // instead of using a channel,
    // an Rc<()> could be used to tell when a reader/writer is dead
    //  - the rtps reader holds an rc::Weak, data reader holds a rc::Strong

    discovery_db.remove_local_topic_writer(guid);

    use crate::discovery::Endpoint_GUID;
    self
      .caches
      .publications
      .dispose::<PlCdrSerializerAdapter<DiscoveredWriterData>>(
        &Endpoint_GUID(guid),
        udp_sender,
        ring,
        true,
      )
      .unwrap();
  }

  pub fn remove_reader(
    &mut self,
    guid: GUID,
    discovery_db: &mut DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    discovery_db.remove_local_topic_reader(guid);

    use crate::discovery::Endpoint_GUID;
    self
      .caches
      .subscriptions
      .dispose::<PlCdrSerializerAdapter<DiscoveredReaderData>>(
        &Endpoint_GUID(guid),
        udp_sender,
        ring,
        true,
      )
      .unwrap();
  }

  pub fn publish_topic(
    &mut self,
    topic_name: &str,
    discovery_db: &DiscoveryDB,
    udp_sender: &UDPSender,
    ring: &mut IoUring,
  ) {
    let Some(topic_data) = discovery_db.get_topic(topic_name) else {
      warn!("Did not find topic data with topic name {topic_name}");
      return;
    };

    // Only user-defined topics are published to discovery
    let is_user_defined = !topic_data.topic_name().starts_with("DCPS");
    if !is_user_defined {
      return;
    }

    self
      .caches
      .topics
      .write::<PlCdrSerializerAdapter<DiscoveredTopicData>>(
        topic_data.clone(),
        udp_sender,
        ring,
        self.initialized,
      )
      .unwrap();
  }
}

use crate::messages::submessages::submessages::{AckSubmessage, ReaderSubmessage};

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

/*
#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use chrono::Utc;
  use speedy::{Endianness, Writable};
  use mio_06::Token;

  use super::*;
  use crate::{
    dds::adapters::no_key::DeserializerAdapter,
    discovery::sedp_messages::TopicBuiltinTopicData,
    messages::submessages::submessages::{InterpreterSubmessage, WriterSubmessage},
    network::{constant::*, udp_listener::UDPListener, udp_sender::UDPSender},
    rtps::submessage::*,
    test::{
      shape_type::ShapeType,
      test_data::{
        create_cdr_pl_rtps_data_message, spdp_participant_msg_mod, spdp_publication_msg,
        spdp_subscription_msg,
      },
    },
    RepresentationIdentifier,
  };

  #[test]
  fn discovery_participant_data_test() {
    let poll = Poll::new().unwrap();
    const LISTENER_PORT: u16 = spdp_well_known_unicast_port(12, 0);

    let mut udp_listener =
      UDPListener::new_unicast("127.0.0.1", LISTENER_PORT).expect("udp listener creation");
    poll
      .register(
        udp_listener.mio_socket(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();

    // sending participant data to discovery
    let udp_sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses = vec![SocketAddr::new("127.0.0.1".parse().unwrap(), LISTENER_PORT)];

    let tdata = spdp_participant_msg_mod(11000);
    let msg_data = tdata
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write msg data");

    udp_sender.send_to_all(&msg_data, &addresses);

    let mut events = Events::with_capacity(10);
    poll
      .poll(&mut events, Some(StdDuration::from_secs(1)))
      .unwrap();

    let _data2 = udp_listener.get_message();
    // TODO: we should have received our own participants info decoding the
    // actual message might be good idea
  }

  #[test]
  fn discovery_reader_data_test() {
    use crate::{
      serialization::pl_cdr_adapters::PlCdrSerialize, structure::locator::Locator, TopicKind,
    };

    let participant = DomainParticipant::new(0).expect("participant creation");

    let topic = participant
      .create_topic(
        "Square".to_string(),
        "ShapeType".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();

    let publisher = participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let _writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None)
      .unwrap();

    let subscriber = participant
      .create_subscriber(&QosPolicies::qos_none())
      .unwrap();
    let _reader =
      subscriber.create_datareader::<ShapeType, CDRDeserializerAdapter<ShapeType>>(&topic, None);

    let poll: Poll = Poll::new().unwrap();
    const LISTENER_PORT: u16 = spdp_well_known_unicast_port(14, 0);

    let mut udp_listener = UDPListener::new_unicast("127.0.0.1", LISTENER_PORT).unwrap();
    poll
      .register(
        udp_listener.mio_socket(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();

    let udp_sender: UDPSender =
      UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses: Vec<SocketAddr> =
      vec![SocketAddr::new("127.0.0.1".parse().unwrap(), LISTENER_PORT)];

    let mut tdata: crate::rtps::Message = spdp_subscription_msg();
    let mut data: bytes::Bytes;
    for submsg in &mut tdata.submessages {
      match &mut submsg.body {
        SubmessageBody::Writer(WriterSubmessage::Data(d, _)) => {
          let mut drd: DiscoveredReaderData = PlCdrDeserializerAdapter::from_bytes(
            &d.unwrap_serialized_payload_value(),
            RepresentationIdentifier::PL_CDR_LE,
          )
          .unwrap();
          drd.reader_proxy.unicast_locator_list.clear();
          drd
            .reader_proxy
            .unicast_locator_list
            .push(Locator::from(SocketAddr::new(
              "127.0.0.1".parse().unwrap(),
              11001,
            )));
          drd.reader_proxy.multicast_locator_list.clear();

          data = drd
            .to_pl_cdr_bytes(RepresentationIdentifier::PL_CDR_LE)
            .unwrap();
          d.update_serialized_payload_value(data.clone());
        }
        SubmessageBody::Interpreter(_) => (),
        _ => continue,
      }
    }

    let msg_data: Vec<u8> = tdata
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write msg data");

    udp_sender.send_to_all(&msg_data, &addresses);

    let mut events: Events = Events::with_capacity(10);
    poll
      .poll(&mut events, Some(StdDuration::from_secs(1)))
      .unwrap();

    let _data2: Vec<u8> = udp_listener.get_message();
  }

  #[test]
  fn discovery_writer_data_test() {
    use crate::TopicKind;
    let participant = DomainParticipant::new(0).expect("Failed to create participant");

    let topic = participant
      .create_topic(
        "Square".to_string(),
        "ShapeType".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();

    let publisher = participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let _writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None)
      .unwrap();

    let subscriber = participant
      .create_subscriber(&QosPolicies::qos_none())
      .unwrap();
    let _reader =
      subscriber.create_datareader::<ShapeType, CDRDeserializerAdapter<ShapeType>>(&topic, None);

    let poll = Poll::new().unwrap();
    let mut udp_listener = UDPListener::new_unicast("127.0.0.1", 0).unwrap();
    poll
      .register(
        udp_listener.mio_socket(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();

    let udp_sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses = vec![SocketAddr::new(
      "127.0.0.1".parse().unwrap(),
      spdp_well_known_unicast_port(15, 0),
    )];

    let mut tdata = spdp_publication_msg();
    for submsg in &mut tdata.submessages {
      match &mut submsg.body {
        SubmessageBody::Interpreter(v) => match v {
          InterpreterSubmessage::InfoDestination(dst, _flags) => {
            dst.guid_prefix = participant.guid_prefix();
          }
          _ => continue,
        },
        SubmessageBody::Writer(_) => (),
        SubmessageBody::Reader(_) => (),
        #[cfg(feature = "security")]
        SubmessageBody::Security(_) => (),
      }
    }

    let par_msg_data = spdp_participant_msg_mod(udp_listener.port())
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write participant data.");

    let msg_data = tdata
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write msg data");

    udp_sender.send_to_all(&par_msg_data, &addresses);
    udp_sender.send_to_all(&msg_data, &addresses);

    let mut events = Events::with_capacity(10);
    poll
      .poll(&mut events, Some(StdDuration::from_secs(1)))
      .unwrap();

    for _ in udp_listener.messages() {
      info!("Message received");
    }
  }

  #[test]
  fn discovery_topic_data_test() {
    let _participant = DomainParticipant::new(0);

    let topic_data = DiscoveredTopicData::new(
      Utc::now(),
      TopicBuiltinTopicData {
        key: None,
        name: String::from("Square"),
        type_name: String::from("ShapeType"),
        durability: None,
        deadline: None,
        latency_budget: None,
        liveliness: None,
        reliability: None,
        lifespan: None,
        destination_order: None,
        presentation: None,
        history: None,
        resource_limits: None,
        ownership: None,
      },
    );

    let rtps_message = create_cdr_pl_rtps_data_message(
      &topic_data,
      EntityId::SEDP_BUILTIN_TOPIC_READER,
      EntityId::SEDP_BUILTIN_TOPIC_WRITER,
    );

    let udp_sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses = vec![SocketAddr::new(
      "127.0.0.1".parse().unwrap(),
      spdp_well_known_unicast_port(16, 0),
    )];

    let rr = rtps_message
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .unwrap();

    udp_sender.send_to_all(&rr, &addresses);
  }
}
*/
