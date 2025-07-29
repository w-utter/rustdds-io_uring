use std::collections::{btree_map::Entry, BTreeMap};

use enumflags2::BitFlags;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use bytes::Bytes;

use crate::structure::dds_cache::TopicCache;

use crate::{
  messages::{protocol_version::ProtocolVersion, submessages::submessages::*, vendor_id::VendorId},
  rtps::{Message, Submessage, SubmessageBody},
  structure::{
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
    time::Timestamp,
  },
};

use crate::io_uring::rtps::reader::Reader;
use crate::io_uring::timer::timer_state;

use crate::rtps::message_receiver::{SecureReceiverState, MessageReceiverState};

#[cfg(feature = "security")]
use crate::rtps::message_receiver::SecureWrapping;

#[cfg(feature = "security")]
use crate::security::{
  cryptographic::{DecodeOutcome, DecodedSubmessage},
  security_plugins::SecurityPluginsHandle,
};
#[cfg(feature = "security")]
use crate::messages::submessages::{secure_postfix::SecurePostfix, secure_prefix::SecurePrefix};
#[cfg(not(feature = "security"))]
use crate::no_security::SecurityPluginsHandle;
#[cfg(test)]
use crate::dds::ddsdata::DDSData;
#[cfg(test)]
use crate::structure::sequence_number::SequenceNumber;

const RTPS_MESSAGE_HEADER_SIZE: usize = 20;

pub enum SubmessageSideEffect {
  AckNack(GuidPrefix, AckSubmessage),
  SpdpLiveness(GuidPrefix),
}

#[derive(Debug)]
pub enum PassedSubmessage {
  Reader(ReaderSubmessage, GuidPrefix),
  Writer(WriterSubmessage),
}

pub struct SubmessageIter2<'a> {
  pub(crate) msg_recv: &'a mut MessageReceiver,
  submsgs: std::vec::IntoIter<Submessage>,
}

impl<'a> SubmessageIter2<'a> {
  fn new(msg_recv: &'a mut MessageReceiver, submsgs: std::vec::IntoIter<Submessage>) -> Self {
    Self { msg_recv, submsgs }
  }
}

impl<'a> Iterator for SubmessageIter2<'a> {
  type Item = PassedSubmessage;
  fn next(&mut self) -> Option<Self::Item> {
    let sub_msg = self.submsgs.next()?;
    self.msg_recv.submessage_count += 1;

    if matches!(
      &sub_msg.body,
      /*SubmessageBody::Writer(_) |*/ SubmessageBody::Reader(_)
    ) {
      //println!("\n{sub_msg:?}\n");
    }

    match sub_msg.body {
      SubmessageBody::Interpreter(m) => {
        self.msg_recv.handle_interpreter_submessage(m);
        self.next()
      }
      SubmessageBody::Writer(msg) => {
        if self.msg_recv.dest_guid_prefix != self.msg_recv.own_guid_prefix
          && self.msg_recv.dest_guid_prefix != GuidPrefix::UNKNOWN
        {
          debug!(
            "Message is not for this participant. Dropping. dest_guid_prefix={:?} participant \
                     guid={:?}",
            self.msg_recv.dest_guid_prefix, self.msg_recv.own_guid_prefix
          );
          return self.next();
        }

        Some(PassedSubmessage::Writer(msg))
      }
      SubmessageBody::Reader(submsg) => {
        let source_guid_prefix = self.msg_recv.source_guid_prefix;
        match self.msg_recv.security_plugins.as_ref() {
          #[cfg(not(feature = "security"))]
          Some(_) => Some(PassedSubmessage::Reader(submsg, source_guid_prefix)),
          None => Some(PassedSubmessage::Reader(submsg, source_guid_prefix)),
          #[cfg(feature = "security")]
          Some(security) => {
            let destination_guid = GUID {
              prefix: self.msg_recv.dest_guid_prefix,
              entity_id: submsg.receiver_entity_id(),
            };
            if security
              .get_plugins()
              .submessage_not_protected(&destination_guid)
            {
              Some(PassedSubmessage::Reader(submsg, source_guid_prefix))
            } else {
              error!(
                "No writer with unprotected submessages found for the GUID \
                             {destination_guid:?}"
              );
              self.next()
            }
          }
        }
      }
      //TODO: add security support later
      //this wold still allow for only reader/writer submsgs to be passed along
      _ => None,
    }
  }
}

/// [`MessageReceiver`] is the submessage sequence interpreter described in
/// RTPS spec v2.3 Section 8.3.4 "The RTPS Message Receiver".
/// It calls the message/submessage deserializers to parse the sequence of
/// submessages. Then it processes the instructions in the Interpreter
/// SUbmessages and forwards data in Entity Submessages to the appropriate
/// Entities. (See RTPS spec Section 8.3.7)
pub struct MessageReceiver {
  //TODO: remove
  pub available_readers: BTreeMap<EntityId, Reader<timer_state::Init>>,
  // GuidPrefix sent in this channel needs to be RTPSMessage source_guid_prefix. Writer needs this
  // to locate RTPSReaderProxy if negative acknack.
  // We send notification of remote DomainParticipant liveness to Discovery to
  // bypass Reader, DDSCache, DatasampleCache, and DataReader, because these will drop
  // repeated messages with duplicate SequenceNumbers, but Discovery needs to see them.
  security_plugins: Option<SecurityPluginsHandle>,

  own_guid_prefix: GuidPrefix,
  pub source_version: ProtocolVersion,
  pub source_vendor_id: VendorId,
  pub source_guid_prefix: GuidPrefix,
  pub dest_guid_prefix: GuidPrefix,
  pub unicast_reply_locator_list: Vec<Locator>,
  pub multicast_reply_locator_list: Vec<Locator>,
  pub source_timestamp: Option<Timestamp>,

  submessage_count: usize, // Used in tests and error messages only?
  secure_receiver_state: Option<SecureReceiverState>,
  #[cfg(feature = "security")]
  secure_rtps_wrapped: Option<SecureWrapping>,
  #[cfg(feature = "security")]
  // For certain topics we have to allow unprotected rtps messages even if the domain is
  // rtps-protected
  must_be_rtps_protection_special_case: bool,
}

impl MessageReceiver {
  pub fn new(
    participant_guid_prefix: GuidPrefix,
    security_plugins: Option<SecurityPluginsHandle>,
  ) -> Self {
    Self {
      available_readers: BTreeMap::new(),
      security_plugins,
      own_guid_prefix: participant_guid_prefix,

      source_version: ProtocolVersion::THIS_IMPLEMENTATION,
      source_vendor_id: VendorId::VENDOR_UNKNOWN,
      source_guid_prefix: GuidPrefix::UNKNOWN,
      dest_guid_prefix: GuidPrefix::UNKNOWN,
      unicast_reply_locator_list: vec![Locator::Invalid],
      multicast_reply_locator_list: vec![Locator::Invalid],
      source_timestamp: None,

      submessage_count: 0,
      secure_receiver_state: None,
      #[cfg(feature = "security")]
      secure_rtps_wrapped: None,
      #[cfg(feature = "security")]
      // Protection on by default
      must_be_rtps_protection_special_case: true,
    }
  }

  pub fn reset(&mut self) {
    self.source_version = ProtocolVersion::THIS_IMPLEMENTATION;
    self.source_vendor_id = VendorId::VENDOR_UNKNOWN;
    self.source_guid_prefix = GuidPrefix::UNKNOWN;
    self.dest_guid_prefix = GuidPrefix::UNKNOWN;
    self.unicast_reply_locator_list.clear();
    self.multicast_reply_locator_list.clear();
    self.source_timestamp = None;

    self.submessage_count = 0;

    self.secure_receiver_state = None; // This exists regardless of security

    #[cfg(feature = "security")]
    {
      self.secure_rtps_wrapped = None;
    }
  }

  pub fn clone_partial_message_receiver_state(&self) -> MessageReceiverState {
    MessageReceiverState {
      source_guid_prefix: self.source_guid_prefix,
      unicast_reply_locator_list: self.unicast_reply_locator_list.clone(),
      multicast_reply_locator_list: self.multicast_reply_locator_list.clone(),
      source_timestamp: self.source_timestamp,
      #[cfg(feature = "security")]
      secure_rtps_wrapped: self.secure_rtps_wrapped.clone(),
    }
  }

  pub fn add_reader(&mut self, new_reader: Reader<timer_state::Init>) {
    let eid = new_reader.guid().entity_id;
    match self.available_readers.entry(eid) {
      Entry::Occupied(_) => warn!("Already have Reader {:?} - not adding.", eid),
      Entry::Vacant(e) => {
        e.insert(new_reader);
      }
    }
  }

  pub fn remove_reader(&mut self, old_reader_guid: GUID) -> Option<Reader<timer_state::Init>> {
    self.available_readers.remove(&old_reader_guid.entity_id)
  }

  pub fn reader_mut(&mut self, reader_id: EntityId) -> Option<&mut Reader<timer_state::Init>> {
    self.available_readers.get_mut(&reader_id)
  }

  pub fn handle_received_packet_2<'a>(
    &'a mut self,
    msg_bytes: &Bytes,
  ) -> Option<SubmessageIter2<'a>> {
    // Check for RTPS ping message. At least RTI implementation sends these.
    // What should we do with them? The spec does not say.
    if msg_bytes.len() < RTPS_MESSAGE_HEADER_SIZE {
      if msg_bytes.len() >= 16
        && msg_bytes[0..4] == b"RTPS"[..]
        && msg_bytes[9..16] == b"DDSPING"[..]
      {
        // TODO: Add some sensible ping message handling here.
        info!("Received RTPS PING. Do not know how to respond.");
        debug!("Data was {:?}", &msg_bytes);
      } else {
        warn!("Message is shorter than RTPS header. Cannot deserialize.");
        debug!("Data was {:?}", &msg_bytes);
      }
      return None;
    }

    // Check that the 4-byte magic string matches "RTPS"
    if msg_bytes.len() >= 4 {
      let magic = &msg_bytes[0..4];
      if *magic == b"RTPS"[..] {
        // go ahead and try to decode message
      } else if *magic == b"RTPX"[..] {
        // RTI Connext sends also packets with magic RTPX in the header.
        // We do not know are these really same as RTPS or different, so let's
        // ignore those.
        info!("Received message with RTPX header. Ignoring.");
        return None;
      } else {
        warn!(
          "Received message with unknown start of header {:x?}. Ignoring.",
          magic
        );
        return None;
      }
    }

    // call Speedy reader
    // Bytes .clone() is cheap, so no worries
    let rtps_message = match Message::read_from_buffer(msg_bytes) {
      Ok(m) => m,
      Err(speedy_err) => {
        warn!("RTPS deserialize error {:?}", speedy_err);
        debug!("Data was {:?}", msg_bytes);
        return None;
      }
    };

    //println!("\n{rtps_message:?}\n");

    Some(self.handle_parsed_message_2(rtps_message))
  }

  pub fn handle_parsed_message_2<'a>(&'a mut self, rtps_message: Message) -> SubmessageIter2<'a> {
    //println!("\n{rtps_message:?}\n");
    self.reset();
    self.dest_guid_prefix = self.own_guid_prefix;
    self.source_guid_prefix = rtps_message.header.guid_prefix;
    self.source_version = rtps_message.header.protocol_version;
    self.source_vendor_id = rtps_message.header.vendor_id;

    #[cfg(not(feature = "security"))]
    let decoded_message = rtps_message;

    #[cfg(feature = "security")]
    let decoded_message = match &self.security_plugins {
      None => {
        self.must_be_rtps_protection_special_case = false; // No plugins, no protection
        rtps_message
      }

      Some(security_plugins_handle) => {
        let security_plugins = security_plugins_handle.get_plugins();

        // If the first submessage is SecureRTPSPrefix, the message has to be decoded
        // using the cryptographic plugin
        if let Some(Submessage {
          body: SubmessageBody::Security(SecuritySubmessage::SecureRTPSPrefix(..)),
          ..
        }) = rtps_message.submessages.first()
        {
          match security_plugins.decode_rtps_message(rtps_message, &self.source_guid_prefix) {
            Ok(DecodeOutcome::Success(message)) => {
              self.must_be_rtps_protection_special_case = false; // Message was protected
              message
            }
            Ok(DecodeOutcome::KeysNotFound(header_key_id)) => {
              return trace!(
                "No matching message decode keys found for the key id {:?} for the remote \
                 participant {:?}",
                header_key_id,
                self.source_guid_prefix
              )
            }
            Ok(DecodeOutcome::ValidatingReceiverSpecificMACFailed) => {
              return trace!("Failed to validate the receiver-specif MAC for the rtps message.");
            }
            Ok(DecodeOutcome::ParticipantCryptoHandleNotFound(guid_prefix)) => {
              return trace!(
                "No participant crypto handle found for the participant {:?} for rtps message \
                 decoding.",
                guid_prefix
              )
            }
            Err(e) => return error!("{e:?}"),
          }
        } else {
          if security_plugins.rtps_not_protected(&self.dest_guid_prefix) {
            // The domain is not rtps-protected, the additional check does not apply
            self.must_be_rtps_protection_special_case = false;
          } else {
            // The messages in a rtps-protected domain are expected to start
            // with SecureRTPSPrefix. The only exception is if the
            // message contains only submessages for the following
            // builtin topics: DCPSParticipants,
            // DCPSParticipantStatelessMessage,
            // DCPSParticipantVolatileMessageSecure
            // (8.4.2.4, table 27).
            self.must_be_rtps_protection_special_case = true;
          }
          rtps_message
        }
      }
    };

    SubmessageIter2::new(self, decoded_message.submessages.into_iter())
  }

  fn handle_writer_submessage(
    &mut self,
    target_reader_entity_id: EntityId,
    submessage: WriterSubmessage,
    udp_sender: &UDPSender,
    ring: &mut io_uring::IoUring,
    topic_cache: &mut TopicCache,
  ) -> Option<SubmessageSideEffect> {
    if self.dest_guid_prefix != self.own_guid_prefix && self.dest_guid_prefix != GuidPrefix::UNKNOWN
    {
      //println!("\nmessage not meant for this participant\n");
      debug!(
        "Message is not for this participant. Dropping. dest_guid_prefix={:?} participant \
         guid={:?}",
        self.dest_guid_prefix, self.own_guid_prefix
      );
      return None;
    }

    #[cfg(feature = "security")]
    if self.must_be_rtps_protection_special_case {
      match target_reader_entity_id {
        // These submessages are the special case
        EntityId::SPDP_BUILTIN_PARTICIPANT_READER
        | EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER
        | EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_READER => (),
        // Otherwise we have to reject
        other => {
          error!(
            "Received an unprotected message containing a writer submessage for the reader \
             {other:?} in an rtps-protected domain."
          );
          return None;
        }
      }
    }

    let mr_state = self.clone_partial_message_receiver_state();
    let writer_entity_id = submessage.sender_entity_id();
    let source_guid_prefix = mr_state.source_guid_prefix;
    let source_guid = &GUID {
      prefix: source_guid_prefix,
      entity_id: writer_entity_id,
    };

    let security_plugins = self.security_plugins.clone();

    let target_reader = if let Some(target_reader) = self.reader_mut(target_reader_entity_id) {
      target_reader
    } else {
      error!("No reader matching the CryptoHandle found");
      return None;
    };

    match submessage {
      WriterSubmessage::Data(data, data_flags) => {
        Self::decode_and_handle_data(
          security_plugins.as_ref(),
          source_guid,
          data,
          data_flags,
          target_reader,
          &mr_state,
          topic_cache,
        );

        // Notify discovery that the remote PArticipant seems to be alive
        if writer_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
          && target_reader_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_READER
        {
          return Some(SubmessageSideEffect::SpdpLiveness(source_guid_prefix));
        }
        return None;
      }

      WriterSubmessage::Heartbeat(heartbeat, flags) => {
        target_reader.handle_heartbeat_msg(
          &heartbeat,
          flags.contains(HEARTBEAT_Flags::Final),
          &mr_state,
          udp_sender,
          ring,
          topic_cache,
        );
        return None;
      }

      WriterSubmessage::Gap(gap, _flags) => {
        target_reader.handle_gap_msg(&gap, &mr_state, topic_cache);
        return None;
      }

      WriterSubmessage::DataFrag(datafrag, flags) => {
        Self::decode_and_handle_datafrag(
          security_plugins.as_ref(),
          source_guid,
          datafrag.clone(),
          flags,
          target_reader,
          &mr_state,
          topic_cache,
        );
        return None;
      }

      WriterSubmessage::HeartbeatFrag(heartbeatfrag, _flags) => {
        target_reader.handle_heartbeatfrag_msg(&heartbeatfrag, &mr_state);
        return None;
      }
    }
  }

  // see security version of the same function below
  #[cfg(not(feature = "security"))]
  fn decode_and_handle_data(
    _security_plugins: Option<&SecurityPluginsHandle>,
    _source_guid: &GUID,
    data: Data,
    data_flags: BitFlags<DATA_Flags, u8>,
    reader: &mut Reader<timer_state::Init>,
    mr_state: &MessageReceiverState,
    topic_cache: &mut TopicCache,
  ) {
    reader.handle_data_msg(data, data_flags, mr_state, topic_cache);
  }

  #[cfg(feature = "security")]
  fn decode_and_handle_data(
    security_plugins: Option<&SecurityPluginsHandle>,
    source_guid: &GUID,
    data: Data,
    data_flags: BitFlags<DATA_Flags, u8>,
    reader: &mut Reader,
    mr_state: &MessageReceiverState,
  ) {
    let Data {
      inline_qos,
      serialized_payload,
      ..
    } = data.clone();

    serialized_payload
      // If there is an encoded_payload, decode it
      .map(
        |encoded_payload| match security_plugins.map(SecurityPluginsHandle::get_plugins) {
          Some(security_plugins) => security_plugins
            .decode_serialized_payload(
              encoded_payload,
              inline_qos.unwrap_or_default(),
              source_guid,
              &reader.guid(),
            )
            .map_err(|e| error!("{e:?}")),
          None => Ok(encoded_payload),
        },
      )
      .transpose()
      // If there were no errors, give to the reader
      .map(|decoded_payload| {
        reader.handle_data_msg(
          Data {
            serialized_payload: decoded_payload,
            ..data
          },
          data_flags,
          mr_state,
        );
      })
      // Errors have already been printed
      .ok();
  }

  #[cfg(not(feature = "security"))]
  // see security version below
  fn decode_and_handle_datafrag(
    _security_plugins: Option<&SecurityPluginsHandle>,
    _source_guid: &GUID,
    datafrag: DataFrag,
    datafrag_flags: BitFlags<DATAFRAG_Flags, u8>,
    reader: &mut Reader<timer_state::Init>,
    mr_state: &MessageReceiverState,
    topic_cache: &mut TopicCache,
  ) {
    let payload_buffer_length = datafrag.serialized_payload.len();
    if payload_buffer_length
      > (datafrag.fragments_in_submessage as usize) * (datafrag.fragment_size as usize)
    {
      error!(
        "{:?}",
        std::io::Error::new(
          std::io::ErrorKind::Other,
          format!(
            "Invalid DataFrag. serializedData length={} should be less than or equal to \
             (fragments_in_submessage={}) x (fragment_size={})",
            payload_buffer_length, datafrag.fragments_in_submessage, datafrag.fragment_size
          ),
        )
      );
      // and we're done
    } else {
      reader.handle_datafrag_msg(&datafrag, datafrag_flags, mr_state, topic_cache);
    }
    // Consume to keep the same method signature as in the security case
    drop(datafrag);
  }

  #[cfg(feature = "security")]
  fn decode_and_handle_datafrag(
    security_plugins: Option<&SecurityPluginsHandle>,
    source_guid: &GUID,
    datafrag: DataFrag,
    datafrag_flags: BitFlags<DATAFRAG_Flags, u8>,
    reader: &mut Reader,
    mr_state: &MessageReceiverState,
  ) {
    let DataFrag {
      inline_qos,
      serialized_payload: encoded_payload,
      ..
    } = datafrag.clone();

    match security_plugins.map(SecurityPluginsHandle::get_plugins) {
      Some(security_plugins) => {
        // Decode
        security_plugins
          .decode_serialized_payload(
            encoded_payload,
            inline_qos.unwrap_or_default(),
            source_guid,
            &reader.guid(),
          )
          .map_err(|e| error!("{e:?}"))
      }
      None => Ok(encoded_payload),
    }
    .ok()
    // Deserialize
    .and_then(|serialized_payload| {
      // The check that used to be in DataFrag deserialization
      if serialized_payload.len()
        > (datafrag.fragments_in_submessage as usize) * (datafrag.fragment_size as usize)
      {
        error!(
          "{:?}",
          std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
              "Invalid DataFrag. serializedData length={} should be less than or equal to \
               (fragments_in_submessage={}) x (fragment_size={})",
              serialized_payload.len(),
              datafrag.fragments_in_submessage,
              datafrag.fragment_size
            ),
          )
        );
        None
      } else {
        Some(serialized_payload)
      }
    })
    // If there were no errors, give DecodedDataFrag to the reader
    .map(|decoded_payload| {
      reader.handle_datafrag_msg(
        &DataFrag {
          serialized_payload: decoded_payload,
          ..datafrag
        },
        datafrag_flags,
        mr_state,
        topic_cache,
      );
    });
  }

  fn handle_reader_submessage(&self, submessage: ReaderSubmessage) -> Option<SubmessageSideEffect> {
    if self.dest_guid_prefix != self.own_guid_prefix && self.dest_guid_prefix != GuidPrefix::UNKNOWN
    {
      debug!(
        "Message is not for this participant. Dropping. dest_guid_prefix={:?} participant \
         guid={:?}",
        self.dest_guid_prefix, self.own_guid_prefix
      );
      return None;
    }

    #[cfg(feature = "security")]
    if self.must_be_rtps_protection_special_case {
      match submessage.receiver_entity_id() {
        // These submessages are the special case
        EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
        | EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER
        | EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER => (),
        // Otherwise we have to reject
        other => {
          error!(
            "Received an unprotected message containing a reader submessage for the writer \
             {other:?} in an rtps-protected domain."
          );
          return None;
        }
      }
    }

    match submessage {
      ReaderSubmessage::AckNack(acknack, _) => {
        return Some(SubmessageSideEffect::AckNack(
          self.source_guid_prefix,
          AckSubmessage::AckNack(acknack),
        ))
      }

      ReaderSubmessage::NackFrag(_, _) => {
        // TODO: Implement NackFrag handling
        return None;
      }
    }
  }

  #[cfg(feature = "security")]
  fn handle_secure_submessage(
    &mut self,
    sec_prefix: &SecurePrefix,
    encoded_submessage: &Submessage,
    sec_postfix: &SecurePostfix,
    ring: &mut io_uring::IoUring,
  ) -> Option<SubmessageSideEffect> {
    let security_plugins = self.security_plugins.clone();
    match security_plugins {
      None => {
        warn!("Cannot handle secure submessage: No security plugins configured.");
        return None;
      }
      Some(ref security_plugins_handle) => {
        // Call 8.5.1.9.6 Operation: preprocess_secure_submsg to determine what
        // the submessage contains and then proceed to decode and process accordingly.

        let decode_result = security_plugins_handle.get_plugins().decode_submessage(
          (
            sec_prefix.clone(),
            encoded_submessage.clone(),
            sec_postfix.clone(),
          ),
          &self.source_guid_prefix,
        );
        match decode_result {
          Err(e) => {
            error!("Submessage decoding failed: {e:?}");
            return None;
          }
          Ok(DecodeOutcome::Success(DecodedSubmessage::Writer(
            decoded_writer_submessage,
            approved_receiving_datareader_crypto_handles,
          ))) => {
            let receiver_entity_id = decoded_writer_submessage.receiver_entity_id();

            // If the receiver entity ID is unknown, we try to find the correct id based on
            // whether it matches the crypto handle
            if receiver_entity_id == EntityId::UNKNOWN {
              let sending_writer_entity_id = decoded_writer_submessage.sender_entity_id();

              if let Some(target_reader)=self.available_readers.values().find(|target_reader| {
                (
                // Reader must contain the writer
                target_reader.contains_writer(sending_writer_entity_id)
                    // But there are two exceptions:
                    // 1. SPDP reader must read from unknown SPDP writers
                    //  TODO: This logic here is uglyish. Can we just inject a
                    //  presupposed writer (proxy) to the built-in reader as it is created?
                    || (sending_writer_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
                      && target_reader.entity_id() == EntityId::SPDP_BUILTIN_PARTICIPANT_READER)
                    // 2. ParticipantStatelessReader does not contain any writers, since it is stateless
                    || (sending_writer_entity_id == EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER
                      && target_reader.entity_id() == EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER)
                    )
                    &&
                    security_plugins_handle.get_plugins()
                    .confirm_local_endpoint_guid(&approved_receiving_datareader_crypto_handles,
                      &GUID { prefix: self.dest_guid_prefix,entity_id: target_reader.entity_id() })
              }){
                return self.handle_writer_submessage(target_reader.entity_id(), decoded_writer_submessage);
              }else{
                error!("No reader matching the CryptoHandle found");
                return None;
              }
            } else {
              let receiver_guid = GUID {
                prefix: self.dest_guid_prefix,
                entity_id: receiver_entity_id,
              };
              if security_plugins_handle
                .get_plugins()
                .confirm_local_endpoint_guid(
                  &approved_receiving_datareader_crypto_handles,
                  &receiver_guid,
                )
              {
                return self
                  .handle_writer_submessage(receiver_entity_id, decoded_writer_submessage);
              } else {
                error!("Destination GUID did not match the handle used for decoding.");
                return None;
              }
            }
          }
          Ok(DecodeOutcome::Success(DecodedSubmessage::Reader(
            decoded_reader_submessage,
            approved_receiving_datawriter_crypto_handles,
          ))) => {
            let receiver_entity_id = decoded_reader_submessage.receiver_entity_id();
            let receiver_guid = GUID {
              prefix: self.dest_guid_prefix,
              entity_id: receiver_entity_id,
            };
            if security_plugins_handle
              .get_plugins()
              .confirm_local_endpoint_guid(
                &approved_receiving_datawriter_crypto_handles,
                &receiver_guid,
              )
            {
              return self.handle_reader_submessage(decoded_reader_submessage);
            } else {
              error!("Destination GUID did not match the handle used for decoding.");
              return None;
            }
          }
          Ok(DecodeOutcome::Success(DecodedSubmessage::Interpreter(interpreter_submessage))) => {
            // This is not defined in the specification, but we accept for compatibility, as
            // we would also accept unprotected ones.
            self.handle_interpreter_submessage(interpreter_submessage);
            return None;
          }
          Ok(DecodeOutcome::KeysNotFound(header_key_id)) => {
            trace!(
              "No matching submessage decode keys found for the key id {:?} for the remote \
               participant {:?}",
              header_key_id,
              self.source_guid_prefix
            );
            return None;
          }
          Ok(DecodeOutcome::ValidatingReceiverSpecificMACFailed) => {
            trace!("No endpoints passed the receiver-specific MAC validation for the submessage.");
            return None;
          }
          Ok(DecodeOutcome::ParticipantCryptoHandleNotFound(guid_prefix)) => {
            trace!(
              "No participant crypto handle found for the participant {:?} for submessage \
               decoding.",
              guid_prefix
            );
            return None;
          }
        }
      }
    };
  }

  fn handle_interpreter_submessage(&mut self, interpreter_submessage: InterpreterSubmessage)
  // no return value, just change state of self.
  {
    match interpreter_submessage {
      InterpreterSubmessage::InfoTimestamp(ts_struct, _flags) => {
        // flags value was used already when parsing timestamp into an Option
        self.source_timestamp = ts_struct.timestamp;
      }
      InterpreterSubmessage::InfoSource(info_src, _flags) => {
        self.source_guid_prefix = info_src.guid_prefix;
        self.source_version = info_src.protocol_version;
        self.source_vendor_id = info_src.vendor_id;

        // TODO: Why are the following set on InfoSource?
        self.unicast_reply_locator_list.clear(); // Or invalid?
        self.multicast_reply_locator_list.clear(); // Or invalid?
        self.source_timestamp = None; // TODO: Why does InfoSource set timestamp
                                      // to None?
      }
      InterpreterSubmessage::InfoReply(info_reply, flags) => {
        self.unicast_reply_locator_list = info_reply.unicast_locator_list;
        self.multicast_reply_locator_list = match (
          flags.contains(INFOREPLY_Flags::Multicast),
          info_reply.multicast_locator_list,
        ) {
          (true, Some(ll)) => ll, // expected case
          (true, None) => {
            warn!(
              "InfoReply submessage flag indicates multicast_reply_locator_list, but none found."
            );
            vec![]
          }
          (false, None) => vec![], // This one is normal again
          (false, Some(_)) => {
            warn!("InfoReply submessage has unexpected multicast_reply_locator_list, ignoring.");
            vec![]
          }
        };
      }
      InterpreterSubmessage::InfoDestination(info_dest, _flags) => {
        if info_dest.guid_prefix == GUID::GUID_UNKNOWN.prefix {
          self.dest_guid_prefix = self.own_guid_prefix;
        } else {
          self.dest_guid_prefix = info_dest.guid_prefix;
        }
      }
    }
  }

  // sends 0 seqnum acknacks for those writer that haven't had any action
  pub fn send_preemptive_acknacks(&mut self, udp_sender: &UDPSender, ring: &mut io_uring::IoUring) {
    for reader in self.available_readers.values_mut() {
      reader.send_preemptive_acknacks(udp_sender, ring);
    }
  }

  // use for test and debugging only
  #[cfg(test)]
  fn get_reader_and_history_cache_change(
    &self,
    reader_id: EntityId,
    sequence_number: SequenceNumber,
    topic_cache: &mut TopicCache,
  ) -> Option<DDSData> {
    Some(
      self
        .available_readers
        .get(&reader_id)
        .unwrap()
        .history_cache_change_data(sequence_number, topic_cache)
        .unwrap(),
    )
  }

  #[cfg(test)]
  fn get_reader_history_cache_start_and_end_seq_num(
    &self,
    reader_id: EntityId,
  ) -> Vec<SequenceNumber> {
    self
      .available_readers
      .get(&reader_id)
      .unwrap()
      .history_cache_sequence_start_and_end_numbers()
  }
} // impl messageReceiver

use crate::io_uring::network::UDPSender;

// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::{
    rc::Rc,
    sync::{Arc, Mutex, RwLock},
  };

  use speedy::{Readable, Writable};
  use log::info;
  use serde::{Deserialize, Serialize};
  use mio_extras::channel as mio_channel;
  use byteorder::LittleEndian;

  use crate::{
    dds::{
      qos::QosPolicies,
      statusevents::{sync_status_channel, DataReaderStatus},
      typedesc::TypeDesc,
      with_key::simpledatareader::ReaderCommand,
    },
    messages::header::Header,
    mio_source,
    serialization::from_bytes,
    structure::{dds_cache::DDSCache, guid::EntityKind},
  };

  use crate::io_uring::rtps::reader::ReaderIngredients;
  use crate::io_uring::network::udp_sender::UDPSender;
  use super::*;

  #[test]

  fn test_shapes_demo_message_deserialization() {
    let mut ring = io_uring::IoUring::new(16).expect("no ring");
    // The following message bytes contain serialized INFO_DST, INFO_TS, DATA &
    // HEARTBEAT submessages. The DATA submessage contains a ShapeType value.
    // The bytes have been captured from WireShark.
    let udp_bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x09, 0x01, 0x08, 0x00, 0x1a, 0x15, 0xf3, 0x5e, 0x00,
      0xcc, 0xfb, 0x13, 0x15, 0x05, 0x2c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x07,
      0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x5b, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x69, 0x00, 0x00, 0x00, 0x17, 0x00,
      0x00, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x07, 0x01, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
      0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x5b, 0x00, 0x00, 0x00, 0x1f, 0x00, 0x00, 0x00,
    ]);

    // The message bytes contain the following guid prefix as the message target.
    let target_gui_prefix = GuidPrefix::new(&[
      0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d, 0x31, 0xa2, 0x28, 0x20, 0x02, 0x08,
    ]);

    // The message bytes contain the following guid as the message source
    let remote_writer_guid = GUID::new(
      GuidPrefix::new(&[
        0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
      ]),
      EntityId::create_custom_entity_id([0, 0, 1], EntityKind::WRITER_WITH_KEY_USER_DEFINED),
    );

    // Create a message receiver
    let mut message_receiver = MessageReceiver::new(target_gui_prefix, None);

    // Create a reader to process the message
    let entity =
      EntityId::create_custom_entity_id([0, 0, 0], EntityKind::READER_WITH_KEY_USER_DEFINED);
    let reader_guid = GUID::new_with_prefix_and_id(target_gui_prefix, entity);

    let qos_policy = QosPolicies::qos_none();

    let mut dds_cache = DDSCache::new();

    let topic_cache_handle = dds_cache.add_new_topic(
      "test".to_string(),
      TypeDesc::new("test".to_string()),
      &qos_policy,
    );

    let mut topic_cache = topic_cache_handle.lock().unwrap();
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      topic_name: "test".to_string(),
      like_stateless: false,
      qos_policy,
      security_plugins: None,
    };

    let mut new_reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new_with_random_port().unwrap()),
    )
    .register(&mut ring, 0)
    .unwrap();

    // Add info of the writer to the reader
    new_reader.matched_writer_add(
      remote_writer_guid,
      EntityId::UNKNOWN,
      vec![],
      vec![],
      &QosPolicies::qos_none(),
    );

    // Add reader to message reader and process the bytes message
    message_receiver.add_reader(new_reader);

    let res = message_receiver
      .handle_received_packet(&udp_bits1, &mut ring, &mut topic_cache)
      .expect("could not parse bits");

    for _a in res {}

    // Verify the message reader has recorded the right amount of submessages
    assert_eq!(message_receiver.submessage_count, 4);

    // This is not correct way to read history cache values but it serves as a test
    let sequence_numbers =
      message_receiver.get_reader_history_cache_start_and_end_seq_num(reader_guid.entity_id);
    info!(
      "history change sequence number range: {:?}",
      sequence_numbers
    );

    // Get the DDSData (serialized) from the topic cache / history cache
    let a = message_receiver
      .get_reader_and_history_cache_change(
        reader_guid.entity_id,
        *sequence_numbers.first().unwrap(),
        &mut topic_cache,
      )
      .expect("No data in topic cache");
    info!("reader history cache DATA: {:?}", a.data());

    // Deserialize the ShapesType value from the data
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct ShapeType {
      color: String,
      x: i32,
      y: i32,
      size: i32,
    }
    let (deserialized_shape_type, _) = from_bytes::<ShapeType, LittleEndian>(&a.data()).unwrap();
    info!("deserialized shapeType: {:?}", deserialized_shape_type);

    // Verify the color in the deserialized value is correct
    assert_eq!(deserialized_shape_type.color, "RED");
  }

  #[test]
  fn mr_test_submsg_count() {
    let mut ring = io_uring::IoUring::new(16).expect("no ring");
    // Udp packet with INFO_DST, INFO_TS, DATA, HEARTBEAT
    let udp_bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x09, 0x01, 0x08, 0x00, 0x18, 0x15, 0xf3, 0x5e, 0x00,
      0x5c, 0xf0, 0x34, 0x15, 0x05, 0x2c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x07,
      0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x43, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x21, 0x00, 0x00, 0x00, 0x89, 0x00,
      0x00, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x07, 0x01, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
      0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x43, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
    ]);
    // Udp packet with INFO_DST, ACKNACK
    let udp_bits2 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x06, 0x03, 0x18, 0x00, 0x00, 0x00, 0x04, 0xc7, 0x00,
      0x00, 0x04, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x03, 0x00, 0x00, 0x00,
    ]);

    let mut dds_cache = DDSCache::new();
    let qos_policy = QosPolicies::qos_none();

    let topic_cache_handle = dds_cache.add_new_topic(
      "test".to_string(),
      TypeDesc::new("test".to_string()),
      &qos_policy,
    );

    let mut topic_cache = topic_cache_handle.lock().unwrap();

    let guid_new = GUID::default();
    let mut message_receiver = MessageReceiver::new(guid_new.prefix, None);

    let mut res = message_receiver
      .handle_received_packet(&udp_bits1, &mut ring, &mut topic_cache)
      .expect("could not parse udp bits");
    for _a in res {}
    assert_eq!(message_receiver.submessage_count, 4);

    let mut res_2 = message_receiver
      .handle_received_packet(&udp_bits2, &mut ring, &mut topic_cache)
      .expect("could not parse udp bits");
    for _a in res_2 {}
    assert_eq!(message_receiver.submessage_count, 2);
  }

  #[test]
  fn mr_test_header() {
    let guid_new = GUID::default();
    let header = Header::new(guid_new.prefix);

    let bytes = header.write_to_vec().unwrap();
    let new_header = Header::read_from_buffer(&bytes).unwrap();
    assert_eq!(header, new_header);
  }
}

struct TargetReaderEntityIdIter {
  iter: std::vec::IntoIter<EntityId>,
}

impl From<std::vec::IntoIter<EntityId>> for TargetReaderEntityIdIter {
  fn from(iter: std::vec::IntoIter<EntityId>) -> TargetReaderEntityIdIter {
    Self { iter }
  }
}

impl Iterator for TargetReaderEntityIdIter {
  type Item = EntityId;
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next()
  }
}
