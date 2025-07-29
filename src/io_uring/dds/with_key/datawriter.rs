use std::{
  marker::PhantomData,
  sync::atomic::{AtomicI64, Ordering},
  time::Duration,
};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    adapters::with_key::SerializerAdapter,
    ddsdata::DDSData,
    pubsub::Publisher,
    qos::{policy::Liveliness, HasQoSPolicy, QosPolicies},
    result::{WriteError, WriteResult},
  },
  discovery::{discovery::DiscoveryCommand, sedp_messages::SubscriptionBuiltinTopicData},
  messages::submessages::elements::serialized_payload::SerializedPayload,
  serialization::CDRSerializerAdapter,
  structure::{
    cache_change::ChangeKind, entity::RTPSEntity, guid::GUID, rpc::SampleIdentity,
    sequence_number::SequenceNumber, time::Timestamp,
  },
  Keyed,
};

use crate::io_uring::dds::Topic;

use crate::io_uring::rtps::writer::WriterCommand;
use crate::dds::with_key::WriteOptions;

/// Simplified type for CDR encoding
pub type DataWriterCdr<D> = DataWriter<D, CDRSerializerAdapter<D>>;

/// DDS DataWriter for keyed topics
///
/// # Examples
///
/// ```
/// use serde::{Serialize, Deserialize};
/// use rustdds::*;
/// use rustdds::with_key::DataWriter;
/// use rustdds::serialization::CDRSerializerAdapter;
///
/// let domain_participant = DomainParticipant::new(0).unwrap();
/// let qos = QosPolicyBuilder::new().build();
/// let publisher = domain_participant.create_publisher(&qos).unwrap();
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct SomeType { a: i32 }
/// impl Keyed for SomeType {
///   type K = i32;
///
///   fn key(&self) -> Self::K {
///     self.a
///   }
/// }
///
/// // WithKey is important
/// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
/// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None);
/// ```
pub struct DataWriter<D: Keyed, SA: SerializerAdapter<D> = CDRSerializerAdapter<D>> {
  data_phantom: PhantomData<D>,
  ser_phantom: PhantomData<SA>,
  my_topic: Topic,
  qos_policy: QosPolicies,
  my_guid: GUID,
  available_sequence_number: AtomicI64,
}

impl<D, SA> Drop for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn drop(&mut self) {
    /*
    // Tell Publisher to drop the corresponding RTPS Writer
    self.my_publisher.remove_writer(self.my_guid);

    // Notify Discovery that we are no longer
    match self
      .discovery_command
      .send(DiscoveryCommand::RemoveLocalWriter { guid: self.guid() })
    {
      Ok(_) => {}

      // This is fairly normal at shutdown, as the other end is down already.
      Err(SendError::Disconnected(_cmd)) => {
        debug!("Failed to send REMOVE_LOCAL_WRITER DiscoveryCommand: Disconnected.");
      }
      // other errors must be taken more seriously
      Err(e) => error!("Failed to send REMOVE_LOCAL_WRITER DiscoveryCommand. {e:?}"),
    }
    */
  }
}

pub struct DataSample<'a, D: Keyed, SA: SerializerAdapter<D>> {
  data_writer: &'a DataWriter<D, SA>,
  cmd: WriterCommand,
  refresh_manual_liveliness: bool,
}

use crate::io_uring::rtps::{Domain, DomainRef};
use crate::io_uring::discovery::Discovery2;
use crate::io_uring::timer::timer_state;
use io_uring_buf_ring::buf_ring_state;
use crate::io_uring::network::UDPSender;
impl<D: Keyed, SA: SerializerAdapter<D>> DataSample<'_, D, SA> {
  pub fn write_to(self, domain: &mut DomainRef<'_, '_>) {
    let Self {
      data_writer,
      cmd,
      refresh_manual_liveliness,
    } = self;

    let DomainRef {
      writers,
      discovery,
      ring,
      udp_sender,
    } = domain;

    if let Some(writer) = writers.get_mut(&data_writer.my_guid.entity_id) {
      writer.process_command(udp_sender, ring, cmd);
    }

    if refresh_manual_liveliness {
      discovery
        .liveliness_state
        .manual_participant_liveness_refresh_requested = true;
    }
  }
}

impl<D, SA> DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(topic: Topic, qos: QosPolicies, guid: GUID) -> (Self, bool) {
    let discovery = Self::discovery_cmd_from_qos(&qos);

    (
      Self {
        data_phantom: PhantomData,
        ser_phantom: PhantomData,
        my_topic: topic,
        qos_policy: qos,
        my_guid: guid,
        available_sequence_number: AtomicI64::new(1), // valid numbering starts from 1
      },
      discovery,
    )
  }

  fn next_sequence_number(&self) -> SequenceNumber {
    SequenceNumber::from(
      self
        .available_sequence_number
        .fetch_add(1, Ordering::Relaxed),
    )
  }

  fn undo_sequence_number(&self) {
    self
      .available_sequence_number
      .fetch_sub(1, Ordering::Relaxed);
  }

  fn discovery_cmd_from_qos(qos: &QosPolicies) -> bool {
    matches!(qos.liveliness, Some(Liveliness::ManualByParticipant { .. }))
  }

  /// Manually refreshes liveliness
  ///
  /// Corresponds to DDS Spec 1.4 Section 2.2.2.4.2.22 assert_liveliness.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// data_writer.refresh_manual_liveliness();
  /// ```
  pub fn refresh_manual_liveliness(&self) -> bool {
    Self::discovery_cmd_from_qos(&self.qos())
  }

  /// Writes single data instance to a topic.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data = SomeType { a: 1 };
  /// data_writer.write(some_data, None).unwrap();
  /// ```
  pub fn write(
    &self,
    data: D,
    source_timestamp: Option<Timestamp>,
  ) -> WriteResult<DataSample<'_, D, SA>, D> {
    let (_, writer) = self.write_with_options(data, WriteOptions::from(source_timestamp))?;
    Ok(writer)
  }

  pub fn write_with_options(
    &self,
    data: D,
    write_options: WriteOptions,
  ) -> WriteResult<(SampleIdentity, DataSample<'_, D, SA>), D> {
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

    let ddsdata = DDSData::new(SerializedPayload::new_from_bytes(
      SA::output_encoding(),
      send_buffer,
    ));
    let sequence_number = self.next_sequence_number();
    let writer_command = WriterCommand::DDSData {
      ddsdata,
      write_options,
      sequence_number,
    };

    let timeout = self.qos().reliable_max_blocking_time();

    let sample_identity = SampleIdentity {
      writer_guid: self.my_guid,
      sequence_number,
    };
    let refresh_manual_liveliness = self.refresh_manual_liveliness();

    let data_sample = DataSample {
      data_writer: self,
      cmd: writer_command,
      refresh_manual_liveliness,
    };
    Ok((sample_identity, data_sample))
  }

  /// This operation blocks the calling thread until either all data written by
  /// the reliable DataWriter entities is acknowledged by all
  /// matched reliable DataReader entities, or else the duration specified by
  /// the `max_wait` parameter elapses, whichever happens first.
  ///
  /// See DDS Spec 1.4 Section 2.2.2.4.1.12 wait_for_acknowledgments.
  ///
  /// If this DataWriter is not set to Reliable, or there are no matched
  /// DataReaders with Reliable QoS, the call succeeds immediately.
  ///
  /// Return values
  /// * `Ok(true)` - all acknowledged
  /// * `Ok(false)`- timed out waiting for acknowledgments
  /// * `Err(_)` - something went wrong
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data = SomeType { a: 1 };
  /// data_writer.write(some_data, None).unwrap();
  /// data_writer.wait_for_acknowledgments(std::time::Duration::from_millis(100));
  /// ```
  pub fn wait_for_acknowledgments(&self, max_wait: Duration) -> WriteResult<bool, ()> {
    //TODO: find a better way for this
    // maybe (timer fd, can cancel it with the encoding and then handle it from there)
    todo!()
    /*
    match &self.qos_policy.reliability {
      None | Some(Reliability::BestEffort) => Ok(true),
      Some(Reliability::Reliable { .. }) => {
        let (acked_sender, mut acked_receiver) = sync_status_channel::<()>(1)?;
        let poll = mio_06::Poll::new()?;
        poll.register(
          acked_receiver.as_status_evented(),
          Token(0),
          Ready::readable(),
          PollOpt::edge(),
        )?;
        self
          .cc_upload
          .try_send(WriterCommand::WaitForAcknowledgments {
          })
          .unwrap_or_else(|e| {
            warn!("wait_for_acknowledgments: cannot initiate waiting. This will timeout. {e}");
          });

        let mut events = Events::with_capacity(1);
        poll.poll(&mut events, Some(max_wait))?;
        if let Some(_event) = events.iter().next() {
          match acked_receiver.try_recv() {
            Ok(_) => Ok(true), // got token
            Err(e) => {
              warn!("wait_for_acknowledgments - Spurious poll event? - {e}");
              Ok(false) // TODO: We could also loop here
            }
          }
        } else {
          // no token, so presumably timed out
          Ok(false)
        }
      }
    } // match
    */
  }

  /*

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when functional
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Liveliness lost status has changed
  ///
  /// if let Ok(lls) = data_writer.get_liveliness_lost_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_liveliness_lost_status(&self) -> Result<LivelinessLostStatus> {
    todo!()
  }

  /// Should get latest offered deadline missed status. <b>Do not use yet</b> use `get_status_lister` instead for the moment.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Deadline missed status has changed
  ///
  /// if let Ok(odms) = data_writer.get_offered_deadline_missed_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_offered_deadline_missed_status(&self) -> Result<OfferedDeadlineMissedStatus> {
    let mut fstatus = OfferedDeadlineMissedStatus::new();
    while let Ok(status) = self.status_receiver.try_recv() {
      match status {
        StatusChange::OfferedDeadlineMissedStatus(status) => fstatus = status,
  // TODO: possibly save old statuses
        _ => (),
      }
    }

    match self
      .cc_upload
      .try_send(WriterCommand::ResetOfferedDeadlineMissedStatus {
        writer_guid: self.guid(),
      }) {
      Ok(_) => (),
      Err(e) => error!("Unable to send ResetOfferedDeadlineMissedStatus. {e:?}"),
    };

    Ok(fstatus)
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when functional
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Liveliness lost status has changed
  ///
  /// if let Ok(oiqs) = data_writer.get_offered_incompatible_qos_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_offered_incompatible_qos_status(&self) -> Result<OfferedIncompatibleQosStatus> {
    todo!()
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when functional
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Liveliness lost status has changed
  ///
  /// if let Ok(pms) = data_writer.get_publication_matched_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_publication_matched_status(&self) -> Result<PublicationMatchedStatus> {
    todo!()
  }

  */

  /// Topic assigned to this DataWriter
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// assert_eq!(data_writer.topic(), &topic);
  /// ```
  pub fn topic(&self) -> &Topic {
    &self.my_topic
  }

  /// Manually asserts liveliness (use this instead of refresh) according to QoS
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// data_writer.assert_liveliness().unwrap();
  /// ```
  ///
  /// An `Err` result means that livelines assertion message could not be sent,
  /// likely because Discovery has too much work to do.
  pub fn assert_liveliness(&self) -> Option<DiscoveryCommand> {
    match self.qos().liveliness? {
      Liveliness::ManualByParticipant { .. } => Some(DiscoveryCommand::ManualAssertLiveliness),
      Liveliness::ManualByTopic { lease_duration: _ } => {
        Some(DiscoveryCommand::AssertTopicLiveliness {
          writer_guid: self.guid(),
          manual_assertion: true, // by definition of this function
        })
      }
      _ => None,
    }
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when available
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(),
  /// "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType,
  /// CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// for sub in data_writer.get_matched_subscriptions().iter() {
  ///   // do something
  /// }
  pub fn get_matched_subscriptions(&self) -> Vec<SubscriptionBuiltinTopicData> {
    todo!()
  }

  /// Disposes data instance with specified key
  ///
  /// # Arguments
  ///
  /// * `key` - Key of the instance
  /// * `source_timestamp` - DDS source timestamp (None uses now as time as
  ///   specified in DDS spec)
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32, val: usize }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data_1_1 = SomeType { a: 1, val: 3};
  /// let some_data_1_2 = SomeType { a: 1, val: 4};
  /// // different key
  /// let some_data_2_1 = SomeType { a: 2, val: 5};
  /// let some_data_2_2 = SomeType { a: 2, val: 6};
  ///
  /// data_writer.write(some_data_1_1, None).unwrap();
  /// data_writer.write(some_data_1_2, None).unwrap();
  /// data_writer.write(some_data_2_1, None).unwrap();
  /// data_writer.write(some_data_2_2, None).unwrap();
  ///
  /// // disposes both some_data_1_1 and some_data_1_2. They are no longer offered by this writer to this topic.
  /// data_writer.dispose(&1, None).unwrap();
  /// ```
  pub fn dispose(
    &self,
    key: &<D as Keyed>::K,
    source_timestamp: Option<Timestamp>,
  ) -> WriteResult<(WriterCommand, bool), ()> {
    let send_buffer = SA::key_to_bytes(key).map_err(|e| WriteError::Serialization {
      reason: format!("{e}"),
      data: (),
    })?; // serialize key

    let ddsdata = DDSData::new_disposed_by_key(
      ChangeKind::NotAliveDisposed,
      SerializedPayload::new_from_bytes(SA::output_encoding(), send_buffer),
    );

    let writer_cmd = WriterCommand::DDSData {
      ddsdata,
      write_options: WriteOptions::from(source_timestamp),
      sequence_number: self.next_sequence_number(),
    };

    let discovery_cmd = self.refresh_manual_liveliness();
    Ok((writer_cmd, discovery_cmd))
  }
}

impl<D, SA> RTPSEntity for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn guid(&self) -> GUID {
    self.my_guid
  }
}

impl<D, SA> HasQoSPolicy for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn qos(&self) -> QosPolicies {
    self.qos_policy.clone()
  }
}

/* TODO: uncomment
#[cfg(test)]
mod tests {
  use std::thread;

  use byteorder::LittleEndian;
  use log::info;

  use super::*;
  use crate::{
    dds::{key::Key, participant::DomainParticipant},
    structure::topic_kind::TopicKind,
    test::random_data::*,
  };

  #[test]
  fn dw_write_test() {
    let domain_participant = DomainParticipant::new(0).expect("Publisher creation failed!");
    let qos = QosPolicies::qos_none();
    let _default_dw_qos = QosPolicies::qos_none();
    let publisher = domain_participant
      .create_publisher(&qos)
      .expect("Failed to create publisher");
    let topic = domain_participant
      .create_topic(
        "Aasii".to_string(),
        "Huh?".to_string(),
        &qos,
        TopicKind::WithKey,
      )
      .expect("Failed to create topic");

    let data_writer: DataWriter<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>> =
      publisher
        .create_datawriter(&topic, None)
        .expect("Failed to create datawriter");

    let mut data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    data_writer
      .write(data.clone(), None)
      .expect("Unable to write data");

    data.a = 5;
    let timestamp = Timestamp::now();
    data_writer
      .write(data, Some(timestamp))
      .expect("Unable to write data with timestamp");

    // TODO: verify that data is sent/written correctly
    // TODO: write also with timestamp
  }

  #[test]
  fn dw_dispose_test() {
    let domain_participant = DomainParticipant::new(0).expect("Publisher creation failed!");
    let qos = QosPolicies::qos_none();
    let publisher = domain_participant
      .create_publisher(&qos)
      .expect("Failed to create publisher");
    let topic = domain_participant
      .create_topic(
        "Aasii".to_string(),
        "Huh?".to_string(),
        &qos,
        TopicKind::WithKey,
      )
      .expect("Failed to create topic");

    let data_writer: DataWriter<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>> =
      publisher
        .create_datawriter(&topic, None)
        .expect("Failed to create datawriter");

    let data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    let key = &data.key().hash_key(false);
    info!("key: {key:?}");

    data_writer
      .write(data.clone(), None)
      .expect("Unable to write data");

    thread::sleep(Duration::from_millis(100));
    data_writer
      .dispose(&data.key(), None)
      .expect("Unable to dispose data");

    // TODO: verify that dispose is sent correctly
  }

  #[test]
  fn dw_wait_for_ack_test() {
    let domain_participant = DomainParticipant::new(0).expect("Participant creation failed!");
    let qos = QosPolicies::qos_none();
    let publisher = domain_participant
      .create_publisher(&qos)
      .expect("Failed to create publisher");
    let topic = domain_participant
      .create_topic(
        "Aasii".to_string(),
        "Huh?".to_string(),
        &qos,
        TopicKind::WithKey,
      )
      .expect("Failed to create topic");

    let data_writer: DataWriter<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>> =
      publisher
        .create_datawriter(&topic, None)
        .expect("Failed to create datawriter");

    let data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    data_writer.write(data, None).expect("Unable to write data");

    let res = data_writer
      .wait_for_acknowledgments(Duration::from_secs(2))
      .unwrap();
    assert!(res); // we should get "true" immediately, because we have
                  // no Reliable QoS
  }
}
*/
