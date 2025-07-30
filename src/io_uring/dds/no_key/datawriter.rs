use std::time::Duration;

use crate::{
  dds::{
    adapters::no_key::SerializerAdapter,
    qos::{HasQoSPolicy, QosPolicies},
    result::{unwrap_no_key_write_error, WriteResult},
  },
  discovery::sedp_messages::SubscriptionBuiltinTopicData,
  serialization::CDRSerializerAdapter,
  structure::{entity::RTPSEntity, rpc::SampleIdentity, time::Timestamp},
  GUID,
};

use crate::io_uring::dds::Topic;
use crate::no_key::wrappers::{NoKeyWrapper, SAWrapper};
use crate::io_uring::dds::with_key::datawriter as datawriter_with_key;
use crate::io_uring::dds::with_key::datawriter::DataSample;
use crate::dds::with_key::datawriter::WriteOptions;
use crate::discovery::discovery::DiscoveryCommand;

/// Simplified type for CDR encoding
pub type DataWriterCdr<D> = DataWriter<D, CDRSerializerAdapter<D>>;

/// DDS DataWriter for no key topics
///
/// # Examples
///
/// ```
/// use serde::{Serialize, Deserialize};
/// use rustdds::*;
/// use rustdds::no_key::DataWriter;
/// use rustdds::serialization::CDRSerializerAdapter;
///
/// let domain_participant = DomainParticipant::new(0).unwrap();
/// let qos = QosPolicyBuilder::new().build();
/// let publisher = domain_participant.create_publisher(&qos).unwrap();
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct SomeType {}
///
/// // NoKey is important
/// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
/// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(&topic, None);
/// ```
pub struct DataWriter<D, SA: SerializerAdapter<D> = CDRSerializerAdapter<D>> {
  keyed_datawriter: datawriter_with_key::DataWriter<NoKeyWrapper<D>, SAWrapper<SA>>,
}

impl<D, SA> DataWriter<D, SA>
where
  SA: SerializerAdapter<D>,
{
  pub(crate) fn from_keyed(
    keyed: datawriter_with_key::DataWriter<NoKeyWrapper<D>, SAWrapper<SA>>,
  ) -> Self {
    Self {
      keyed_datawriter: keyed,
    }
  }

  /// Writes single data instance to a topic.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize, Debug)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data = SomeType {};
  /// data_writer.write(some_data, None).unwrap();
  /// ```
  pub fn write(
    &self,
    data: D,
    source_timestamp: Option<Timestamp>,
  ) -> WriteResult<DataSample<'_, NoKeyWrapper<D>, SAWrapper<SA>>, D> {
    self
      .keyed_datawriter
      .write(NoKeyWrapper::<D> { d: data }, source_timestamp)
      .map_err(unwrap_no_key_write_error)
  }

  pub fn write_with_options(
    &self,
    data: D,
    write_options: WriteOptions,
  ) -> WriteResult<
    (
      SampleIdentity,
      DataSample<'_, NoKeyWrapper<D>, SAWrapper<SA>>,
    ),
    D,
  > {
    self
      .keyed_datawriter
      .write_with_options(NoKeyWrapper::<D> { d: data }, write_options)
      .map_err(unwrap_no_key_write_error)
  }

  /// Waits for all acknowledgements to finish
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use std::time::Duration;
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// data_writer.wait_for_acknowledgments(Duration::from_millis(100));
  /// ```
  pub fn wait_for_acknowledgments(&self, max_wait: Duration) -> WriteResult<bool, ()> {
    self.keyed_datawriter.wait_for_acknowledgments(max_wait)
  }
  /*
  // status queries
  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  // TODO: enable run when implemented
  /// ```no_run
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// if let Ok(status) = data_writer.get_liveliness_lost_status() {
  ///   // Do something
  /// }
  /// ```
  pub fn get_liveliness_lost_status(&self) -> Result<LivelinessLostStatus> {
    self.keyed_datawriter.get_liveliness_lost_status()
  }

  /// Should get latest offered deadline missed status. <b>Do not use yet</b> use `get_status_lister` instead for the moment.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use std::time::Duration;
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// if let Ok(odl_status) = data_writer.get_offered_deadline_missed_status() {
  ///   // Do something
  /// }
  /// ```
  pub fn get_offered_deadline_missed_status(&self) -> Result<OfferedDeadlineMissedStatus> {
    self.keyed_datawriter.get_offered_deadline_missed_status()
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  // TODO: enable run when implemented
  /// ```no_run
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// if let Ok(status) = data_writer.get_offered_incompatible_qos_status() {
  ///   // Do something
  /// }
  /// ```
  pub fn get_offered_incompatible_qos_status(&self) -> Result<OfferedIncompatibleQosStatus> {
    self.keyed_datawriter.get_offered_incompatible_qos_status()
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  // TODO: enable run when implemented
  /// ```no_run
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// if let Ok(status) = data_writer.get_publication_matched_status() {
  ///   // Do something
  /// }
  /// ```
  pub fn get_publication_matched_status(&self) -> Result<PublicationMatchedStatus> {
    self.keyed_datawriter.get_publication_matched_status()
  }
  */
  /// Topic this DataWriter is connected to.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// assert_eq!(&topic, data_writer.topic());
  /// ```
  pub fn topic(&self) -> &Topic {
    self.keyed_datawriter.topic()
  }

  /// Manually asserts liveliness if QoS agrees
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// data_writer.assert_liveliness();
  /// ```
  pub fn assert_liveliness(&self) -> Option<DiscoveryCommand> {
    self.keyed_datawriter.assert_liveliness()
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  // TODO: enable run when implemented
  /// ```no_run ignore
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// for sub in data_writer.get_matched_subscriptions().iter() {
  ///   // handle subscriptions
  /// }
  /// ```
  pub fn get_matched_subscriptions(&self) -> Vec<SubscriptionBuiltinTopicData> {
    self.keyed_datawriter.get_matched_subscriptions()
  }
  /*
  /// Gets mio receiver for all implemented Status changes
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::no_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// # #[derive(Serialize, Deserialize)]
  /// # struct SomeType {}
  /// #
  /// // NoKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::NoKey).unwrap();
  /// let data_writer = publisher.create_datawriter_no_key::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Some status has changed
  ///
  /// if let Ok(status) = data_writer.get_status_listener().try_recv() {
  ///   // handle status change
  /// }
  /// ```
  pub fn get_status_listener(&self) -> &Receiver<DataWriterStatus> {
    self.keyed_datawriter.get_status_listener()
  } */
}

impl<D, SA: SerializerAdapter<D>> RTPSEntity for DataWriter<D, SA> {
  fn guid(&self) -> GUID {
    self.keyed_datawriter.guid()
  }
}

impl<D, SA: SerializerAdapter<D>> HasQoSPolicy for DataWriter<D, SA> {
  fn qos(&self) -> QosPolicies {
    self.keyed_datawriter.qos()
  }
}

/* TODO:
#[cfg(test)]
mod tests {
  use byteorder::LittleEndian;

  use super::*;
  use crate::{
    dds::{participant::DomainParticipant, topic::TopicKind},
    serialization::*,
    test::random_data::*,
  };

  #[test]
  fn dw_write_test() {
    let domain_participant = DomainParticipant::new(0).expect("Failed to create participant");
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
        TopicKind::NoKey,
      )
      .expect("Failed to create topic");

    let data_writer: DataWriter<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>> =
      publisher
        .create_datawriter_no_key(&topic, None)
        .expect("Failed to create datawriter");

    let mut data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    data_writer
      .write(data.clone(), None)
      .expect("Unable to write data");

    data.a = 5;
    let timestamp: Timestamp = Timestamp::now();
    data_writer
      .write(data, Some(timestamp))
      .expect("Unable to write data with timestamp");

    // TODO: verify that data is sent/written correctly
    // TODO: write also with timestamp
  }

  #[test]
  fn dw_wait_for_ack_test() {
    let domain_participant = DomainParticipant::new(0).expect("Failed to create participant");
    let qos = QosPolicies::qos_none();
    let publisher = domain_participant
      .create_publisher(&qos)
      .expect("Failed to create publisher");
    let topic = domain_participant
      .create_topic(
        "Aasii".to_string(),
        "Huh?".to_string(),
        &qos,
        TopicKind::NoKey,
      )
      .expect("Failed to create topic");

    let data_writer: DataWriter<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>> =
      publisher
        .create_datawriter_no_key(&topic, None)
        .expect("Failed to create datawriter");

    let data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    data_writer.write(data, None).expect("Unable to write data");

    let res = data_writer
      .wait_for_acknowledgments(Duration::from_secs(2))
      .unwrap();
    assert!(res); // no "Reliable" QoS policy => "succeeds" immediately
  }
}
*/
