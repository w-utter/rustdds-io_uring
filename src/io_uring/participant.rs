use io_uring_buf_ring::buf_ring_state;
use io_uring::IoUring;

use crate::{
  dds::CreateResult,
  io_uring::{
    dds::{cache::DDSCache, no_key, topic::Topic, with_key},
    discovery::{Discovery2, DiscoveryDB},
    network::UDPSender,
    rtps::Domain,
    timer::timer_state,
  },
  structure::guid::EntityKind,
  EntityId, QosPolicies, TopicKind, TypeDesc, GUID,
};

pub struct Participant {
  pub guid: GUID,
  entity_id_gen: u32,
  //participant_id: u16,
  //domain_id: u16,
}

impl Participant {
  pub fn new() -> Self {
    let guid = GUID::new_participant_guid();
    let entity_id_gen = 0;
    Self {
      guid,
      entity_id_gen,
    }
  }
}

pub struct TopicComponent<'a> {
  topic: Topic,
  participant: &'a mut Participant,
}

impl Participant {
  pub fn create_topic(
    &mut self,
    topic_name: String,
    type_name: String,
    qos: &QosPolicies,
    kind: TopicKind,
    dds_cache: &mut DDSCache,
  ) -> TopicComponent<'_> {
    let typedesc = TypeDesc::new(type_name);
    let topic = Topic::new(topic_name.clone(), typedesc.clone(), qos, kind);

    dds_cache.add_new_topic(topic_name, typedesc, qos);
    TopicComponent {
      topic,
      participant: self,
    }
  }

  fn new_entity_id(&mut self, entity_kind: EntityKind) -> EntityId {
    let [_goldilocks, papa_byte, mama_byte, baby_byte] = self.entity_id_gen.to_be_bytes();
    self.entity_id_gen += 1;
    EntityId::new([papa_byte, mama_byte, baby_byte], entity_kind)
  }

  pub fn publisher<'a, 'q>(&'a mut self, topic: &'a Topic, qos: &'q QosPolicies) -> Publisher<'a, 'q> {
      Publisher {
          topic,
          participant: self,
          qos,
      }
  }

  pub fn subscriber<'a, 'q>(&'a mut self, topic: &'a Topic, qos: &'q QosPolicies) -> Subscriber<'a, 'q> {
      Subscriber {
          topic,
          participant: self,
          qos,
      }
  }
}

impl TopicComponent<'_> {
  pub fn publisher<'q>(&mut self, qos: &'q QosPolicies) -> Publisher<'_, 'q> {
    Publisher {
      topic: &self.topic,
      participant: self.participant,
      qos,
    }
  }

  pub fn subscriber<'q>(&mut self, qos: &'q QosPolicies) -> Subscriber<'_, 'q> {
    Subscriber {
      topic: &self.topic,
      participant: self.participant,
      qos,
    }
  }

  pub fn topic(&self) -> &Topic {
      &self.topic
  }

  pub fn inner(self) -> Topic {
      self.topic
  }
}

pub struct Publisher<'a, 'q> {
  topic: &'a Topic,
  participant: &'a mut Participant,
  qos: &'q QosPolicies,
}

use crate::{with_key::SerializerAdapter, Keyed};

impl Publisher<'_, '_> {
  pub fn create_datawriter_cdr<D: Keyed + serde::Serialize>(
    &mut self,
    qos: Option<QosPolicies>,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> with_key::DataWriterCdr<D>
  where
    <D as Keyed>::K: serde::Serialize,
  {
    self.create_datawriter(qos, discovery_db, domain, ring, discovery, udp_sender, user)
  }
  pub fn create_datawriter<D: Keyed, SA: SerializerAdapter<D>>(
    &mut self,
    qos: Option<QosPolicies>,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> with_key::DataWriter<D, SA> {
    // 1) pubsub line 160
    // 2) pubsub line 446
    use crate::dds::qos::HasQoSPolicy;
    let writer_qos = self
      .qos
      .modify_by(&self.topic.qos())
      .modify_by(&qos.unwrap_or(QosPolicies::qos_none()));

    let entity_id = self
      .participant
      .new_entity_id(EntityKind::WRITER_WITH_KEY_USER_DEFINED);

    use crate::GUID;
    let guid = GUID::new(self.participant.guid.prefix, entity_id);

    let (datawriter, manual_assertion) =
      with_key::DataWriter::<D, SA>::new(self.topic.clone(), writer_qos.clone(), guid);

    let crate::io_uring::discovery::traffic::UserTrafficLocators {
      unicast_user_traffic,
      multicast_user_traffic,
    } = domain.listeners.user_traffic_locators();

    use crate::discovery::DiscoveredWriterData;
    let discovered_data = DiscoveredWriterData::new_io_uring(
      &datawriter,
      self.topic,
      self.participant.guid,
      None,
      unicast_user_traffic,
      multicast_user_traffic,
    );

    discovery_db.update_local_topic_writer(discovered_data);
    discovery_db.update_topic_data_p(self.topic);

    use crate::io_uring::dds::topic::TopicDescription;

    let writer_like_stateless = false;
    use crate::io_uring::rtps::WriterIngredients;
    let writer_ing = WriterIngredients {
      guid,
      topic_name: self.topic.name(),
      like_stateless: writer_like_stateless,
      qos_policies: writer_qos,
      security_plugins: None,
    };

    domain
      .add_local_writer(writer_ing, udp_sender, ring, discovery.initialized(), user)
      .unwrap();

    discovery.publish_writer(guid, discovery_db, udp_sender, ring);
    discovery.publish_topic(&self.topic.name(), discovery_db, udp_sender, ring);

    if manual_assertion {
      discovery
        .liveliness_state
        .manual_participant_liveness_refresh_requested = true;
    }
    datawriter
  }

  pub fn create_datawriter_no_key<D: Keyed, SA: SerializerAdapter<D>>(
    &mut self,
    qos: Option<QosPolicies>,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> no_key::DataWriter<D, SA> {
    let writer =
      self.create_datawriter(qos, discovery_db, domain, ring, discovery, udp_sender, user);
    no_key::DataWriter::from_keyed(writer)
  }

  pub fn create_datawriter_no_key_cdr<D: Keyed + serde::Serialize>(
    &mut self,
    qos: Option<QosPolicies>,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> no_key::DataWriterCdr<D>
  where
    <D as Keyed>::K: serde::Serialize,
  {
    self.create_datawriter_no_key(qos, discovery_db, domain, ring, discovery, udp_sender, user)
  }
}

pub struct Subscriber<'a, 'q> {
  topic: &'a Topic,
  participant: &'a mut Participant,
  qos: &'q QosPolicies,
}

use crate::{io_uring::dds::with_key::SimpleDataReader, with_key::DeserializerAdapter};
impl Subscriber<'_, '_> {
  pub fn create_datareader_cdr<D: Keyed + serde::de::DeserializeOwned + 'static>(
    &mut self,
    qos: Option<QosPolicies>,
    dds_cache: &mut DDSCache,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> CreateResult<with_key::DataReaderCdr<D>>
  where
    <D as Keyed>::K: serde::de::DeserializeOwned,
  {
    self.create_datareader(
      qos,
      dds_cache,
      discovery_db,
      domain,
      ring,
      discovery,
      udp_sender,
      user,
    )
  }

  pub fn create_datareader<D: Keyed + 'static, SA: DeserializerAdapter<D>>(
    &mut self,
    qos: Option<QosPolicies>,
    dds_cache: &mut DDSCache,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> CreateResult<with_key::DataReader<D, SA>> {
    let simple = self.create_simple_datareader(
      qos,
      dds_cache,
      discovery_db,
      domain,
      ring,
      discovery,
      udp_sender,
      user,
    )?;

    Ok(with_key::DataReader::from_simple_data_reader(simple))
  }

  fn create_simple_datareader<D: Keyed + 'static, SA: DeserializerAdapter<D>>(
    &mut self,
    qos: Option<QosPolicies>,
    dds_cache: &mut DDSCache,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> CreateResult<SimpleDataReader<D, SA>> {
    // pubsub line 786
    // pubsub line 1216
    // pubsub line 986
    // pubsub line 1010

    use crate::dds::qos::HasQoSPolicy;
    let reader_qos = self
      .qos
      .modify_by(&self.topic.qos())
      .modify_by(&qos.unwrap_or(QosPolicies::qos_none()));

    let entity_id = self
      .participant
      .new_entity_id(EntityKind::WRITER_WITH_KEY_USER_DEFINED);

    use crate::GUID;
    let guid = GUID::new(self.participant.guid.prefix, entity_id);

    use crate::io_uring::dds::topic::TopicDescription;

    let topic_cache = dds_cache
      .get_existing_topic_cache(&self.topic.name())
      .unwrap();
    topic_cache.update_keep_limits(&reader_qos);

    let reader_like_stateless = false;
    use crate::io_uring::rtps::ReaderIngredients;
    let reader_ing = ReaderIngredients {
      guid,
      topic_name: self.topic.name(),
      like_stateless: reader_like_stateless,
      qos_policy: reader_qos.clone(),
      security_plugins: None,
    };

    let crate::io_uring::discovery::traffic::UserTrafficLocators {
      unicast_user_traffic,
      multicast_user_traffic,
    } = domain.listeners.user_traffic_locators();

    use crate::rtps::rtps_reader_proxy::RtpsReaderProxy;
    let proxy = RtpsReaderProxy::from_io_uring_reader(
      guid,
      reader_qos.clone(),
      unicast_user_traffic,
      multicast_user_traffic,
    );

    use crate::discovery::SubscriptionBuiltinTopicData;
    let subscription_data = SubscriptionBuiltinTopicData::new(
      guid,
      Some(self.participant.guid),
      self.topic.name(),
      self.topic.get_type().name().to_string(),
      &reader_qos,
      None,
    );

    let content_filter = None;

    use crate::discovery::{DiscoveredReaderData, ReaderProxy};
    let discovered_data = DiscoveredReaderData {
      reader_proxy: ReaderProxy::from(proxy),
      subscription_topic_data: subscription_data,
      content_filter,
    };
    discovery_db.update_local_topic_reader(discovered_data);
    discovery_db.update_topic_data_p(self.topic);

    domain.add_local_reader(reader_ing, ring, user).unwrap();

    discovery.publish_reader(guid, discovery_db, udp_sender, ring);
    discovery.publish_topic(&self.topic.name(), discovery_db, udp_sender, ring);

    SimpleDataReader::new(guid.prefix, guid.entity_id, self.topic.clone(), reader_qos)
  }

  pub fn create_datareader_no_key<D: Keyed + 'static, SA: DeserializerAdapter<D>>(
    &mut self,
    qos: Option<QosPolicies>,
    dds_cache: &mut DDSCache,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> CreateResult<no_key::DataReader<D, SA>> {
    let reader = self.create_datareader(
      qos,
      dds_cache,
      discovery_db,
      domain,
      ring,
      discovery,
      udp_sender,
      user,
    )?;

    Ok(no_key::DataReader::from_keyed(reader))
  }

  pub fn create_datareader_no_key_cdr<D: Keyed + serde::de::DeserializeOwned + 'static>(
    &mut self,
    qos: Option<QosPolicies>,
    dds_cache: &mut DDSCache,
    discovery_db: &mut DiscoveryDB,
    domain: &mut Domain<timer_state::Init, buf_ring_state::Init>,
    ring: &mut IoUring,
    discovery: &mut Discovery2<timer_state::Init>,
    udp_sender: &UDPSender,
    user: u8,
  ) -> CreateResult<no_key::DataReaderCdr<D>>
  where
    <D as Keyed>::K: serde::de::DeserializeOwned,
  {
    self.create_datareader_no_key(
      qos,
      dds_cache,
      discovery_db,
      domain,
      ring,
      discovery,
      udp_sender,
      user,
    )
  }
}
