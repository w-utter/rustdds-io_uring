use crate::{
  io_uring::dds::{
    cache::DDSCache,
    pubsub::{Publisher, Subscriber},
    topic::Topic,
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

  pub(crate) fn new_entity_id(&mut self, entity_kind: EntityKind) -> EntityId {
    let [_goldilocks, papa_byte, mama_byte, baby_byte] = self.entity_id_gen.to_be_bytes();
    self.entity_id_gen += 1;
    EntityId::new([papa_byte, mama_byte, baby_byte], entity_kind)
  }

  pub fn publisher<'a, 'q>(
    &'a mut self,
    topic: &'a Topic,
    qos: &'q QosPolicies,
  ) -> Publisher<'a, 'q> {
    Publisher {
      topic,
      participant: self,
      qos,
    }
  }

  pub fn subscriber<'a, 'q>(
    &'a mut self,
    topic: &'a Topic,
    qos: &'q QosPolicies,
  ) -> Subscriber<'a, 'q> {
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
