use std::collections::HashMap;

use crate::structure::dds_cache::TopicCache;
use crate::dds::typedesc::TypeDesc;
use crate::dds::qos::QosPolicies;

use crate::dds::{CreateError, CreateResult};
use crate::create_error_internal;
use crate::Timestamp;

#[derive(Default)]
pub struct DDSCache {
  caches: HashMap<String, TopicCache>,
}

impl DDSCache {
  pub fn new() -> Self {
    Self::default()
  }

  pub(crate) fn add_new_topic(
    &mut self,
    topic_name: String,
    topic_data_type: TypeDesc,
    qos: &QosPolicies,
  ) -> &mut TopicCache {
    self
      .caches
      .entry(topic_name.clone())
      .and_modify(|tc| tc.update_keep_limits(qos))
      .or_insert(TopicCache::new(topic_name, topic_data_type, qos))
  }

  pub(crate) fn get_existing_topic_cache(
    &mut self,
    topic_name: &str,
  ) -> CreateResult<&mut TopicCache> {
    match self.caches.get_mut(topic_name) {
      Some(tc) => Ok(tc),
      None => create_error_internal!("Topic cache for topic {topic_name} not found in DDS cache"),
    }
  }

  pub fn garbage_collect(&mut self) {
    for tc in self.caches.values_mut() {
      if let Some((last_timestamp, _)) = tc.changes.iter().next_back() {
        if *last_timestamp > tc.changes_reallocated_up_to {
          tc.remove_changes_before(Timestamp::ZERO);
        }
      }
    }
  }
}
