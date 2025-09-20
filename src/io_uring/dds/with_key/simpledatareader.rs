use std::{cmp::max, collections::BTreeMap, marker::PhantomData, sync::Mutex};

use serde::de::DeserializeOwned;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    adapters::with_key::{Decode, DefaultDecoder, DeserializerAdapter},
    ddsdata::*,
    key::*,
    qos::*,
    result::*,
    with_key::{
      datasample::{DeserializedCacheChange, Sample},
      simpledatareader::ReadState,
    },
  },
  io_uring::dds::topic::{Topic, TopicDescription},
  serialization::CDRDeserializerAdapter,
  structure::{
    cache_change::CacheChange,
    dds_cache::TopicCache,
    entity::RTPSEntity,
    guid::{EntityId, GUID},
    sequence_number::SequenceNumber,
    time::Timestamp,
  },
};

/// SimpleDataReaders can only do "take" semantics and does not have
/// any deduplication or other DataSampleCache functionality.
pub struct SimpleDataReader<D: Keyed, DA: DeserializerAdapter<D> = CDRDeserializerAdapter<D>> {
  my_topic: Topic,
  qos_policy: QosPolicies,
  my_guid: GUID,

  // SimpleDataReader stores a pointer to a mutex on the topic cache
  //topic_cache: Arc<Mutex<TopicCache>>,
  read_state: Mutex<ReadState<<D as Keyed>::K>>,

  deserializer_type: PhantomData<DA>, // This is to provide use for DA
}

impl<D, DA> Drop for SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  fn drop(&mut self) {
    /* TODO
    // Tell dp_event_loop
    self.my_subscriber.remove_reader(self.my_guid);

    // Tell discovery
    match self
      .discovery_command
      .send(DiscoveryCommand::RemoveLocalReader { guid: self.my_guid })
    {
      Ok(_) => {}
      Err(mio_channel::SendError::Disconnected(_)) => {
        debug!("Failed to send DiscoveryCommand::RemoveLocalReader . Maybe shutting down?");
      }
      Err(e) => error!(
        "Failed to send DiscoveryCommand::RemoveLocalReader. {:?}",
        e
      ),
    }
    */
  }
}

impl<D: 'static, DA> SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    dp_guid_prefix: crate::structure::guid::GuidPrefix,
    my_id: EntityId,
    topic: Topic,
    qos_policy: QosPolicies,
    // Each notification sent to this channel must be try_recv'd
    //topic_cache: Arc<Mutex<TopicCache>>,
  ) -> CreateResult<Self> {
    let my_guid = GUID::new_with_prefix_and_id(dp_guid_prefix, my_id);

    // Verify that the topic cache corresponds to the topic of the Reader
    /*
    let topic_cache_name = topic_cache.lock().unwrap().topic_name();
    if topic.name() != topic_cache_name {
      return Err(CreateError::Internal {
        reason: format!(
          "Topic name = {} and topic cache name = {} not equal when creating a SimpleDataReader",
          topic.name(),
          topic_cache_name
        ),
      });
    }
    */

    Ok(Self {
      qos_policy,
      my_guid,
      //topic_cache,
      read_state: Mutex::new(ReadState::new()),
      my_topic: topic,
      deserializer_type: PhantomData,
    })
  }

  fn try_take_undecoded<'a>(
    is_reliable: bool,
    topic_cache: &'a TopicCache,
    latest_instant: Timestamp,
    last_read_sn: &'a BTreeMap<GUID, SequenceNumber>,
  ) -> Box<dyn Iterator<Item = (Timestamp, &'a CacheChange)> + 'a> {
    if is_reliable {
      topic_cache.get_changes_in_range_reliable(last_read_sn)
    } else {
      topic_cache.get_changes_in_range_best_effort(latest_instant, Timestamp::now())
    }
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

  fn deserialize_with<S>(
    &self,
    timestamp: Timestamp,
    cc: &CacheChange,
    hash_to_key_map: &mut BTreeMap<KeyHash, D::K>,
    decoder: S,
  ) -> ReadResult<DeserializedCacheChange<D>>
  where
    S: Decode<DA::Decoded, DA::DecodedKey>,
  {
    match cc.data_value {
      DDSData::Data {
        ref serialized_payload,
      } => {
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
                "Failed to deserialize sample bytes: {}, , Topic = {}, Type = {:?}",
                e,
                self.my_topic.name(),
                self.my_topic.get_type()
              ),
            }),
          }
        } else {
          info!(
            "Unknown representation id: {:?} , Topic = {}, Type = {:?} data = {:02x?}",
            serialized_payload.representation_identifier,
            self.my_topic.name(),
            self.my_topic.get_type(),
            serialized_payload.value,
          );
          Err(ReadError::Deserialization {
            reason: format!(
              "Unknown representation id {:?} , Topic = {}, Type = {:?}",
              serialized_payload.representation_identifier,
              self.my_topic.name(),
              self.my_topic.get_type()
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
              "Failed to deserialize key {}, Topic = {}, Type = {:?}",
              e,
              self.my_topic.name(),
              self.my_topic.get_type()
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
            details: format!(
              "Received dispose with unknown key hash: {:x?}, Topic = {}, Type = {:?}",
              key_hash,
              self.my_topic.name(),
              self.my_topic.get_type()
            ),
          })
        }
      }
    } // match
  }

  /// Note: Always remember to call .drain_read_notifications() just before
  /// calling this one. Otherwise, new notifications may not appear.
  pub fn try_take_one(
    &self,
    topic_cache: &TopicCache,
  ) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    DA: DeserializerAdapter<D> + DefaultDecoder<D>,
  {
    Self::try_take_one_with(self, DA::DECODER, topic_cache)
  }

  /// Note: Always remember to call .drain_read_notifications() just before
  /// calling this one. Otherwise, new notifications may not appear.
  #[allow(clippy::needless_pass_by_value)]
  pub fn try_take_one_with<S>(
    &self,
    decoder: S,
    topic_cache: &TopicCache,
  ) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    S: Decode<DA::Decoded, DA::DecodedKey> + Clone,
  {
    let is_reliable = matches!(
      self.qos_policy.reliability(),
      Some(policy::Reliability::Reliable { .. })
    );

    //let topic_cache = self.acquire_the_topic_cache_guard();

    let mut read_state_ref = self.read_state.lock().unwrap();
    let latest_instant = read_state_ref.latest_instant;
    let (last_read_sn, hash_to_key_map) = read_state_ref.get_sn_map_and_hash_map();

    // loop in case we get a sample that should be ignored, so we try next.
    loop {
      let (timestamp, cc) =
        match Self::try_take_undecoded(is_reliable, &topic_cache, latest_instant, last_read_sn)
          .next()
        {
          None => return Ok(None), // no more data available right now
          Some((ts, cc)) => (ts, cc),
        };

      let result = self.deserialize_with(timestamp, cc, hash_to_key_map, decoder.clone());

      if let Err(ReadError::UnknownKey { .. }) = result {
        // ignore unknown key hash, continue looping
      } else {
        // return with this result
        // make copies of guid and SN to calm down borrow checker.
        let writer_guid = cc.writer_guid;
        let sequence_number = cc.sequence_number;
        // Advance read pointer, error or not, because otherwise
        // the SimpleDatareader is stuck.
        read_state_ref.latest_instant = max(latest_instant, timestamp);
        read_state_ref
          .last_read_sn
          .insert(writer_guid, sequence_number);

        // // Debug sanity check:
        // use crate::Duration;
        // if Timestamp::now().duration_since(timestamp) > Duration::from_secs(1) {
        //   error!("Sample delayed by {:?} , Topic = {} {:?}",
        //     Timestamp::now().duration_since(timestamp), self.topic().name(),
        //     sequence_number,
        //      );
        // }

        return result.map(Some);
      }
    }
  }

  pub fn qos(&self) -> &QosPolicies {
    &self.qos_policy
  }

  pub fn guid(&self) -> GUID {
    self.my_guid
  }

  pub fn topic(&self) -> &Topic {
    &self.my_topic
  }

  /*
  fn acquire_the_topic_cache_guard(&self) -> MutexGuard<TopicCache> {
    self.topic_cache.lock().unwrap_or_else(|e| {
      panic!(
        "The topic cache of topic {} is poisoned. Error: {}",
        &self.my_topic.name(),
        e
      )
    })
  }
  */
}

impl<D, DA> RTPSEntity for SimpleDataReader<D, DA>
where
  D: Keyed + DeserializeOwned,
  DA: DeserializerAdapter<D>,
{
  fn guid(&self) -> GUID {
    self.my_guid
  }
}
