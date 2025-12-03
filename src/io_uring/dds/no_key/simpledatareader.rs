#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    adapters::no_key::*,
    no_key::{datasample::DeserializedCacheChange, wrappers::DecodeWrapper},
    qos::*,
    result::ReadResult,
  },
  io_uring::dds::with_key,
  serialization::CDRDeserializerAdapter,
  structure::entity::RTPSEntity,
  GUID,
};

use crate::no_key::wrappers::{NoKeyWrapper, DAWrapper};

/// SimpleDataReaders can only do "take" semantics and does not have
/// any deduplication or other DataSampleCache functionality.
pub struct SimpleDataReader<D, DA: DeserializerAdapter<D> = CDRDeserializerAdapter<D>> {
  keyed_simpledatareader: with_key::SimpleDataReader<NoKeyWrapper<D>, DAWrapper<DA>>,
}

/// Simplified type for CDR encoding
pub type SimpleDataReaderCdr<D> = SimpleDataReader<D, CDRDeserializerAdapter<D>>;

impl<D: 'static, DA> SimpleDataReader<D, DA>
where
  DA: DeserializerAdapter<D> + 'static,
{
  // TODO: Make it possible to construct SimpleDataReader (particularly, no_key
  // version) from the public API. That is, From a Subscriber object like a
  // normal Datareader. This is to be then used from the ros2-client package.
  pub(crate) fn from_keyed(
    keyed_simpledatareader: with_key::SimpleDataReader<NoKeyWrapper<D>, DAWrapper<DA>>,
  ) -> Self {
    Self {
      keyed_simpledatareader,
    }
  }

  pub fn try_take_one(&self, cache: &mut crate::TopicCache) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    DA: DefaultDecoder<D>,
  {
    Self::try_take_one_with(self, DA::DECODER, cache)
  }

  pub fn try_take_one_with<S>(&self, decoder: S, cache: &mut crate::TopicCache) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    S: Decode<DA::Decoded> + Clone,
  {
    match self
      .keyed_simpledatareader
      .try_take_one_with(DecodeWrapper::new(decoder), cache)
    {
      Err(e) => Err(e),
      Ok(None) => Ok(None),
      Ok(Some(kdcc)) => match DeserializedCacheChange::<D>::from_keyed(kdcc) {
        Some(dcc) => Ok(Some(dcc)),
        None => Ok(None),
      },
    }
  }

  pub fn qos(&self) -> &QosPolicies {
    self.keyed_simpledatareader.qos()
  }

  pub fn guid(&self) -> GUID {
    self.keyed_simpledatareader.guid()
  }
}

impl<D, DA> RTPSEntity for SimpleDataReader<D, DA>
where
  D: 'static,
  DA: DeserializerAdapter<D>,
{
  fn guid(&self) -> GUID {
    self.keyed_simpledatareader.guid()
  }
}
