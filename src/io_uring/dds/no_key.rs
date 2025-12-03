pub(crate) mod datareader;
pub(crate) mod datawriter;
pub(crate) mod simpledatareader;

pub use datareader::{DataReader, DataReaderCdr};
pub use datawriter::{DataWriter, DataWriterCdr};
use crate::no_key::wrappers::{NoKeyWrapper, SAWrapper};

pub type DataSample<'a, D, SA> = super::with_key::DataSample<'a, NoKeyWrapper<D>, SAWrapper<SA>>;
