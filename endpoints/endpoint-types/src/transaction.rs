use std::{
    any::Any,
    fmt::Debug,
    io::{self, Read},
};

use crate::EpRequest;
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::EndpointType;
pub use ep_core::database::schema::endpoint::EndpointTransactionInput;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use linkme::distributed_slice;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::Value;

pub trait EpTransaction: Send + Sync + Debug {
    fn kind(&self) -> EpKind;
    fn as_request(self: Box<Self>) -> Box<dyn EpTransaction>;
    fn as_any(&self) -> &dyn Any;

    fn to_value(&self) -> Result<Value, serde_json::Error>;
    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()>;
}

impl Serialize for Box<dyn EpTransaction> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Helper struct to first capture the kind and data
        #[derive(Serialize)]
        struct TransactionHelper {
            kind: EpKind,
            #[serde(flatten)]
            data: Value,
        }

        let kind = self.kind();
        let value = self.to_value().map_err(serde::ser::Error::custom)?;
        let helper = TransactionHelper { kind, data: value };

        helper.serialize(serializer)
    }
}

impl BorshSerialize for Box<dyn EpTransaction> {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.borsh_serialize(writer)
    }
}

#[distributed_slice]
pub static TRANSACTION_DESERIALIZERS: [(EpKind, fn(Value) -> Result<Box<dyn EpTransaction>, Box<dyn std::error::Error>>)];

impl<'de> Deserialize<'de> for Box<dyn EpTransaction> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Helper struct to first capture the kind and data
        #[derive(Deserialize)]
        struct TransactionHelper {
            kind: EpKind,
            // #[serde(flatten)]
            data: Value,
        }

        let TransactionHelper { kind, data } = TransactionHelper::deserialize(deserializer)?;

        let transaction = 'transaction: {
            for &(de_kind, ref de_fn) in TRANSACTION_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'transaction de_fn(data).map_err(serde::de::Error::custom)?;
                }
            }

            return Err(serde::de::Error::custom(format!(
                "{kind} not supported; enable the corresponding feature in Cargo.toml"
            )));
        };

        Ok(transaction)
    }
}

#[distributed_slice]
pub static TRANSACTION_BORSH_DESERIALIZERS: [(EpKind, fn(&mut dyn io::Read) -> io::Result<Box<dyn EpTransaction>>)];

impl BorshDeserialize for Box<dyn EpTransaction> {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        // First deserialize the kind
        let kind = EpKind::deserialize_reader(reader)?;

        // Then deserialize the specific request based on the kind
        let transaction = 'transaction: {
            for &(de_kind, ref de_fn) in TRANSACTION_BORSH_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'transaction de_fn(reader)?;
                }
            }

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{kind} not supported; enable the corresponding feature in Cargo.toml"),
            ));
        };

        Ok(transaction)
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone)]
pub struct Transaction<R: EpRequest + EndpointType + Serialize + 'static>(pub Vec<R>); // req with type

struct WriteWrapper<'a>(&'a mut dyn io::Write);

impl<'a> io::Write for WriteWrapper<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<R: EpRequest + EndpointType + Debug + Serialize + BorshSerialize + DeserializeOwned + Send + Sync + 'static> EpTransaction
    for Transaction<R>
{
    fn kind(&self) -> EpKind {
        R::r#type()
    }

    fn as_request(self: Box<Self>) -> Box<dyn EpTransaction> {
        self
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        BorshSerialize::serialize(self, &mut WriteWrapper(writer))
    }
}

impl TryFrom<EndpointTransactionInput> for Box<dyn EpTransaction> {
    type Error = EpError;
    fn try_from(value: EndpointTransactionInput) -> ResultEP<Self> {
        serde_json::from_value(value.request().to_owned()).map_err(EpError::serde)
    }
}

impl TryFrom<&EndpointTransactionInput> for Box<dyn EpTransaction> {
    type Error = EpError;
    fn try_from(value: &EndpointTransactionInput) -> ResultEP<Self> {
        serde_json::from_value(value.request().to_owned()).map_err(EpError::serde)
    }
}
