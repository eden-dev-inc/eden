// Suppress async_fn_in_trait warning because we don't need to specify auto trait bounds for these traits.
#![allow(async_fn_in_trait)]

use std::{
    any::Any,
    fmt::Debug,
    io::{self, Read},
};

use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ReqType;
pub use ep_core::database::schema::endpoint::EndpointRequestInput;
use ep_core::database::schema::endpoint::EpRequestWrapper;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use linkme::distributed_slice;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

pub trait EpWireRequest<A>: From<Vec<u8>> + From<bytes::Bytes> {
    fn kind(&self) -> EpKind;
    /// Determine whether this request is a read or write operation
    fn request_type(&self) -> ResultEP<ReqType>;
    /// Send raw bytes using the appropriate connection (read or write)
    /// Returns (Bytes, network_latency_us) - the response bytes and the raw network I/O latency in microseconds
    async fn send_raw_bytes(&self, context: &A) -> ResultEP<(bytes::Bytes, u64)>;
}

pub trait EpRequest: Send + Sync + Debug {
    fn kind(&self) -> EpKind;
    fn as_request(self: Box<Self>) -> Box<dyn EpRequest>;
    fn as_any(&self) -> &dyn Any;

    fn to_value(&self) -> Result<Value, serde_json::Error>;
    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()>;
}

impl TryFrom<EpRequestWrapper> for Box<dyn EpRequest> {
    type Error = EpError;

    fn try_from(value: EpRequestWrapper) -> Result<Self, Self::Error> {
        serde_json::from_value(value.0).map_err(EpError::serde)
    }
}

impl TryInto<EpRequestWrapper> for Box<dyn EpRequest> {
    type Error = EpError;
    fn try_into(self) -> Result<EpRequestWrapper, Self::Error> {
        serde_json::to_value(self).map_err(EpError::serde).map(EpRequestWrapper)
    }
}

#[distributed_slice]
pub static REQUEST_SERIALIZERS: [(EpKind, fn(&Box<dyn EpRequest>) -> Result<Value, Box<dyn std::error::Error>>)];

impl Serialize for Box<dyn EpRequest> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Helper struct to first capture the kind and data
        #[derive(Serialize)]
        struct RequestHelper {
            kind: EpKind,
            #[serde(flatten)]
            data: Value,
        }

        let kind = self.kind();
        let value = 'value: {
            for &(ser_kind, ref ser_fn) in REQUEST_SERIALIZERS.iter() {
                if ser_kind == kind {
                    break 'value ser_fn(self).map_err(serde::ser::Error::custom)?;
                }
            }

            return Err(serde::ser::Error::custom(format!(
                "{kind} not supported; enable the corresponding feature in Cargo.toml"
            )));
        };

        let helper = RequestHelper { kind, data: value };

        helper.serialize(serializer)
    }
}

#[distributed_slice]
pub static REQUEST_BORSH_SERIALIZERS: [(EpKind, fn(&Box<dyn EpRequest>, &mut dyn io::Write) -> io::Result<()>)];

impl BorshSerialize for Box<dyn EpRequest> {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let kind = self.kind();
        'result: {
            for &(ser_kind, ref ser_fn) in REQUEST_BORSH_SERIALIZERS.iter() {
                if ser_kind == kind {
                    break 'result ser_fn(self, writer)?;
                }
            }

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{kind} not supported; enable the corresponding feature in Cargo.toml"),
            ));
        };

        Ok(())
    }
}

#[distributed_slice]
pub static REQUEST_DESERIALIZERS: [(EpKind, fn(Value) -> Result<Box<dyn EpRequest>, Box<dyn std::error::Error>>)];

impl<'de> Deserialize<'de> for Box<dyn EpRequest> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Helper struct to first capture the kind and data
        #[derive(Deserialize)]
        struct RequestHelper {
            kind: EpKind,
            #[serde(flatten)]
            data: Value,
        }

        let RequestHelper { kind, data } = RequestHelper::deserialize(deserializer)?;

        let req = 'req: {
            for &(de_kind, ref de_fn) in REQUEST_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'req de_fn(data).map_err(serde::de::Error::custom)?;
                }
            }

            return Err(serde::de::Error::custom(format!(
                "{kind} not supported; enable the corresponding feature in Cargo.toml"
            )));
        };

        Ok(req)
    }
}

#[distributed_slice]
pub static REQUEST_BORSH_DESERIALIZERS: [(EpKind, fn(&mut dyn io::Read) -> io::Result<Box<dyn EpRequest>>)];

impl BorshDeserialize for Box<dyn EpRequest> {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        // First deserialize the kind
        let kind = EpKind::deserialize_reader(reader)?;

        // Then deserialize the specific request based on the kind
        let req = 'req: {
            for &(de_kind, ref de_fn) in REQUEST_BORSH_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'req de_fn(reader)?;
                }
            }

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{kind} not supported; enable the corresponding feature in Cargo.toml"),
            ));
        };

        Ok(req)
    }
}

impl TryFrom<(EndpointRequestInput, EpKind)> for Box<dyn EpRequest> {
    type Error = EpError;

    fn try_from((input, kind): (EndpointRequestInput, EpKind)) -> ResultEP<Self> {
        let req = 'req: {
            for &(de_kind, ref de_fn) in REQUEST_DESERIALIZERS.iter() {
                if de_kind == kind {
                    break 'req de_fn(input.request().clone()).map_err(EpError::request)?;
                }
            }

            return Err(EpError::Request(error::RequestError::InvalidFormat));
        };

        Ok(req)
    }
}

impl TryFrom<&EndpointRequestInput> for Box<dyn EpRequest> {
    type Error = EpError;
    fn try_from(value: &EndpointRequestInput) -> ResultEP<Self> {
        serde_json::from_value(value.request().to_owned()).map_err(EpError::serde)
    }
}
