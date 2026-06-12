use std::any::Any;
use std::ops::Deref;

use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpRequest;
use error::{EpError, ParseError, ResultEP};
use format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use format::endpoint::EpKind;
use format::{EndpointUuid, OrganizationUuid};
use proto::proto::EndpointRequest;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub trait RequestSerde {
    fn serde_deserialize<T>(&self, s: &str) -> ResultEP<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(s).map_err(EpError::serde)
    }

    fn serde_serialize<T>(&self) -> ResultEP<String>
    where
        Self: Serialize + RequestDowncast,
        T: 'static + BorshSerialize,
    {
        if let Some(data) = self.downcast_ref::<T>() {
            serde_json::to_string(&self).map_err(EpError::serde)
        } else {
            Err(EpError::Parse(ParseError::FailedToDowncastInput))
        }
    }

    fn borsh_deserialize<T>(data: &[u8]) -> ResultEP<T>
    where
        T: BorshDeserialize,
    {
        borsh::from_slice::<T>(data).map_err(EpError::serde)
    }

    fn borsh_serialize<T>(&self) -> ResultEP<Vec<u8>>
    where
        Self: RequestDowncast,
        T: 'static + BorshSerialize,
    {
        if let Some(data) = self.downcast_ref::<T>() {
            borsh::to_vec(data).map_err(EpError::serde)
        } else {
            Err(EpError::Parse(ParseError::FailedToDowncastInput))
        }
    }
}

impl RequestSerde for dyn EpRequest {}

pub trait RequestDowncast {
    fn downcast_ref<T: 'static>(&self) -> Option<&T>;
}

impl<T: EpRequest + ?Sized> RequestDowncast for T {
    fn downcast_ref<U: 'static>(&self) -> Option<&U> {
        self.as_any().downcast_ref::<U>()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EndpointRequestInput {
    // Store the raw JSON data until we know the endpoint type
    raw_data: Value,
    // Optionally cache the parsed request
    parsed_request: Option<Box<dyn EpRequest>>,
}

// Manual Borsh implementation that only serializes the raw_data
impl BorshSerialize for EndpointRequestInput {
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        // Convert Value to JSON string and serialize it
        let json_string = serde_json::to_string(&self.raw_data).map_err(|e| borsh::io::Error::new(borsh::io::ErrorKind::InvalidData, e))?;
        borsh::BorshSerialize::serialize(&json_string, writer)
    }
}

// Manual Borsh implementation that reconstructs from the raw_data only
impl BorshDeserialize for EndpointRequestInput {
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        // Deserialize the JSON string and convert back to Value
        let json_string = String::deserialize_reader(reader)?;
        let raw_data: Value =
            serde_json::from_str(&json_string).map_err(|e| borsh::io::Error::new(borsh::io::ErrorKind::InvalidData, e))?;

        Ok(EndpointRequestInput {
            raw_data,
            parsed_request: None, // Always start with None, re-parse as needed
        })
    }
}

impl EndpointRequestInput {
    pub fn new(raw_data: Value) -> Self {
        Self { raw_data, parsed_request: None }
    }

    /// Parse the raw JSON into a concrete request type based on the endpoint kind
    pub fn parse_with_kind(&mut self, kind: EpKind) -> Result<&mut dyn EpRequest, EpError> {
        match &mut self.parsed_request {
            Some(request) => Ok(&*request),
            None => {
                let request = Self::create_request_from_value(self.raw_data.clone(), kind).map_err(EpError::parse)?;
                Ok(request)
            }
        }
    }

    /// Get the parsed request, assuming it was already parsed
    pub fn request(&self) -> &Option<Box<dyn EpRequest>> {
        &self.parsed_request
    }

    /// Create a new EndpointRequestInput from JSON and immediately parse it
    pub fn from_json_with_kind(json_str: &str, kind: EpKind) -> Result<Self, Box<dyn std::error::Error>> {
        let raw_data: Value = serde_json::from_str(json_str)?;
        let request = Self::create_request_from_value(raw_data.clone(), kind)?;
        Ok(Self { raw_data, parsed_request: Some(request) })
    }

    /// Create a new EndpointRequestInput from a Value and immediately parse it
    pub fn from_value_with_kind(raw_data: Value, kind: EpKind) -> Result<Self, Box<dyn std::error::Error>> {
        let request = Self::create_request_from_value(raw_data.clone(), kind)?;
        Ok(Self { raw_data, parsed_request: Some(request) })
    }

    /// Get the raw JSON data
    pub fn raw_data(&self) -> &Value {
        &self.raw_data
    }

    fn create_request_from_value(json_data: Value, kind: EpKind) -> Result<Box<dyn EpRequest>, Box<dyn std::error::Error>> {
        match kind {
            EpKind::Cassandra => {
                let req: CassandraRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Clickhouse => {
                let req: ClickhouseRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Http => {
                let req: HttpRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Mongo => {
                let req: MongoRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Mysql => {
                let req: MysqlRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Mssql => {
                let req: MssqlRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Redis => {
                let req: RedisRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Postgres => {
                let req: PostgresRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Pinecone => {
                let req: PineconeRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }

            EpKind::Oracle => {
                let req: OracleRequest = serde_json::from_value(json_data)?;
                Ok(Box::new(req))
            }
        }
    }
}

// Custom deserializer that just stores the raw JSON
impl<'de> Deserialize<'de> for EndpointRequestInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_data = Value::deserialize(deserializer)?;
        Ok(EndpointRequestInput::new(raw_data))
    }
}

/// Data structure for endpoint requests
#[derive(Debug, Clone)]
pub struct EndpointRequestData {
    org_cache_key: OrganizationCacheUuid,
    request: Vec<u8>,
    endpoint_uuid: EndpointUuid,
}

impl EndpointRequestData {
    pub fn new(org_cache_key: OrganizationCacheUuid, request: Vec<u8>, endpoint_uuid: EndpointUuid) -> Self {
        Self { org_cache_key, request, endpoint_uuid }
    }
    pub fn org_cache_key(&self) -> &OrganizationCacheUuid {
        &self.org_cache_key
    }
    pub fn request(&self) -> &Vec<u8> {
        &self.request
    }
    pub fn endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }
}

impl TryFrom<EndpointRequest> for EndpointRequestData {
    type Error = EpError;

    fn try_from(value: EndpointRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            org_cache_key: OrganizationCacheUuid::new(
                None,
                OrganizationUuid::from(Uuid::from_slice(&value.org_uuid).map_err(EpError::parse)?),
            ),
            request: value.request,
            endpoint_uuid: EndpointUuid::from(Uuid::from_slice(&value.endpoint_uuid).map_err(EpError::parse)?),
        })
    }
}
#[cfg(test)]
mod tests {
    use crate::request::EndpointRequestInput;

    #[test]
    fn deserialize_redis_request() {
        const REQ: &str = r#"{
            "kind": "Redis",
            "type": "gEt",
            "key": "rnd",
            "value": 5
        }"#;
        serde_json::from_str::<EndpointRequestInput>(&REQ).unwrap_or_default();
    }

    #[test]
    fn deserialize_mongo_request() {
        const REQ: &str = r#"{
            "kind": "Mongo",
            "type": "database_collection_aggregate",
            "database": "sample_mflix",
            "collection": "movies",
            "pipeline": [
                {
                    "$group": {
                        "_id": "$title"
                    }
                },
                {
                    "$limit": 3
                }
            ],
            "options": null
        }"#;
        serde_json::from_str::<EndpointRequestInput>(&REQ);
    }
}
