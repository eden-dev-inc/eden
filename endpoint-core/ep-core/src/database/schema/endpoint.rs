use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EpRequestWrapper(pub Value);

impl From<Value> for EpRequestWrapper {
    fn from(value: Value) -> Self {
        EpRequestWrapper(value)
    }
}

impl BorshSerialize for EpRequestWrapper {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&self.0).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        BorshSerialize::serialize(&bytes, writer)
    }
}

impl BorshDeserialize for EpRequestWrapper {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let bytes: Vec<u8> = BorshDeserialize::deserialize_reader(reader)?;
        let value = serde_json::from_slice(&bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(EpRequestWrapper(value))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, BorshSerialize, BorshDeserialize)]
pub struct EndpointRequestInput {
    pub request: EpRequestWrapper,
}

impl EndpointRequestInput {
    pub fn new(request: impl Into<EpRequestWrapper>) -> Self {
        Self { request: request.into() }
    }

    /// Get the raw JSON data
    pub fn request(&self) -> &Value {
        &self.request.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct EndpointTransactionInput {
    pub request: EpRequestWrapper,
}

impl EndpointTransactionInput {
    pub fn new(raw_data: Value) -> Self {
        Self { request: EpRequestWrapper(raw_data) }
    }
    pub fn request(&self) -> &Value {
        &self.request.0
    }
}
