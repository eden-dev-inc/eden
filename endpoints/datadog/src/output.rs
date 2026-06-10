use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct DatadogJsonOutput(pub Value);

impl ToOutput for DatadogJsonOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Datadog, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        serde_json::to_vec(&self.0).map(bytes::Bytes::from).map_err(EpError::serde)
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(self.0.clone())
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
}

#[derive(Debug, Deserialize)]
pub struct DatadogEmptyOutput;

impl ToOutput for DatadogEmptyOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Datadog, EndpointResponse::ok("success"))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(Value::Null)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
}

#[derive(Debug, Deserialize)]
pub struct DatadogListOutput(pub Vec<Value>);

impl ToOutput for DatadogListOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Datadog, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        serde_json::to_vec(&self.0).map(bytes::Bytes::from).map_err(EpError::serde)
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(&self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
}

#[derive(Debug, Deserialize)]
pub struct DatadogBoolOutput(pub bool);

impl ToOutput for DatadogBoolOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Datadog, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self.0).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }
}
