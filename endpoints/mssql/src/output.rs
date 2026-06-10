use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct MssqlJsonOutput(pub Value);

impl ToOutput for MssqlJsonOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Mssql, EndpointResponse::Response(self))
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
