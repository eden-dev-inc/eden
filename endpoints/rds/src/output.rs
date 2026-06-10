use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RdsControlPlaneOutput(pub Value);

impl ToOutput for RdsControlPlaneOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Rds, EndpointResponse::Response(self))
    }

    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }

    fn try_serde_serialize(&self) -> ResultEP<Value> {
        Ok(self.0.to_owned())
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for RDS control-plane"))
    }
}
