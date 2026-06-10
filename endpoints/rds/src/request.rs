use std::any::Any;
use std::io::{self, Read};

use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::{EndpointOperation, EpRequest, Operation, OperationExecutor, RequestConstructor, RunOutput, RunRequest};
use ep_core::define_request_serializer_stuff;
use ep_core::settings::EdenSettings;
use ep_core::{EndpointType, ReqType};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use postgres::request::PostgresRequest;
use postgres::serde::PostgresOperation;
use postgres_core::{PostgresAsync, PostgresTx};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use telemetry::TelemetryWrapper;

use crate::api::control_plane::RdsApi;
use crate::control_plane_ep::RdsControlPlaneEp;
use crate::serde::RdsOperation;

#[derive(Debug)]
pub struct RdsRequest(pub Box<dyn RdsOperation>);

#[derive(Debug)]
pub struct RdsPostgresOperation {
    inner: Box<dyn PostgresOperation>,
}

impl RdsPostgresOperation {
    pub fn new(inner: Box<dyn PostgresOperation>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &dyn PostgresOperation {
        &*self.inner
    }

    pub fn inner_clone(&self) -> Box<dyn PostgresOperation> {
        let value = serde_json::to_value(&self.inner).expect("serialize postgres operation");
        serde_json::from_value(value).expect("deserialize postgres operation")
    }
}

impl From<Box<dyn PostgresOperation>> for RdsRequest {
    fn from(op: Box<dyn PostgresOperation>) -> Self {
        RdsRequest(Box::new(RdsPostgresOperation::new(op)))
    }
}

impl From<PostgresRequest> for RdsRequest {
    fn from(req: PostgresRequest) -> Self {
        req.0.into()
    }
}

impl RdsRequest {
    pub fn postgres_operation(&self) -> Option<&RdsPostgresOperation> {
        self.0.as_any().downcast_ref::<RdsPostgresOperation>()
    }

    pub fn as_postgres_request(&self) -> Option<PostgresRequest> {
        self.postgres_operation().map(|op| PostgresRequest(op.inner_clone()))
    }
}

impl RequestConstructor for RdsRequest {
    type AsyncType = PostgresAsync;
    type ApiKindType = RdsApi;
    type TxType = PostgresTx;
    type OperationType = dyn RdsOperation;

    fn new(op: Box<Self::OperationType>) -> Self {
        Self(op)
    }
}

impl EndpointType for RdsRequest {
    fn r#type() -> EpKind {
        EpKind::Rds
    }
}

impl RunRequest<PostgresAsync, RdsApi, PostgresTx> for RdsRequest {
    fn operation(&self) -> &dyn Operation<PostgresAsync, RdsApi, PostgresTx> {
        &*self.0
    }

    async fn run_request(
        &self,
        context: PostgresAsync,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Box<dyn ep_core::EpOutput>> {
        match self.operation().kind() {
            RdsApi::Postgres => match self.operation().as_exec() {
                Some(exec) => exec.run_operation_request(context, telemetry_wrapper.clone()).await,
                None => Err(EpError::database("Operation does not implement OperationExecutor")),
            },
            _ => {
                let control_plane = RdsControlPlaneEp::new();
                control_plane.run(self.operation(), &context, telemetry_wrapper).await
            }
        }
    }
}

impl EpRequest for RdsRequest {
    fn kind(&self) -> EpKind {
        EpKind::Rds
    }

    fn as_request(self: Box<Self>) -> Box<dyn EpRequest> {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_value(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        struct WriteWrapper<'a>(&'a mut dyn io::Write);

        impl<'a> io::Write for WriteWrapper<'a> {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.write(buf)
            }

            fn flush(&mut self) -> io::Result<()> {
                self.0.flush()
            }
        }

        BorshSerialize::serialize(self, &mut WriteWrapper(writer))
    }
}

impl Serialize for RdsRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(pg) = self.postgres_operation() {
            return Serialize::serialize(&PostgresRequest(pg.inner_clone()), serializer);
        }

        Serialize::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for RdsRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;

        if let Ok(pg_req) = serde_json::from_value::<PostgresRequest>(value.clone()) {
            return Ok(pg_req.into());
        }

        let op: Box<dyn RdsOperation> = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(RdsRequest(op))
    }
}

impl BorshSerialize for RdsRequest {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        if let Some(pg) = self.postgres_operation() {
            return BorshSerialize::serialize(&PostgresRequest(pg.inner_clone()), writer);
        }

        BorshSerialize::serialize(&self.0, writer)
    }
}

impl BorshDeserialize for RdsRequest {
    fn deserialize(buf: &mut &[u8]) -> io::Result<Self> {
        let buffer = buf.to_vec();

        if let Ok(pg_req) = borsh::from_slice::<PostgresRequest>(&buffer) {
            *buf = &[];
            return Ok(pg_req.into());
        }

        let op: Box<dyn RdsOperation> = borsh::from_slice(&buffer)?;
        *buf = &[];
        Ok(RdsRequest(op))
    }

    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        let mut slice = buffer.as_slice();
        BorshDeserialize::deserialize(&mut slice)
    }
}

impl Operation<PostgresAsync, RdsApi, PostgresTx> for RdsPostgresOperation {
    fn kind(&self) -> RdsApi {
        RdsApi::Postgres
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn request_type(&self) -> ReqType {
        self.inner.request_type()
    }

    fn as_operation(self: Box<Self>) -> Box<dyn Operation<PostgresAsync, RdsApi, PostgresTx>> {
        self
    }

    fn as_exec(&self) -> Option<&dyn OperationExecutor<PostgresAsync, RdsApi, PostgresTx>> {
        Some(self)
    }

    fn clone_box(&self) -> Box<dyn Operation<PostgresAsync, RdsApi, PostgresTx>> {
        Box::new(Self { inner: self.inner_clone() })
    }
}

impl EndpointOperation for RdsPostgresOperation {}

impl OperationExecutor<PostgresAsync, RdsApi, PostgresTx> for RdsPostgresOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn run_operation_request(&self, context: PostgresAsync, telemetry_wrapper: TelemetryWrapper) -> RunOutput<'_> {
        match self.inner.as_exec() {
            Some(exec) => exec.run_operation_request(context, telemetry_wrapper),
            None => Box::pin(async { Err(EpError::database("Operation does not implement OperationExecutor")) }),
        }
    }

    fn run_operation_transaction(&self, tx_context: &mut PostgresTx, telemetry_wrapper: &mut TelemetryWrapper) {
        if let Some(exec) = self.inner.as_exec() {
            exec.run_operation_transaction(tx_context, telemetry_wrapper);
        }
    }
}

define_request_serializer_stuff!(EpKind::Rds => RdsRequest);
