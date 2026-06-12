use endpoint_types::Operation;
use error::{EpError, ResultEP};
use redis_core::{RedisAsync, RedisTx};
use telemetry::TelemetryWrapper;

use crate::api::control_plane::ElasticacheApi;

#[derive(Debug, Default, Clone)]
pub struct ElasticacheControlPlaneEp;

impl ElasticacheControlPlaneEp {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(
        &self,
        op: &dyn Operation<RedisAsync, ElasticacheApi, RedisTx>,
        context: &RedisAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Box<dyn ep_core::EpOutput>> {
        match op.as_exec() {
            Some(exec) => exec.run_operation_request(context.clone(), telemetry_wrapper.clone()).await,
            None => Err(EpError::database("Operation does not implement OperationExecutor")),
        }
    }
}
