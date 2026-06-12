use endpoint_types::Operation;
use error::{EpError, ResultEP};
use postgres_core::{PostgresAsync, PostgresTx};
use telemetry::TelemetryWrapper;

use crate::api::control_plane::RdsApi;

#[derive(Debug, Default, Clone)]
pub struct RdsControlPlaneEp;

impl RdsControlPlaneEp {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(
        &self,
        op: &dyn Operation<PostgresAsync, RdsApi, PostgresTx>,
        context: &PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Box<dyn ep_core::EpOutput>> {
        match op.as_exec() {
            Some(exec) => exec.run_operation_request(context.clone(), telemetry_wrapper.clone()).await,
            None => Err(EpError::database("Operation does not implement OperationExecutor")),
        }
    }
}
