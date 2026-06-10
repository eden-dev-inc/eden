use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, GameLiftDeleteFleetInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::GameLiftDeleteFleet, "gamelift_delete_fleet", ReqType::Write, true);

crate::aws_endpoint! {
    GameLiftDeleteFleet,
    API_INFO,
    struct {
        fleet_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({ "FleetId": self.fleet_id });
        let result = client.execute_json_target("gamelift", "GameLift_20150910.DeleteFleet", Some(&body), "1.1").await?;

        span.add_event("received result from aws gamelift", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = GameLiftDeleteFleetInputBuilder::default().fleet_id("fleet-abc123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "gamelift_delete_fleet");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({ "fleet_id": "fleet-abc123" });
        let _: GameLiftDeleteFleetInput = serde_json::from_value(json).unwrap();
    }
}
