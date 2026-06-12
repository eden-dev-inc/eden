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

const API_INFO: ApiInfo<AwsApi, DynamoDbUpdateTimeToLiveInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbUpdateTimeToLive,
    "Enables or disables Time to Live (TTL) for the specified table",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbUpdateTimeToLive,
    API_INFO,
    struct {
        table_name: String,
        time_to_live_specification: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"TableName": self.table_name, "TimeToLiveSpecification": self.time_to_live_specification});
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.UpdateTimeToLive", Some(&body_val), "1.0").await?;

        span.add_event("received result from aws dynamodb", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = DynamoDbUpdateTimeToLiveInputBuilder::default()
            .table_name("t")
            .time_to_live_specification(serde_json::json!({"Enabled": true, "AttributeName": "ttl"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_update_time_to_live");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"table_name": "t", "time_to_live_specification": {"Enabled": true, "AttributeName": "ttl"}});
        let _: DynamoDbUpdateTimeToLiveInput = serde_json::from_value(json).unwrap();
    }
}
