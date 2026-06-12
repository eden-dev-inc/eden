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

const API_INFO: ApiInfo<AwsApi, DynamoDbCreateGlobalTableInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbCreateGlobalTable,
    "Creates a DynamoDB global table",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbCreateGlobalTable,
    API_INFO,
    struct {
        global_table_name: String,
        replication_group: Vec<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "GlobalTableName": self.global_table_name,
            "ReplicationGroup": self.replication_group
        });
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.CreateGlobalTable", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbCreateGlobalTableInputBuilder::default()
            .global_table_name("gt")
            .replication_group(vec![serde_json::json!({"RegionName": "us-east-1"})])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_create_global_table");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"global_table_name": "gt", "replication_group": [{"RegionName": "us-east-1"}]});
        let _: DynamoDbCreateGlobalTableInput = serde_json::from_value(json).unwrap();
    }
}
