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

const API_INFO: ApiInfo<AwsApi, DynamoDbBatchGetItemInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbBatchGetItem,
    "Returns the attributes of one or more items from one or more tables",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    DynamoDbBatchGetItem,
    API_INFO,
    struct {
        request_items: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"RequestItems": self.request_items});
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.BatchGetItem", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbBatchGetItemInputBuilder::default()
            .request_items(serde_json::json!({"Table1": {"Keys": [{"id": {"S": "1"}}]}}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_batch_get_item");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"request_items": {"Table1": {"Keys": [{"id": {"S": "1"}}]}}});
        let _: DynamoDbBatchGetItemInput = serde_json::from_value(json).unwrap();
    }
}
