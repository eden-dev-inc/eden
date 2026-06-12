use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, DynamoDbGetItemInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbGetItem,
    "Returns a set of attributes for the item with the given primary key",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    DynamoDbGetItem,
    API_INFO,
    struct {
        table_name: String,
        key: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"TableName": self.table_name, "Key": self.key});
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.GetItem", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbGetItemInputBuilder::default()
            .table_name("t")
            .key(serde_json::json!({"id": {"S": "1"}}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_get_item");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"table_name": "t", "key": {"id": {"S": "1"}}});
        let _: DynamoDbGetItemInput = serde_json::from_value(json).unwrap();
    }
}
