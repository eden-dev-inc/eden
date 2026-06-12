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

const API_INFO: ApiInfo<AwsApi, DynamoDbTransactWriteItemsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbTransactWriteItems,
    "Synchronous write operation that groups up to 100 action requests",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbTransactWriteItems,
    API_INFO,
    struct {
        transact_items: serde_json::Value,
        client_request_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("TransactItems".to_string(), self.transact_items.clone());
        if let Some(token) = &self.client_request_token {
            body.insert("ClientRequestToken".to_string(), serde_json::json!(token));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.TransactWriteItems", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbTransactWriteItemsInputBuilder::default()
            .transact_items(serde_json::json!([{"Put": {"TableName": "t", "Item": {"id": {"S": "1"}}}}]))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_transact_write_items");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"transact_items": [{"Put": {"TableName": "t", "Item": {"id": {"S": "1"}}}}]});
        let _: DynamoDbTransactWriteItemsInput = serde_json::from_value(json).unwrap();
    }
}
