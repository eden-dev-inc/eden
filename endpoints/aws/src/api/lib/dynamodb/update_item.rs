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

const API_INFO: ApiInfo<AwsApi, DynamoDbUpdateItemInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DynamoDbUpdateItem, "dynamodb_update_item", ReqType::Write, true);

crate::aws_endpoint! {
    DynamoDbUpdateItem,
    API_INFO,
    struct {
        table_name: String,
        key: serde_json::Value,
        update_expression: Option<String>,
        expression_attribute_names: Option<serde_json::Value>,
        expression_attribute_values: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::json!({
            "TableName": self.table_name,
            "Key": self.key
        });
        if let Some(ue) = &self.update_expression {
            body["UpdateExpression"] = serde_json::Value::String(ue.clone());
        }
        if let Some(ean) = &self.expression_attribute_names {
            body["ExpressionAttributeNames"] = ean.clone();
        }
        if let Some(eav) = &self.expression_attribute_values {
            body["ExpressionAttributeValues"] = eav.clone();
        }
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.UpdateItem", Some(&body), "1.0").await?;

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
        let input = DynamoDbUpdateItemInputBuilder::default().table_name("t").key(serde_json::json!({"id": {"S": "1"}})).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_update_item");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"table_name": "t", "key": {"id": {"S": "1"}}});
        let _: DynamoDbUpdateItemInput = serde_json::from_value(json).unwrap();
    }
}
