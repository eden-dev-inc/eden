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

const API_INFO: ApiInfo<AwsApi, DynamoDbScanInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbScan,
    "Returns one or more items by accessing every item in a DynamoDB table",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    DynamoDbScan,
    API_INFO,
    struct {
        table_name: String,
        filter_expression: Option<String>,
        expression_attribute_names: Option<serde_json::Value>,
        expression_attribute_values: Option<serde_json::Value>,
        limit: Option<i64>,
        exclusive_start_key: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("TableName".to_string(), Value::String(self.table_name.clone()));
        if let Some(fe) = &self.filter_expression {
            body.insert("FilterExpression".to_string(), Value::String(fe.clone()));
        }
        if let Some(n) = &self.expression_attribute_names {
            body.insert("ExpressionAttributeNames".to_string(), n.clone());
        }
        if let Some(v) = &self.expression_attribute_values {
            body.insert("ExpressionAttributeValues".to_string(), v.clone());
        }
        if let Some(l) = self.limit {
            body.insert("Limit".to_string(), serde_json::json!(l));
        }
        if let Some(sk) = &self.exclusive_start_key {
            body.insert("ExclusiveStartKey".to_string(), sk.clone());
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.Scan", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbScanInputBuilder::default().table_name("t").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_scan");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"table_name": "t"});
        let _: DynamoDbScanInput = serde_json::from_value(json).unwrap();
    }
}
