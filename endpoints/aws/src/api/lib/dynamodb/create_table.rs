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

const API_INFO: ApiInfo<AwsApi, DynamoDbCreateTableInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DynamoDbCreateTable, "dynamodb_create_table", ReqType::Write, true);

crate::aws_endpoint! {
    DynamoDbCreateTable,
    API_INFO,
    struct {
        table_name: String,
        attribute_definitions: Vec<serde_json::Value>,
        key_schema: Vec<serde_json::Value>,
        billing_mode: Option<String>,
        provisioned_throughput: Option<serde_json::Value>
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
            "AttributeDefinitions": self.attribute_definitions,
            "KeySchema": self.key_schema
        });
        if let Some(bm) = &self.billing_mode {
            body["BillingMode"] = serde_json::Value::String(bm.clone());
        }
        if let Some(pt) = &self.provisioned_throughput {
            body["ProvisionedThroughput"] = pt.clone();
        }
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.CreateTable", Some(&body), "1.0").await?;

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
        let input = DynamoDbCreateTableInputBuilder::default()
            .table_name("t")
            .attribute_definitions(vec![serde_json::json!({"AttributeName": "id", "AttributeType": "S"})])
            .key_schema(vec![serde_json::json!({"AttributeName": "id", "KeyType": "HASH"})])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_create_table");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "table_name": "t",
            "attribute_definitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "key_schema": [{"AttributeName": "id", "KeyType": "HASH"}]
        });
        let _: DynamoDbCreateTableInput = serde_json::from_value(json).unwrap();
    }
}
