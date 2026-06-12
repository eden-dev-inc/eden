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

const API_INFO: ApiInfo<AwsApi, DynamoDbUpdateTableInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbUpdateTable,
    "Modifies the provisioned throughput settings, global secondary indexes, or DynamoDB Streams settings for a given table",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbUpdateTable,
    API_INFO,
    struct {
        table_name: String,
        attribute_definitions: Option<serde_json::Value>,
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

        let mut body = serde_json::Map::new();
        body.insert("TableName".to_string(), serde_json::json!(self.table_name));
        if let Some(ad) = &self.attribute_definitions {
            body.insert("AttributeDefinitions".to_string(), ad.clone());
        }
        if let Some(bm) = &self.billing_mode {
            body.insert("BillingMode".to_string(), serde_json::json!(bm));
        }
        if let Some(pt) = &self.provisioned_throughput {
            body.insert("ProvisionedThroughput".to_string(), pt.clone());
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.UpdateTable", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbUpdateTableInputBuilder::default().table_name("t").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_update_table");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"table_name": "t"});
        let _: DynamoDbUpdateTableInput = serde_json::from_value(json).unwrap();
    }
}
