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

const API_INFO: ApiInfo<AwsApi, DynamoDbExportTableToPointInTimeInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbExportTableToPointInTime,
    "Exports a DynamoDB table to an S3 bucket",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbExportTableToPointInTime,
    API_INFO,
    struct {
        table_arn: String,
        s3_bucket: String,
        s3_prefix: Option<String>,
        export_format: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_val = serde_json::json!({
            "TableArn": self.table_arn,
            "S3Bucket": self.s3_bucket
        });
        if let Some(p) = &self.s3_prefix {
            body_val["S3Prefix"] = Value::String(p.clone());
        }
        if let Some(f) = &self.export_format {
            body_val["ExportFormat"] = Value::String(f.clone());
        }
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.ExportTableToPointInTime", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbExportTableToPointInTimeInputBuilder::default()
            .table_arn("arn:aws:dynamodb:us-east-1:123:table/t")
            .s3_bucket("my-bucket")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_export_table_to_point_in_time");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"table_arn": "arn", "s3_bucket": "bucket"});
        let _: DynamoDbExportTableToPointInTimeInput = serde_json::from_value(json).unwrap();
    }
}
