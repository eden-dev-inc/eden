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

const API_INFO: ApiInfo<AwsApi, DynamoDbRestoreTableToPointInTimeInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbRestoreTableToPointInTime,
    "Restores a DynamoDB table to a point in time",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbRestoreTableToPointInTime,
    API_INFO,
    struct {
        source_table_name: String,
        target_table_name: String,
        restore_date_time: Option<String>,
        use_latest_restorable_time: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_val = serde_json::json!({
            "SourceTableName": self.source_table_name,
            "TargetTableName": self.target_table_name
        });
        if let Some(dt) = &self.restore_date_time {
            body_val["RestoreDateTime"] = Value::String(dt.clone());
        }
        if let Some(u) = &self.use_latest_restorable_time {
            body_val["UseLatestRestorableTime"] = serde_json::json!(u);
        }
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.RestoreTableToPointInTime", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbRestoreTableToPointInTimeInputBuilder::default()
            .source_table_name("src")
            .target_table_name("tgt")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_restore_table_to_point_in_time");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"source_table_name": "src", "target_table_name": "tgt"});
        let _: DynamoDbRestoreTableToPointInTimeInput = serde_json::from_value(json).unwrap();
    }
}
