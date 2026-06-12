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

const API_INFO: ApiInfo<AwsApi, DynamoDbRestoreTableFromBackupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DynamoDbRestoreTableFromBackup,
    "Restores a DynamoDB table from a backup",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DynamoDbRestoreTableFromBackup,
    API_INFO,
    struct {
        target_table_name: String,
        backup_arn: String,
        billing_mode_override: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_val = serde_json::json!({
            "TargetTableName": self.target_table_name,
            "BackupArn": self.backup_arn
        });
        if let Some(b) = &self.billing_mode_override {
            body_val["BillingModeOverride"] = Value::String(b.clone());
        }
        let result = client.execute_json_target("dynamodb", "DynamoDB_20120810.RestoreTableFromBackup", Some(&body_val), "1.0").await?;

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
        let input = DynamoDbRestoreTableFromBackupInputBuilder::default().target_table_name("t").backup_arn("arn").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "dynamodb_restore_table_from_backup");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"target_table_name": "t", "backup_arn": "arn"});
        let _: DynamoDbRestoreTableFromBackupInput = serde_json::from_value(json).unwrap();
    }
}
