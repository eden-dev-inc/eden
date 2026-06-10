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

const API_INFO: ApiInfo<AwsApi, BackupListBackupPlansInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::BackupListBackupPlans, "backup_list_backup_plans", ReqType::Read, true);

crate::aws_endpoint! {
    BackupListBackupPlans,
    API_INFO,
    struct {
        next_token: Option<String>,
        max_results: Option<i64>,
        include_deleted: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute("backup", "GET", "/backup/plans/", None, None, None).await?;

        span.add_event("received result from aws backup", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = BackupListBackupPlansInputBuilder::default()
            .next_token(None::<String>)
            .max_results(None::<i64>)
            .include_deleted(None::<bool>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "backup_list_backup_plans");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: BackupListBackupPlansInput = serde_json::from_value(json).unwrap();
    }
}
