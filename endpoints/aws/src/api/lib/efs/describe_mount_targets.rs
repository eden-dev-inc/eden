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

const API_INFO: ApiInfo<AwsApi, EfsDescribeMountTargetsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EfsDescribeMountTargets, "efs_describe_mount_targets", ReqType::Read, true);

crate::aws_endpoint! {
    EfsDescribeMountTargets,
    API_INFO,
    struct {
        file_system_id: Option<String>,
        mount_target_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute("elasticfilesystem", "GET", "/2015-02-01/mount-targets", None, None, None).await?;

        span.add_event(
            "received result from aws elasticfilesystem",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = EfsDescribeMountTargetsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "efs_describe_mount_targets");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: EfsDescribeMountTargetsInput = serde_json::from_value(json).unwrap();
    }
}
