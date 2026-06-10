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

const API_INFO: ApiInfo<AwsApi, EfsCreateMountTargetInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EfsCreateMountTarget, "efs_create_mount_target", ReqType::Write, true);

crate::aws_endpoint! {
    EfsCreateMountTarget,
    API_INFO,
    struct {
        file_system_id: String,
        subnet_id: String,
        security_groups: Option<Vec<String>>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"FileSystemId": self.file_system_id, "SubnetId": self.subnet_id});
        let result = client.execute("elasticfilesystem", "POST", "/2015-02-01/mount-targets", None, Some(&body_val), None).await?;

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
        let input = EfsCreateMountTargetInputBuilder::default()
            .file_system_id("fs-12345678")
            .subnet_id("subnet-12345678")
            .security_groups(None::<Vec<String>>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "efs_create_mount_target");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"file_system_id": "fs-12345678", "subnet_id": "subnet-12345678"});
        let _: EfsCreateMountTargetInput = serde_json::from_value(json).unwrap();
    }
}
