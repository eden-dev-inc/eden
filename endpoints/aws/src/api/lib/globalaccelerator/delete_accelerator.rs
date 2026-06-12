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

const API_INFO: ApiInfo<AwsApi, GlobalAcceleratorDeleteAcceleratorInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::GlobalAcceleratorDeleteAccelerator,
    "Delete an accelerator",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    GlobalAcceleratorDeleteAccelerator,
    API_INFO,
    struct {
        accelerator_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"AcceleratorArn": self.accelerator_arn});
        let result = client
            .execute_json_target("globalaccelerator", "GlobalAccelerator_V20180706.DeleteAccelerator", Some(&body_val), "1.1")
            .await?;

        span.add_event(
            "received result from aws globalaccelerator",
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
        let input = GlobalAcceleratorDeleteAcceleratorInputBuilder::default()
            .accelerator_arn("arn:aws:globalaccelerator::123456789012:accelerator/acc")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "globalaccelerator_delete_accelerator");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"accelerator_arn": "arn:aws:globalaccelerator::123456789012:accelerator/acc"});
        let _: GlobalAcceleratorDeleteAcceleratorInput = serde_json::from_value(json).unwrap();
    }
}
