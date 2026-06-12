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

const API_INFO: ApiInfo<AwsApi, LightsailCreateInstancesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::LightsailCreateInstances, "lightsail_create_instances", ReqType::Write, true);

crate::aws_endpoint! {
    LightsailCreateInstances,
    API_INFO,
    struct {
        instance_names: Vec<String>,
        availability_zone: String,
        blueprint_id: String,
        bundle_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "instanceNames": self.instance_names,
            "availabilityZone": self.availability_zone,
            "blueprintId": self.blueprint_id,
            "bundleId": self.bundle_id
        });
        let result = client.execute_json_target("lightsail", "Lightsail_20161128.CreateInstances", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws lightsail", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LightsailCreateInstancesInputBuilder::default()
            .instance_names(vec!["inst".to_string()])
            .availability_zone("us-east-1a")
            .blueprint_id("amazon_linux_2")
            .bundle_id("nano_2_0")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lightsail_create_instances");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "instance_names": ["inst"],
            "availability_zone": "us-east-1a",
            "blueprint_id": "amazon_linux_2",
            "bundle_id": "nano_2_0"
        });
        let _: LightsailCreateInstancesInput = serde_json::from_value(json).unwrap();
    }
}
