use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, Ec2RequestSpotInstancesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2RequestSpotInstances, "ec2_request_spot_instances", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2RequestSpotInstances,
    API_INFO,
    struct {
        spot_price: String,
        instance_count: Option<i64>,
        type_field: Option<String>,
        launch_specification: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("SpotPrice".to_string(), self.spot_price.clone());
        if let Some(v) = self.instance_count {
            params.insert("InstanceCount".to_string(), v.to_string());
        }
        if let Some(v) = &self.type_field {
            params.insert("Type".to_string(), v.clone());
        }
        if let Some(v) = &self.launch_specification {
            params.insert("LaunchSpecification".to_string(), serde_json::to_string(v).unwrap_or_default());
        }
        let form_body = build_query_body("RequestSpotInstances", "2016-11-15", &params);
        let result = client.execute_form("ec2", &form_body).await?;

        span.add_event("received result from aws ec2", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = Ec2RequestSpotInstancesInputBuilder::default().spot_price("0.05").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_request_spot_instances");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"spot_price": "0.05"});
        let _: Ec2RequestSpotInstancesInput = serde_json::from_value(json).unwrap();
    }
}
