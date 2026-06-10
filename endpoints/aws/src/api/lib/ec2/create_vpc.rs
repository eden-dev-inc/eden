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

const API_INFO: ApiInfo<AwsApi, Ec2CreateVpcInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2CreateVpc, "ec2_create_vpc", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2CreateVpc,
    API_INFO,
    struct {
        cidr_block: String,
        instance_tenancy: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("CidrBlock".to_string(), self.cidr_block.clone());
        if let Some(tenancy) = &self.instance_tenancy {
            params.insert("InstanceTenancy".to_string(), tenancy.clone());
        }
        let form_body = build_query_body("CreateVpc", "2016-11-15", &params);
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
        let input = Ec2CreateVpcInputBuilder::default().cidr_block("10.0.0.0/16").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_create_vpc");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cidr_block": "10.0.0.0/16"});
        let _: Ec2CreateVpcInput = serde_json::from_value(json).unwrap();
    }
}
