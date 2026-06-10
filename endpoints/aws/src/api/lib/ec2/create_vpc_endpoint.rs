use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
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

const API_INFO: ApiInfo<AwsApi, Ec2CreateVpcEndpointInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2CreateVpcEndpoint, "ec2_create_vpc_endpoint", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2CreateVpcEndpoint,
    API_INFO,
    struct {
        vpc_id: String,
        service_name: String,
        vpc_endpoint_type: Option<String>,
        route_table_ids: Option<Vec<String>>,
        subnet_ids: Option<Vec<String>>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("VpcId".to_string(), self.vpc_id.clone());
        params.insert("ServiceName".to_string(), self.service_name.clone());
        if let Some(v) = &self.vpc_endpoint_type {
            params.insert("VpcEndpointType".to_string(), v.clone());
        }
        if let Some(ids) = &self.route_table_ids {
            params.extend(indexed_list_params("RouteTableId", ids));
        }
        if let Some(ids) = &self.subnet_ids {
            params.extend(indexed_list_params("SubnetId", ids));
        }
        let form_body = build_query_body("CreateVpcEndpoint", "2016-11-15", &params);
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
        let input = Ec2CreateVpcEndpointInputBuilder::default()
            .vpc_id("vpc-123")
            .service_name("com.amazonaws.us-east-1.s3")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_create_vpc_endpoint");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"vpc_id": "vpc-123", "service_name": "com.amazonaws.us-east-1.s3"});
        let _: Ec2CreateVpcEndpointInput = serde_json::from_value(json).unwrap();
    }
}
