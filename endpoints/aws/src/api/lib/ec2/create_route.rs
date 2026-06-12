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

const API_INFO: ApiInfo<AwsApi, Ec2CreateRouteInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2CreateRoute, "ec2_create_route", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2CreateRoute,
    API_INFO,
    struct {
        route_table_id: String,
        destination_cidr_block: Option<String>,
        gateway_id: Option<String>,
        nat_gateway_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("RouteTableId".to_string(), self.route_table_id.clone());
        if let Some(cidr) = &self.destination_cidr_block {
            params.insert("DestinationCidrBlock".to_string(), cidr.clone());
        }
        if let Some(gw_id) = &self.gateway_id {
            params.insert("GatewayId".to_string(), gw_id.clone());
        }
        if let Some(nat_gw_id) = &self.nat_gateway_id {
            params.insert("NatGatewayId".to_string(), nat_gw_id.clone());
        }
        let form_body = build_query_body("CreateRoute", "2016-11-15", &params);
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
        let input = Ec2CreateRouteInputBuilder::default().route_table_id("rtb-12345").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_create_route");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"route_table_id": "rtb-12345"});
        let _: Ec2CreateRouteInput = serde_json::from_value(json).unwrap();
    }
}
