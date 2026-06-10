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

const API_INFO: ApiInfo<AwsApi, NetworkFirewallCreateFirewallInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::NetworkFirewallCreateFirewall,
    "Creates an AWS Network Firewall firewall instance",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    NetworkFirewallCreateFirewall,
    API_INFO,
    struct {
        firewall_name: String,
        firewall_policy_arn: String,
        vpc_id: String,
        subnet_mappings: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"FirewallName": self.firewall_name, "FirewallPolicyArn": self.firewall_policy_arn, "VpcId": self.vpc_id, "SubnetMappings": self.subnet_mappings});
        let result = client
            .execute_json_target("network-firewall", "NetworkFirewall_20201112.CreateFirewall", Some(&body_val), "1.1")
            .await?;

        span.add_event(
            "received result from aws network-firewall",
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
        let input = NetworkFirewallCreateFirewallInputBuilder::default()
            .firewall_name("fw")
            .firewall_policy_arn("arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/fp")
            .vpc_id("vpc-123")
            .subnet_mappings(serde_json::json!([{"SubnetId": "subnet-123"}]))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "networkfirewall_create_firewall");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"firewall_name": "fw", "firewall_policy_arn": "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/fp", "vpc_id": "vpc-123", "subnet_mappings": [{"SubnetId": "subnet-123"}]});
        let _: NetworkFirewallCreateFirewallInput = serde_json::from_value(json).unwrap();
    }
}
