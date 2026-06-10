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

const API_INFO: ApiInfo<AwsApi, NetworkFirewallDescribeFirewallInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::NetworkFirewallDescribeFirewall,
    "Returns the data objects for the specified firewall",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    NetworkFirewallDescribeFirewall,
    API_INFO,
    struct {
        firewall_name: Option<String>,
        firewall_arn: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute_json_target("network-firewall", "NetworkFirewall_20201112.DescribeFirewall", None, "1.1").await?;

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
        let input = NetworkFirewallDescribeFirewallInputBuilder::default()
            .firewall_name(None::<String>)
            .firewall_arn(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "networkfirewall_describe_firewall");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: NetworkFirewallDescribeFirewallInput = serde_json::from_value(json).unwrap();
    }
}
