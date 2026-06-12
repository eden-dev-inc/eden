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

const API_INFO: ApiInfo<AwsApi, Ec2CreateNetworkAclEntryInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2CreateNetworkAclEntry, "ec2_create_network_acl_entry", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2CreateNetworkAclEntry,
    API_INFO,
    struct {
        network_acl_id: String,
        rule_number: i64,
        protocol: String,
        rule_action: String,
        egress: bool,
        cidr_block: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("NetworkAclId".to_string(), self.network_acl_id.clone());
        params.insert("RuleNumber".to_string(), self.rule_number.to_string());
        params.insert("Protocol".to_string(), self.protocol.clone());
        params.insert("RuleAction".to_string(), self.rule_action.clone());
        params.insert("Egress".to_string(), self.egress.to_string());
        if let Some(v) = &self.cidr_block {
            params.insert("CidrBlock".to_string(), v.clone());
        }
        let form_body = build_query_body("CreateNetworkAclEntry", "2016-11-15", &params);
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
        let input = Ec2CreateNetworkAclEntryInputBuilder::default()
            .network_acl_id("acl-123")
            .rule_number(100_i64)
            .protocol("-1")
            .rule_action("allow")
            .egress(false)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_create_network_acl_entry");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "network_acl_id": "acl-123",
            "rule_number": 100,
            "protocol": "-1",
            "rule_action": "allow",
            "egress": false
        });
        let _: Ec2CreateNetworkAclEntryInput = serde_json::from_value(json).unwrap();
    }
}
