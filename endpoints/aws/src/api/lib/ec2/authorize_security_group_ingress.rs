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

const API_INFO: ApiInfo<AwsApi, Ec2AuthorizeSecurityGroupIngressInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Ec2AuthorizeSecurityGroupIngress,
    "ec2_authorize_security_group_ingress",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    Ec2AuthorizeSecurityGroupIngress,
    API_INFO,
    struct {
        group_id: String,
        ip_protocol: Option<String>,
        from_port: Option<i64>,
        to_port: Option<i64>,
        cidr_ip: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("GroupId".to_string(), self.group_id.clone());
        if let Some(proto) = &self.ip_protocol {
            params.insert("IpProtocol".to_string(), proto.clone());
        }
        if let Some(from) = self.from_port {
            params.insert("FromPort".to_string(), from.to_string());
        }
        if let Some(to) = self.to_port {
            params.insert("ToPort".to_string(), to.to_string());
        }
        if let Some(cidr) = &self.cidr_ip {
            params.insert("CidrIp".to_string(), cidr.clone());
        }
        let form_body = build_query_body("AuthorizeSecurityGroupIngress", "2016-11-15", &params);
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
        let input = Ec2AuthorizeSecurityGroupIngressInputBuilder::default().group_id("sg-12345678").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_authorize_security_group_ingress");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"group_id": "sg-12345678"});
        let _: Ec2AuthorizeSecurityGroupIngressInput = serde_json::from_value(json).unwrap();
    }
}
