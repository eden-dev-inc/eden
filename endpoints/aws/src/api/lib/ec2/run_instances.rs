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

const API_INFO: ApiInfo<AwsApi, RunInstancesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2RunInstances, "Launches one or more EC2 instances", ReqType::Write, true);

crate::aws_endpoint! {
    RunInstances,
    API_INFO,
    struct {
        image_id: String,
        instance_type: String,
        min_count: i64,
        max_count: i64,
        key_name: Option<String>,
        security_group_ids: Option<Vec<String>>,
        subnet_id: Option<String>,
        user_data: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ImageId".to_string(), self.image_id.clone());
        params.insert("InstanceType".to_string(), self.instance_type.clone());
        params.insert("MinCount".to_string(), self.min_count.to_string());
        params.insert("MaxCount".to_string(), self.max_count.to_string());
        if let Some(kn) = &self.key_name {
            params.insert("KeyName".to_string(), kn.clone());
        }
        if let Some(sg_ids) = &self.security_group_ids {
            params.extend(indexed_list_params("SecurityGroupId", sg_ids));
        }
        if let Some(sid) = &self.subnet_id {
            params.insert("SubnetId".to_string(), sid.clone());
        }
        if let Some(ud) = &self.user_data {
            params.insert("UserData".to_string(), ud.clone());
        }
        let form_body = build_query_body("RunInstances", "2016-11-15", &params);
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
        let input = RunInstancesInputBuilder::default()
            .image_id("ami-12345678".to_string())
            .instance_type("t3.micro".to_string())
            .min_count(1_i64)
            .max_count(1_i64)
            .key_name(None::<String>)
            .security_group_ids(None::<Vec<String>>)
            .subnet_id(None::<String>)
            .user_data(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_run_instances");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "image_id": "ami-12345678",
            "instance_type": "t3.micro",
            "min_count": 1,
            "max_count": 1
        });
        let _: RunInstancesInput = serde_json::from_value(json).unwrap();
    }
}
