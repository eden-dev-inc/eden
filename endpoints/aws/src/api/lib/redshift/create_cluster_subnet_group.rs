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

const API_INFO: ApiInfo<AwsApi, RedshiftCreateClusterSubnetGroupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::RedshiftCreateClusterSubnetGroup,
    "redshift_create_cluster_subnet_group",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    RedshiftCreateClusterSubnetGroup,
    API_INFO,
    struct {
        cluster_subnet_group_name: String,
        description: String,
        subnet_ids: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ClusterSubnetGroupName".to_string(), self.cluster_subnet_group_name.clone());
        params.insert("Description".to_string(), self.description.clone());
        params.extend(indexed_list_params("SubnetIds.SubnetIdentifier", &self.subnet_ids));
        let form_body = build_query_body("CreateClusterSubnetGroup", "2012-12-01", &params);
        let result = client.execute_form("redshift", &form_body).await?;

        span.add_event("received result from aws redshift", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RedshiftCreateClusterSubnetGroupInputBuilder::default()
            .cluster_subnet_group_name("my-subnet-group")
            .description("My subnet group")
            .subnet_ids(vec!["subnet-123".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "redshift_create_cluster_subnet_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_subnet_group_name": "my-subnet-group", "description": "My subnet group", "subnet_ids": ["subnet-123"]});
        let _: RedshiftCreateClusterSubnetGroupInput = serde_json::from_value(json).unwrap();
    }
}
