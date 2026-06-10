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

const API_INFO: ApiInfo<AwsApi, RedshiftCreateClusterInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RedshiftCreateCluster, "redshift_create_cluster", ReqType::Write, true);

crate::aws_endpoint! {
    RedshiftCreateCluster,
    API_INFO,
    struct {
        cluster_identifier: String,
        node_type: String,
        master_username: String,
        master_user_password: String,
        cluster_type: Option<String>,
        number_of_nodes: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ClusterIdentifier".to_string(), self.cluster_identifier.clone());
        params.insert("NodeType".to_string(), self.node_type.clone());
        params.insert("MasterUsername".to_string(), self.master_username.clone());
        params.insert("MasterUserPassword".to_string(), self.master_user_password.clone());
        if let Some(v) = &self.cluster_type {
            params.insert("ClusterType".to_string(), v.clone());
        }
        if let Some(v) = self.number_of_nodes {
            params.insert("NumberOfNodes".to_string(), v.to_string());
        }
        let form_body = build_query_body("CreateCluster", "2012-12-01", &params);
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
        let input = RedshiftCreateClusterInputBuilder::default()
            .cluster_identifier("my-cluster")
            .node_type("dc2.large")
            .master_username("admin")
            .master_user_password("password")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "redshift_create_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_identifier": "my-cluster", "node_type": "dc2.large", "master_username": "admin", "master_user_password": "password"});
        let _: RedshiftCreateClusterInput = serde_json::from_value(json).unwrap();
    }
}
