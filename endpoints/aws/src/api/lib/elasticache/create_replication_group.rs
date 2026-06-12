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

const API_INFO: ApiInfo<AwsApi, ElastiCacheCreateReplicationGroupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElastiCacheCreateReplicationGroup,
    "elasticache_create_replication_group",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ElastiCacheCreateReplicationGroup,
    API_INFO,
    struct {
        replication_group_id: String,
        replication_group_description: String,
        cache_node_type: Option<String>,
        num_cache_clusters: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ReplicationGroupId".to_string(), self.replication_group_id.clone());
        params.insert("ReplicationGroupDescription".to_string(), self.replication_group_description.clone());
        if let Some(v) = &self.cache_node_type {
            params.insert("CacheNodeType".to_string(), v.clone());
        }
        if let Some(v) = self.num_cache_clusters {
            params.insert("NumCacheClusters".to_string(), v.to_string());
        }
        let form_body = build_query_body("CreateReplicationGroup", "2015-02-02", &params);
        let result = client.execute_form("elasticache", &form_body).await?;

        span.add_event(
            "received result from aws elasticache",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ElastiCacheCreateReplicationGroupInputBuilder::default()
            .replication_group_id("my-rg")
            .replication_group_description("My replication group")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticache_create_replication_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"replication_group_id": "my-rg", "replication_group_description": "desc"});
        let _: ElastiCacheCreateReplicationGroupInput = serde_json::from_value(json).unwrap();
    }
}
