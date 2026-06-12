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

const API_INFO: ApiInfo<AwsApi, ElastiCacheCreateCacheClusterInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElastiCacheCreateCacheCluster,
    "elasticache_create_cache_cluster",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ElastiCacheCreateCacheCluster,
    API_INFO,
    struct {
        cache_cluster_id: String,
        cache_node_type: String,
        engine: String,
        num_cache_nodes: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("CacheClusterId".to_string(), self.cache_cluster_id.clone());
        params.insert("CacheNodeType".to_string(), self.cache_node_type.clone());
        params.insert("Engine".to_string(), self.engine.clone());
        if let Some(v) = self.num_cache_nodes {
            params.insert("NumCacheNodes".to_string(), v.to_string());
        }
        let form_body = build_query_body("CreateCacheCluster", "2015-02-02", &params);
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
        let input = ElastiCacheCreateCacheClusterInputBuilder::default()
            .cache_cluster_id("my-cache")
            .cache_node_type("cache.t3.micro")
            .engine("redis")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticache_create_cache_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cache_cluster_id": "my-cache", "cache_node_type": "cache.t3.micro", "engine": "redis"});
        let _: ElastiCacheCreateCacheClusterInput = serde_json::from_value(json).unwrap();
    }
}
