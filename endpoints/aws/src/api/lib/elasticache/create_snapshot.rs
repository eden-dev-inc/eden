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

const API_INFO: ApiInfo<AwsApi, ElastiCacheCreateSnapshotInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ElastiCacheCreateSnapshot, "elasticache_create_snapshot", ReqType::Write, true);

crate::aws_endpoint! {
    ElastiCacheCreateSnapshot,
    API_INFO,
    struct {
        snapshot_name: String,
        replication_group_id: Option<String>,
        cache_cluster_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("SnapshotName".to_string(), self.snapshot_name.clone());
        if let Some(v) = &self.replication_group_id {
            params.insert("ReplicationGroupId".to_string(), v.clone());
        }
        if let Some(v) = &self.cache_cluster_id {
            params.insert("CacheClusterId".to_string(), v.clone());
        }
        let form_body = build_query_body("CreateSnapshot", "2015-02-02", &params);
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
        let input = ElastiCacheCreateSnapshotInputBuilder::default().snapshot_name("my-snapshot").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticache_create_snapshot");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"snapshot_name": "snap"});
        let _: ElastiCacheCreateSnapshotInput = serde_json::from_value(json).unwrap();
    }
}
