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

const API_INFO: ApiInfo<AwsApi, ElastiCacheDescribeCacheSubnetGroupsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElastiCacheDescribeCacheSubnetGroups,
    "elasticache_describe_cache_subnet_groups",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ElastiCacheDescribeCacheSubnetGroups,
    API_INFO,
    struct {
        cache_subnet_group_name: Option<String>,
        marker: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(v) = &self.cache_subnet_group_name {
            params.insert("CacheSubnetGroupName".to_string(), v.clone());
        }
        if let Some(v) = &self.marker {
            params.insert("Marker".to_string(), v.clone());
        }
        let form_body = build_query_body("DescribeCacheSubnetGroups", "2015-02-02", &params);
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
        let input = ElastiCacheDescribeCacheSubnetGroupsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticache_describe_cache_subnet_groups");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: ElastiCacheDescribeCacheSubnetGroupsInput = serde_json::from_value(json).unwrap();
    }
}
