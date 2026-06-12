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

const API_INFO: ApiInfo<AwsApi, ElastiCacheCreateCacheSubnetGroupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElastiCacheCreateCacheSubnetGroup,
    "elasticache_create_cache_subnet_group",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ElastiCacheCreateCacheSubnetGroup,
    API_INFO,
    struct {
        cache_subnet_group_name: String,
        cache_subnet_group_description: String,
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
        params.insert("CacheSubnetGroupName".to_string(), self.cache_subnet_group_name.clone());
        params.insert("CacheSubnetGroupDescription".to_string(), self.cache_subnet_group_description.clone());
        for (i, id) in self.subnet_ids.iter().enumerate() {
            params.insert(format!("SubnetIdentifier.member.{}", i + 1), id.clone());
        }
        let form_body = build_query_body("CreateCacheSubnetGroup", "2015-02-02", &params);
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
        let input = ElastiCacheCreateCacheSubnetGroupInputBuilder::default()
            .cache_subnet_group_name("my-subnet-group")
            .cache_subnet_group_description("desc")
            .subnet_ids(vec!["subnet-1".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticache_create_cache_subnet_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cache_subnet_group_name": "sg", "cache_subnet_group_description": "d", "subnet_ids": ["s1"]});
        let _: ElastiCacheCreateCacheSubnetGroupInput = serde_json::from_value(json).unwrap();
    }
}
