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

const API_INFO: ApiInfo<AwsApi, NeptuneDeleteDbClusterInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::NeptuneDeleteDbCluster, "neptune_delete_db_cluster", ReqType::Write, true);

crate::aws_endpoint! {
    NeptuneDeleteDbCluster,
    API_INFO,
    struct {
        db_cluster_identifier: String,
        skip_final_snapshot: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("DBClusterIdentifier".to_string(), self.db_cluster_identifier.clone());
        if let Some(v) = self.skip_final_snapshot {
            params.insert("SkipFinalSnapshot".to_string(), if v { "true".to_string() } else { "false".to_string() });
        }
        let form_body = build_query_body("DeleteDBCluster", "2014-10-31", &params);
        let result = client.execute_form("rds", &form_body).await?;

        span.add_event("received result from aws neptune", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = NeptuneDeleteDbClusterInputBuilder::default()
            .db_cluster_identifier("my-cluster")
            .skip_final_snapshot(None::<bool>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "neptune_delete_db_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({ "db_cluster_identifier": "my-cluster" });
        let _: NeptuneDeleteDbClusterInput = serde_json::from_value(json).unwrap();
    }
}
