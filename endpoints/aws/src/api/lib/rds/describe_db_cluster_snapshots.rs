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

const API_INFO: ApiInfo<AwsApi, RdsDescribeDbClusterSnapshotsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::RdsDescribeDbClusterSnapshots,
    "rds_describe_db_cluster_snapshots",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    RdsDescribeDbClusterSnapshots,
    API_INFO,
    struct {
        db_cluster_identifier: Option<String>,
        db_cluster_snapshot_identifier: Option<String>,
        max_records: Option<i64>,
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
        if let Some(v) = &self.db_cluster_identifier {
            params.insert("DBClusterIdentifier".to_string(), v.clone());
        }
        if let Some(v) = &self.db_cluster_snapshot_identifier {
            params.insert("DBClusterSnapshotIdentifier".to_string(), v.clone());
        }
        if let Some(v) = self.max_records {
            params.insert("MaxRecords".to_string(), v.to_string());
        }
        if let Some(v) = &self.marker {
            params.insert("Marker".to_string(), v.clone());
        }
        let form_body = build_query_body("DescribeDBClusterSnapshots", "2014-10-31", &params);
        let result = client.execute_form("rds", &form_body).await?;

        span.add_event("received result from aws rds", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RdsDescribeDbClusterSnapshotsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rds_describe_db_cluster_snapshots");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: RdsDescribeDbClusterSnapshotsInput = serde_json::from_value(json).unwrap();
    }
}
