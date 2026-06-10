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

const API_INFO: ApiInfo<AwsApi, RdsCreateDbSnapshotInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RdsCreateDbSnapshot, "rds_create_db_snapshot", ReqType::Write, true);

crate::aws_endpoint! {
    RdsCreateDbSnapshot,
    API_INFO,
    struct {
        db_instance_identifier: String,
        db_snapshot_identifier: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("DBInstanceIdentifier".to_string(), self.db_instance_identifier.clone());
        params.insert("DBSnapshotIdentifier".to_string(), self.db_snapshot_identifier.clone());
        let form_body = build_query_body("CreateDBSnapshot", "2014-10-31", &params);
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
        let input = RdsCreateDbSnapshotInputBuilder::default()
            .db_instance_identifier("my-db")
            .db_snapshot_identifier("my-snap")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rds_create_db_snapshot");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"db_instance_identifier": "my-db", "db_snapshot_identifier": "my-snap"});
        let _: RdsCreateDbSnapshotInput = serde_json::from_value(json).unwrap();
    }
}
