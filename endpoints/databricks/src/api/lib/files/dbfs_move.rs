use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, DbfsMoveInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::DbfsMove, "Move a file or directory in DBFS", ReqType::Write);

crate::databricks_endpoint! {
    DbfsMove,
    API_INFO,
    struct {
        source_path: String,
        destination_path: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let body = serde_json::json!({
            "source_path": self.source_path,
            "destination_path": self.destination_path,
        });

        let client = context.get().await.map_err(EpError::connect)?;
        let _value = client.post("/api/2.0/dbfs/move", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "moved file or directory in DBFS",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        let result = serde_json::json!({"success": true, "source_path": self.source_path, "destination_path": self.destination_path, "message": "File or directory moved"});
        Ok(Box::new(DatabricksJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatabricksTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Databricks transaction support not implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dbfs_move_builder_serde() {
        let input = DbfsMoveInputBuilder::default().source_path("/source").destination_path("/dest").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "dbfsmove");
        assert_eq!(json["source_path"], "/source");
        assert_eq!(json["destination_path"], "/dest");
    }
}
