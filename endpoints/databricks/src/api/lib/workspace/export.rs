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

const API_INFO: ApiInfo<DatabricksApi, ExportWorkspaceInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ExportWorkspace,
    "Export a notebook or file from the Databricks workspace",
    ReqType::Read,
);

crate::databricks_endpoint! {
    ExportWorkspace,
    API_INFO,
    struct {
        path: String,
        format: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client
            .get(&format!(
                "/api/2.0/workspace/export?path={}&format={}",
                self.path,
                self.format.as_deref().unwrap_or("SOURCE")
            ))
            .await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "exported workspace object from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(DatabricksJsonOutput(value).to_output()) as Box<dyn EpOutput>)
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
    fn export_workspace_builder_serde() {
        let input = ExportWorkspaceInputBuilder::default().path("/test/notebook").format(None::<String>).build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "exportworkspace");
        assert_eq!(json["path"], "/test/notebook");
    }
}
