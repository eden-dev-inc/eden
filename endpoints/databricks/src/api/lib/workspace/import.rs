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

const API_INFO: ApiInfo<DatabricksApi, ImportWorkspaceInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ImportWorkspace,
    "Import a notebook or file into the Databricks workspace",
    ReqType::Write,
);

crate::databricks_endpoint! {
    ImportWorkspace,
    API_INFO,
    struct {
        path: String,
        content: String,
        format: Option<String>,
        language: Option<String>,
        overwrite: Option<bool>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let mut body = serde_json::json!({
            "path": self.path,
            "content": self.content,
        });

        let map = body.as_object_mut().expect("body is an object");
        if let Some(ref v) = self.format {
            map.insert("format".to_string(), serde_json::json!(v));
        }
        if let Some(ref v) = self.language {
            map.insert("language".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.overwrite {
            map.insert("overwrite".to_string(), serde_json::json!(v));
        }

        let client = context.get().await.map_err(EpError::connect)?;
        let _response = client.post("/api/2.0/workspace/import", Some(body)).await?;

        let value = serde_json::json!({"success": true, "path": self.path, "message": "Import completed"});

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "imported workspace object to databricks",
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
    fn import_workspace_builder_serde() {
        let input = ImportWorkspaceInputBuilder::default()
            .path("/test/notebook")
            .content("cHJpbnQoJ2hlbGxvJyk=")
            .format(None::<String>)
            .language(None::<String>)
            .overwrite(None::<bool>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "importworkspace");
        assert_eq!(json["path"], "/test/notebook");
        assert_eq!(json["content"], "cHJpbnQoJ2hlbGxvJyk=");
    }
}
