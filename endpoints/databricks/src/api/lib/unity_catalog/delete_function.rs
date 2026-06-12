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

const API_INFO: ApiInfo<DatabricksApi, DeleteFunctionInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::DeleteFunction,
    "Delete a function from Databricks Unity Catalog",
    ReqType::Write,
);

crate::databricks_endpoint! {
    DeleteFunction,
    API_INFO,
    struct {
        name: String,
        force: Option<bool>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let force = self.force.unwrap_or(false);
        client.delete(&format!("/api/2.1/unity-catalog/functions/{}?force={}", self.name, force)).await?;

        let value = serde_json::json!({
            "success": true,
            "name": self.name,
            "message": "Function deleted"
        });

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "deleted function on databricks",
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
    fn delete_function_builder_serde() {
        let input = DeleteFunctionInputBuilder::default()
            .name("main.my_schema.my_function")
            .force(Some(true))
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "deletefunction");
        assert_eq!(json["name"], "main.my_schema.my_function");
        assert_eq!(json["force"], true);
    }

    #[test]
    fn delete_function_builder_serde_no_force() {
        let input = DeleteFunctionInputBuilder::default()
            .name("main.my_schema.my_function")
            .force(None::<bool>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "deletefunction");
        assert_eq!(json["name"], "main.my_schema.my_function");
    }
}
