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

const API_INFO: ApiInfo<DatabricksApi, UpdateGrantsInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::UpdateGrants,
    "Update grants for a securable object in Unity Catalog",
    ReqType::Write,
);

crate::databricks_endpoint! {
    UpdateGrants,
    API_INFO,
    struct {
        securable_type: String,
        full_name: String,
        changes: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let changes = serde_json::from_str::<serde_json::Value>(&self.changes).map_err(EpError::parse)?;

        let body = serde_json::json!({
            "changes": changes,
        });

        let value = client
            .post(
                &format!("/api/2.1/unity-catalog/permissions/{}/{}", self.securable_type, self.full_name),
                Some(body),
            )
            .await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "updated grants on databricks",
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
    fn update_grants_builder_serde() {
        let input = UpdateGrantsInputBuilder::default()
            .securable_type("catalog")
            .full_name("my_catalog")
            .changes(r#"[{"add":{"principal":"user@example.com","privileges":["USE_CATALOG"]}}]"#)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "updategrants");
        assert_eq!(json["securable_type"], "catalog");
        assert_eq!(json["full_name"], "my_catalog");
    }
}
