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

const API_INFO: ApiInfo<DatabricksApi, UpdateSchemaInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::UpdateSchema,
    "Update a schema in Databricks Unity Catalog",
    ReqType::Write,
);

crate::databricks_endpoint! {
    UpdateSchema,
    API_INFO,
    struct {
        full_name: String,
        comment: Option<String>,
        owner: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let mut body = serde_json::json!({});

        if let Some(comment) = &self.comment {
            body["comment"] = serde_json::Value::String(comment.clone());
        }

        if let Some(owner) = &self.owner {
            body["owner"] = serde_json::Value::String(owner.clone());
        }

        let value = client.post(&format!("/api/2.1/unity-catalog/schemas/{}", self.full_name), Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "updated schema on databricks",
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
    fn update_schema_builder_serde() {
        let input = UpdateSchemaInputBuilder::default()
            .full_name("main.my_schema")
            .comment(Some("Updated comment".to_string()))
            .owner(Some("new_owner".to_string()))
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "updateschema");
        assert_eq!(json["full_name"], "main.my_schema");
        assert_eq!(json["comment"], "Updated comment");
        assert_eq!(json["owner"], "new_owner");
    }

    #[test]
    fn update_schema_builder_serde_minimal() {
        let input = UpdateSchemaInputBuilder::default()
            .full_name("main.my_schema")
            .comment(None::<String>)
            .owner(None::<String>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "updateschema");
        assert_eq!(json["full_name"], "main.my_schema");
    }
}
