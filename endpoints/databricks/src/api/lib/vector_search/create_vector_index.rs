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

const API_INFO: ApiInfo<DatabricksApi, CreateVectorIndexInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::CreateVectorIndex, "Create a vector search index", ReqType::Write);

crate::databricks_endpoint! {
    CreateVectorIndex,
    API_INFO,
    struct {
        name: String,
        endpoint_name: String,
        primary_key: String,
        index_type: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let body = serde_json::json!({
            "name": self.name,
            "endpoint_name": self.endpoint_name,
            "primary_key": self.primary_key,
            "index_type": self.index_type,
        });

        let value = client.post("/api/2.0/vector-search/indexes", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created vector index on databricks",
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
    fn create_vector_index_builder_serde() {
        let input = CreateVectorIndexInputBuilder::default()
            .name("my-index")
            .endpoint_name("my-endpoint")
            .primary_key("id")
            .index_type("DELTA_SYNC")
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createvectorindex");
        assert_eq!(json["name"], "my-index");
        assert_eq!(json["endpoint_name"], "my-endpoint");
        assert_eq!(json["primary_key"], "id");
        assert_eq!(json["index_type"], "DELTA_SYNC");
    }
}
