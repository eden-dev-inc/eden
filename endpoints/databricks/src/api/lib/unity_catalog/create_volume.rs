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

const API_INFO: ApiInfo<DatabricksApi, CreateVolumeInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::CreateVolume,
    "Create a new volume in Databricks Unity Catalog",
    ReqType::Write,
);

crate::databricks_endpoint! {
    CreateVolume,
    API_INFO,
    struct {
        catalog_name: String,
        schema_name: String,
        name: String,
        volume_type: String,
        storage_location: Option<String>,
        comment: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let mut body = serde_json::json!({
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "name": self.name,
            "volume_type": self.volume_type,
        });

        if let Some(storage_location) = &self.storage_location {
            body["storage_location"] = serde_json::Value::String(storage_location.clone());
        }

        if let Some(comment) = &self.comment {
            body["comment"] = serde_json::Value::String(comment.clone());
        }

        let value = client.post("/api/2.1/unity-catalog/volumes", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created volume on databricks",
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
    fn create_volume_builder_serde() {
        let input = CreateVolumeInputBuilder::default()
            .catalog_name("main")
            .schema_name("my_schema")
            .name("my_volume")
            .volume_type("MANAGED")
            .storage_location(Some("s3://bucket/path".to_string()))
            .comment(Some("A test volume".to_string()))
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createvolume");
        assert_eq!(json["catalog_name"], "main");
        assert_eq!(json["schema_name"], "my_schema");
        assert_eq!(json["name"], "my_volume");
        assert_eq!(json["volume_type"], "MANAGED");
        assert_eq!(json["storage_location"], "s3://bucket/path");
        assert_eq!(json["comment"], "A test volume");
    }

    #[test]
    fn create_volume_builder_serde_minimal() {
        let input = CreateVolumeInputBuilder::default()
            .catalog_name("main")
            .schema_name("my_schema")
            .name("my_volume")
            .volume_type("MANAGED")
            .storage_location(None::<String>)
            .comment(None::<String>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createvolume");
        assert_eq!(json["catalog_name"], "main");
        assert_eq!(json["name"], "my_volume");
    }
}
