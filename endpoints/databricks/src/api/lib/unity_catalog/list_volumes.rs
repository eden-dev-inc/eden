use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::Deserialize;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, ListVolumesInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ListVolumes,
    "List volumes in a Databricks Unity Catalog schema",
    ReqType::Read,
);

#[derive(Debug, Deserialize)]
struct VolumeListResponse {
    #[serde(default)]
    volumes: Vec<VolumeInfo>,
}

#[derive(Debug, Deserialize)]
struct VolumeInfo {
    name: String,
    catalog_name: String,
    schema_name: String,
    #[serde(default)]
    volume_type: Option<String>,
    #[serde(default)]
    comment: Option<String>,
}

crate::databricks_endpoint! {
    ListVolumes,
    API_INFO,
    struct {
        catalog_name: String,
        schema_name: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let path = format!("/api/2.1/unity-catalog/volumes?catalog_name={}&schema_name={}", self.catalog_name, self.schema_name);
        let response = client.get(&path).await?;
        let volume_list: VolumeListResponse = serde_json::from_value(response).map_err(EpError::serde)?;

        let volumes: Vec<serde_json::Value> = volume_list
            .volumes
            .iter()
            .map(|v| {
                serde_json::json!({
                    "name": v.name,
                    "catalog_name": v.catalog_name,
                    "schema_name": v.schema_name,
                    "volume_type": v.volume_type,
                    "comment": v.comment,
                })
            })
            .collect();

        let value = serde_json::to_value(&volumes).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "listed volumes from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("count", volumes.len().to_string()),
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
    fn list_volumes_builder_serde() {
        let input = ListVolumesInputBuilder::default().catalog_name("main").schema_name("default").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "listvolumes");
        assert_eq!(json["catalog_name"], "main");
        assert_eq!(json["schema_name"], "default");
    }
}
