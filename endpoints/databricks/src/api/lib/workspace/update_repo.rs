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

const API_INFO: ApiInfo<DatabricksApi, UpdateRepoInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::UpdateRepo, "Update a Databricks Repo", ReqType::Write);

crate::databricks_endpoint! {
    UpdateRepo,
    API_INFO,
    struct {
        repo_id: String,
        branch: Option<String>,
        tag: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let mut body = serde_json::json!({});

        let map = body.as_object_mut().expect("body is an object");
        if let Some(ref v) = self.branch {
            map.insert("branch".to_string(), serde_json::json!(v));
        }
        if let Some(ref v) = self.tag {
            map.insert("tag".to_string(), serde_json::json!(v));
        }

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.post(&format!("/api/2.0/repos/{}", self.repo_id), Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "updated repo on databricks",
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
    fn update_repo_builder_serde() {
        let input = UpdateRepoInputBuilder::default()
            .repo_id("12345")
            .branch(None::<String>)
            .tag(None::<String>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "updaterepo");
        assert_eq!(json["repo_id"], "12345");
    }
}
