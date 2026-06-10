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

const API_INFO: ApiInfo<DatabricksApi, ListRunsInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::ListRuns, "List runs for Databricks jobs", ReqType::Read);

crate::databricks_endpoint! {
    ListRuns,
    API_INFO,
    struct {
        job_id: Option<String>,
        active_only: Option<bool>,
        limit: Option<u32>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let mut path = String::from("/api/2.1/jobs/runs/list");
        let mut params = Vec::new();

        if let Some(ref job_id) = self.job_id {
            params.push(format!("job_id={}", job_id));
        }
        if let Some(active_only) = self.active_only {
            params.push(format!("active_only={}", active_only));
        }
        if let Some(limit) = self.limit {
            params.push(format!("limit={}", limit));
        }

        if !params.is_empty() {
            path.push('?');
            path.push_str(&params.join("&"));
        }

        let value = client.get(&path).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "listed runs from databricks",
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
    fn list_runs_builder_serde() {
        let input = ListRunsInputBuilder::default()
            .job_id(None::<String>)
            .active_only(None::<bool>)
            .limit(None::<u32>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "listruns");
    }
}
