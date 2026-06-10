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

const API_INFO: ApiInfo<DatabricksApi, CreateTokenInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::CreateToken,
    "Create a new personal access token in Databricks",
    ReqType::Write,
);

crate::databricks_endpoint! {
    CreateToken,
    API_INFO,
    struct {
        comment: Option<String>,
        lifetime_seconds: Option<i64>,
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
        if let Some(ref v) = self.comment {
            map.insert("comment".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.lifetime_seconds {
            map.insert("lifetime_seconds".to_string(), serde_json::json!(v));
        }

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.post("/api/2.0/token/create", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created token on databricks",
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
    fn create_token_builder_serde() {
        let input = CreateTokenInputBuilder::default()
            .comment(None::<String>)
            .lifetime_seconds(None::<i64>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createtoken");
    }
}
