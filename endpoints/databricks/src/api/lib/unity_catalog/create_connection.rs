use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use opentelemetry::trace::TraceContextExt;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, CreateConnectionInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::CreateConnection,
    "Create a connection in Databricks Unity Catalog",
    ReqType::Write,
);

crate::databricks_endpoint! {
    CreateConnection,
    API_INFO,
    struct {
        name: String,
        connection_type: String,
        options_json: String,
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

        let options_kvpairs = serde_json::from_str::<serde_json::Value>(&self.options_json).map_err(EpError::parse)?;

        let mut body = serde_json::json!({
            "name": self.name,
            "connection_type": self.connection_type,
            "options_kvpairs": options_kvpairs,
        });

        if let Some(comment) = &self.comment {
            body["comment"] = serde_json::Value::String(comment.clone());
        }

        let value = client.post("/api/2.1/unity-catalog/connections", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created connection on databricks",
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
    fn create_connection_builder_serde() {
        let input = CreateConnectionInputBuilder::default()
            .name("my-connection")
            .connection_type("MYSQL")
            .options_json(r#"{"host":"localhost","port":"3306"}"#)
            .comment(Some("A test connection".to_string()))
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createconnection");
        assert_eq!(json["name"], "my-connection");
        assert_eq!(json["connection_type"], "MYSQL");
        assert_eq!(json["comment"], "A test connection");
    }

    #[test]
    fn create_connection_builder_serde_minimal() {
        let input = CreateConnectionInputBuilder::default()
            .name("my-connection")
            .connection_type("MYSQL")
            .options_json(r#"{"host":"localhost"}"#)
            .comment(None::<String>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createconnection");
        assert_eq!(json["name"], "my-connection");
    }
}
