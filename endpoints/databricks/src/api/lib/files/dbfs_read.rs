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

const API_INFO: ApiInfo<DatabricksApi, DbfsReadInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::DbfsRead, "Read the contents of a file in DBFS", ReqType::Read);

crate::databricks_endpoint! {
    DbfsRead,
    API_INFO,
    struct {
        path: String,
        offset: Option<i64>,
        length: Option<i64>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let mut path_with_query = format!("/api/2.0/dbfs/read?path={}", self.path);
        if let Some(offset) = self.offset {
            path_with_query.push_str(&format!("&offset={}", offset));
        }
        if let Some(length) = self.length {
            path_with_query.push_str(&format!("&length={}", length));
        }

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.get(&path_with_query).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "read file contents from DBFS",
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
    fn dbfs_read_builder_serde() {
        let input = DbfsReadInputBuilder::default().path("/test").offset(None::<i64>).length(None::<i64>).build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "dbfsread");
        assert_eq!(json["path"], "/test");
    }
}
