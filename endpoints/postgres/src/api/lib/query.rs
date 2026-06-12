use crate::api::lib::PostgresApi;
use crate::api::wrapper::input::SqlParam;
use crate::api::wrapper::output::PostgresRowsOutput;
use crate::request::PostgresRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use postgres_core::{PostgresAsync, PostgresTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<PostgresApi, QueryInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::Query,
    "Executes a parameterized SQL query and returns results from PostgreSQL",
    ReqType::Write,
    true,
);

crate::postgres_endpoint! {
    Query,
    API_INFO,
    struct {
        query: String,
        params: Vec<SqlParam>
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

impl SimpleInput {
    fn text_params(&self) -> Vec<Option<String>> {
        self.params.iter().map(|p| p.to_pg_text()).collect()
    }

    pub async fn run_query(&self, context: PostgresAsync) -> ResultEP<bytes::Bytes> {
        let mut client = context.get().await.map_err(EpError::request)?;

        let text_params = self.text_params();
        let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
        client.query_params_raw(self.query(), &param_refs).await
    }

    /// Execute the query and return parsed rows (for metadata system).
    pub async fn run_query_parsed(&self, context: PostgresAsync) -> ResultEP<Vec<postgres_core::PgSimpleRow>> {
        let raw = self.run_query(context).await?;
        postgres_core::parse_simple_query_response(&raw)
    }

    #[named]
    pub async fn run_async_generic(&self, context: PostgresAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let result = self.run_query(context).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(PostgresRowsOutput(result).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_builder_serde() {
        let query = "SELECT * FROM a WHERE b > 0 ORDER BY c ASC";

        let query = QueryInputBuilder::default().query(query).params(&[]).build().expect("Failed to build query");

        let query = serde_json::to_value(query).expect("Failed to serialize query");
        println!("{:?}", query);
    }
}
