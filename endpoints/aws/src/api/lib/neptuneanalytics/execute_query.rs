use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, NeptuneAnalyticsExecuteQueryInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::NeptuneAnalyticsExecuteQuery,
    "neptuneanalytics_execute_query",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    NeptuneAnalyticsExecuteQuery,
    API_INFO,
    struct {
        graph_identifier: String,
        query_string: String,
        language: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/queries/{}", self.graph_identifier);
        let body = serde_json::json!({
            "queryString": self.query_string,
            "language": self.language
        });
        let result = client.execute("neptune-graph", "POST", &path, None, Some(&body), None).await?;

        span.add_event(
            "received result from aws neptuneanalytics",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = NeptuneAnalyticsExecuteQueryInputBuilder::default()
            .graph_identifier("g-abc123")
            .query_string("MATCH (n) RETURN n LIMIT 1")
            .language("OPEN_CYPHER")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "neptuneanalytics_execute_query");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "graph_identifier": "g-abc123",
            "query_string": "MATCH (n) RETURN n LIMIT 1",
            "language": "OPEN_CYPHER"
        });
        let _: NeptuneAnalyticsExecuteQueryInput = serde_json::from_value(json).unwrap();
    }
}
