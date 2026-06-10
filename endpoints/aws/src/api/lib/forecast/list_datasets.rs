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

const API_INFO: ApiInfo<AwsApi, ForecastListDatasetsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ForecastListDatasets, "forecast_list_datasets", ReqType::Read, true);

crate::aws_endpoint! {
    ForecastListDatasets,
    API_INFO,
    struct {
        next_token: Option<String>,
        max_results: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({});
        let result = client.execute("forecast", "POST", "/ListDatasets", None, Some(&body_val), None).await?;

        span.add_event("received result from aws forecast", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ForecastListDatasetsInputBuilder::default().next_token(None::<String>).max_results(None::<i64>).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "forecast_list_datasets");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: ForecastListDatasetsInput = serde_json::from_value(json).unwrap();
    }
}
