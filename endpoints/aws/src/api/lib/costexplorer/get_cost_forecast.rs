use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, CostExplorerGetCostForecastInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CostExplorerGetCostForecast,
    "cost_explorer_get_cost_forecast",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CostExplorerGetCostForecast,
    API_INFO,
    struct {
        time_period: serde_json::Value,
        metric: String,
        granularity: String,
        filter: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("TimePeriod".to_string(), self.time_period.clone());
        body.insert("Metric".to_string(), Value::String(self.metric.clone()));
        body.insert("Granularity".to_string(), Value::String(self.granularity.clone()));
        if let Some(f) = &self.filter {
            body.insert("Filter".to_string(), f.clone());
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ce", "AmazonWebServiceCostExplorer.GetCostForecast", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws cost explorer",
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
        let input = CostExplorerGetCostForecastInputBuilder::default()
            .time_period(serde_json::json!({"Start": "2024-02-01", "End": "2024-03-01"}))
            .metric("BLENDED_COST")
            .granularity("MONTHLY")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cost_explorer_get_cost_forecast");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "time_period": {"Start": "2024-02-01", "End": "2024-03-01"},
            "metric": "BLENDED_COST",
            "granularity": "MONTHLY"
        });
        let _: CostExplorerGetCostForecastInput = serde_json::from_value(json).unwrap();
    }
}
