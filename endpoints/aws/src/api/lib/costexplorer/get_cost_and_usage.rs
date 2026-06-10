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

const API_INFO: ApiInfo<AwsApi, CostExplorerGetCostAndUsageInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CostExplorerGetCostAndUsage,
    "cost_explorer_get_cost_and_usage",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CostExplorerGetCostAndUsage,
    API_INFO,
    struct {
        time_period: serde_json::Value,
        granularity: String,
        metrics: Vec<String>,
        filter: Option<serde_json::Value>,
        group_by: Option<Vec<serde_json::Value>>
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
        body.insert("Granularity".to_string(), Value::String(self.granularity.clone()));
        body.insert("Metrics".to_string(), Value::Array(self.metrics.iter().map(|m| Value::String(m.clone())).collect()));
        if let Some(f) = &self.filter {
            body.insert("Filter".to_string(), f.clone());
        }
        if let Some(gb) = &self.group_by {
            body.insert("GroupBy".to_string(), Value::Array(gb.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ce", "AmazonWebServiceCostExplorer.GetCostAndUsage", Some(&body_val), "1.1").await?;

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
        let input = CostExplorerGetCostAndUsageInputBuilder::default()
            .time_period(serde_json::json!({"Start": "2024-01-01", "End": "2024-02-01"}))
            .granularity("MONTHLY")
            .metrics(vec!["BlendedCost".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cost_explorer_get_cost_and_usage");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "time_period": {"Start": "2024-01-01", "End": "2024-02-01"},
            "granularity": "MONTHLY",
            "metrics": ["BlendedCost"]
        });
        let _: CostExplorerGetCostAndUsageInput = serde_json::from_value(json).unwrap();
    }
}
