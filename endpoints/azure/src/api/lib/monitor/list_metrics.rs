use crate::api::lib::AzureApi;
use crate::api::wrapper::output::AzureJsonOutput;
use crate::request::AzureRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use azure_core::{AzureAsync, AzureTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_VERSION: &str = "2024-02-01";

const API_INFO: ApiInfo<AzureApi, MonitorListMetricsInput> =
    ApiInfo::new(EpKind::Azure, AzureApi::MonitorListMetrics, "List metrics for a resource", ReqType::Read, true);

crate::azure_endpoint! {
    MonitorListMetrics,
    API_INFO,
    struct {
        resource_uri: String,
        timespan: Option<String>,
        interval: Option<String>,
        metricnames: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/{}/providers/Microsoft.Insights/metrics", self.resource_uri.trim_start_matches('/'));

        let mut extra_query = Vec::new();
        if let Some(ts) = &self.timespan {
            extra_query.push(("timespan", ts.as_str()));
        }
        if let Some(iv) = &self.interval {
            extra_query.push(("interval", iv.as_str()));
        }
        if let Some(mn) = &self.metricnames {
            extra_query.push(("metricnames", mn.as_str()));
        }

        let query = if extra_query.is_empty() {
            None
        } else {
            Some(extra_query.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&"))
        };

        let result = client.execute("GET", &path, API_VERSION, None, query.as_deref()).await?;

        span.add_event("received result from azure", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AzureJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AzureTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = MonitorListMetricsInputBuilder::default()
            .resource_uri("/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "monitor_list_metrics");
    }

    #[test]
    fn deserialize_minimal() {
        let json =
            serde_json::json!({"resource_uri": "/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"});
        let _: MonitorListMetricsInput = serde_json::from_value(json).unwrap();
    }
}
