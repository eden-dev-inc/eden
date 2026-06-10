use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_metrics::{ListTagConfigurationsOptionalParams, MetricsAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListTagConfigurationsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListTagConfigurations,
    "Lists tag configurations for metrics in Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListTagConfigurations,
    API_INFO,
    struct {
        filter_configured: Option<bool>,
        filter_tags_configured: Option<String>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = MetricsAPI::with_config(client.dd_config.clone());

        let mut params = ListTagConfigurationsOptionalParams::default();
        if let Some(v) = self.filter_configured {
            params = params.filter_configured(v);
        }
        if let Some(ref v) = self.filter_tags_configured {
            params = params.filter_tags_configured(v.clone());
        }

        let result = api.list_tag_configurations(params).await.map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::to_value(result).map_err(EpError::serde)?).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatadogTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_tag_configurations_builder_serde() {
        let input = ListTagConfigurationsInputBuilder::default()
            .filter_configured(Some(true))
            .filter_tags_configured(Some("env:prod".to_string()))
            .build()
            .expect("Failed to build ListTagConfigurationsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_tag_configurations");
        assert_eq!(json["filter_configured"], true);
        assert_eq!(json["filter_tags_configured"], "env:prod");
    }

    #[test]
    fn list_tag_configurations_deserialize() {
        let json = serde_json::json!({});
        let input: ListTagConfigurationsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.filter_configured.is_none());
        assert!(input.filter_tags_configured.is_none());
    }
}
