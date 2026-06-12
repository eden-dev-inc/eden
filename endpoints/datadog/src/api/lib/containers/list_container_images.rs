use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_container_images::{ContainerImagesAPI, ListContainerImagesOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListContainerImagesInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListContainerImages,
    "Lists container images from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListContainerImages,
    API_INFO,
    struct {
        filter_tags: Option<String>,
        page_size: Option<i32>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = ContainerImagesAPI::with_config(client.dd_config.clone());

        let mut params = ListContainerImagesOptionalParams::default();
        if let Some(ref v) = self.filter_tags {
            params = params.filter_tags(v.clone());
        }
        if let Some(v) = self.page_size {
            params = params.page_size(v);
        }

        let result = api.list_container_images(params).await.map_err(EpError::request)?;

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
    fn list_container_images_builder_serde() {
        let input = ListContainerImagesInputBuilder::default()
            .filter_tags(Some("env:staging".to_string()))
            .page_size(Some(20))
            .build()
            .expect("Failed to build ListContainerImagesInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_container_images");
        assert_eq!(json["filter_tags"], "env:staging");
        assert_eq!(json["page_size"], 20);
    }

    #[test]
    fn list_container_images_deserialize() {
        let json = serde_json::json!({});
        let input: ListContainerImagesInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.filter_tags.is_none());
        assert!(input.page_size.is_none());
    }
}
