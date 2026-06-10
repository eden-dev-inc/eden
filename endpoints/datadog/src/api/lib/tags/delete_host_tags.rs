use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_tags::{DeleteHostTagsOptionalParams, TagsAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, DeleteHostTagsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::DeleteHostTags,
    "Removes all tags from a host in Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    DeleteHostTags,
    API_INFO,
    struct {
        host_name: String,
        source: Option<String>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = TagsAPI::with_config(client.dd_config.clone());
        let mut params = DeleteHostTagsOptionalParams::default();
        if let Some(s) = &self.source {
            params = params.source(s.clone());
        }
        api.delete_host_tags(self.host_name.clone(), params).await.map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::json!({"success": true})).to_output()) as Box<dyn EpOutput>)
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
    fn delete_host_tags_builder_serde() {
        let input = DeleteHostTagsInputBuilder::default()
            .host_name("web-01".to_string())
            .source(None::<String>)
            .build()
            .expect("Failed to build DeleteHostTagsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "delete_host_tags");
        assert_eq!(json["host_name"], "web-01");
    }

    #[test]
    fn delete_host_tags_deserialize() {
        let json = serde_json::json!({"host_name": "web-01"});
        let input: DeleteHostTagsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.host_name, "web-01");
    }
}
