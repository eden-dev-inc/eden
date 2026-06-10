use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_logs_archives::LogsArchivesAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetLogsArchiveInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetLogsArchive,
    "Gets a specific log archive from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetLogsArchive,
    API_INFO,
    struct {
        archive_id: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = LogsArchivesAPI::with_config(client.dd_config.clone());
        let result = api.get_logs_archive(self.archive_id.clone()).await.map_err(EpError::request)?;

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
    fn get_logs_archive_builder_serde() {
        let input = GetLogsArchiveInputBuilder::default()
            .archive_id("abc-123".to_string())
            .build()
            .expect("Failed to build GetLogsArchiveInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_logs_archive");
        assert_eq!(json["archive_id"], "abc-123");
    }

    #[test]
    fn get_logs_archive_deserialize() {
        let json = serde_json::json!({"archive_id": "abc-123"});
        let input: GetLogsArchiveInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.archive_id, "abc-123");
    }
}
