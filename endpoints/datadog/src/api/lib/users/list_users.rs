use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_users::{ListUsersOptionalParams, UsersAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListUsersInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListUsers,
    "Lists users in the Datadog organization",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListUsers,
    API_INFO,
    struct {
        filter: Option<String>,
        page_size: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = UsersAPI::with_config(client.dd_config.clone());
        let mut params = ListUsersOptionalParams::default();
        if let Some(f) = &self.filter {
            params = params.filter(f.clone());
        }
        if let Some(size) = self.page_size {
            params = params.page_size(size);
        }
        let result = api.list_users(params).await.map_err(EpError::request)?;

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
    fn list_users_builder_serde() {
        let input = ListUsersInputBuilder::default()
            .filter(Some("admin".to_string()))
            .page_size(Some(50))
            .build()
            .expect("Failed to build ListUsersInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_users");
        assert_eq!(json["filter"], "admin");
        assert_eq!(json["page_size"], 50);
    }

    #[test]
    fn list_users_deserialize() {
        let json = serde_json::json!({});
        let input: ListUsersInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.filter.is_none());
        assert!(input.page_size.is_none());
    }
}
