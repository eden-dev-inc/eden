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

const API_VERSION: &str = "2024-06-01-preview";

const API_INFO: ApiInfo<AzureApi, EventGridListEventSubscriptionsInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::EventGridListEventSubscriptions,
    "List EventGrid event subscriptions",
    ReqType::Read,
    true,
);

crate::azure_endpoint! {
    EventGridListEventSubscriptions,
    API_INFO,
    struct {
        subscription_id: Option<String>,
        scope: String
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let _sub = self
            .subscription_id
            .as_deref()
            .or(client.subscription_id())
            .ok_or_else(|| EpError::request("subscription_id required"))?;

        let path = format!("/{}/providers/Microsoft.EventGrid/eventSubscriptions", self.scope);

        let result = client.execute("GET", &path, API_VERSION, None, None).await?;

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
        let input = EventGridListEventSubscriptionsInputBuilder::default()
            .scope("subscriptions/sub-id/resourceGroups/my-rg")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "event_grid_list_event_subscriptions");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"scope": "subscriptions/sub-id/resourceGroups/my-rg"});
        let _: EventGridListEventSubscriptionsInput = serde_json::from_value(json).unwrap();
    }
}
