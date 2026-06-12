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

const API_VERSION: &str = "2023-01-01";

const API_INFO: ApiInfo<AzureApi, AdvisorSuppressRecommendationInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::AdvisorSuppressRecommendation,
    "Suppress advisor recommendation",
    ReqType::Write,
    true,
);

crate::azure_endpoint! {
    AdvisorSuppressRecommendation,
    API_INFO,
    struct {
        resource_uri: String,
        recommendation_id: String,
        suppression_name: String
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!(
            "{}/providers/Microsoft.Advisor/recommendations/{}/suppressions/{}",
            self.resource_uri, self.recommendation_id, self.suppression_name
        );

        let body = serde_json::json!({});

        let result = client.execute("PUT", &path, API_VERSION, Some(&body), None).await?;

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
        let input = AdvisorSuppressRecommendationInputBuilder::default()
            .resource_uri("/subscriptions/sub-id")
            .recommendation_id("rec-id")
            .suppression_name("my-suppression")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "advisor_suppress_recommendation");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_uri": "/subscriptions/sub-id", "recommendation_id": "rec-id", "suppression_name": "my-suppression"});
        let _: AdvisorSuppressRecommendationInput = serde_json::from_value(json).unwrap();
    }
}
