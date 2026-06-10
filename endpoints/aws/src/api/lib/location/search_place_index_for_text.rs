use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, LocationSearchPlaceIndexForTextInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LocationSearchPlaceIndexForText,
    "Searches a place index for text",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    LocationSearchPlaceIndexForText,
    API_INFO,
    struct {
        index_name: String,
        text: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/places/v0/indexes/{}/search/text", self.index_name);
        let body_val = serde_json::json!({"Text": self.text});
        let result = client.execute("location", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws location", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LocationSearchPlaceIndexForTextInputBuilder::default().index_name("my-index").text("Seattle").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "location_search_place_index_for_text");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"index_name": "my-index", "text": "Seattle"});
        let _: LocationSearchPlaceIndexForTextInput = serde_json::from_value(json).unwrap();
    }
}
