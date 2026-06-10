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

const API_INFO: ApiInfo<AwsApi, GroundStationListContactsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::GroundStationListContacts, "groundstation_list_contacts", ReqType::Read, true);

crate::aws_endpoint! {
    GroundStationListContacts,
    API_INFO,
    struct {
        start_time: String,
        end_time: String,
        status_list: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "startTime": self.start_time,
            "endTime": self.end_time,
            "statusList": self.status_list
        });
        let result = client.execute("groundstation", "POST", "/contacts", None, Some(&body), None).await?;

        span.add_event(
            "received result from aws groundstation",
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
        let input = GroundStationListContactsInputBuilder::default()
            .start_time("2024-01-01T00:00:00Z")
            .end_time("2024-01-02T00:00:00Z")
            .status_list(vec!["SCHEDULED".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "groundstation_list_contacts");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z",
            "status_list": ["SCHEDULED"]
        });
        let _: GroundStationListContactsInput = serde_json::from_value(json).unwrap();
    }
}
