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

const API_INFO: ApiInfo<AwsApi, GroundStationReserveContactInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::GroundStationReserveContact,
    "groundstation_reserve_contact",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    GroundStationReserveContact,
    API_INFO,
    struct {
        end_time: String,
        ground_station: String,
        mission_profile_arn: String,
        satellite_arn: String,
        start_time: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "endTime": self.end_time,
            "groundStation": self.ground_station,
            "missionProfileArn": self.mission_profile_arn,
            "satelliteArn": self.satellite_arn,
            "startTime": self.start_time
        });
        let result = client.execute("groundstation", "POST", "/contact", None, Some(&body), None).await?;

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
        let input = GroundStationReserveContactInputBuilder::default()
            .end_time("2024-01-01T01:00:00Z")
            .ground_station("gs-name")
            .mission_profile_arn("arn:aws:groundstation:us-east-1:123456789012:mission-profile/mp-abc")
            .satellite_arn("arn:aws:groundstation::123456789012:satellite/sat-abc")
            .start_time("2024-01-01T00:00:00Z")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "groundstation_reserve_contact");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "end_time": "2024-01-01T01:00:00Z",
            "ground_station": "gs-name",
            "mission_profile_arn": "arn:aws:groundstation:us-east-1:123456789012:mission-profile/mp-abc",
            "satellite_arn": "arn:aws:groundstation::123456789012:satellite/sat-abc",
            "start_time": "2024-01-01T00:00:00Z"
        });
        let _: GroundStationReserveContactInput = serde_json::from_value(json).unwrap();
    }
}
