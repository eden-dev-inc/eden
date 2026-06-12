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

const API_INFO: ApiInfo<AwsApi, LakeFormationGetDataLakeSettingsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LakeFormationGetDataLakeSettings,
    "lakeformation_get_data_lake_settings",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    LakeFormationGetDataLakeSettings,
    API_INFO,
    struct {
        catalog_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({});
        let result = client.execute("lakeformation", "POST", "/GetDataLakeSettings", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws lakeformation",
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
        let input = LakeFormationGetDataLakeSettingsInputBuilder::default().catalog_id(None::<String>).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lakeformation_get_data_lake_settings");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: LakeFormationGetDataLakeSettingsInput = serde_json::from_value(json).unwrap();
    }
}
