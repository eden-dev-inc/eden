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

const API_INFO: ApiInfo<AwsApi, ServiceQuotasRequestServiceQuotaIncreaseInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ServiceQuotasRequestServiceQuotaIncrease,
    "Submits a quota increase request for the specified quota",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ServiceQuotasRequestServiceQuotaIncrease,
    API_INFO,
    struct {
        service_code: String,
        quota_code: String,
        desired_value: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val =
            serde_json::json!({"ServiceCode": self.service_code, "QuotaCode": self.quota_code, "DesiredValue": self.desired_value});
        let result = client
            .execute_json_target("servicequotas", "ServiceQuotasV20190624.RequestServiceQuotaIncrease", Some(&body_val), "1.1")
            .await?;

        span.add_event(
            "received result from aws servicequotas",
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
        let input = ServiceQuotasRequestServiceQuotaIncreaseInputBuilder::default()
            .service_code("s3")
            .quota_code("L-DC2B2D3D")
            .desired_value(serde_json::json!(100.0))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "servicequotas_request_service_quota_increase");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"service_code": "s3", "quota_code": "L-DC2B2D3D", "desired_value": 100.0});
        let _: ServiceQuotasRequestServiceQuotaIncreaseInput = serde_json::from_value(json).unwrap();
    }
}
