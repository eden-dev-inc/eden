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

const API_INFO: ApiInfo<AwsApi, ServiceQuotasListServicesInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ServiceQuotasListServices,
    "Lists the names and codes for the Amazon Web Services integrated with Service Quotas",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ServiceQuotasListServices,
    API_INFO,
    struct {
        next_token: Option<String>,
        max_results: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute_json_target("servicequotas", "ServiceQuotasV20190624.ListServices", None, "1.1").await?;

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
        let input = ServiceQuotasListServicesInputBuilder::default().next_token(None::<String>).max_results(None::<i64>).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "servicequotas_list_services");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: ServiceQuotasListServicesInput = serde_json::from_value(json).unwrap();
    }
}
