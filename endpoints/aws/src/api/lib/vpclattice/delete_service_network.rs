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

const API_INFO: ApiInfo<AwsApi, VpcLatticeDeleteServiceNetworkInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::VpcLatticeDeleteServiceNetwork,
    "vpclattice_delete_service_network",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    VpcLatticeDeleteServiceNetwork,
    API_INFO,
    struct {
        service_network_identifier: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;
        let path = format!("/servicenetworks/{}", self.service_network_identifier);
        let result = client.execute("vpc-lattice", "DELETE", &path, None, None, None).await?;
        span.add_event(
            "received result from aws vpc-lattice",
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
        let input = VpcLatticeDeleteServiceNetworkInputBuilder::default().service_network_identifier("sn-abc123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "vpclattice_delete_service_network");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"service_network_identifier": "sn-abc123"});
        let _: VpcLatticeDeleteServiceNetworkInput = serde_json::from_value(json).unwrap();
    }
}
