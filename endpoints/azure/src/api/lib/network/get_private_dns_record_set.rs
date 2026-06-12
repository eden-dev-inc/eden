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

const API_VERSION: &str = "2024-01-01";

const API_INFO: ApiInfo<AzureApi, NetworkGetPrivateDnsRecordSetInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::NetworkGetPrivateDnsRecordSet,
    "Get a private DNS record set",
    ReqType::Read,
    true,
);

crate::azure_endpoint! {
    NetworkGetPrivateDnsRecordSet,
    API_INFO,
    struct {
        subscription_id: Option<String>,
        resource_group: String,
        zone_name: String,
        record_type: String,
        record_set_name: String
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let sub = self
            .subscription_id
            .as_deref()
            .or(client.subscription_id())
            .ok_or_else(|| EpError::request("subscription_id required"))?;

        let path = format!(
            "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network/privateDnsZones/{}/{}/{}",
            sub, self.resource_group, self.zone_name, self.record_type, self.record_set_name
        );

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
        let input = NetworkGetPrivateDnsRecordSetInputBuilder::default()
            .resource_group("my-rg")
            .zone_name("my-zone")
            .record_type("A")
            .record_set_name("my-record")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "network_get_private_dns_record_set");
    }

    #[test]
    fn deserialize_minimal() {
        let json =
            serde_json::json!({"resource_group": "my-rg", "zone_name": "my-zone", "record_type": "A", "record_set_name": "my-record"});
        let _: NetworkGetPrivateDnsRecordSetInput = serde_json::from_value(json).unwrap();
    }
}
