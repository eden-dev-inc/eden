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

const API_INFO: ApiInfo<AzureApi, StorageListBlobsInput> =
    ApiInfo::new(EpKind::Azure, AzureApi::StorageListBlobs, "List blobs in a container", ReqType::Read, true);

crate::azure_endpoint! {
    StorageListBlobs,
    API_INFO,
    struct {
        storage_account_url: String,
        container_name: String
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let base_url = format!("{}/{}", self.storage_account_url.trim_end_matches('/'), self.container_name);
        let path = format!("/{}?restype=container&comp=list", self.container_name);

        let result = client.execute_data_plane(&base_url, "GET", &path, "2023-11-03", None).await?;

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
        let input = StorageListBlobsInputBuilder::default()
            .storage_account_url("https://mystorage.blob.core.windows.net")
            .container_name("mycontainer")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "storage_list_blobs");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "storage_account_url": "https://mystorage.blob.core.windows.net",
            "container_name": "mycontainer"
        });
        let _: StorageListBlobsInput = serde_json::from_value(json).unwrap();
    }
}
