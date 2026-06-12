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

const API_INFO: ApiInfo<AzureApi, KeyVaultDeleteKeyInput> =
    ApiInfo::new(EpKind::Azure, AzureApi::KeyVaultDeleteKey, "Delete a key from key vault", ReqType::Write, true);

crate::azure_endpoint! {
    KeyVaultDeleteKey,
    API_INFO,
    struct {
        vault_url: String,
        key_name: String
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/keys/{}", self.key_name);

        let result = client.execute_data_plane(&self.vault_url, "DELETE", &path, "7.4", None).await?;

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
        let input = KeyVaultDeleteKeyInputBuilder::default()
            .vault_url("https://my-vault.vault.azure.net")
            .key_name("my-key")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "keyvault_delete_key");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "vault_url": "https://my-vault.vault.azure.net",
            "key_name": "my-key"
        });
        let _: KeyVaultDeleteKeyInput = serde_json::from_value(json).unwrap();
    }
}
