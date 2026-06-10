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

const API_INFO: ApiInfo<AzureApi, KeyVaultCreateOrImportCertificateInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::KeyVaultCreateOrImportCertificate,
    "Import a certificate into key vault",
    ReqType::Write,
    true,
);

crate::azure_endpoint! {
    KeyVaultCreateOrImportCertificate,
    API_INFO,
    struct {
        vault_url: String,
        cert_name: String,
        value: String,
        password: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/certificates/{}/import", self.cert_name);

        let mut body = serde_json::json!({
            "value": self.value
        });
        if let Some(pwd) = &self.password {
            body["pwd"] = serde_json::Value::String(pwd.clone());
        }

        let result = client.execute_data_plane(&self.vault_url, "POST", &path, "7.4", Some(&body)).await?;

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
        let input = KeyVaultCreateOrImportCertificateInputBuilder::default()
            .vault_url("https://my-vault.vault.azure.net")
            .cert_name("my-cert")
            .value("base64encodedcert")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "keyvault_create_or_import_certificate");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "vault_url": "https://my-vault.vault.azure.net",
            "cert_name": "my-cert",
            "value": "base64encodedcert"
        });
        let _: KeyVaultCreateOrImportCertificateInput = serde_json::from_value(json).unwrap();
    }
}
