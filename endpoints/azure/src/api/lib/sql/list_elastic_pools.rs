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

const API_VERSION: &str = "2021-11-01";

const API_INFO: ApiInfo<AzureApi, SqlListElasticPoolsInput> =
    ApiInfo::new(EpKind::Azure, AzureApi::SqlListElasticPools, "List SQL elastic pools", ReqType::Read, true);

crate::azure_endpoint! {
    SqlListElasticPools,
    API_INFO,
    struct {
        subscription_id: Option<String>,
        resource_group: String,
        server_name: String
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
            "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Sql/servers/{}/elasticPools",
            sub, self.resource_group, self.server_name
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
        let input = SqlListElasticPoolsInputBuilder::default().resource_group("my-rg").server_name("my-server").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sql_list_elastic_pools");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "resource_group": "my-rg",
            "server_name": "my-server"
        });
        let _: SqlListElasticPoolsInput = serde_json::from_value(json).unwrap();
    }
}
