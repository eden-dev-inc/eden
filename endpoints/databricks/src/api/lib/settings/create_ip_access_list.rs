use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, CreateIpAccessListInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::CreateIpAccessList, "Create an IP access list", ReqType::Write);

crate::databricks_endpoint! {
    CreateIpAccessList,
    API_INFO,
    struct {
        label: String,
        list_type: String,
        ip_addresses: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let ip_addresses_value = serde_json::from_str::<serde_json::Value>(&self.ip_addresses).map_err(EpError::parse)?;

        let body = serde_json::json!({
            "label": self.label,
            "list_type": self.list_type,
            "ip_addresses": ip_addresses_value,
        });

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.post("/api/2.0/ip-access-lists", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created ip access list on databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(DatabricksJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatabricksTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Databricks transaction support not implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_ip_access_list_builder_serde() {
        let input = CreateIpAccessListInputBuilder::default()
            .label("my-list")
            .list_type("ALLOW")
            .ip_addresses("[\"10.0.0.1\"]")
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createipaccesslist");
        assert_eq!(json["label"], "my-list");
        assert_eq!(json["list_type"], "ALLOW");
    }
}
