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

const API_INFO: ApiInfo<DatabricksApi, SetPermissionsInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::SetPermissions, "Set permissions for an object", ReqType::Write);

crate::databricks_endpoint! {
    SetPermissions,
    API_INFO,
    struct {
        object_type: String,
        object_id: String,
        access_control_list: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let acl = serde_json::from_str::<serde_json::Value>(&self.access_control_list).map_err(EpError::parse)?;

        let body = serde_json::json!({
            "access_control_list": acl,
        });

        let value = client.post(&format!("/api/2.0/permissions/{}/{}", self.object_type, self.object_id), Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "set permissions on databricks",
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
    fn set_permissions_builder_serde() {
        let input = SetPermissionsInputBuilder::default()
            .object_type("clusters")
            .object_id("1234-567890-abcde")
            .access_control_list(r#"[{"user_name":"user@example.com","permission_level":"CAN_MANAGE"}]"#)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "setpermissions");
        assert_eq!(json["object_type"], "clusters");
        assert_eq!(json["object_id"], "1234-567890-abcde");
    }
}
