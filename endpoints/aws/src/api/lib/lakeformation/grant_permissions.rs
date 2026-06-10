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

const API_INFO: ApiInfo<AwsApi, LakeFormationGrantPermissionsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LakeFormationGrantPermissions,
    "lakeformation_grant_permissions",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    LakeFormationGrantPermissions,
    API_INFO,
    struct {
        principal: serde_json::Value,
        resource: serde_json::Value,
        permissions: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "Principal": self.principal,
            "Resource": self.resource,
            "Permissions": self.permissions
        });
        let result = client.execute("lakeformation", "POST", "/GrantPermissions", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws lakeformation",
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
        let input = LakeFormationGrantPermissionsInputBuilder::default()
            .principal(serde_json::json!({"k": "v"}))
            .resource(serde_json::json!({"k": "v"}))
            .permissions(vec!["SELECT".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lakeformation_grant_permissions");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "principal": {},
            "resource": {},
            "permissions": ["SELECT"]
        });
        let _: LakeFormationGrantPermissionsInput = serde_json::from_value(json).unwrap();
    }
}
