use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, IamListInstanceProfilesForRoleInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::IamListInstanceProfilesForRole,
    "iam_list_instance_profiles_for_role",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    IamListInstanceProfilesForRole,
    API_INFO,
    struct {
        role_name: String,
        marker: Option<String>,
        max_items: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("RoleName".to_string(), self.role_name.clone());
        if let Some(m) = &self.marker {
            params.insert("Marker".to_string(), m.clone());
        }
        if let Some(v) = &self.max_items {
            params.insert("MaxItems".to_string(), v.to_string());
        }
        let form_body = build_query_body("ListInstanceProfilesForRole", "2010-05-08", &params);
        let result = client.execute_form("iam", &form_body).await?;

        span.add_event("received result from aws iam", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = IamListInstanceProfilesForRoleInputBuilder::default().role_name("r").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_list_instance_profiles_for_role");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"role_name": "r"});
        let _: IamListInstanceProfilesForRoleInput = serde_json::from_value(json).unwrap();
    }
}
