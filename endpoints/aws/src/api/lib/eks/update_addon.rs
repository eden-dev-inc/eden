use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, EksUpdateAddonInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EksUpdateAddon, "Updates an EKS add-on", ReqType::Write, true);

crate::aws_endpoint! {
    EksUpdateAddon,
    API_INFO,
    struct {
        cluster_name: String,
        addon_name: String,
        addon_version: Option<String>,
        service_account_role_arn: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::json!({});
        if let Some(v) = &self.addon_version {
            body["addonVersion"] = Value::String(v.clone());
        }
        if let Some(r) = &self.service_account_role_arn {
            body["serviceAccountRoleArn"] = Value::String(r.clone());
        }
        let path = format!("/clusters/{}/addons/{}", self.cluster_name, self.addon_name);
        let result = client.execute("eks", "POST", &path, None, Some(&body), None).await?;

        span.add_event("received result from aws eks", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = EksUpdateAddonInputBuilder::default().cluster_name("c").addon_name("vpc-cni").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eks_update_addon");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_name": "c", "addon_name": "vpc-cni"});
        let _: EksUpdateAddonInput = serde_json::from_value(json).unwrap();
    }
}
