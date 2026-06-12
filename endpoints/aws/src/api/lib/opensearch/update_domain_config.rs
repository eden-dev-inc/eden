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

const API_INFO: ApiInfo<AwsApi, OpenSearchUpdateDomainConfigInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::OpenSearchUpdateDomainConfig,
    "Updates an OpenSearch domain config",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    OpenSearchUpdateDomainConfig,
    API_INFO,
    struct {
        domain_name: String,
        cluster_config: Option<serde_json::Value>,
        ebs_options: Option<serde_json::Value>,
        access_policies: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(v) = &self.cluster_config {
            body.insert("ClusterConfig".to_string(), v.clone());
        }
        if let Some(v) = &self.ebs_options {
            body.insert("EBSOptions".to_string(), v.clone());
        }
        if let Some(v) = &self.access_policies {
            body.insert("AccessPolicies".to_string(), serde_json::Value::String(v.clone()));
        }
        let body_val = serde_json::Value::Object(body);
        let path = format!("/2021-01-01/opensearch/domain/{}/config", self.domain_name);
        let result = client.execute("es", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws es", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = OpenSearchUpdateDomainConfigInputBuilder::default()
            .domain_name("my-domain")
            .cluster_config(None::<serde_json::Value>)
            .ebs_options(None::<serde_json::Value>)
            .access_policies(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "opensearch_update_domain_config");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"domain_name": "my-domain"});
        let _: OpenSearchUpdateDomainConfigInput = serde_json::from_value(json).unwrap();
    }
}
