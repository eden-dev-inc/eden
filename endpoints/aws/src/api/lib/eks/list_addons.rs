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

const API_INFO: ApiInfo<AwsApi, EksListAddonsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::EksListAddons,
    "Lists the installed add-ons for an EKS cluster",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    EksListAddons,
    API_INFO,
    struct {
        cluster_name: String,
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut query_parts = Vec::new();
        if let Some(m) = self.max_results {
            query_parts.push(format!("maxResults={}", m));
        }
        if let Some(t) = &self.next_token {
            query_parts.push(format!("nextToken={}", t));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let path = format!("/clusters/{}/addons", self.cluster_name);
        let result = client.execute("eks", "GET", &path, query.as_deref(), None, None).await?;

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
        let input = EksListAddonsInputBuilder::default().cluster_name("c").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eks_list_addons");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_name": "c"});
        let _: EksListAddonsInput = serde_json::from_value(json).unwrap();
    }
}
