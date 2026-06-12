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

const API_INFO: ApiInfo<AwsApi, KendraQueryInput> = ApiInfo::new(EpKind::Aws, AwsApi::KendraQuery, "kendra_query", ReqType::Read, true);

crate::aws_endpoint! {
    KendraQuery,
    API_INFO,
    struct {
        index_id: String,
        query_text: Option<String>,
        attribute_filter: Option<serde_json::Value>,
        page_number: Option<i64>,
        page_size: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_val = serde_json::json!({ "IndexId": self.index_id });
        if let Some(ref v) = self.query_text {
            body_val["QueryText"] = serde_json::Value::String(v.clone());
        }
        if let Some(ref v) = self.attribute_filter {
            body_val["AttributeFilter"] = v.clone();
        }
        if let Some(v) = self.page_number {
            body_val["PageNumber"] = serde_json::json!(v);
        }
        if let Some(v) = self.page_size {
            body_val["PageSize"] = serde_json::json!(v);
        }
        let result = client.execute_json_target("kendra", "AmazonKendra.Query", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws kendra", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = KendraQueryInputBuilder::default().index_id("index-123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kendra_query");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"index_id": "index-123"});
        let _: KendraQueryInput = serde_json::from_value(json).unwrap();
    }
}
