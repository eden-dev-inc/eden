use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, EndpointOperation, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, QueryInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Query,
    "Executes a query-protocol AWS API request (EC2, IAM, STS, CloudFormation, SQS, SNS, RDS, \
     CloudWatch, AutoScaling, ElastiCache, Redshift, etc.) using form-encoded parameters",
    ReqType::Write,
    true,
);

#[derive(Debug, Clone, Default, utoipa::ToSchema, schemars::JsonSchema, Deserialize)]
pub struct QueryInput {
    pub service: String,
    pub action: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub params: HashMap<String, String>,
}

impl Serialize for QueryInput {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("QueryInput", 5)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("service", &self.service)?;
        state.serialize_field("action", &self.action)?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("params", &self.params)?;
        state.end()
    }
}

type SimpleInput = QueryInput;

impl EndpointOperation for QueryInput {}

#[allow(non_snake_case)]
#[ctor::ctor]
fn __register_aws_operation_for_query() {
    crate::serde::register_operation::<QueryInput>();
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

fn default_version_for_service(service: &str) -> &'static str {
    match service.to_lowercase().as_str() {
        "ec2" => "2016-11-15",
        "iam" => "2010-05-08",
        "sts" => "2011-06-15",
        "cloudformation" => "2010-05-15",
        "sqs" => "2012-11-05",
        "sns" => "2010-03-31",
        "autoscaling" => "2011-01-01",
        "rds" => "2014-10-31",
        "cloudwatch" | "monitoring" => "2010-08-01",
        "elasticache" => "2015-02-02",
        "redshift" => "2012-12-01",
        "route53" => "2013-04-01",
        "elb" | "elasticloadbalancing" => "2012-06-01",
        "elbv2" | "elasticloadbalancingv2" => "2015-12-01",
        "emr" => "2009-03-31",
        "glacier" => "2012-06-01",
        _ => "2023-01-01",
    }
}

fn percent_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b => encoded.push_str(&format!("%{b:02X}")),
        }
    }
    encoded
}

fn build_query_body(action: &str, version: &str, params: &HashMap<String, String>) -> String {
    let mut parts = vec![
        format!("Action={}", percent_encode(action)),
        format!("Version={}", percent_encode(version)),
    ];
    for (k, v) in params {
        parts.push(format!("{}={}", percent_encode(k), percent_encode(v)));
    }
    parts.join("&")
}

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;

        let version = self.version.as_deref().unwrap_or_else(|| default_version_for_service(&self.service));

        let form_body = build_query_body(&self.action, version, &self.params);

        let result = client.execute_form(&self.service, &form_body).await?;

        span.add_event("received query result from aws", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // AWS does not support transactions.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_query_body_basic() {
        let body = build_query_body("DescribeInstances", "2016-11-15", &HashMap::new());
        assert_eq!(body, "Action=DescribeInstances&Version=2016-11-15");
    }

    #[test]
    fn build_query_body_with_params() {
        let mut params = HashMap::new();
        params.insert("MaxResults".to_string(), "10".to_string());
        let body = build_query_body("ListUsers", "2010-05-08", &params);
        assert!(body.contains("Action=ListUsers"));
        assert!(body.contains("Version=2010%2D05%2D08") || body.contains("Version=2010-05-08"));
        assert!(body.contains("MaxResults=10"));
    }

    #[test]
    fn default_versions_known_services() {
        assert_eq!(default_version_for_service("ec2"), "2016-11-15");
        assert_eq!(default_version_for_service("iam"), "2010-05-08");
        assert_eq!(default_version_for_service("sts"), "2011-06-15");
        assert_eq!(default_version_for_service("cloudformation"), "2010-05-15");
    }

    #[test]
    fn serialize_adds_type_field() {
        let input = QueryInput {
            service: "ec2".to_string(),
            action: "DescribeInstances".to_string(),
            version: None,
            params: HashMap::new(),
        };
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "query");
        assert_eq!(json["service"], "ec2");
        assert_eq!(json["action"], "DescribeInstances");
    }

    #[test]
    fn deserialize_defaults_version_and_params() {
        let json = serde_json::json!({ "service": "iam", "action": "ListUsers" });
        let input: QueryInput = serde_json::from_value(json).unwrap();
        assert_eq!(input.service, "iam");
        assert_eq!(input.action, "ListUsers");
        assert!(input.version.is_none());
        assert!(input.params.is_empty());
    }
}
