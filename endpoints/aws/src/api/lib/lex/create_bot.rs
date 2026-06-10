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

const API_INFO: ApiInfo<AwsApi, LexCreateBotInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::LexCreateBot, "lex_create_bot", ReqType::Write, true);

crate::aws_endpoint! {
    LexCreateBot,
    API_INFO,
    struct {
        bot_name: String,
        role_arn: String,
        data_privacy: serde_json::Value,
        idle_session_ttl_in_seconds: i64
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "botName": self.bot_name,
            "roleArn": self.role_arn,
            "dataPrivacy": self.data_privacy,
            "idleSessionTTLInSeconds": self.idle_session_ttl_in_seconds
        });
        let result = client.execute("lex", "PUT", "/bots/", None, Some(&body_val), None).await?;

        span.add_event("received result from aws lex", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LexCreateBotInputBuilder::default()
            .bot_name("my-bot")
            .role_arn("arn:aws:iam::123456789012:role/LexRole")
            .data_privacy(serde_json::json!({"childDirected": false}))
            .idle_session_ttl_in_seconds(300_i64)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lex_create_bot");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "bot_name": "my-bot",
            "role_arn": "arn:aws:iam::123456789012:role/LexRole",
            "data_privacy": {"childDirected": false},
            "idle_session_ttl_in_seconds": 300
        });
        let _: LexCreateBotInput = serde_json::from_value(json).unwrap();
    }
}
