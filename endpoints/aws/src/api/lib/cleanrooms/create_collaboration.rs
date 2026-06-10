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

const API_INFO: ApiInfo<AwsApi, CleanRoomsCreateCollaborationInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CleanRoomsCreateCollaboration,
    "cleanrooms_create_collaboration",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    CleanRoomsCreateCollaboration,
    API_INFO,
    struct {
        name: String,
        description: String,
        members: serde_json::Value,
        creator_member_abilities: Vec<String>,
        query_log_status: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "name": self.name,
            "description": self.description,
            "members": self.members,
            "creatorMemberAbilities": self.creator_member_abilities,
            "queryLogStatus": self.query_log_status
        });
        let result = client.execute("cleanrooms", "POST", "/collaborations", None, Some(&body_val), None).await?;

        span.add_event("received result from aws service", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = CleanRoomsCreateCollaborationInputBuilder::default()
            .name("n")
            .description("d")
            .members(serde_json::json!([]))
            .creator_member_abilities(vec![])
            .query_log_status("ENABLED")
            .build()
            .unwrap();
        assert_eq!(serde_json::to_value(&input).unwrap()["type"], "cleanrooms_create_collaboration");
    }

    #[test]
    fn deserialize_minimal() {
        let _: CleanRoomsCreateCollaborationInput = serde_json::from_value(serde_json::json!({
            "name": "n",
            "description": "d",
            "members": [],
            "creator_member_abilities": [],
            "query_log_status": "ENABLED"
        }))
        .unwrap();
    }
}
