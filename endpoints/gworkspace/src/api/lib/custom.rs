use crate::api::lib::GoogleWorkspaceApi;
use crate::output::GoogleWorkspaceJsonOutput;
use crate::request::GoogleWorkspaceRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use gworkspace_core::{GoogleWorkspaceAsync, GoogleWorkspaceTx};
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<GoogleWorkspaceApi, CustomInput> = ApiInfo::new(
    EpKind::GoogleWorkspace,
    GoogleWorkspaceApi::Custom,
    "Execute a custom Google Workspace API request",
    ReqType::Read,
);

crate::gworkspace_endpoint! {
    Custom,
    API_INFO,
    struct {
        service: String,
        method: String,
        path: String,
        body: Option<Value>,
        query_params: Option<Value>
    }
}

impl_simple_operation!(SimpleInput, GoogleWorkspaceAsync, GoogleWorkspaceTx, GoogleWorkspaceApi, GoogleWorkspaceRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(
        &self,
        context: GoogleWorkspaceAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("google_workspace.{}.{}", API_INFO.api(), function_name!()));

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.request(&self.service, &self.method, &self.path, self.body.clone(), self.query_params.clone()).await?;

        span.add_event(
            "received result from google workspace",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );

        Ok(Box::new(GoogleWorkspaceJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut GoogleWorkspaceTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Google Workspace transaction support not implemented")
    }
}
