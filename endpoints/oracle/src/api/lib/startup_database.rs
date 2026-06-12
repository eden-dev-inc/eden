use crate::api::input::StartupModeWrapper;
use crate::api::lib::OracleApi;
use crate::api::output::EmptyOutput;
use crate::request::OracleRequest;
use crate::{ApiInfo, ReqType, RunOutput};
use ep_core::{EpOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use oracle_client::StartupMode;
use oracle_core::{OracleAsync, OracleTx};

use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<OracleApi, StartupDatabaseInput> =
    ApiInfo::new(EpKind::Oracle, OracleApi::StartupDatabase, "Oracle execute named", ReqType::Write, true);

crate::oracle_endpoint! {
    struct StartupDatabaseInput {
        mode: Vec<StartupModeWrapper>,
    }
}

impl_simple_operation!(SimpleInput, OracleAsync, OracleTx, OracleApi, OracleRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint(), API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::request)?;

        client
            .startup_database(self.mode.iter().map(|m| m.clone().into()).collect::<Vec<StartupMode>>().as_ref())
            .map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from oracle",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(EmptyOutput(()).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut OracleTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
