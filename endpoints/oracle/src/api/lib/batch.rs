mod append_row;
mod append_row_common;
mod append_row_named;
mod bind_count;
mod bind_internal;
mod bind_names;
mod check_batch_index;
mod close;
mod execute;
mod execute_sub;
mod is_dml;
mod is_plsql;
mod row_counts;
mod set;
mod set_batch_index;
mod set_type;
mod statement_type;

use crate::api::lib::OracleApi;
use crate::api::output::EmptyOutput;
use crate::request::OracleRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use oracle_core::{OracleAsync, OracleTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<OracleApi, BatchInput> = ApiInfo::new(EpKind::Oracle, OracleApi::Batch, "Oracle Batch", ReqType::Write, true);

crate::oracle_endpoint! {
    struct BatchInput {
        sql: String,
        max_batch_size: usize,
    }
}

impl_simple_operation!(SimpleInput, OracleAsync, OracleTx, OracleApi, OracleRequest);

impl SimpleInput {
    pub async fn run_batch(&self, context: OracleAsync) -> ResultEP<()> {
        let client = context.get().await.map_err(EpError::request)?;

        let _batch = client.batch(&self.sql, self.max_batch_size).build().map_err(EpError::request)?;

        Ok(())
    }

    #[named]
    async fn run_async_generic(&self, context: OracleAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint(), API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        self.run_batch(context).await?;

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
