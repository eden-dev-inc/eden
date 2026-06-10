use crate::api::lib::MysqlApi;
use crate::api::wrapper::output::MySqlRowsOutput;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{impl_simple_operation, EpOutput};
use mysql_core::{MysqlAsync, MysqlTx};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mysql_async::prelude::Query;
use mysql_async::Row;
use opentelemetry::trace::{TraceContextExt};
use telemetry::FastSpanAttribute;
use serde::Serialize;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MysqlApi, QueryFoldReadOnlyInput> = ApiInfo::new(
    EpKind::Mysql,
    MysqlApi::QueryFoldReadOnly,
    "Executes a read-only SQL query and returns results from MySql",
    ReqType::Read,
    true,
);

crate::mysql_endpoint! {
    QueryFoldReadOnly,
    API_INFO,
    struct {
        sql: String,
        init: InitInput,
        next: String,
    }
}

impl_simple_operation!(SimpleInput, MysqlAsync, MysqlTx, MysqlApi, MysqlRequest);

impl SimpleInput {
    pub async fn run_query<U>(&self, context: MysqlAsync) -> ResultEP<U>
    where
        U: Serialize,
    {
        self.sql
            .to_string()
            .reduce(context, &self.init, &self.next)
            .await
            .map_err(EpError::request)
    }

    #[named]
    pub async fn run_async_generic(
        &self,
        context: MysqlAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!(
            "{}.{}.{}",
            API_INFO.endpoint(),
            API_INFO.api,
            function_name!()
        ));


        let start = std::time::SystemTime::now();

        let result = self.run_query(context).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(MySqlRowsOutput(result).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(
        &self,
        context: &mut MysqlTx,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) {
        todo!("")
    }
}
