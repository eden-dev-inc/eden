use crate::api::lib::MongoApi;
use crate::output::StringOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, RunOutput, ToOutput};
use ep_core::{ReqType, impl_simple_operation};
use error::EpError;
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, RawCommandReadOnlyInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::RawCommandReadOnly,
    "Execute a read-only raw MongoDB command string (for tools compatibility)",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct RawCommandReadOnlyInput {
        command: String,
        database: Option<String>,
    }
}

impl_simple_operation!(RawCommandReadOnlyInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl RawCommandReadOnlyInput {
    #[named]
    fn run_async_generic<'a>(&'a self, context: MongoAsync, telemetry_wrapper: &'a mut TelemetryWrapper) -> RunOutput<'a> {
        Box::pin(async move {
            let _span = telemetry_wrapper.client_tracer(format!("mongo.{}.{}", API_INFO.api, function_name!()));

            let mongo_context = context.get().await.map_err(EpError::connect)?;

            let json_val = serde_json::from_str::<serde_json::Value>(&self.command)
                .map_err(|e| EpError::metadata(format!("Invalid JSON command: {}", e)))?;

            let command_doc = mongodb::bson::to_document(&json_val)
                .map_err(|e| EpError::metadata(format!("Failed to convert command to BSON: {}", e)))?;

            let database = if let Some(db_name) = &self.database {
                mongo_context.database(db_name)
            } else {
                match mongo_context.default_database() {
                    Some(db) => db,
                    None => {
                        // For commands that don't require a specific database context,
                        // use 'admin' database as it's always available in MongoDB
                        mongo_context.database("admin")
                    }
                }
            };

            match database.run_command(command_doc, None).await {
                Ok(result) => {
                    let json_string = match serde_json::to_string_pretty(&result) {
                        Ok(s) => s,
                        Err(_) => {
                            format!("{}", result)
                        }
                    };

                    Ok(Box::new(StringOutput(json_string).to_output()) as Box<dyn EpOutput>)
                }
                Err(e) => Err(EpError::database(e)),
            }
        })
    }

    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}
