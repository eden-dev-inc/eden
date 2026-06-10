use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::methods::insert::InsertMethod;
use database::methods::insert::pipeline::InsertPipeline;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::pipeline::{PipelineConstructor, PipelineSchema};
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Pipelines"],
    path="/pipelines",
    operation_id = "create_pipeline",
    request_body = PipelineConstructor,
    responses((status = OK, body = CreatePipelineResponse))
)]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<PipelineConstructor>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let constructor = input.into_inner();

    // Validate CDC config
    if constructor.cdc_config.tables.is_empty() {
        return Err(error_handling(EpError::parse("cdc_config.tables must contain at least one table"), &mut span));
    }
    for table in &constructor.cdc_config.tables {
        crate::pipeline::cdc::postgres::validate_sql_identifier(table).map_err(|e| error_handling(e, &mut span))?;
    }
    if let Some(ref slot) = constructor.cdc_config.slot_name {
        crate::pipeline::cdc::postgres::validate_sql_identifier(slot).map_err(|e| error_handling(e, &mut span))?;
    }
    if let Some(ref pub_name) = constructor.cdc_config.publication_name {
        crate::pipeline::cdc::postgres::validate_sql_identifier(pub_name).map_err(|e| error_handling(e, &mut span))?;
    }

    // Validate filter syntax if provided
    if let Some(ref filter) = constructor.filter {
        if filter.trim().is_empty() {
            return Err(error_handling(EpError::parse("filter must not be empty when provided"), &mut span));
        }
        if let Err(e) = validate_filter_syntax(filter) {
            return Err(error_handling(e, &mut span));
        }
    }

    // Resolve source and target endpoint UUIDs
    let source_uuid = resolve_endpoint_uuid(&database, org_uuid, &constructor.source_endpoint, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let target_uuid = resolve_endpoint_uuid(&database, org_uuid, &constructor.target_endpoint, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let pipeline_schema = PipelineSchema::new(
        constructor.id,
        constructor.description,
        source_uuid,
        target_uuid,
        constructor.filter,
        constructor.cdc_config,
        constructor.write_template_uuid,
        constructor.read_template_uuid,
        auth.user_uuid().clone(),
    );

    let uuid = *pipeline_schema.uuid();

    let insert = InsertPipeline::new(org_uuid.to_owned(), pipeline_schema);
    <EdenDb as InsertMethod<PipelineSchema, (), (), InsertPipeline>>::insert(&database, insert, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(CreatePipelineResponse { uuid: uuid.to_string() }).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct CreatePipelineResponse {
    pub uuid: String,
}

/// Validate that a filter string is a syntactically valid SQL WHERE expression.
fn validate_filter_syntax(filter: &str) -> Result<(), EpError> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let wrapped = format!("SELECT 1 WHERE {filter}");
    let dialect = PostgreSqlDialect {};

    let statements = Parser::parse_sql(&dialect, &wrapped).map_err(|e| EpError::parse(format!("Invalid filter syntax: {e}")))?;

    if statements.is_empty() {
        return Err(EpError::parse("Filter produced no valid SQL statement"));
    }

    let sql_str = statements[0].to_string();
    let lower = sql_str.to_lowercase();
    if lower.contains("select ") && lower.matches("select ").count() > 1 {
        return Err(EpError::parse("Subqueries are not allowed in filter expressions"));
    }

    Ok(())
}

/// Resolve an endpoint identifier (id or uuid string) to its UUID.
async fn resolve_endpoint_uuid(
    database: &EdenDb,
    _org_uuid: &eden_core::format::OrganizationUuid,
    endpoint_ref: &str,
    telemetry: &mut eden_core::telemetry::TelemetryWrapper,
) -> Result<uuid::Uuid, eden_core::error::EpError> {
    use database::db::cache::CacheFunctions;
    use eden_core::format::cache_id::EndpointCacheId;
    use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
    use eden_core::format::{CacheObjectType, EdenUuid, EndpointId, EndpointUuid};
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_schema::endpoint::EndpointSchema;

    let org_key = OrganizationCacheUuid::new(None, _org_uuid.clone());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            database,
            &CacheObjectType::from((Some(org_key), endpoint_ref.to_string())),
            telemetry,
        )
        .await?;

    Ok(endpoint_schema.uuid().uuid().to_owned())
}
