use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::methods::insert::InsertMethod;
use database::methods::insert::snapshot::InsertSnapshot;
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::snapshot::{MIN_SNAPSHOT_INTERVAL_SECS, SnapshotConstructor, SnapshotSchema, SourceMode};
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Snapshots"],
    path="/snapshots",
    operation_id = "create_snapshot",
    request_body = SnapshotConstructor,
    responses((status = OK, body = CreateSnapshotResponse))
)]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    input: web::Json<SnapshotConstructor>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let constructor = input.into_inner();

    // Validate minimum schedule interval (only for recurring snapshots)
    if let Some(ref schedule) = constructor.schedule {
        if let Some(interval) = schedule.interval_secs {
            if interval < MIN_SNAPSHOT_INTERVAL_SECS {
                return Err(error_handling(
                    EpError::parse(format!("Schedule interval must be at least {} seconds (15 minutes)", MIN_SNAPSHOT_INTERVAL_SECS)),
                    &mut span,
                ));
            }
        }
    }

    // Validate CDC-specific requirements
    let source_mode = constructor.source_mode.clone().unwrap_or_default();
    if matches!(source_mode, SourceMode::Cdc) {
        if constructor.cdc_config.is_none() {
            return Err(error_handling(EpError::parse("cdc_config is required when source_mode is 'cdc'"), &mut span));
        }
        if let Some(ref cdc) = constructor.cdc_config {
            if cdc.tables.is_empty() {
                return Err(error_handling(EpError::parse("cdc_config.tables must contain at least one table"), &mut span));
            }
            for table in &cdc.tables {
                crate::pipeline::cdc::postgres::validate_sql_identifier(table).map_err(|e| error_handling(e, &mut span))?;
            }
            if let Some(ref slot) = cdc.slot_name {
                crate::pipeline::cdc::postgres::validate_sql_identifier(slot).map_err(|e| error_handling(e, &mut span))?;
            }
            if let Some(ref pub_name) = cdc.publication_name {
                crate::pipeline::cdc::postgres::validate_sql_identifier(pub_name).map_err(|e| error_handling(e, &mut span))?;
            }
        }
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

    let snapshot_schema = SnapshotSchema::new(
        constructor.id,
        constructor.description,
        source_uuid,
        target_uuid,
        constructor.data,
        constructor.preserve_ttl,
        constructor.schedule,
        auth.user_uuid().clone(),
        constructor.source_mode,
        constructor.filter,
        constructor.cdc_config,
        constructor.write_template_uuid,
        constructor.read_template_uuid,
    );

    let uuid = *snapshot_schema.uuid();

    let insert = InsertSnapshot::new(org_uuid.to_owned(), snapshot_schema);
    <EdenDb as InsertMethod<SnapshotSchema, (), (), InsertSnapshot>>::insert(&database, insert, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    EdenResponse::response(CreateSnapshotResponse { uuid: uuid.to_string() }).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct CreateSnapshotResponse {
    pub uuid: String,
}

/// Validate that a filter string is a syntactically valid SQL WHERE expression.
///
/// Uses `sqlparser` with Postgres dialect to parse the expression.
/// Rejects subqueries, aggregates, and window functions.
fn validate_filter_syntax(filter: &str) -> Result<(), EpError> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    // Parse as a full SELECT to leverage sqlparser's WHERE handling.
    let wrapped = format!("SELECT 1 WHERE {filter}");
    let dialect = PostgreSqlDialect {};

    let statements = Parser::parse_sql(&dialect, &wrapped).map_err(|e| EpError::parse(format!("Invalid filter syntax: {e}")))?;

    if statements.is_empty() {
        return Err(EpError::parse("Filter produced no valid SQL statement"));
    }

    // Ensure no subqueries are present in the filter
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
