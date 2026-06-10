use crate::EdenDb;
use crate::comm::endpoints::runtime_cleanup::evict_endpoint_runtime_resources;
use crate::comm::interlays::shard::ShardRouter;
use crate::comm::notifications::NotificationService;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use chrono::{DateTime, Utc};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::db::methods::delete::endpoint::DeleteEndpoint;
use database::db::methods::user_notifications::NotificationSeverity;
use database::db::rbac::ControlPlaneRbac;
use database::methods::delete::UuidsToUpdate;
use eden_core::auth::ParsedJwt;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::{CacheId, EndpointCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EdenId, EdenUuid, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{AllMetrics, MetadataMapWrapper, TelemetryDurations, TelemetryWrapper, TraceContext};
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::ep::EpConfig;
use endpoint_core::ep_core::settings::EdenSettings;
#[cfg(feature = "llm")]
use endpoint_core::llm_core::tools::clear_tool_discovery_cache;
use endpoint_schema::endpoint::EndpointSchema;
use ep_runtime::comp::MyEngineService;
use ep_runtime::servers::engine::method::endpoint::disconnect::DisconnectInfo;
use function_name::named;
use serde::Serialize;
use serde::ser::SerializeMap;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

/// Delete (disconnect) from an Endpoint
/// **Permissions**: See exact permission-bit checks in the handler body.
#[named]
// #[telemetry_with_error]
#[utoipa::path(
    delete,
    tags = ["Endpoints"],
    path="/endpoints/{endpoint}",
    operation_id = "delete_endpoint",
    responses((status = OK, body = EdenResponse<Response>))
)]
/// TODO add a telemetry_error macro that sets the span status to ERROR if returning an error
/// TODO add a endpoint_request_error macro that makes sure to run `finish_request()` on an error
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    engine_service: web::Data<MyEngineService>,
    database: web::Data<EdenDb>,
    shard_router: web::Data<ShardRouter>,
    // telemetry data
    metrics: web::Data<AllMetrics>,
    metadata: MetadataMapWrapper,
    labels: TelemetryLabels,
    durations: TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    // Initialize telemetry wrapper and span using fast-telemetry
    let mut telemetry_wrapper_value =
        TelemetryWrapper::new_with_telemetry(TraceContext::from(metadata.metadata().clone()), metrics.into_inner(), labels, durations);
    let telemetry_wrapper = &mut telemetry_wrapper_value;
    let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

    // Record endpoint request start time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_request(chrono::Utc::now()));

    let _settings = EdenSettings::from(req.headers());

    let org_uuid = auth.org_uuid();

    let organization_cache_uuid = OrganizationCacheUuid::new(None, org_uuid.to_owned());

    let endpoint_path = endpoint.into_inner();
    let endpoint_cache_object = if let Ok(endpoint_uuid) = uuid::Uuid::parse_str(&endpoint_path) {
        CacheObjectType::new(
            Some(EndpointCacheUuid::new(Some(organization_cache_uuid.clone()), EndpointUuid::new(endpoint_uuid))),
            None,
        )
    } else {
        CacheObjectType::new(
            None,
            Some(EndpointCacheId::new(Some(organization_cache_uuid.clone()), EndpointId::new(endpoint_path))),
        )
    };

    let endpoint_schema: EndpointSchema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &endpoint_cache_object,
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    telemetry_wrapper.mut_labels(|labels| {
        labels.set_endpoint_uuid(endpoint_schema.uuid());
        labels.set_endpoint_id(endpoint_schema.id());
        labels.set_endpoint_kind(endpoint_schema.kind());
    });

    verify_control_perms(
        &database,
        &auth,
        Some(endpoint_schema.endpoint_uuid().clone()),
        ControlPerms::DESTROY,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    engine_service
        .disconnect(
            &DisconnectInfo::new(endpoint_schema.endpoint_uuid().clone(), endpoint_schema.kind()),
            organization_cache_uuid.clone(),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_event("disconnected from endpoint", vec![]);

    let endpoint_uuid = endpoint_schema.endpoint_uuid();
    evict_endpoint_runtime_resources(&shard_router, auth.org_uuid(), &endpoint_uuid, "endpoint_deleted", telemetry_wrapper).await;

    let del_endpoint = <DeleteEndpoint as DeleteMethod<
        EndpointSchema,
        EndpointCacheUuid,
        EndpointUuid,
        EndpointCacheId,
        EndpointId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(endpoint_cache_object);

    let version_ms = chrono::Utc::now().timestamp_millis();
    database
        .control_plane_remove_entity(organization_cache_uuid.uuid(), IdKind::Endpoint, endpoint_schema.uuid().uuid(), version_ms, 0i64)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    span.add_event("deleted endpoint from RBAC", vec![]);

    let removed_uuids = delete_endpoint(&database, telemetry_wrapper, &del_endpoint).await.map_err(|e| error_handling(e, &mut span))?;

    span.add_event("removed endpoint from databases", vec![]);

    // Notify user about endpoint deletion
    let _ = NotificationService::notify_security_alert(
        &database,
        organization_cache_uuid.uuid(),
        auth.user_uuid().uuid(),
        &format!("Endpoint '{}' deleted", endpoint_schema.id()),
        &format!("Your {} endpoint has been disconnected and removed.", endpoint_schema.kind()),
        NotificationSeverity::Warning,
        None,
        None,
        telemetry_wrapper,
    )
    .await;

    let response: Result<actix_web::HttpResponse, actix_web::error::Error> = EdenResponse::response(
        Response::new(
            endpoint_schema.id(),
            endpoint_schema.uuid(),
            endpoint_schema.description(),
            endpoint_schema.config().clone(),
            endpoint_schema.kind(),
            endpoint_schema.created_at(),
            endpoint_schema.updated_at(),
            Modified { objects: removed_uuids },
        )
        .map_err(|e| error_handling(e, &mut span))?,
    )
    .into();

    // Record endpoint response end time for duration tracking
    telemetry_wrapper.mut_durations(|durations| durations.set_endpoint_response(chrono::Utc::now()));

    response.map(|mut response| {
        let mut extensions = response.extensions_mut();
        extensions.insert(telemetry_wrapper.labels().clone());
        extensions.insert(telemetry_wrapper.durations().clone())
    })
}

pub(crate) async fn delete_endpoint(
    db_manager: &EdenDb,
    telemetry_wrapper: &mut TelemetryWrapper,
    delete_endpoint: &DeleteEndpoint,
) -> ResultEP<UuidsToUpdate> {
    let deleted = <DeleteEndpoint as DeleteMethod<
        EndpointSchema,
        EndpointCacheUuid,
        EndpointUuid,
        EndpointCacheId,
        EndpointId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete(delete_endpoint, db_manager, telemetry_wrapper)
    .await?;
    #[cfg(feature = "llm")]
    {
        clear_tool_discovery_cache();
    }
    Ok(deleted)
}

#[derive(Debug, PartialEq)]
pub struct Response {
    id: EndpointId,
    uuid: EndpointUuid,
    description: Option<String>,
    config: serde_json::Value, // Box<dyn EpConfig> serialized
    kind: EpKind,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    modified_objects: Modified,
}

impl Serialize for Response {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("uuid", &self.uuid)?;
        map.serialize_entry("description", &self.description)?;
        map.serialize_entry("config", &self.config)?;
        // match self.kind {
        //     EpKind::Cassandra => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<CassandraConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Clickhouse => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<ClickhouseConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Http => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<HttpConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Llm => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<LlmConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Mongo => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<MongoConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Mssql => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<MssqlConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Mysql => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<MysqlConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Oracle => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<OracleConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Pinecone => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<PineconeConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Postgres => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<PostgresConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        //     EpKind::Redis => map.serialize_entry(
        //         "config",
        //         self.config
        //             .as_any()
        //             .downcast_ref::<RedisConfig>()
        //             .ok_or(ser::Error::custom("invalid config"))?,
        //     )?,
        // }
        map.serialize_entry("kind", &self.kind)?;
        map.serialize_entry("created_at", &self.created_at)?;
        map.serialize_entry("updated_at", &self.updated_at)?;
        map.serialize_entry("modified_objects", &self.modified_objects)?;
        map.end()
    }
}

impl PartialSchema for Response {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("id", EndpointId::schema())
                .property("uuid", EndpointUuid::schema())
                .property("description", String::schema())
                .property("config", Box::<dyn EpConfig>::schema())
                .property("kind", EpKind::schema())
                .property("created_at", String::schema())
                .property("updated_at", String::schema())
                .property("modified_objects", Modified::schema())
                .required("id")
                .required("uuid")
                .required("config")
                .required("kind")
                .required("created_at")
                .required("updated_at")
                .required("modified_objects")
                .build(),
        ))
    }
}

impl ToSchema for Response {}

impl Response {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: EndpointId,
        uuid: EndpointUuid,
        description: Option<String>,
        config: Box<dyn EpConfig>,
        kind: EpKind,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        modified_objects: Modified,
    ) -> ResultEP<Self> {
        Ok(Self {
            id,
            uuid,
            description,
            config: config.serialize()?,
            kind,
            created_at,
            updated_at,
            modified_objects,
        })
    }
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Modified {
    objects: UuidsToUpdate,
}
