use crate::EdenDb;
use chrono::{DateTime, Utc};
use database::cache::CacheFunctions;
use eden_core::error::ResultEP;
use eden_core::format::cache_id::ApiCacheId;
use eden_core::format::cache_uuid::ApiCacheUuid;
use eden_core::format::{ApiId, ApiUuid, CacheObjectType};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::api::ApiSchema;
use endpoint_core::ep_core::database::schema::template::TemplateSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub mod delete;
pub mod get;
pub mod patch;
pub mod post;
pub mod run;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiResponse {
    id: ApiId,
    uuid: ApiUuid,
    templates: Vec<TemplateSchema>,
    response_logic: Option<serde_json::Value>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ApiResponse {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ApiId,
        uuid: ApiUuid,
        templates: Vec<TemplateSchema>,
        response_logic: Option<serde_json::Value>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self { id, uuid, templates, response_logic, created_at, updated_at }
    }
}

pub(crate) async fn get_api_schema(
    database_manager: &EdenDb,
    api_cache_object: &CacheObjectType<ApiCacheUuid, ApiCacheId>,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<ApiSchema> {
    <EdenDb as CacheFunctions<ApiSchema, ApiCacheUuid, ApiUuid, ApiCacheId, ApiId>>::get_from_cache(
        database_manager,
        api_cache_object,
        telemetry_wrapper,
    )
    .await
}
