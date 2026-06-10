use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::els::{ELS_MAX_PAGE_LIMIT, ElsCommands, ElsPolicyRedacted, ElsStrategy, PaginationParams, UserPolicyAssignmentRedacted};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, EndpointId, EndpointUuid};
use eden_core::response::EdenResponse;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_schema::endpoint::EndpointSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
#[cfg(feature = "llm")]
use std::collections::HashMap;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
#[cfg(feature = "llm")]
use uuid::Uuid;

/// Get a redacted IAM credentials summary for an endpoint.
/// **Permissions**: See exact permission-bit checks in the handler body.
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/security/endpoints/{endpoint}",
    operation_id = "get_endpoint_security_summary",
    responses((status = OK, body = Response))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint = endpoint.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    verify_control_perms(&database, &auth, Some(endpoint_schema.endpoint_uuid()), ControlPerms::GRANT, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_cache = EndpointCacheUuid::new(Some(org_cache), endpoint_schema.uuid());
    let config_json = endpoint_schema.config().serialize().map_err(|e| error_handling(e, &mut span))?;

    let shared_credentials =
        summarize_shared_credentials(endpoint_schema.kind(), &config_json).map_err(|e| error_handling(e, &mut span))?;
    let shared_credentials = hydrate_llm_credential_refs(shared_credentials, auth.org_uuid(), &database, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let pagination = PaginationParams { limit: ELS_MAX_PAGE_LIMIT, offset: 0 };
    let policies = database
        .els_list_policies(&endpoint_cache, pagination)
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .map_items(ElsPolicyRedacted::from);
    let human_assignments = database
        .els_list_user_assignments(&endpoint_cache, pagination)
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .map_items(UserPolicyAssignmentRedacted::from);

    let response = Response {
        personal_credentials_supported: ElsStrategy::from_ep_kind(endpoint_schema.kind()).is_some(),
        shared_credentials,
        policies_total: policies.total,
        policies: policies.items,
        human_assignments_total: human_assignments.total,
        human_assignments: human_assignments.items,
    };

    EdenResponse::response(response).into()
}

const LEGACY_CONNECTION_SLOTS: &[(&str, &str)] = &[
    ("Read", "read_conn"),
    ("Write", "write_conn"),
    ("Admin", "admin_conn"),
    ("System", "system_conn"),
];

const TARGET_CREDENTIAL_SLOTS: &[(&str, &str)] = &[
    ("Read", "read_credentials"),
    ("Write", "write_credentials"),
    ("Admin", "admin_credentials"),
    ("System", "system_credentials"),
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SharedCredentialSummary {
    pub slot: String,
    pub config_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_label: Option<String>,
    pub uses_inline_secret: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response {
    pub personal_credentials_supported: bool,
    pub shared_credentials: Vec<SharedCredentialSummary>,
    pub policies_total: i64,
    pub policies: Vec<ElsPolicyRedacted>,
    pub human_assignments_total: i64,
    pub human_assignments: Vec<UserPolicyAssignmentRedacted>,
}

fn summarize_shared_credentials(
    kind: eden_core::format::endpoint::EpKind,
    config: &Value,
) -> Result<Vec<SharedCredentialSummary>, EpError> {
    let obj = config.as_object().ok_or_else(|| EpError::serde("Endpoint config must serialize to a JSON object"))?;

    let legacy_slots = LEGACY_CONNECTION_SLOTS
        .iter()
        .filter_map(|(slot, key)| summarize_slot(slot, key, obj.get(*key), obj, kind))
        .collect::<Vec<_>>();
    if !legacy_slots.is_empty() {
        return Ok(legacy_slots);
    }

    let target = obj.get("target");
    let target_slots = TARGET_CREDENTIAL_SLOTS
        .iter()
        .filter_map(|(slot, key)| summarize_slot(slot, key, obj.get(*key), obj, kind))
        .collect::<Vec<_>>();
    if !target_slots.is_empty() {
        return Ok(target_slots);
    }

    if let Some(target_value) = target.filter(|value| value.is_object()) {
        if looks_like_shared_connection(target_value, kind) {
            if let Some(summary) = summarize_slot("Read", "target", Some(target_value), obj, kind) {
                return Ok(vec![summary]);
            }
        }
    }

    Ok(Vec::new())
}

fn summarize_slot(
    slot: &str,
    key: &str,
    value: Option<&Value>,
    config: &Map<String, Value>,
    kind: eden_core::format::endpoint::EpKind,
) -> Option<SharedCredentialSummary> {
    let value = value?.as_object()?;
    let provider = extract_provider(value).or_else(|| config.get("target").and_then(extract_provider_from_value)).or_else(|| {
        if kind == eden_core::format::endpoint::EpKind::Llm {
            Some("llm".to_string())
        } else {
            None
        }
    });

    Some(SharedCredentialSummary {
        slot: slot.to_string(),
        config_key: key.to_string(),
        provider,
        credential_id: value.get("credential_id").and_then(Value::as_str).map(ToOwned::to_owned),
        credential_label: None,
        uses_inline_secret: object_uses_inline_secret(value),
    })
}

fn looks_like_shared_connection(value: &Value, kind: eden_core::format::endpoint::EpKind) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    object_uses_inline_secret(obj)
        || obj.get("credential_id").is_some()
        || kind == eden_core::format::endpoint::EpKind::Llm
        || obj.get("provider").is_some()
}

fn extract_provider(obj: &Map<String, Value>) -> Option<String> {
    obj.get("provider").and_then(Value::as_str).map(ToOwned::to_owned)
}

fn extract_provider_from_value(value: &Value) -> Option<String> {
    value.as_object().and_then(extract_provider)
}

fn object_uses_inline_secret(obj: &Map<String, Value>) -> bool {
    const SECRET_FIELDS: &[&str] = &[
        "access_key_id",
        "access_token",
        "api_key",
        "app_key",
        "auth",
        "headers",
        "inline_api_key",
        "oauth_token",
        "password",
        "private_key",
        "secret_access_key",
        "session_token",
        "token",
    ];

    obj.iter().any(|(key, value)| {
        SECRET_FIELDS.contains(&key.as_str())
            && !value.is_null()
            && (!value.is_string() || value.as_str().is_some_and(|inner| !inner.is_empty()))
    })
}

#[cfg(feature = "llm")]
async fn hydrate_llm_credential_refs(
    summaries: Vec<SharedCredentialSummary>,
    organization_uuid: &eden_core::format::OrganizationUuid,
    database: &EdenDb,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> Result<Vec<SharedCredentialSummary>, EpError> {
    let credential_ids = summaries
        .iter()
        .filter_map(|summary| summary.credential_id.as_deref())
        .filter_map(|id| Uuid::parse_str(id).ok())
        .collect::<Vec<_>>();

    let credentials = database.fetch_llm_credentials_by_ids(organization_uuid, &credential_ids, telemetry_wrapper).await?;
    let credential_map = credentials.into_iter().map(|credential| (credential.id, credential)).collect::<HashMap<_, _>>();

    Ok(summaries
        .into_iter()
        .map(|mut summary| {
            if let Some(credential_id) = summary.credential_id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) {
                if let Some(credential) = credential_map.get(&credential_id) {
                    summary.provider = summary.provider.clone().or_else(|| Some(credential.provider.to_string()));
                    summary.credential_label = credential.label.clone();
                }
            }
            summary
        })
        .collect())
}

#[cfg(not(feature = "llm"))]
async fn hydrate_llm_credential_refs(
    summaries: Vec<SharedCredentialSummary>,
    _organization_uuid: &eden_core::format::OrganizationUuid,
    _database: &EdenDb,
    _telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) -> Result<Vec<SharedCredentialSummary>, EpError> {
    Ok(summaries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eden_core::format::endpoint::EpKind;
    use serde_json::json;

    #[test]
    fn summarizes_legacy_connection_slots() {
        let config = json!({
            "read_conn": { "url": "mongo://example", "auth": { "username": "reader" } },
            "admin_conn": { "url": "mongo://example-admin", "password": "secret" }
        });

        let summaries = summarize_shared_credentials(EpKind::Mongo, &config).expect("summaries");

        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].slot, "Read");
        assert_eq!(summaries[0].config_key, "read_conn");
        assert!(summaries[0].uses_inline_secret);
        assert_eq!(summaries[1].slot, "Admin");
    }

    #[test]
    fn summarizes_target_split_credentials() {
        let config = json!({
            "target": { "provider": "openai", "defaults": { "model": "gpt-5.4-mini" } },
            "read_credentials": { "credential_id": "2294ad2f-c356-4cfc-b0d3-fd85a9281cff" },
            "write_credentials": { "inline_api_key": "sk-inline" }
        });

        let summaries = summarize_shared_credentials(EpKind::Llm, &config).expect("summaries");

        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].slot, "Read");
        assert_eq!(summaries[0].provider.as_deref(), Some("openai"));
        assert_eq!(summaries[0].credential_id.as_deref(), Some("2294ad2f-c356-4cfc-b0d3-fd85a9281cff"));
        assert!(!summaries[0].uses_inline_secret);
        assert_eq!(summaries[1].slot, "Write");
        assert!(summaries[1].uses_inline_secret);
    }

    #[test]
    fn falls_back_to_target_when_no_split_credentials_exist() {
        let config = json!({
            "target": { "provider": "datadog", "api_key": "dd-key", "app_key": "dd-app" }
        });

        let summaries = summarize_shared_credentials(EpKind::Datadog, &config).expect("summaries");

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].slot, "Read");
        assert_eq!(summaries[0].config_key, "target");
        assert_eq!(summaries[0].provider.as_deref(), Some("datadog"));
        assert!(summaries[0].uses_inline_secret);
    }
}
