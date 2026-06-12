use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use chrono::{DateTime, Utc};
use database::db::methods::llm::{NewLlmCredential, StoredLlmCredential, UpdateLlmCredential};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use endpoint_core::llm_core::connection::LlmProvider;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateLlmCredentialRequest {
    pub provider: LlmProvider,
    pub api_key: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateLlmCredentialRequest {
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
pub struct LlmCredentialResponse {
    pub id: String,
    pub provider: LlmProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub has_api_key: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_last_four: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn stored_to_response(record: StoredLlmCredential) -> LlmCredentialResponse {
    let has_api_key = !record.api_key.trim().is_empty();
    let api_key_last_four = if has_api_key {
        let trimmed = record.api_key.trim();
        let suffix: String = trimmed.chars().rev().take(4).collect::<Vec<char>>().into_iter().rev().collect();
        Some(suffix)
    } else {
        None
    };

    LlmCredentialResponse {
        id: record.id.to_string(),
        provider: record.provider,
        label: record.label,
        description: record.description,
        base_url: record.base_url,
        has_api_key,
        api_key_last_four,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["LLM"],
    path = "/llm/credentials",
    operation_id = "llm_list_credentials",
    responses((status = 200, body = EdenResponse<Vec<LlmCredentialResponse>>))
)]
pub async fn list(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::READ, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let credentials = database
        .list_llm_credentials(auth.org_uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?
        .into_iter()
        .map(stored_to_response)
        .collect::<Vec<_>>();

    let response: Result<HttpResponse, actix_web::Error> = EdenResponse::response(credentials).into();
    response
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["LLM"],
    path = "/llm/credentials",
    operation_id = "llm_create_credential",
    request_body = CreateLlmCredentialRequest,
    responses((status = 201, body = EdenResponse<LlmCredentialResponse>))
)]
pub async fn create(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    payload: web::Json<CreateLlmCredentialRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let request = payload.into_inner();
    let api_key = request.api_key.trim().to_string();
    if api_key.is_empty() {
        return Err(error_handling(EpError::request("api_key must not be empty"), &mut span));
    }

    let credential_id = Uuid::new_v4();

    let record = database
        .insert_llm_credential(
            NewLlmCredential {
                id: credential_id,
                organization_uuid: auth.org_uuid(),
                provider: request.provider,
                label: request.label.as_deref(),
                description: request.description.as_deref(),
                base_url: request.base_url.as_deref(),
                api_key: &api_key,
            },
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let response = stored_to_response(record);
    Ok::<HttpResponse, actix_web::Error>(HttpResponse::Created().json(EdenResponse::response(response)))
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    patch,
    tags = ["LLM"],
    path = "/llm/credentials/{credential_id}",
    operation_id = "llm_update_credential",
    params(("credential_id" = String, Path, description = "Credential identifier")),
    request_body = UpdateLlmCredentialRequest,
    responses((status = 200, body = EdenResponse<LlmCredentialResponse>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn update(
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<String>,
    database: web::Data<EdenDb>,
    payload: web::Json<UpdateLlmCredentialRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let credential_id =
        Uuid::parse_str(path.as_str()).map_err(|_| error_handling(EpError::request("credential_id must be a valid UUID"), &mut span))?;

    let request = payload.into_inner();

    let record = database
        .update_llm_credential(
            UpdateLlmCredential {
                organization_uuid: auth.org_uuid(),
                credential_id,
                label: request.label.as_deref(),
                description: request.description.as_deref(),
                base_url: request.base_url.as_deref(),
                api_key: request.api_key.as_deref(),
            },
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let response = stored_to_response(record);
    EdenResponse::response(response).into()
}

/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["LLM"],
    path = "/llm/credentials/{credential_id}",
    operation_id = "llm_delete_credential",
    params(("credential_id" = String, Path, description = "Credential identifier")),
    responses((status = 204))
)]
pub async fn delete(
    auth: web::ReqData<ParsedJwt>,
    path: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let credential_id =
        Uuid::parse_str(path.as_str()).map_err(|_| error_handling(EpError::request("credential_id must be a valid UUID"), &mut span))?;

    let removed = database
        .delete_llm_credential(auth.org_uuid(), credential_id, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    if !removed {
        return Err(error_handling(EpError::request("credential not found"), &mut span));
    }

    Ok::<HttpResponse, actix_web::Error>(HttpResponse::NoContent().finish())
}
