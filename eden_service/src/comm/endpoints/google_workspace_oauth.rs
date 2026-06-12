use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

const GOOGLE_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[derive(Debug, Deserialize, ToSchema)]
pub struct GoogleWorkspaceOAuthExchangeRequest {
    pub client_id: String,
    pub client_secret: String,
    pub code: String,
    pub redirect_uri: String,
}

#[derive(Debug, Deserialize)]
struct GoogleWorkspaceOAuthTokenResponse {
    refresh_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema, PartialEq)]
pub struct GoogleWorkspaceOAuthExchangeResponse {
    pub refresh_token: String,
}

#[with_telemetry]
#[utoipa::path(
    post,
    tags = ["Endpoints"],
    path = "/endpoints/google_workspace/oauth/exchange",
    operation_id = "exchange_google_workspace_oauth_code",
    request_body = GoogleWorkspaceOAuthExchangeRequest,
    responses((status = 200, body = EdenResponse<GoogleWorkspaceOAuthExchangeResponse>))
)]
pub async fn exchange(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    payload: web::Json<GoogleWorkspaceOAuthExchangeRequest>,
) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let request = payload.into_inner();
    if request.client_id.trim().is_empty() {
        return Err(error_handling(EpError::request("client_id must not be empty"), &mut span));
    }
    if request.client_secret.trim().is_empty() {
        return Err(error_handling(EpError::request("client_secret must not be empty"), &mut span));
    }
    if request.code.trim().is_empty() {
        return Err(error_handling(EpError::request("code must not be empty"), &mut span));
    }
    if request.redirect_uri.trim().is_empty() {
        return Err(error_handling(EpError::request("redirect_uri must not be empty"), &mut span));
    }

    let client = Client::new();
    let response = client
        .post(GOOGLE_OAUTH_TOKEN_URL)
        .form(&[
            ("client_id", request.client_id.as_str()),
            ("client_secret", request.client_secret.as_str()),
            ("code", request.code.as_str()),
            ("grant_type", "authorization_code"),
            ("redirect_uri", request.redirect_uri.as_str()),
        ])
        .send()
        .await
        .map_err(|e| error_handling(EpError::request(format!("failed to contact Google OAuth endpoint: {e}")), &mut span))?;

    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|e| error_handling(EpError::request(format!("failed to read Google OAuth response: {e}")), &mut span))?;

    if !status.is_success() {
        let body_text = String::from_utf8_lossy(&body);
        return Err(error_handling(
            EpError::request(format!("Google OAuth exchange failed with status {status}: {body_text}")),
            &mut span,
        ));
    }

    let token_response: GoogleWorkspaceOAuthTokenResponse = serde_json::from_slice(&body)
        .map_err(|e| error_handling(EpError::request(format!("failed to parse Google OAuth token response: {e}")), &mut span))?;

    let Some(refresh_token) = token_response.refresh_token.filter(|token| !token.trim().is_empty()) else {
        return Err(error_handling(
            EpError::request("Google did not return a refresh token. Re-run consent with offline access, or revoke the app and try again."),
            &mut span,
        ));
    };

    Ok::<HttpResponse, actix_web::Error>(
        HttpResponse::Ok().json(EdenResponse::response(GoogleWorkspaceOAuthExchangeResponse { refresh_token })),
    )
}
