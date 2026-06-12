use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct GoogleWorkspaceConnection {
    /// OAuth2 client ID from Google Cloud Console.
    pub client_id: String,
    /// OAuth2 client secret from Google Cloud Console.
    pub client_secret: String,
    /// OAuth2 refresh token obtained from the consent flow.
    pub refresh_token: String,
    /// Optional: email of the user to impersonate (for domain-wide delegation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

impl_connection!(GoogleWorkspaceConnection, EpKind::GoogleWorkspace);
