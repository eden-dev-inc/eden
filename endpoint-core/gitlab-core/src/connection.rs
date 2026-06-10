use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct GitlabConnection {
    /// The GitLab personal access token, project token, or OAuth token.
    pub token: String,
    /// Optional base URL override for self-managed GitLab instances. Defaults to `https://gitlab.com`.
    #[serde(default)]
    pub base_url: Option<String>,
}

impl_connection!(GitlabConnection, EpKind::Gitlab);
