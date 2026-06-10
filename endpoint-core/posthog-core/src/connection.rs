use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct PosthogConnection {
    /// PostHog personal API key (prefix `phx_`).
    pub api_key: String,
    /// PostHog project ID (required, used in API URL path).
    pub project_id: String,
    /// Optional base URL override. Defaults to `https://us.posthog.com`.
    /// Use `https://eu.posthog.com` for EU cloud or a custom URL for self-hosted.
    #[serde(default)]
    pub base_url: Option<String>,
}

impl_connection!(PosthogConnection, EpKind::Posthog);
