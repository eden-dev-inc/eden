use crate::PosthogAsync;
use crate::connection::PosthogConnection;

use super::comm::PosthogClient;
use borsh::{BorshDeserialize, BorshSerialize};
use deadpool::unmanaged::Pool;
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_generic;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "PosthogConfig")]
pub struct PosthogConfig {
    pub read_conn: Option<PosthogConnection>,
    pub write_conn: Option<PosthogConnection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_conn: Option<PosthogConnection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_conn: Option<PosthogConnection>,
}

impl_ep_config_generic!(PosthogConfig, PosthogConnection, EpKind::Posthog);

impl fmt::Display for PosthogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read_conn: {:?}, write_conn: {:?}, admin_conn: {:?}, system_conn: {:?}",
            self.read_conn, self.write_conn, self.admin_conn, self.system_conn
        )
    }
}

impl RWPool<PosthogAsync> for PosthogConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Pool<PosthogClient>, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));
        let connection = match connection.as_any().downcast_ref::<PosthogConnection>() {
            Some(config) => config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };
        let mut clients = vec![];
        for _ in 0..4 {
            clients.push(PosthogClient::new(&connection).await?)
        }
        Ok(deadpool::unmanaged::Pool::from(clients))
    }
}
