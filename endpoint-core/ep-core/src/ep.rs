// Suppress async_fn_in_trait warning because we don't need to specify auto trait bounds for these traits.
#![allow(async_fn_in_trait)]
use error::{ConnectError, EpError, ResultEP};
use format::cache_uuid::EndpointCacheUuid;
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Debug},
    future::Future,
};
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use tokio_postgres::types::ToSql;
use utoipa::openapi::{OneOfBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};
// pub use schema::{EndpointSchemaInput, deserialize_config};

#[derive(Debug, Clone)]
pub struct EndpointAPIRequest {}

impl ToSchema for EndpointAPIRequest {}

impl PartialSchema for EndpointAPIRequest {
    fn schema() -> RefOr<Schema> {
        crate::database::schema::endpoint::EndpointRequestInput::schema()
    }
}

pub trait Route {
    fn is_async(&self) -> bool;
}

pub trait EpOperation {
    fn kind(&self) -> EpKind;
    fn as_any(&self) -> &dyn Any;
    fn as_operation(self: Box<Self>) -> Box<dyn EpOperation>;
}

pub trait EpConnection: Send + Sync + Debug {
    fn as_connection(self: Box<Self>) -> Box<dyn EpConnection>;
    fn as_any(&self) -> &dyn Any;
    fn kind(&self) -> EpKind;
    fn clone_box(&self) -> Box<dyn EpConnection>;
}

impl Clone for Box<dyn EpConnection> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub trait EpRouter: Send + Sync + Debug {
    fn as_router(self: Box<Self>) -> Box<dyn EpRouter>;
    fn as_any(&self) -> &dyn Any;
    fn any_mut(&mut self) -> &mut dyn Any;
}

pub trait EpClient: Send + Sync {}

/// Which privilege tier a connection operates at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionTier {
    Read,
    Write,
    Admin,
    System,
}

pub trait EpConfig: Send + Sync + Debug + ToSql {
    fn as_config(&self) -> Box<dyn EpConfig>;
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn kind(&self) -> EpKind;
    fn clone_box(&self) -> Box<dyn EpConfig>;
    fn read_conn(&self) -> Option<Box<dyn EpConnection>>;
    fn write_conn(&self) -> Option<Box<dyn EpConnection>>;
    fn admin_conn(&self) -> Option<Box<dyn EpConnection>>;
    fn system_conn(&self) -> Option<Box<dyn EpConnection>>;
    fn update_read_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()>;
    fn update_write_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()>;
    fn update_admin_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()>;
    fn update_system_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()>;
    fn serialize(&self) -> ResultEP<Value>;

    /// Get the default connection for a privilege tier.
    fn connection_for_tier(&self, tier: ConnectionTier) -> Option<Box<dyn EpConnection>> {
        match tier {
            ConnectionTier::Read => self.read_conn(),
            ConnectionTier::Write => self.write_conn(),
            ConnectionTier::Admin => self.admin_conn(),
            ConnectionTier::System => self.system_conn(),
        }
    }

    /// Compose a connection using the shared target and ELS-provided
    /// credentials for a specific tier. Returns `None` if not supported.
    ///
    /// Default implementation returns `None` — endpoint types that support
    /// ELS credential override implement this by composing their target
    /// with the provided `EpAuth`.
    fn connection_with_auth(&self, _tier: ConnectionTier, _auth: &dyn crate::ep_auth::EpAuth) -> Option<Box<dyn EpConnection>> {
        None
    }
}

impl Clone for Box<dyn EpConfig> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialSchema for Box<dyn EpConfig> {
    fn schema() -> RefOr<Schema> {
        let conn_config = OneOfBuilder::new();
        RefOr::T(Schema::OneOf(conn_config.build()))
    }
}

impl ToSchema for Box<dyn EpConfig> {}

#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
struct ConfigHelper {
    kind: EpKind,
    #[serde(flatten)]
    data: Value,
}

pub trait UpdateConfig: Send + Sync + Debug {
    fn as_update_config(self: Box<Self>) -> Box<dyn EpConfig>;
    fn as_any(&self) -> &dyn Any;
}

pub enum ClientKind {
    Api,
    Driver,
}

impl fmt::Display for ClientKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Api => write!(f, "api"),
            Self::Driver => write!(f, "driver"),
        }
    }
}

pub trait Pool: Send + Sync {
    fn kind(&self) -> ClientKind;
}

pub trait EpResponse: fmt::Display {
    fn as_response(self: Box<Self>) -> Box<dyn EpResponse>;
    fn as_any(&self) -> &dyn Any;
    fn kind(&self) -> EpKind;
}

pub trait RWPool<A: Send>: EpConfig + Send + Sync {
    #[named]
    fn init_conn_async(&self, telemetry_wrapper: &mut TelemetryWrapper) -> impl Future<Output = Result<ConnSet<A>, EpError>> + Send {
        async move {
            let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

            let read = match &self.read_conn() {
                Some(read) => Some(self.conn_async(read.clone(), telemetry_wrapper).await?),
                None => None,
            };

            let write = match &self.write_conn() {
                Some(write) => Some(self.conn_async(write.clone(), telemetry_wrapper).await?),
                None => None,
            };

            let admin = match &self.admin_conn() {
                Some(admin) => Some(self.conn_async(admin.clone(), telemetry_wrapper).await?),
                None => None,
            };

            let system = match &self.system_conn() {
                Some(system) => Some(self.conn_async(system.clone(), telemetry_wrapper).await?),
                None => None,
            };

            if read.is_none() && write.is_none() && admin.is_none() {
                let error = "no connection provided";
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
                return Err(EpError::connect(error));
            }

            Ok(ConnSet { read, write, admin, system })
        }
    }
    fn conn_async(
        &self,
        conn: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = Result<A, EpError>> + Send;
}

/// Set of connection pools initialized from an endpoint config.
#[derive(Debug, Clone)]
pub struct ConnSet<A> {
    pub read: Option<A>,
    pub write: Option<A>,
    pub admin: Option<A>,
    pub system: Option<A>,
}

#[derive(Debug, Clone)]
pub struct PoolType<A> {
    // r2d2: Option<EpConn<R>>,
    conn: EpConn<A>,
}

impl<A> PoolType<A> {
    fn new(conn: EpConn<A>) -> Self {
        Self { conn }
    }
    fn insert_connection(&mut self, conn: EpConn<A>) {
        self.conn = conn;
    }
    pub fn conn(&self) -> &EpConn<A> {
        &self.conn
    }
}

#[derive(Clone)]
pub struct EpPool<A> {
    pool: HashMap<EndpointCacheUuid, PoolType<A>>,
}

impl<A> Debug for EpPool<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys = self.pool.keys().collect::<Vec<&EndpointCacheUuid>>();
        write!(f, "{keys:?}")
    }
}

impl<A> Default for EpPool<A> {
    fn default() -> Self {
        Self { pool: HashMap::default() }
    }
}

impl<A> EpPool<A> {
    /// **** SYNC CODE BLOCK **** ///
    pub fn new() -> Self {
        Self { pool: HashMap::new() }
    }

    pub fn pool(&self) -> &HashMap<EndpointCacheUuid, PoolType<A>> {
        &self.pool
    }

    pub fn endpoints(&self) -> Vec<EndpointCacheUuid> {
        self.pool.keys().cloned().collect()
    }

    pub async fn endpoints_with_context(&self) -> Vec<(EndpointCacheUuid, &A)> {
        let mut endpoints = vec![];

        for k in self.pool.keys() {
            match self.read_conn_async(k).await {
                Ok(context) => endpoints.push((k.clone(), context)),
                Err(_) => continue,
            }
        }

        endpoints
    }

    /// **** ASYNC CODE BLOCK **** ///
    // TODO: revisit when telemetry is added to this function
    #[allow(unused_macros)]
    #[named]
    pub async fn connect_async(&mut self, endpoint_cache_uuid: &EndpointCacheUuid, conn_set: ConnSet<A>) -> Option<PoolType<A>> {
        let conn = EpConn::from_conn_set(conn_set);
        match self.pool.contains_key(endpoint_cache_uuid) {
            true => {
                if let Some(pool) = self.pool.get_mut(endpoint_cache_uuid) {
                    pool.insert_connection(conn);
                }
                None
            }
            false => self.pool.insert(endpoint_cache_uuid.to_owned(), PoolType::new(conn)),
        }
    }
    // TODO: revisit when telemetry is added to this function
    #[allow(unused_macros)]
    #[named]
    pub async fn disconnect_async(&mut self, endpoint_cache_uuid: &EndpointCacheUuid) -> Option<PoolType<A>> {
        self.pool.remove(endpoint_cache_uuid)
    }
    pub async fn read_conn_async(&self, endpoint_cache_uuid: &EndpointCacheUuid) -> Result<&A, EpError> {
        match self.pool.get(endpoint_cache_uuid) {
            Some(conn) => conn.conn.read_conn_async().await,
            None => Err(EpError::Connect(ConnectError::ConnectionNotFound)),
        }
    }

    pub async fn write_conn_async(&self, endpoint_cache_uuid: &EndpointCacheUuid) -> Result<&A, EpError> {
        match self.pool.get(endpoint_cache_uuid) {
            Some(conn) => conn.conn.write_conn_async().await,
            None => Err(EpError::Connect(ConnectError::ConnectionNotFound)),
        }
    }
    #[named]
    pub async fn update_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        conn_set: ConnSet<A>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<ConnSet<A>, EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        match self.pool.get_mut(endpoint_cache_uuid) {
            Some(pool) => Ok(ConnSet {
                read: conn_set.read.and_then(|r| pool.conn.update_read(r)),
                write: conn_set.write.and_then(|w| pool.conn.update_write(w)),
                admin: conn_set.admin.and_then(|a| pool.conn.update_admin(a)),
                system: conn_set.system.and_then(|s| pool.conn.update_system(s)),
            }),
            None => {
                self.pool.insert(endpoint_cache_uuid.to_owned(), PoolType::new(EpConn::from_conn_set(conn_set)));
                Ok(ConnSet { read: None, write: None, admin: None, system: None })
            }
        }
    }
    #[named]
    pub async fn update_read_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        update: A,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Option<A>, EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        match self.pool.get_mut(endpoint_cache_uuid) {
            Some(conn) => Ok(conn.conn.update_read(update)),
            None => Err(EpError::Connect(ConnectError::ConnectionNotFound)),
        }
    }
    #[named]
    pub async fn update_write_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        update: A,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Option<A>, EpError> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        match self.pool.get_mut(endpoint_cache_uuid) {
            Some(conn) => Ok(conn.conn.update_write(update)),
            None => Err(EpError::Connect(ConnectError::ConnectionNotFound)),
        }
    }
    #[named]
    pub async fn admin_conn_async(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<&A, EpError> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        match self.pool.get(endpoint_cache_uuid) {
            Some(conn) => conn.conn.admin_conn_async().await,
            None => {
                span.set_status(FastSpanStatus::Error { message: Cow::Borrowed("could not find connection") });
                Err(EpError::Connect(ConnectError::ConnectionNotFound))
            }
        }
    }
    #[named]
    pub async fn system_conn_async(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<&A, EpError> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

        match self.pool.get(endpoint_cache_uuid) {
            Some(conn) => conn.conn.system_conn_async().await,
            None => {
                span.set_status(FastSpanStatus::Error { message: Cow::Borrowed("could not find connection") });
                Err(EpError::Connect(ConnectError::ConnectionNotFound))
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct EpConn<T> {
    read: Option<T>,
    write: Option<T>,
    admin: Option<T>,
    system: Option<T>,
}

impl<T> EpConn<T> {
    pub fn new(read: Option<T>, write: Option<T>, admin: Option<T>, system: Option<T>) -> Self {
        Self { read, write, admin, system }
    }
    pub fn from_conn_set(conn_set: ConnSet<T>) -> Self {
        Self {
            read: conn_set.read,
            write: conn_set.write,
            admin: conn_set.admin,
            system: conn_set.system,
        }
    }
    /// read → write → admin
    pub fn read_conn(&self) -> Result<&T, EpError> {
        self.read
            .as_ref()
            .or(self.write.as_ref())
            .or(self.admin.as_ref())
            .ok_or(EpError::Connect(ConnectError::CouldNotGetConnection))
    }
    /// write → admin
    pub fn write_conn(&self) -> Result<&T, EpError> {
        self.write.as_ref().or(self.admin.as_ref()).ok_or(EpError::Connect(ConnectError::CouldNotGetConnection))
    }
    /// admin only
    pub fn admin_conn(&self) -> Result<&T, EpError> {
        self.admin.as_ref().ok_or(EpError::Connect(ConnectError::CouldNotGetConnection))
    }
    /// system → admin → write → read
    pub fn system_conn(&self) -> Result<&T, EpError> {
        self.system
            .as_ref()
            .or(self.admin.as_ref())
            .or(self.write.as_ref())
            .or(self.read.as_ref())
            .ok_or(EpError::Connect(ConnectError::CouldNotGetConnection))
    }
    /// read → write → admin
    pub async fn read_conn_async(&self) -> Result<&T, EpError> {
        self.read_conn()
    }
    /// write → admin
    pub async fn write_conn_async(&self) -> Result<&T, EpError> {
        self.write_conn()
    }
    /// admin only
    pub async fn admin_conn_async(&self) -> Result<&T, EpError> {
        self.admin_conn()
    }
    /// system → admin → write → read
    pub async fn system_conn_async(&self) -> Result<&T, EpError> {
        self.system_conn()
    }
    pub fn update_read(&mut self, conn: T) -> Option<T> {
        self.read.replace(conn)
    }
    pub fn update_write(&mut self, conn: T) -> Option<T> {
        self.write.replace(conn)
    }
    pub fn update_admin(&mut self, conn: T) -> Option<T> {
        self.admin.replace(conn)
    }
    pub fn update_system(&mut self, conn: T) -> Option<T> {
        self.system.replace(conn)
    }
}
