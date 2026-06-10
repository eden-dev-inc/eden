#![cfg_attr(test, allow(clippy::unwrap_used))]
pub use endpoint_schema;

#[cfg(not(embedded_db))]
pub mod backups;
/// # Database Management
///
/// Core database abstraction layer providing PostgreSQL, internal ShardMap cache, and internal ClickHouse
/// integration
/// with comprehensive caching, RBAC enforcement, and connection pooling.
///
/// ## Overview
///
/// This crate implements the data persistence layer for Eve, managing:
/// - User authentication and authorization data
/// - Organization and endpoint configurations
/// - Workflow and template definitions
/// - Cache-aside reads with ShardMap + PostgreSQL
/// - Internal ClickHouse pool for analytics/telemetry (currently unused)
/// - Role-Based Access Control (RBAC) enforcement
///
/// ## Architecture
///
/// ### Two-Tier Storage
///
/// ```text
/// ┌─────────────┐
/// │   Request   │
/// └──────┬──────┘
///        │
///        ▼
/// ┌─────────────┐  Cache Hit
/// │  ShardMap   │──────────────► Return
/// │   (Cache)   │
/// └──────┬──────┘
///        │ Cache Miss
///        ▼
/// ┌─────────────┐
/// │ PostgreSQL  │──► Write to ShardMap
/// │  (Source)   │    Return
/// └─────────────┘
/// ```
///
/// - **ShardMap**: Fast in-process cache layer with TTL-based expiration
/// - **PostgreSQL**: Authoritative data source with ACID guarantees
/// - **ClickHouse (internal)**: Analytics/telemetry store (not yet used here)
///
/// ### RBAC Integration
///
/// Every database operation enforces permissions through the RBAC system:
/// - Subject verification (users, roles)
/// - Resource access control (endpoints, workflows, templates)
/// - Action authorization (read, write, delete, execute, admin)
///
/// ## Core Components
///
/// ### [`DatabaseManager`](db::DatabaseManager)
///
/// Central interface for all database operations. Manages the internal cache,
/// PostgreSQL, and internal ClickHouse connections with connection pooling.
///
/// ```ignore
/// use database::lib::{
///     ClickhouseConn, ClickhouseDbConfig, DatabaseManager, PgConn, RedisConn,
///     DEFAULT_CLICKHOUSE_POOL_SIZE,
/// };
///
/// let clickhouse_config = ClickhouseDbConfig::new(
///     "http://localhost:8123".to_string(),
///     None,
///     None,
///     None,
///     DEFAULT_CLICKHOUSE_POOL_SIZE,
/// )?;
/// let db_manager = DatabaseManager::<RedisConn, PgConn, ClickhouseConn>::new(
///     redis_connection,
///     postgres_connection,
///     clickhouse_config,
/// );
/// ```
///
/// Note: the internal ClickHouse pool is required; provide Clickhouse config before initialization.
///
/// ### Operations
///
/// #### Select Operations
/// - [`select_user`](db::DatabaseManager::select_user) - Retrieve user by ID or UUID
/// - [`select_organization`](db::DatabaseManager::select_organization) - Get organization details
/// - [`select_endpoint`](db::DatabaseManager::select_endpoint) - Fetch endpoint configuration
/// - [`select_workflow`](db::DatabaseManager::select_workflow) - Load workflow definition
///
/// #### Insert Operations
/// - [`insert_user`](db::DatabaseManager::insert_user) - Create new user with password hash
/// - [`insert_organization`](db::DatabaseManager::insert_organization) - Register organization
/// - [`insert_endpoint`](db::DatabaseManager::insert_endpoint) - Connect new endpoint
///
/// #### Update Operations
/// - [`update_user`](db::DatabaseManager::update_user) - Modify user details
/// - [`update_endpoint`](db::DatabaseManager::update_endpoint) - Update endpoint config
///
/// #### Delete Operations
/// - [`delete_user`](db::DatabaseManager::delete_user) - Remove user and invalidate cache
/// - [`delete_endpoint`](db::DatabaseManager::delete_endpoint) - Disconnect endpoint
///
/// ### RBAC Module
///
/// The [`rbac`](db::rbac) module provides fine-grained access control:
///
/// ```ignore
/// use database::db::rbac::{check_permission, Action, Resource};
/// use format::rbac::{Subject, ResourceId};
///
/// // Check if user can read endpoint
/// check_permission(
///     &db_manager,
///     &Subject::User(user_id),
///     &Resource::Endpoint,
///     &ResourceId::Endpoint(endpoint_id),
///     Action::Read,
/// ).await?;
/// ```
///
/// ## Caching Strategy
///
/// ### Cache Keys
///
/// Type-safe cache keys using newtype pattern:
/// - [`CacheId`](format::CacheId) - String-based keys (e.g., "user:123")
/// - [`CacheUuid`](format::CacheUuid) - UUID-based keys with prefixes
///
/// ### Cache Invalidation
///
/// - **Write-through**: Updates write to both cache and database
/// - **Delete-through**: Deletions remove from both cache and database
/// - **TTL expiration**: cache keys auto-expire (configurable per entity)
///
/// ### Cache Operations
///
/// ```ignore
/// use database::db::cache::{get_from_cache, set_in_cache, invalidate_cache};
///
/// // Try cache first
/// if let Some(user) = get_from_cache(&cache, &cache_key).await? {
///     return Ok(user);
/// }
///
/// // Fallback to database
/// let user = query_postgres(&pg, user_id).await?;
///
/// // Populate cache
/// set_in_cache(&cache, &cache_key, &user, ttl).await?;
/// ```
///
/// ## Schema Management
///
/// Database schemas are defined using the [`Table`](ep_core::database::schema::Table) trait:
/// - [`UserSchema`](ep_core::database::schema::UserSchema)
/// - [`OrganizationSchema`](ep_core::database::schema::OrganizationSchema)
/// - [`EndpointSchema`](ep_core::database::schema::EndpointSchema)
/// - [`WorkflowSchema`](ep_core::database::schema::WorkflowSchema)
///
/// ## Error Handling
///
/// All operations return [`ResultDB`](error::ResultDB) which wraps [`DBError`](error::DBError):
///
/// ```ignore
/// use error::{DBError, ResultDB};
///
/// fn query_user(id: &UserId) -> ResultDB<UserSchema> {
///     // Database operations that may fail
///     Ok(user)
/// }
/// ```
///
/// Common errors:
/// - `DBError::ConnectionFailed` - Cannot reach PostgreSQL or ClickHouse
/// - `DBError::QueryFailed` - SQL query execution failed
/// - `DBError::NotFound` - Requested entity doesn't exist
/// - `DBError::CacheMiss` - Item not in cache (non-fatal)
///
/// ## Testing
///
/// Test utilities provided via [`test_utils`] module:
///
/// ```ignore
/// use database::test_utils::{setup_test_db, teardown_test_db};
///
/// #[tokio::test]
/// async fn test_user_operations() {
///     let db = setup_test_db().await;
///     // ... test code ...
///     teardown_test_db(db).await;
/// }
/// ```
///
/// ## Integration
///
/// This crate integrates with:
/// - **`eden_service`** - HTTP API handlers use DatabaseManager for persistence
/// - **`communication`** - gRPC services access database through this layer
/// - **`endpoint_core`** - Schema definitions shared between layers
///
/// ## Performance Considerations
///
/// - **Connection Pooling**: PostgreSQL and ClickHouse use connection pools
/// - **Batch Operations**: Use transactions for multiple related writes
/// - **Cache Warming**: Pre-populate frequently accessed data
/// - **Query Optimization**: All SQL queries use prepared statements with indexes
pub mod db;
#[cfg(not(embedded_db))]
pub mod org_transfer;
pub mod stc;
pub mod test_utils;

#[cfg(not(embedded_db))]
pub use backups::*;
pub use db::*;
pub use stc::*;
