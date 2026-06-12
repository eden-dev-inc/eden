#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Endpoint Core
//!
//! Abstract interface layer for multi-database endpoint management in Eve.
//!
//! ## Overview
//!
//! `ep-core` provides the foundational abstractions that enable Eve to support
//! 12+ different database types through a unified API. It defines the contract that
//! all database-specific implementations must follow.
//!
//! ## Supported Databases
//!
//! Through feature flags, ep-core supports:
//! - **SQL**: PostgreSQL, MySQL, Oracle, MS SQL Server, ClickHouse
//! - **NoSQL**: MongoDB, Redis, Cassandra
//! - **Vector**: Pinecone
//! - **Web**: HTTP/REST endpoints
//! - **LLM**: Language model providers
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │         Eden Service API             │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │      Communication Layer             │
//! └────────────────┬─────────────────────┘
//!                  │
//!                  ▼
//! ┌──────────────────────────────────────┐
//! │         Endpoint Core (this)         │
//! │  - Abstract trait definitions        │
//! │  - Schema management                 │
//! │  - Template system                   │
//! │  - Workflow engine                   │
//! └────────────────┬─────────────────────┘
//!                  │
//!      ┌───────────┴──────────┬──────────────┐
//!      ▼                      ▼              ▼
//! ┌──────────┐         ┌──────────┐    ┌──────────┐
//! │ Postgres │         │  MongoDB │    │  Redis   │
//! │   Core   │   ...   │   Core   │    │   Core   │
//! └──────────┘         └──────────┘    └──────────┘
//! ```
//!
//! ## Core Traits
//!
//! ### [`EndpointCore`](ep::EndpointCore)
//!
//! The primary trait that all database endpoints must implement:
//!
//! ```ignore
//! #[async_trait]
//! pub trait EndpointCore: Send + Sync {
//!     async fn connect(&self, config: EndpointConfig) -> ResultEP<Connection>;
//!     async fn execute(&self, request: Request) -> ResultEP<Response>;
//!     async fn disconnect(&self) -> ResultEP<()>;
//!     fn metadata(&self) -> EndpointMetadata;
//! }
//! ```
//!
//! ### Connection Management
//!
//! - **Connection Pooling**: Each endpoint maintains a pool via [`EpPool`](ep::EpPool)
//! - **Health Checks**: Periodic validation of connection health
//! - **Auto-Reconnect**: Automatic recovery from transient failures
//! - **TLS Support**: Secure connections via [`TlsData`]
//!
//! ## Schema System
//!
//! The [`database::schema`] module defines typed schemas for Eden entities:
//!
//! ### Core Schemas
//!
//! - [`EndpointSchema`](database::schema::EndpointSchema) - Database endpoint configuration
//! - [`OrganizationSchema`](database::schema::OrganizationSchema) - Organization details
//! - [`UserSchema`](database::schema::UserSchema) - User accounts
//! - [`WorkflowSchema`](database::schema::WorkflowSchema) - Workflow definitions
//! - [`TemplateSchema`](database::schema::TemplateSchema) - Reusable operation templates
//!
//! ### Schema Traits
//!
//! All schemas implement:
//! - [`Table`](database::schema::Table) - Table name and primary key
//! - `FromRow` - Deserialize from database rows
//! - `Serialize`/`Deserialize` - JSON serialization for API
//!
//! ## Template System
//!
//! The [`database::template`] module provides reusable operation patterns:
//!
//! ```ignore
//! use ep_core::database::template::{Template, TemplateRegistry};
//!
//! // Define a reusable query template
//! let template = Template::new(
//!     "get_user_by_email",
//!     "SELECT * FROM users WHERE email = {{email}}"
//! );
//!
//! // Register for use
//! registry.register(template);
//!
//! // Execute with parameters
//! let result = registry.execute("get_user_by_email", json!({
//!     "email": "user@example.com"
//! })).await?;
//! ```
//!
//! ### Template Features
//!
//! - **Handlebars Syntax**: `{{variable}}` placeholders
//! - **Conditionals**: `{{#if condition}}...{{/if}}`
//! - **Loops**: `{{#each items}}...{{/each}}`
//! - **Type Safety**: Compile-time validation of required fields
//! - **Caching**: Templates cached for performance
//!
//! ## Workflow Engine
//!
//! The [`database::workflow`] module orchestrates multi-step operations:
//!
//! ```ignore
//! use ep_core::database::workflow::{Workflow, WorkflowStep};
//!
//! let workflow = Workflow::builder()
//!     .add_step(WorkflowStep::Query {
//!         template: "create_user",
//!         output: "user_id"
//!     })
//!     .add_step(WorkflowStep::Query {
//!         template: "assign_role",
//!         input: "{{user_id}}"
//!     })
//!     .build();
//!
//! workflow.execute(&db_manager).await?;
//! ```
//!
//! ### Workflow Features
//!
//! - **Directed Graphs**: Steps with dependencies
//! - **Data Flow**: Output from one step feeds into next
//! - **Error Handling**: Rollback on failure
//! - **Conditional Execution**: Steps based on previous results
//!
//! ## Endpoint Configuration
//!
//! Each endpoint type has specific configuration:
//!
//! ```ignore
//! use ep_core::database::schema::EndpointSchema;
//!
//! // PostgreSQL endpoint
//! let pg_endpoint = EndpointSchema {
//!     kind: EpKind::Postgres,
//!     host: "localhost".into(),
//!     port: 5432,
//!     database: "mydb".into(),
//!     username: "user".into(),
//!     password: Password::new("secret"),
//!     tls: Some(TlsData::default()),
//! };
//! ```
//!
//! ## Error Handling
//!
//! All operations return [`ResultEP<T>`](error::ResultEP) which wraps [`EpError`](error::EpError):
//!
//! - `EpError::Connect` - Connection failures
//! - `EpError::Request` - Invalid request parameters
//! - `EpError::Transaction` - Transaction failures
//! - `EpError::Timeout` - Operation exceeded time limit
//!
//! ## Feature Flags
//!
//! Enable specific database support via Cargo features:
//!
//! ```toml
//! [dependencies]
//! ep-core = { version = "0.1", features = ["postgres", "mongo", "redis"] }
//! ```
//!
//! Available features:
//! - `postgres`, `mysql`, `oracle`, `mssql` - SQL databases
//! - `mongo`, `cassandra` - NoSQL databases
//! - `redis` - Key-value store
//! - `clickhouse` - Analytics database
//! - `pinecone` - Vector database
//! - `http` - HTTP/REST endpoints
//! - `llm` - Language model providers
//! - `full` - Enable all databases
//!
//! ## Telemetry Integration
//!
//! All endpoint operations are automatically instrumented with:
//! - **Tracing**: OpenTelemetry spans for request tracking
//! - **Metrics**: Operation counts, latencies, error rates
//! - **Logging**: Structured logs via `tracing` crate
//!
//! ## Testing
//!
//! Mock implementations for testing:
//!
//! ```ignore
//! use ep_core::testing::MockEndpoint;
//!
//! #[tokio::test]
//! async fn test_endpoint_operations() {
//!     let mock = MockEndpoint::new()
//!         .expect_connect()
//!         .expect_query(/* ... */);
//!
//!     // Test code using mock
//! }
//! ```
//!
//! ## Integration
//!
//! This crate integrates with:
//! - **`communication`** - Routes requests to appropriate endpoint implementations
//! - **`database`** - Stores endpoint configurations and metadata
//! - **`eden_service`** - Exposes endpoint operations via REST API
//! - Database-specific cores (`postgres-core`, `mongo-core`, etc.)

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use borsh::{BorshDeserialize, BorshSerialize};
use ep::EpPool;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use borsh;
pub use bytes;
pub use chrono;
pub use lazy_static;
pub use linkme;
pub use paste;
pub use postgres;
pub use serde;
pub use serde_json;
pub use telemetry;

// pub mod elastic_search;
//
// pub mod cassandra;
//
// pub mod clickhouse;
//
// pub mod http;
//
// pub mod mongo;
//
// pub mod mssql;
//
// pub mod mysql;
//
// pub mod oracle;
//
// pub mod pinecone;
//
// pub mod postgres;
//
// pub mod redis;

// pub mod sqlite;
pub mod database;
pub mod ep;
pub mod ep_auth;
pub mod macros;
pub mod pool;
pub mod runtime;
pub mod settings;
pub mod tls;

pub trait EndpointOperation: Any + Send + Sync + Debug {}

#[derive(Debug, Clone)]
pub struct ApiInfo<K, T>
where
    T: Clone + 'static,
{
    pub endpoint: EpKind,
    pub api: K,
    pub description: &'static str,
    pub request_type: ReqType,
    pub examples: Vec<ApiExample<T>>,
}

impl<K, T> ApiInfo<K, T>
where
    T: Clone + 'static,
{
    pub const fn new(
        endpoint: EpKind,
        api: K,
        description: &'static str,
        request_type: ReqType,
        // examples: &'static [ApiExample<T>],
    ) -> Self {
        Self {
            api,
            endpoint,
            description,
            request_type,
            examples: Vec::new(),
        }
    }

    pub fn endpoint(&self) -> EpKind {
        self.endpoint
    }

    pub fn api(&self) -> &K {
        &self.api
    }

    pub fn description(&self) -> &str {
        self.description
    }

    pub fn request_type(&self) -> &ReqType {
        &self.request_type
    }

    pub fn examples(&self) -> &[ApiExample<T>] {
        &self.examples
    }
}

#[derive(Debug, Clone)]
pub struct ApiExample<T>
where
    T: Clone + 'static,
{
    name: &'static str,
    description: &'static str,
    request: T,
    response: Result<Option<Value>, Option<Value>>,
}

impl<T> ApiExample<T>
where
    T: Clone + 'static,
{
    pub fn new(name: &'static str, description: &'static str, request: T, response: Result<Option<Value>, Option<Value>>) -> Self {
        Self { name, description, request, response }
    }

    pub fn name(&self) -> &str {
        self.name
    }

    pub fn description(&self) -> &str {
        self.description
    }

    pub fn request(&self) -> &T {
        &self.request
    }

    // pub fn response(&self) -> &Value {
    //     &self.response
    // }
    pub fn map<U: From<T> + Clone>(self) -> ApiExample<U> {
        let ApiExample { name, description, request, response } = self;

        ApiExample { name, description, request: U::from(request), response }
    }

    pub fn map_ref<U: From<T> + Clone>(&self) -> ApiExample<U> {
        ApiExample {
            name: self.name,
            description: self.description,
            request: U::from(self.request.clone()),
            response: self.response.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshDeserialize, BorshSerialize, Eq, PartialEq)]
pub enum ReqType {
    Read,
    Write,
}

impl ReqType {
    pub fn is_read(&self) -> bool {
        matches!(self, ReqType::Read)
    }
    pub fn is_write(&self) -> bool {
        matches!(self, ReqType::Write)
    }
}

pub trait OperationKind<K> {
    fn operation_kind() -> K;
}

// pub trait Operation<A, K, X>: Any + Send + Sync + Debug + EndpointOperation {
//     fn as_any(&self) -> &dyn Any;
//     fn kind(&self) -> K;
//     fn request_type(&self) -> ReqType;
//     fn as_operation(self: Box<Self>) -> Box<dyn Operation<A, K, X>>;
//     fn as_exec(&self) -> Option<&dyn OperationExecutor<A, K, X>>;
//     fn clone_box(&self) -> Box<dyn Operation<A, K, X>>;
// }

// impl<A: 'static, K: 'static, X: 'static> Clone for Box<dyn Operation<A, K, X>> {
//     fn clone(&self) -> Self {
//         self.clone_box()
//     }
// }

// pub trait OperationExecutor<A: 'static, K: 'static, X: 'static>:
// Operation<A, K, X> + Send + Sync + 'static
// {
//     // fn kind(&self) -> MongoApiKind;
//     fn as_any(&self) -> &dyn Any;
//     // fn run_sync(&self, context: R, telemetry_context: TelemetryWrapper) -> RunOutput;
//     fn run_async(&self, context: A, telemetry_context: TelemetryWrapper) -> RunOutput;
//     fn run_transaction(&self, tx_context: &mut X, telemetry_context: TelemetryWrapper);
// }

// pub trait ComplexExecutor<'a, T: 'static, A, K, X>: Operation<A, K, X> + Send + Sync {
//     // fn kind(&self) -> MongoApiKind;
//     fn as_any(&self) -> &dyn Any;
//     // fn run_sync(
//     //     &self,
//     //     input: &'a Box<dyn EpOutput>,
//     //     telemetry_wrapper: &mut TelemetryWrapper,
//     // ) -> RunOutput;
//     fn run_async(
//         &self,
//         input: &'a Box<dyn EpOutput>,
//         telemetry_wrapper: &mut TelemetryWrapper,
//     ) -> RunOutput;
//     fn downcast(input: &'a Box<dyn EpOutput>) -> ResultEP<&'a T>;
// }

pub type RunOutput<'a> = Pin<Box<dyn Future<Output = ResultEP<Box<dyn EpOutput>>> + Send + 'a>>;

pub trait EpOutput: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_output(self: Box<Self>) -> Box<dyn EpOutput>;
    fn try_to_bytes(self: Box<Self>) -> ResultEP<bytes::Bytes>;
    fn try_serde_serialize(&self) -> ResultEP<Value>;
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>>;
    fn delete_me(&self) -> bool;
}

pub trait EndpointType {
    fn r#type() -> EpKind;
}

pub trait GetPool<A> {
    fn pool(&self) -> &EpPool<A>;
    fn mut_pool(&mut self) -> &mut EpPool<A>;
}

pub trait ToOutput
where
    Self: Send + Sync + Sized + 'static,
{
    fn to_output(self) -> EndpointOutput<Self>;
    fn try_to_bytes(self) -> ResultEP<bytes::Bytes>;
    fn try_serde_serialize(&self) -> ResultEP<Value>;
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>>;
}

#[derive(Debug)]
pub struct EndpointOutput<T> {
    kind: EpKind,
    response: EndpointResponse<T>,
}

impl<T> EndpointOutput<T> {
    pub fn new(kind: EpKind, response: EndpointResponse<T>) -> Self {
        Self { kind, response }
    }
}

impl<T: Send + ToOutput + Sync + 'static> EpOutput for EndpointOutput<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_output(self: Box<Self>) -> Box<dyn EpOutput> {
        self
    }
    fn try_to_bytes(self: Box<Self>) -> ResultEP<bytes::Bytes> {
        match self.response {
            EndpointResponse::Ok(ok) => Ok(bytes::Bytes::from(ok)),
            EndpointResponse::Err(err) => Ok(bytes::Bytes::from(err)),
            EndpointResponse::Response(response) => response.try_to_bytes(),
        }
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        match &self.response {
            EndpointResponse::Ok(string) => {
                let mut map = Map::with_capacity(2);
                map.insert("kind".to_string(), Value::String(self.kind.to_string()));
                map.insert("ok".to_string(), Value::String(string.to_owned()));
                Ok(Value::Object(map))
            }
            EndpointResponse::Err(error) => {
                let mut map = Map::with_capacity(2);
                map.insert("kind".to_string(), Value::String(self.kind.to_string()));
                map.insert("error".to_string(), Value::String(error.to_owned()));
                Ok(Value::Object(map))
            }
            EndpointResponse::Response(response_data) => {
                let response_value = response_data.try_serde_serialize()?;

                //allow for flattening
                if let Value::Object(response_map) = response_value {
                    let mut map = Map::with_capacity(response_map.len() + 1);
                    map.insert("kind".to_string(), Value::String(self.kind.to_string()));
                    map.extend(response_map); // flatten the response
                    Ok(Value::Object(map))
                } else {
                    let mut map = Map::with_capacity(2);
                    map.insert("kind".to_string(), Value::String(self.kind.to_string()));
                    map.insert("data".to_string(), response_value); // fallback for non-objects
                    Ok(Value::Object(map))
                }
            }
        }
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        let kind_vec = borsh::to_vec(&self.kind).map_err(EpError::serde)?;

        let response_vec = match &self.response {
            EndpointResponse::Ok(string) => borsh::to_vec(&string).map_err(EpError::serde)?,
            EndpointResponse::Err(error) => borsh::to_vec(&error).map_err(EpError::serde)?,
            EndpointResponse::Response(response) => response.try_borsh_serialize()?,
        };

        Ok(kind_vec.into_iter().chain(response_vec).collect())
    }
    fn delete_me(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub enum EndpointResponse<T> {
    Ok(String),
    Err(String),
    Response(T),
}

impl<T> EndpointResponse<T> {
    pub fn ok(str: &str) -> EndpointResponse<T> {
        Self::Ok(str.to_lowercase())
    }
    pub fn err(str: &str) -> EndpointResponse<T> {
        Self::Err(str.to_lowercase())
    }
    pub fn response(self, response: T) -> EndpointResponse<T> {
        Self::Response(response)
    }
}
