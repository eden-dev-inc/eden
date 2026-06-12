//! Gateway bridge entry point.
//!
//! This crate wires together the per-protocol processor crates
//! (`gateway_redis`, `gateway_postgres`, `gateway_mongo`, and the LLM HTTP
//! adapter) and the shared
//! plumbing in `eden_gateway_core` into a single gateway runtime. It owns
//! the bridge tasks that:
//!
//! - read bytes from the client socket and either forward them as raw
//!   chunks (`ProxyRequestChunk`) or, for Redis, run the direct
//!   lane-pool gateway path;
//! - drain the per-connection response queue (`BytesQueueSender` /
//!   `QueuedBytes`) and write completed responses back to the client;
//! - enforce the bridge-side backpressure limits (pending message and
//!   pending byte caps) so a slow client cannot exhaust memory.
//!
//! Per-protocol processing logic lives in the per-protocol sub-crates or
//! local adapters and is re-exported here as `redis`, `postgres`, `mongo`
//! for consumers that wire the bridge into a service binary.

mod bridge;
mod gateway_telemetry;
mod processor;
mod protocol;
mod replication;
#[doc(hidden)]
#[allow(deprecated)]
pub mod validation;

pub use bridge::{BridgeQueueSnapshot, handle_connection};
pub use eden_gateway_core::audit::{PgQueryRecorder, init_pg_query_recorder};
pub use eden_gateway_core::connection;
pub use eden_gateway_core::runtime;
pub use eden_gateway_core::shard_capacity;
pub use eden_gateway_core::shard_dispatch;
pub use eden_gateway_core::traits;
#[cfg(feature = "agent")]
pub use gateway_agent as agent;
#[cfg(feature = "llm")]
pub use gateway_llm as llm;
#[cfg(feature = "mongo")]
pub use gateway_mongo as mongo;
#[cfg(feature = "postgres")]
pub use gateway_postgres as postgres;
#[cfg(feature = "redis")]
pub use gateway_redis as redis;
pub use protocol::{ProtocolRW, ProxyProtocol};

pub(crate) use bridge::*;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::format::CacheUuid;
use eden_core::format::OrganizationCacheUuid;
use eden_core::format::cache_uuid::InterlayCacheUuid;
use eden_core::telemetry::TelemetryWrapper;
use eden_core::telemetry::metrics::AllMetrics;
use eden_gateway_core::connection::InterlayStream;
use eden_gateway_core::traits::{BytesQueueSender, ProxyRequestChunk, QueuedBytes};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_info, log_trace, log_warn};
use ep_core::database::schema::interlay::InterlayState;
use ep_core::settings::EdenSettings;
use ep_runtime::comp::MyEngineService;
use function_name::named;
use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::unbounded_channel;
use tokio::{io, join, select};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    io::{ReadHalf, WriteHalf},
};
