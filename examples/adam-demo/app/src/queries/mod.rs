//! Per-vertical query definitions.
//!
//! Each vertical module exposes queries for its database silos.
//! The dispatcher functions route based on the `vertical` config value.

use serde_json::{Value, json};

pub mod bird;
pub mod finance;
pub mod healthcare;
pub mod insurance;
pub mod migration;
pub mod retail;
pub mod stonebreaker;
pub mod tech;

/// A named query: (human-readable description, JSON body for Eden API).
pub type Query = (&'static str, Value);

/// Return queries for a specific database endpoint within a vertical.
/// The `endpoint_name` identifies which silo (e.g., "pg_network_security").
pub fn queries_for(vertical: &str, endpoint_name: &str) -> Vec<Query> {
    match vertical {
        "bird" => bird::queries_for(endpoint_name),
        "stonebreaker" => stonebreaker::queries_for(endpoint_name),
        "tech" => tech::queries_for(endpoint_name),
        "finance" => finance::queries_for(endpoint_name),
        "healthcare" => healthcare::queries_for(endpoint_name),
        "insurance" => insurance::queries_for(endpoint_name),
        "migration" => migration::queries_for(endpoint_name),
        _ => retail::queries_for(endpoint_name),
    }
}

/// Return cross-database queries for a vertical.
pub fn cross_db_queries(vertical: &str) -> Vec<Vec<(&'static str, &'static str, Value)>> {
    match vertical {
        "bird" => bird::cross_db_queries(),
        "stonebreaker" => stonebreaker::cross_db_queries(),
        "tech" => tech::cross_db_queries(),
        "finance" => finance::cross_db_queries(),
        "healthcare" => healthcare::cross_db_queries(),
        "insurance" => insurance::cross_db_queries(),
        "migration" => migration::cross_db_queries(),
        _ => retail::cross_db_queries(),
    }
}

// ─── Helper functions to build Eden API request bodies ─────────────
//
// Eden expects: {"request": {"kind": "Postgres", "type": "query", ...}}
// The "request" wrapper is added by eden_client::query(), so helpers
// only build the inner object with "kind" and "type".

/// Build a Postgres read-only query body.
pub fn pg_query(sql: &str) -> Value {
    json!({"kind": "Postgres", "type": "query", "query": sql, "params": []})
}

/// Build a ClickHouse read-only query body.
pub fn ch_query(sql: &str) -> Value {
    json!({"kind": "Clickhouse", "type": "query_read_only", "query": sql})
}

/// Build a MongoDB find query body.
pub fn mongo_find(database: &str, collection: &str, filter: Value) -> Value {
    json!({
        "kind": "Mongo",
        "type": "database_collection_find",
        "database": database,
        "collection": collection,
        "filter": filter
    })
}

/// Build a MongoDB aggregate query body.
pub fn mongo_aggregate(database: &str, collection: &str, pipeline: Value) -> Value {
    json!({
        "kind": "Mongo",
        "type": "database_collection_aggregate",
        "database": database,
        "collection": collection,
        "pipeline": pipeline
    })
}

/// Build a Redis GET query body.
pub fn redis_get(key: &str) -> Value {
    json!({"kind": "Redis", "type": "get", "key": key})
}

/// Build a Redis HGETALL query body.
pub fn redis_hgetall(key: &str) -> Value {
    json!({"kind": "Redis", "type": "hgetall", "key": key})
}

/// Build a Redis ZREVRANGE query body.
pub fn redis_zrevrange(key: &str, start: i64, stop: i64) -> Value {
    json!({"kind": "Redis", "type": "zrevrange", "key": key, "start": start, "stop": stop})
}

/// Build a Redis SMEMBERS query body.
pub fn redis_smembers(key: &str) -> Value {
    json!({"kind": "Redis", "type": "smembers", "key": key})
}

/// Build a Redis DBSIZE query body.
pub fn redis_dbsize() -> Value {
    json!({"kind": "Redis", "type": "dbsize"})
}

/// Build a Weaviate GraphQL query body.
pub fn weaviate_graphql(body: &str) -> Value {
    json!({"kind": "Weaviate", "type": "graphql", "body": body})
}
