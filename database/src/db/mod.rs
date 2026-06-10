pub mod analytics_prefs;
pub mod auth;
pub mod cache;
pub mod cache_ops;
#[cfg(embedded_db)]
pub mod duckdb_analytics;
pub mod els;
pub mod encryption;
pub mod internal_cache;
pub mod lib;
pub mod methods;
pub mod rbac;
#[cfg(not(embedded_db))]
pub mod rbac_pg_sync;
#[cfg(any(all(test, embedded_db), all(test, not(embedded_db), feature = "infra-tests")))]
mod tests_common;
#[cfg(all(test, embedded_db))]
mod tests_embedded_db;
#[cfg(all(test, not(embedded_db), feature = "infra-tests"))]
mod tests_postgres;
#[cfg(embedded_db)]
pub mod turso;
