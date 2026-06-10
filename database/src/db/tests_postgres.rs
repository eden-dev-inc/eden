//! Postgres-backed parity tests — the Postgres half of the dual-backend matrix.
//!
//! Each test builds a real Postgres-backed `DatabaseManager` (via
//! testcontainers) and runs the shared, backend-generic assertions in
//! [`crate::db::tests_common`]. The Turso half lives in
//! [`super::tests_embedded_db`] (the `parity_*` tests) and calls the *same*
//! assertion functions, so the two backends are guaranteed to agree
//! operation-by-operation.
//!
//! Requires Docker (testcontainers) and the `infra-tests` feature, so this
//! module is gated to the external-DB build only. Keep the wrapper list in
//! lockstep with the Turso `parity_*` wrappers.
#![cfg(all(test, not(embedded_db), feature = "infra-tests"))]

use crate::db::tests_common as common;
use crate::test_utils::database_test_utils::create_database_manager;

#[tokio::test]
async fn parity_organization_insert_get() {
    let db = create_database_manager().await;
    common::organization_insert_get(&db).await;
}

#[tokio::test]
async fn parity_organization_select() {
    let db = create_database_manager().await;
    common::organization_select(&db).await;
}

#[tokio::test]
async fn parity_organization_delete() {
    let db = create_database_manager().await;
    common::organization_delete(&db).await;
}

#[tokio::test]
async fn parity_user_insert_get() {
    let db = create_database_manager().await;
    common::user_insert_get(&db).await;
}

#[tokio::test]
async fn parity_admin_user_insert_get() {
    let db = create_database_manager().await;
    common::admin_user_insert_get(&db).await;
}

#[tokio::test]
async fn parity_user_delete() {
    let db = create_database_manager().await;
    common::user_delete(&db).await;
}

#[tokio::test]
async fn parity_eden_node_select() {
    let db = create_database_manager().await;
    common::eden_node_select(&db).await;
}

#[tokio::test]
async fn parity_multiple_users_same_org() {
    let db = create_database_manager().await;
    common::multiple_users_same_org(&db).await;
}

#[tokio::test]
async fn parity_analytics_dashboard_prefs_roundtrip() {
    let db = create_database_manager().await;
    common::analytics_dashboard_prefs_roundtrip(&db).await;
}

#[tokio::test]
async fn parity_llm_skills_roundtrip() {
    let db = create_database_manager().await;
    common::llm_skills_roundtrip(&db).await;
}
