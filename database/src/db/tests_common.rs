//! Backend-generic database operation tests — the shared half of the
//! Postgres/Turso parity matrix.
//!
//! Every assertion here is generic over the three connection traits
//! (`EdenRedisConnection`, `EdenPostgresConnection`, `EdenClickhouseConnection`),
//! so the Turso harness ([`super::tests_embedded_db`]) and the Postgres harness
//! ([`super::tests_postgres`]) run the *same* logic against both backends. If
//! the two backends ever diverge for an operation, one of those two harnesses
//! fails in CI.
//!
//! This module is NOT `embedded_db`-gated: it must compile in both the embedded
//! (`--features embedded-db`) and the external-DB (default) builds. Only the
//! thin per-backend wrapper modules are cfg-gated.
//!
//! Adding an operation: write one `pub(crate) async fn` here, then add a
//! one-line `#[tokio::test]` wrapper to each backend harness.
//!
//! (Gated on the `mod tests_common;` declaration in `super`, so it only compiles
//! when one of the backend parity harnesses can call it.)

use crate::db::analytics_prefs::AnalyticsPrefsStore;
use crate::db::cache::CacheFunctions;
use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::db::methods::delete::DeleteMethod;
use crate::db::methods::delete::organization::DeleteOrganization;
use crate::db::methods::delete::user::DeleteUser;
use crate::db::methods::insert::InsertMethod;
use crate::db::methods::insert::eden_node::InsertEdenNode;
use crate::db::methods::insert::organization::InsertOrganization;
use crate::db::methods::insert::user::{InsertAdminUser, InsertUser};
use crate::db::methods::llm::NewSkill;
use crate::test_utils::telemetry_test_utils::test_telemetry;
use eden_core::auth::Password;
use eden_core::format::cache_id::{EdenNodeCacheId, OrganizationCacheId, UserCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EdenNodeCacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::{CacheObjectType, EdenNodeUuid, OrganizationId, OrganizationUuid, UserId, UserUuid};
use ep_core::database::schema::Table;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::user::UserSchema;
use uuid::Uuid;

// Every fn repeats the connection-trait bound triple: Rust trait aliases are
// unstable, and the explicit bounds keep each parity fn self-documenting.

// ── shared builders (generic over the backend) ───────────────────

/// Insert an eden node, returning its schema.
pub(crate) async fn insert_eden_node<R, P, C>(db: &DatabaseManager<R, P, C>, id: &str) -> EdenNodeSchema
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let schema = EdenNodeSchema::new(id.to_string(), EdenNodeUuid::new_uuid(), vec![], serde_json::Value::default());
    <DatabaseManager<R, P, C> as InsertMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeCacheId, InsertEdenNode>>::insert(
        db,
        InsertEdenNode::new(schema.clone()),
        tel,
    )
    .await
    .expect("insert eden node");
    schema
}

/// Insert an organization linked to `eden_node`, returning its schema.
pub(crate) async fn insert_org<R, P, C>(db: &DatabaseManager<R, P, C>, org_id: &str, eden_node: &EdenNodeSchema) -> OrganizationSchema
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let schema = OrganizationSchema::new(org_id.to_string(), None, vec![(eden_node.id(), eden_node.uuid())], None);
    <DatabaseManager<R, P, C> as InsertMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationCacheId, InsertOrganization>>::insert(
        db,
        InsertOrganization::new(schema.clone()),
        tel,
    )
    .await
    .expect("insert organization");
    schema
}

/// Insert a regular user linked to `org_uuid`, returning its schema.
pub(crate) async fn insert_user<R, P, C>(db: &DatabaseManager<R, P, C>, username: &str, org_uuid: OrganizationUuid) -> UserSchema
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let schema = UserSchema::new(UserId::from(username), Password::new("password".to_string()), org_uuid, None, None, None);
    <DatabaseManager<R, P, C> as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertUser>>::insert(
        db,
        InsertUser::new(schema.clone()),
        tel,
    )
    .await
    .expect("insert user");
    schema
}

/// Insert an admin user linked to `org_uuid`, returning its schema.
pub(crate) async fn insert_admin_user<R, P, C>(db: &DatabaseManager<R, P, C>, username: &str, org_uuid: OrganizationUuid) -> UserSchema
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let schema = UserSchema::new(UserId::from(username), Password::new("admin_password".to_string()), org_uuid, None, None, None);
    <DatabaseManager<R, P, C> as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertAdminUser>>::insert(
        db,
        InsertAdminUser::new(schema.clone()),
        tel,
    )
    .await
    .expect("insert admin user");
    schema
}

// ── parity assertions (called from both backend harnesses) ───────

/// organization: insert, then fetch-by-UUID from cache round-trips.
pub(crate) async fn organization_insert_get<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_org_insert").await;
    let org = insert_org(db, "org_insert_test", &node).await;

    let fetched: OrganizationSchema =
        <DatabaseManager<R, P, C> as CacheFunctions<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationUuid,
            OrganizationCacheId,
            OrganizationId,
        >>::get_from_cache(db, &CacheObjectType::new(Some(OrganizationCacheUuid::new(None, org.uuid())), None), tel)
        .await
        .expect("get organization after insert");

    assert_eq!(fetched.uuid(), org.uuid());
    assert_eq!(fetched.id(), org.id());
}

/// organization: select-by-id and select-by-uuid round-trip.
pub(crate) async fn organization_select<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_org_select").await;
    let org = insert_org(db, "org_select_test", &node).await;

    let by_uuid: OrganizationSchema = db.select_organization_uuid(&org.uuid(), tel).await.expect("select org by uuid");
    assert_eq!(by_uuid.id(), org.id());

    let by_id: OrganizationSchema = db.select_organization_id(&org.id(), tel).await.expect("select org by id");
    assert_eq!(by_id.uuid(), org.uuid());
}

/// organization: delete removes the row (subsequent select errors).
pub(crate) async fn organization_delete<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_org_delete").await;
    let org = insert_org(db, "org_delete_test", &node).await;

    let delete_obj: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> =
        CacheObjectType::new(Some(OrganizationCacheUuid::new(None, org.uuid())), None);
    let deleter = <DeleteOrganization as DeleteMethod<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
        R,
        P,
        C,
    >>::new(delete_obj);
    deleter.delete(db, tel).await.expect("delete organization");

    let result = db.select_organization_uuid::<OrganizationSchema>(&org.uuid(), tel).await;
    assert!(result.is_err(), "organization should not exist after deletion");
}

/// user: insert (regular) then fetch-by-UUID from cache round-trips.
pub(crate) async fn user_insert_get<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_user_insert").await;
    let org = insert_org(db, "org_user_insert", &node).await;
    let user = insert_user(db, "test_user_regular", org.uuid()).await;

    let fetched: UserSchema =
        <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
            db,
            &CacheObjectType::new(Some(UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid())), None),
            tel,
        )
        .await
        .expect("get user after insert");

    assert_eq!(fetched.uuid(), user.uuid());
    assert_eq!(fetched.username(), user.username());
}

/// user: admin insert then fetch-by-UUID from cache round-trips.
pub(crate) async fn admin_user_insert_get<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_admin_user").await;
    let org = insert_org(db, "org_admin_user", &node).await;
    let admin = insert_admin_user(db, "admin_user_1", org.uuid()).await;

    let fetched: UserSchema =
        <DatabaseManager<R, P, C> as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
            db,
            &CacheObjectType::new(Some(UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), admin.uuid())), None),
            tel,
        )
        .await
        .expect("get admin user");
    assert_eq!(fetched.uuid(), admin.uuid());
}

/// user: delete removes the row.
pub(crate) async fn user_delete<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_user_del").await;
    let org = insert_org(db, "org_user_del", &node).await;
    let user = insert_user(db, "user_to_delete", org.uuid()).await;

    let delete_obj: CacheObjectType<UserCacheUuid, UserCacheId> =
        CacheObjectType::new(Some(UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid())), None);
    let deleter = <DeleteUser as DeleteMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId, R, P, C>>::new(delete_obj);
    deleter.delete(db, tel).await.expect("delete user");

    let result: Result<UserSchema, _> = db.select_user_uuid(&user.uuid(), tel).await;
    assert!(result.is_err(), "user should be deleted");
}

/// eden_node: select by both id and uuid round-trips.
pub(crate) async fn eden_node_select<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_select_test").await;

    let by_id: EdenNodeSchema = db.select_eden_node_id(&node.id(), tel).await.expect("select eden node by id");
    assert_eq!(by_id.uuid(), node.uuid());

    let by_uuid: EdenNodeSchema = db.select_eden_node_uuid(&node.uuid(), tel).await.expect("select eden node by uuid");
    assert_eq!(by_uuid.id(), node.id());
}

/// Multiple users in the same org are independently retrievable.
pub(crate) async fn multiple_users_same_org<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let node = insert_eden_node(db, "node_multi_user").await;
    let org = insert_org(db, "org_multi_user", &node).await;
    let user_a = insert_user(db, "user_a", org.uuid()).await;
    let user_b = insert_user(db, "user_b", org.uuid()).await;

    let fetched_a: UserSchema = db.select_user_uuid(&user_a.uuid(), tel).await.expect("select user_a");
    let fetched_b: UserSchema = db.select_user_uuid(&user_b.uuid(), tel).await.expect("select user_b");
    assert_eq!(fetched_a.id(), user_a.id());
    assert_eq!(fetched_b.id(), user_b.id());
    assert_ne!(user_a.uuid(), user_b.uuid());
}

/// analytics_dashboard_prefs: upsert/fetch is scoped by user and organization.
pub(crate) async fn analytics_dashboard_prefs_roundtrip<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let user = Uuid::new_v4();
    let org = Uuid::new_v4();

    assert!(db.get_analytics_prefs(user, org).await.expect("get empty").is_none());

    let blob = r#"{"yranges":{"eden.total_duration":[0,4000]}}"#;
    db.upsert_analytics_prefs(user, org, blob, "1000").await.expect("insert prefs");
    assert_eq!(db.get_analytics_prefs(user, org).await.expect("get prefs").as_deref(), Some(blob));

    let blob2 = r##"{"colorranges":{"eden.error_count":[{"name":"hot","min":0,"max":9,"color":"#ff0000"}]}}"##;
    db.upsert_analytics_prefs(user, org, blob2, "2000").await.expect("update prefs");
    assert_eq!(db.get_analytics_prefs(user, org).await.expect("get updated").as_deref(), Some(blob2));

    let other_user = Uuid::new_v4();
    assert!(db.get_analytics_prefs(other_user, org).await.expect("get other user").is_none());

    let other_org = Uuid::new_v4();
    assert!(db.get_analytics_prefs(user, other_org).await.expect("get other org").is_none());
}

fn skill_fixture<'a>(name: &'a str, display_name: &'a str, is_active: bool, organization_uuid: Option<Uuid>) -> NewSkill<'a> {
    NewSkill {
        name,
        display_name,
        description: "parity skill",
        body_markdown: "# Parity\nBody content",
        tags: vec!["parity".to_string()],
        estimated_tokens: 42,
        source_format: "markdown",
        is_active,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "standard",
        endpoint_kind: Some("postgres"),
        organization_uuid,
    }
}

/// llm_skills: CRUD and list/fetch scoping agree across database backends.
pub(crate) async fn llm_skills_roundtrip<R, P, C>(db: &DatabaseManager<R, P, C>)
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    let tel = &mut test_telemetry();
    let org_a = Uuid::new_v4();
    let org_b = Uuid::new_v4();
    let global_id = Uuid::new_v4();
    let org_skill_id = Uuid::new_v4();
    let other_org_skill_id = Uuid::new_v4();
    let inactive_skill_id = Uuid::new_v4();

    let global = db
        .upsert_skill(global_id, skill_fixture("global_skill", "Global Skill", true, None), tel)
        .await
        .expect("insert global skill");
    assert_eq!(global.id, global_id);
    assert_eq!(global.organization_uuid, None);

    let org_skill = db
        .upsert_skill(org_skill_id, skill_fixture("org_skill", "Org Skill", true, Some(org_a)), tel)
        .await
        .expect("insert org skill");
    assert_eq!(org_skill.organization_uuid, Some(org_a));

    db.upsert_skill(other_org_skill_id, skill_fixture("other_org_skill", "Other Org Skill", true, Some(org_b)), tel)
        .await
        .expect("insert other org skill");
    db.upsert_skill(
        inactive_skill_id,
        skill_fixture("inactive_org_skill", "Inactive Org Skill", false, Some(org_a)),
        tel,
    )
    .await
    .expect("insert inactive org skill");

    let fetched_global = db
        .get_skill_by_name_for_org(org_a, "global_skill", tel)
        .await
        .expect("fetch visible global skill")
        .expect("global skill should be visible to org");
    assert_eq!(fetched_global.id, global_id);
    assert_eq!(fetched_global.organization_uuid, None);

    let fetched_org = db
        .get_skill_by_uuid_for_org(org_a, org_skill_id, tel)
        .await
        .expect("fetch org skill by uuid")
        .expect("org skill should be visible to owning org");
    assert_eq!(fetched_org.id, org_skill_id);
    assert_eq!(fetched_org.organization_uuid, Some(org_a));

    assert!(
        db.get_skill_by_uuid_for_org(org_b, org_skill_id, tel).await.expect("fetch org skill from other org").is_none(),
        "tenant-scoped skill must not be visible to another org"
    );

    let active_for_org_a = db.list_active_skills_for_org(org_a, tel).await.expect("list active skills for org");
    let active_names: Vec<&str> = active_for_org_a.iter().map(|skill| skill.name.as_str()).collect();
    assert!(active_names.contains(&"global_skill"));
    assert!(active_names.contains(&"org_skill"));
    assert!(!active_names.contains(&"inactive_org_skill"));
    assert!(!active_names.contains(&"other_org_skill"));

    let all_for_org_a = db.list_all_skills_for_org(org_a, tel).await.expect("list all skills for org");
    let all_names: Vec<&str> = all_for_org_a.iter().map(|skill| skill.name.as_str()).collect();
    assert!(all_names.contains(&"global_skill"));
    assert!(all_names.contains(&"org_skill"));
    assert!(all_names.contains(&"inactive_org_skill"));
    assert!(!all_names.contains(&"other_org_skill"));

    let wrong_scope_update = db
        .update_skill(org_skill_id, Some(org_b), skill_fixture("org_skill", "Wrong Scope Update", true, Some(org_b)), tel)
        .await
        .expect("wrong scope update");
    assert!(wrong_scope_update.is_none(), "cross-org update should look like not found");

    let updated = db
        .update_skill(org_skill_id, Some(org_a), skill_fixture("org_skill", "Updated Org Skill", true, Some(org_a)), tel)
        .await
        .expect("owning org update")
        .expect("owning org update should return skill");
    assert_eq!(updated.display_name, "Updated Org Skill");
    assert_eq!(updated.organization_uuid, Some(org_a));

    let wrong_scope_delete = db.delete_skill(org_skill_id, Some(org_b), tel).await.expect("wrong scope delete");
    assert!(!wrong_scope_delete, "cross-org delete should look like not found");
    assert!(
        db.get_skill_by_uuid_for_org(org_a, org_skill_id, tel).await.expect("fetch after failed delete").is_some(),
        "failed cross-org delete must leave skill intact"
    );

    let deleted = db.delete_skill(org_skill_id, Some(org_a), tel).await.expect("owning org delete");
    assert!(deleted, "owning org should delete its skill");
    assert!(
        db.get_skill_by_uuid_for_org(org_a, org_skill_id, tel).await.expect("fetch after delete").is_none(),
        "deleted skill should no longer be visible"
    );
}
