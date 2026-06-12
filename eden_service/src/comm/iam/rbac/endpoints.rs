pub mod delete;
pub mod get;
pub mod subjects;

#[cfg(all(test, external_db))]
pub mod tests {
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    #[ignore = "TODO: implement RBAC endpoint CRUD assertions without relying on placeholder infra setup"]
    async fn rbac_endpoint_crud_test() {
        let _db_manager = create_database_manager().await;

        let _test_telemetry = &mut test_telemetry();

        //TODO
    }
}
