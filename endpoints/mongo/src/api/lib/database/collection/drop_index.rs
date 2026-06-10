use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::DropIndexOptionsWrapper;
use crate::output::EmptyOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::Collection;
use mongodb::bson::Document;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, DropIndexInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::DropIndex)))),
    "Drops the index specified by name from this collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct DropIndexInput {
        database: String,
        collection: String,
        name: String,
        options: Option<DropIndexOptionsWrapper>,
    }
}

impl_simple_operation!(DropIndexInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl DropIndexInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_drop_index(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_drop_index(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            EmptyOutput(context.drop_index(&self.name, self.options.to_owned().map(Into::into)).await.map_err(EpError::database)?)
                .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::IndexOptionsWrapper;
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_drop_index_basic() {
        let mut ctx = MongoTestContext::new().await;

        // Create an index, then drop it by the returned name.
        let name_val = ctx.create_index("di_basic", doc! { "temp": 1 }, None).await;
        let index_name = name_val.as_str().expect("create_index should return a string name");

        ctx.drop_index("di_basic", index_name).await;

        // Verify the index is gone.
        let names = ctx.list_index_names("di_basic").await;
        let names_arr = names.as_array().expect("should be array");
        let still_present = names_arr.iter().any(|n| n.as_str() == Some(index_name));
        assert!(!still_present, "dropped index should no longer appear in list_index_names");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_drop_index_named() {
        let mut ctx = MongoTestContext::new().await;

        let opts = IndexOptionsWrapper {
            name: Some("di_my_named_index".to_string()),
            ..Default::default()
        };
        ctx.create_index("di_named", doc! { "category": 1 }, Some(opts)).await;

        // Verify the index was created.
        let before = ctx.list_index_names("di_named").await;
        let before_arr = before.as_array().expect("should be array");
        let created = before_arr.iter().any(|n| n.as_str() == Some("di_my_named_index"));
        assert!(created, "named index should exist before drop");

        // Drop by custom name.
        ctx.drop_index("di_named", "di_my_named_index").await;

        // Verify the index is gone.
        let after = ctx.list_index_names("di_named").await;
        let after_arr = after.as_array().expect("should be array");
        let still_present = after_arr.iter().any(|n| n.as_str() == Some("di_my_named_index"));
        assert!(!still_present, "named index should be removed after drop");

        ctx.stop().await;
    }
}
