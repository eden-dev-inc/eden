use crate::api::lib::{CollectionApi, DatabaseApi, MongoApi};
use crate::api::wrapper::{CreateIndexOptionsWrapper, IndexModelWrapper};
use crate::output::VecStringOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::bson::Document;
use mongodb::{Collection, IndexModel};
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, CreateIndexesInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Database(Some(DatabaseApi::Collection(Some(CollectionApi::CreateIndexes)))),
    "Creates the given indexes on this collection",
    ReqType::Write,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct CreateIndexesInput {
        database: String,
        collection: String,
        models: Vec<IndexModelWrapper>,
        options: Option<CreateIndexOptionsWrapper>,
    }
}

impl_simple_operation!(CreateIndexesInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl CreateIndexesInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        self.run_create_indexes(&context.database(&self.database).collection(&self.collection)).await
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
    async fn run_create_indexes(&self, context: &Collection<Document>) -> ResultEP<Box<dyn EpOutput>> {
        Ok(Box::new(
            VecStringOutput(
                context
                    .create_indexes(
                        self.models.iter().cloned().map(Into::into).collect::<Vec<IndexModel>>(),
                        self.options.to_owned().map(Into::into),
                    )
                    .await
                    .map_err(EpError::database)?
                    .index_names,
            )
            .to_output(),
        ) as Box<dyn EpOutput>)
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    #![allow(unexpected_cfgs)]
    use crate::api::wrapper::{DocumentWrapper, IndexModelWrapper, IndexOptionsWrapper};
    use crate::test_utils::integration_test_utils::MongoTestContext;
    use mongodb::bson::doc;
    use serial_test::serial;

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_indexes_multiple() {
        let mut ctx = MongoTestContext::new().await;

        let models = vec![
            IndexModelWrapper {
                keys: DocumentWrapper::from(doc! { "field_a": 1 }),
                options: None,
            },
            IndexModelWrapper {
                keys: DocumentWrapper::from(doc! { "field_b": -1 }),
                options: None,
            },
        ];

        let result = ctx.create_indexes("cis_multiple", models).await;

        assert!(result.is_array(), "create_indexes should return an array of index names");
        let names = result.as_array().expect("should be array");
        assert_eq!(names.len(), 2, "should have created 2 indexes");
        assert!(names[0].is_string(), "each name should be a string");
        assert!(names[1].is_string(), "each name should be a string");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_indexes_single() {
        let mut ctx = MongoTestContext::new().await;

        let models = vec![IndexModelWrapper {
            keys: DocumentWrapper::from(doc! { "solo_field": 1 }),
            options: None,
        }];

        let result = ctx.create_indexes("cis_single", models).await;

        assert!(result.is_array(), "create_indexes should return an array");
        let names = result.as_array().expect("should be array");
        assert_eq!(names.len(), 1, "should have created 1 index");
        assert!(names[0].is_string(), "name should be a string");

        ctx.stop().await;
    }

    #[cfg(feature = "integration")]
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_create_indexes_with_options() {
        let mut ctx = MongoTestContext::new().await;

        let models = vec![
            IndexModelWrapper {
                keys: DocumentWrapper::from(doc! { "x": 1 }),
                options: Some(IndexOptionsWrapper { name: Some("idx_x".to_string()), ..Default::default() }),
            },
            IndexModelWrapper {
                keys: DocumentWrapper::from(doc! { "y": 1 }),
                options: Some(IndexOptionsWrapper { name: Some("idx_y".to_string()), ..Default::default() }),
            },
        ];

        let result = ctx.create_indexes("cis_options", models).await;

        assert!(result.is_array(), "create_indexes should return an array");
        let names = result.as_array().expect("should be array");
        assert_eq!(names.len(), 2, "should have created 2 indexes");
        assert_eq!(names[0].as_str().expect("should be string"), "idx_x");
        assert_eq!(names[1].as_str().expect("should be string"), "idx_y");

        ctx.stop().await;
    }
}
