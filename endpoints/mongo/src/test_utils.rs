pub(crate) mod database_test_utils {
    #![allow(dead_code)]

    use crate::EP;
    use crate::api::lib::MongoApi;
    use crate::ep::MongoEp;
    use crate::request::MongoRequest;
    use crate::{ApiExample, EpRequest, Operation};
    use endpoint_test_utils::telemetry_test_utils::test_telemetry;
    use ep_core::settings::EdenSettings;
    use format::cache_uuid::EndpointCacheUuid;
    use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
    use mongo_core::config::MongoConfig;
    use mongo_core::connection::MongoConnection;
    use mongo_core::{MongoAsync, MongoTx};
    use std::future::Future;
    use telemetry::TelemetryWrapper;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

    pub(crate) async fn generic_write_sync_test<T: Clone + Operation<MongoAsync, MongoApi, MongoTx>>(example: &[ApiExample<T>]) {
        let (container, endpoint_cache_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        for api_example in example {
            let request = Box::new(MongoRequest(Box::new(api_example.request.clone()))) as Box<dyn EpRequest>;

            let output = ep
                .write(&endpoint_cache_uuid, &*request, EdenSettings::default(), test_telemetry)
                .await
                .expect("Failed to write to mongo");

            match &api_example.response {
                Ok(response) => {
                    if let Some(response) = response {
                        assert_eq!(
                            serde_json::from_str::<serde_json::Value>(&response.to_string()).expect("failed to deserialize"),
                            output
                        );
                    }
                }
                Err(e) => {
                    if let Some(e) = e {
                        assert_eq!(serde_json::from_str::<serde_json::Value>(&e.to_string()).expect("failed to deserialize"), output);
                    }
                }
            }
        }

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn generic_write_async_test<T: Clone + Operation<MongoAsync, MongoApi, MongoTx>>(example: &[ApiExample<T>]) {
        let (container, endpoint_cache_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        for api_example in example {
            let request = Box::new(MongoRequest(Box::new(api_example.request.clone()))) as Box<dyn EpRequest>;

            let output = ep
                .write(&endpoint_cache_uuid, &*request, EdenSettings::default(), test_telemetry)
                .await
                .expect("Failed to write to mongo");

            println!("{output}:{}", output);

            match &api_example.response {
                Ok(response) => {
                    if let Some(response) = response {
                        assert_eq!(
                            serde_json::from_str::<serde_json::Value>(&response.to_string()).expect("failed to deserialize"),
                            output
                        );
                    }
                }
                Err(e) => {
                    if let Some(e) = e {
                        assert_eq!(serde_json::from_str::<serde_json::Value>(&e.to_string()).expect("failed to deserialize"), output);
                    }
                }
            }
        }

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn generic_write_read_async_test<
        T: Clone + Operation<MongoAsync, MongoApi, MongoTx>,
        U: Clone + Operation<MongoAsync, MongoApi, MongoTx>,
    >(
        write_example: &[ApiExample<T>],
        read_example: &[ApiExample<U>],
    ) {
        let (container, endpoint_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        for api_example in write_example {
            let request = Box::new(MongoRequest(Box::new(api_example.request.clone()))) as Box<dyn EpRequest>;

            let output =
                ep.write(&endpoint_uuid, &*request, EdenSettings::default(), test_telemetry).await.expect("Failed to write to mongo");

            println!("{output}:{}", output);

            match &api_example.response {
                Ok(response) => {
                    if let Some(response) = response {
                        assert_eq!(response.to_owned(), output);
                    }
                }
                Err(e) => {
                    if let Some(e) = e {
                        assert_eq!(e.to_owned(), output);
                    }
                }
            }
        }

        for api_example in read_example {
            let mut request = Box::new(MongoRequest(Box::new(api_example.request.clone()))) as Box<dyn EpRequest>;

            let output = ep
                .read(&endpoint_uuid, &mut *request, EdenSettings::default(), test_telemetry)
                .await
                .expect("Failed to read from mongo");

            println!("{output}:{}", output);

            match &api_example.response {
                Ok(response) => {
                    if let Some(_response) = response {
                        // assert_eq!(response.to_owned(), output);
                    }
                }
                Err(e) => {
                    if e.is_some() {
                        // assert_eq!(e.to_owned(), output);
                    }
                }
            }
        }

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn initialize_write<T: Clone + Operation<MongoAsync, MongoApi, MongoTx>>(
        example: ApiExample<T>,
        endpoint_cache_uuid: &EndpointCacheUuid,
        mongo_ep: MongoEp,
        test_telemetry: &mut TelemetryWrapper,
    ) -> T {
        let request = Box::new(MongoRequest(Box::new(example.request.to_owned()))) as Box<dyn EpRequest>;

        let output = mongo_ep
            .write(endpoint_cache_uuid, &*request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to redis");

        match example.response {
            Ok(response) => {
                if let Some(response) = response {
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&response.to_string()).expect("failed to deserialize"),
                        output
                    );
                }
            }
            Err(e) => {
                if let Some(e) = e {
                    assert_eq!(serde_json::from_str::<serde_json::Value>(&e.to_string()).expect("failed to deserialize"), output)
                }
            }
        }

        example.request.clone()
    }

    pub(crate) async fn run_read<T: Clone + Operation<MongoAsync, MongoApi, MongoTx>>(
        request: T,
        endpoint_cache_uuid: &EndpointCacheUuid,
        ep: MongoEp,
        test_telemetry: &mut TelemetryWrapper,
    ) -> serde_json::Value {
        let mut request = Box::new(MongoRequest(Box::new(request))) as Box<dyn EpRequest>;

        ep.read(endpoint_cache_uuid, &mut *request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to redis")
    }

    pub(crate) async fn write_read_async_test<T, F, Fut>(f: F)
    where
        T: Clone + Operation<MongoAsync, MongoApi, MongoTx>,
        F: FnOnce(EndpointCacheUuid, MongoEp, &mut TelemetryWrapper) -> Fut,
        Fut: Future<Output = (serde_json::Value, T)>,
    {
        let (container, endpoint_cache_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        let (value, request) = f(endpoint_cache_uuid.clone(), ep.clone(), test_telemetry).await;

        let mut request = Box::new(MongoRequest(Box::new(request))) as Box<dyn EpRequest>;

        let output = ep
            .read(&endpoint_cache_uuid, &mut *request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to redis");

        assert_eq!(value, output);

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn write_write_read_async_test<T, F, Fut, G>(f: F)
    where
        T: Clone + Operation<MongoAsync, MongoApi, MongoTx>,
        F: FnOnce(EndpointCacheUuid, MongoEp, &mut TelemetryWrapper) -> Fut,
        Fut: Future<Output = (ApiExample<T>, serde_json::Value, G)>,
        G: Clone + Operation<MongoAsync, MongoApi, MongoTx>,
    {
        let (container, endpoint_cache_uuid, ep, mut test_telemetry) = connect_to_mongo().await;

        let test_telemetry = &mut test_telemetry;

        // Pass owned values instead of references
        let (example, value, read_request) = f(endpoint_cache_uuid.clone(), ep.clone(), test_telemetry).await;

        let request = Box::new(MongoRequest(Box::new(example.request))) as Box<dyn EpRequest>;

        let output = ep
            .write(&endpoint_cache_uuid, &*request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to redis");

        match example.response {
            Ok(response) => {
                if let Some(response) = response {
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&response.to_string()).expect("failed to deserialize"),
                        output
                    );
                }
            }
            Err(e) => {
                if let Some(e) = e {
                    assert_eq!(serde_json::from_str::<serde_json::Value>(&e.to_string()).expect("failed to deserialize"), output);
                }
            }
        }

        let output = run_read(read_request, &endpoint_cache_uuid, ep, test_telemetry).await;

        assert_eq!(value, output);

        container.stop().await.expect("Failed to stop database");
    }

    async fn initialize_mongo() -> (ContainerAsync<GenericImage>, String) {
        let container = GenericImage::new("mongo", "8.0.9").start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(27017).await.expect("Failed to get host port");

        let url = format!("mongodb://{host_ip}:{host_port}/");

        (container, url)
    }

    pub(crate) async fn connect_to_mongo() -> (ContainerAsync<GenericImage>, EndpointCacheUuid, MongoEp, TelemetryWrapper) {
        let test_telemetry = &mut test_telemetry();

        let (container, url) = initialize_mongo().await;

        let connection = MongoConnection { url, auth: None };

        let (target, creds) = connection.split().expect("split connection");
        let config = Box::new(MongoConfig {
            auth: None,
            target,
            read_credentials: Some(creds.clone()),
            write_credentials: Some(creds),
            content: Default::default(),
            accept: Default::default(),
            api_key: "".to_string(),
            ..Default::default()
        });

        let endpoint_cache_uuid =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

        let mut ep = MongoEp::new();

        ep.connect_async(&endpoint_cache_uuid, config.clone(), test_telemetry).await.expect("Failed to connect sync to mongo");
        ep.connect_async(&endpoint_cache_uuid, config, test_telemetry).await.expect("Failed to connect async to mongo");

        (container, endpoint_cache_uuid, ep, test_telemetry.clone())
    }

    #[tokio::test]
    async fn mongo_connection() {
        let _ = connect_to_mongo().await;
    }
}

#[cfg(feature = "integration")]
pub(crate) mod integration_test_utils {
    #![allow(dead_code)]

    use crate::EP;
    use crate::EpRequest;
    use crate::api::lib::database::collection::{
        aggregate::CollectionAggregateInput, count_documents::CountDocumentsInput, create_index::CreateIndexInput,
        create_indexes::CreateIndexesInput, delete_many::DeleteManyInput, delete_one::DeleteOneInput, distinct::DistinctInput,
        drop::DropInput as CollectionDropInput, drop_index::DropIndexInput, drop_indexes::DropIndexesInput,
        estimate_document_count::EstimateDocumentCountInput, find::FindInput, find_one::FindOneInput,
        find_one_and_delete::FindOneAndDeleteInput, find_one_and_replace::FindOneAndReplaceInput,
        find_one_and_update::FindOneAndUpdateInput, insert_many::InsertManyInput, insert_one::InsertOneInput,
        list_index_names::ListIndexNamesInput, list_indexes::ListIndexesInput, name::NameInput as CollectionNameInput,
        namespace::NamespaceInput, replace_one::ReplaceOneInput, update_many::UpdateManyInput, update_one::UpdateOneInput,
    };
    use crate::api::lib::database::collection::{
        read_concern::CollectionReadConcernInput, selection_criteria::CollectionSelectionCriteriaInput,
        write_concern::CollectionWriteConcernInput,
    };
    use crate::api::lib::database::gridfs_bucket::{
        delete::DeleteInput as GridfsDeleteInput, drop::DropInput as GridfsDropInput, find::FindInput as GridfsFindInput,
        read_concern::GridfsReadConcernInput, rename::GridfsRenameInput, selection_criteria::GridfsSelectionCriteriaInput,
        write_concern::GridfsWriteConcernInput,
    };
    use crate::api::lib::database::{
        aggregate::DatabaseAggregateInput, create_collection::CreateCollectionInput, drop::DropInput as DatabaseDropInput,
        list_collection_names::ListCollectionNamesInput, list_collections::ListCollectionsInput, name::NameInput as DatabaseNameInput,
        read_concern::DatabaseReadConcernInput, run_command::RunCommandInput, run_cursor_command::RunCursorCommandInput,
        selection_criteria::DatabaseSelectionCriteriaInput, write_concern::DatabaseWriteConcernInput,
    };
    use crate::api::lib::{ListDatabaseNamesInput, ListDatabasesInput};
    use crate::api::wrapper::*;
    use crate::ep::MongoEp;
    use crate::request::MongoRequest;
    use ep_core::settings::EdenSettings;
    use format::cache_uuid::EndpointCacheUuid;
    use mongodb::bson::Document;
    use serde_json::Value;
    use telemetry::TelemetryWrapper;
    use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

    use super::database_test_utils::connect_to_mongo;

    /// Test context providing convenience methods for MongoDB integration tests.
    /// Each test creates its own container for clean isolation.
    pub(crate) struct MongoTestContext {
        pub container: ContainerAsync<GenericImage>,
        pub endpoint_cache_uuid: EndpointCacheUuid,
        pub ep: MongoEp,
        pub telemetry: TelemetryWrapper,
        pub db: String,
    }

    impl MongoTestContext {
        pub async fn new() -> Self {
            let (container, endpoint_cache_uuid, ep, telemetry) = connect_to_mongo().await;
            Self {
                container,
                endpoint_cache_uuid,
                ep,
                telemetry,
                db: "test_db".to_string(),
            }
        }

        pub async fn stop(self) {
            self.container.stop().await.expect("Failed to stop container");
        }

        /// Strip the EndpointOutput wrapper to get the inner data value
        fn unwrap_response(value: Value) -> Value {
            if let Some(obj) = value.as_object()
                && let Some(data) = obj.get("data")
            {
                return data.clone();
            }
            value
        }

        async fn write_op<T: crate::Operation<mongo_core::MongoAsync, crate::api::lib::MongoApi, mongo_core::MongoTx> + Clone>(
            &mut self,
            input: T,
        ) -> Value {
            let request = Box::new(MongoRequest(Box::new(input))) as Box<dyn EpRequest>;
            let output = self
                .ep
                .write(&self.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut self.telemetry)
                .await
                .expect("Write operation failed");
            Self::unwrap_response(output)
        }

        async fn write_op_result<T: crate::Operation<mongo_core::MongoAsync, crate::api::lib::MongoApi, mongo_core::MongoTx> + Clone>(
            &mut self,
            input: T,
        ) -> Result<Value, error::EpError> {
            let request = Box::new(MongoRequest(Box::new(input))) as Box<dyn EpRequest>;
            self.ep
                .write(&self.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut self.telemetry)
                .await
                .map(Self::unwrap_response)
        }

        async fn read_op<T: crate::Operation<mongo_core::MongoAsync, crate::api::lib::MongoApi, mongo_core::MongoTx> + Clone>(
            &mut self,
            input: T,
        ) -> Value {
            let mut request = Box::new(MongoRequest(Box::new(input))) as Box<dyn EpRequest>;
            let output = self
                .ep
                .read(&self.endpoint_cache_uuid, &mut *request, EdenSettings::default(), &mut self.telemetry)
                .await
                .expect("Read operation failed");
            Self::unwrap_response(output)
        }

        async fn read_op_result<T: crate::Operation<mongo_core::MongoAsync, crate::api::lib::MongoApi, mongo_core::MongoTx> + Clone>(
            &mut self,
            input: T,
        ) -> Result<Value, error::EpError> {
            let mut request = Box::new(MongoRequest(Box::new(input))) as Box<dyn EpRequest>;
            self.ep
                .read(&self.endpoint_cache_uuid, &mut *request, EdenSettings::default(), &mut self.telemetry)
                .await
                .map(Self::unwrap_response)
        }

        // --- Insert operations ---

        pub async fn insert_one(&mut self, coll: &str, doc: Document) -> Value {
            self.write_op(InsertOneInput::new(self.db.clone(), coll.to_string(), DocumentWrapper::from(doc), None)).await
        }

        pub async fn insert_one_with_options(&mut self, coll: &str, doc: Document, options: InsertOneOptionsWrapper) -> Value {
            self.write_op(InsertOneInput::new(self.db.clone(), coll.to_string(), DocumentWrapper::from(doc), Some(options))).await
        }

        pub async fn insert_many(&mut self, coll: &str, docs: Vec<Document>) -> Value {
            let wrapper_docs: Vec<DocumentWrapperType> = docs.into_iter().map(DocumentFunction::from_document).collect();
            self.write_op(InsertManyInput::new(self.db.clone(), coll.to_string(), wrapper_docs, None)).await
        }

        pub async fn insert_many_with_options(&mut self, coll: &str, docs: Vec<Document>, options: InsertManyOptionsWrapper) -> Value {
            let wrapper_docs: Vec<DocumentWrapperType> = docs.into_iter().map(DocumentFunction::from_document).collect();
            self.write_op(InsertManyInput::new(self.db.clone(), coll.to_string(), wrapper_docs, Some(options))).await
        }

        // --- Find operations ---

        pub async fn find(&mut self, coll: &str, filter: Option<Document>) -> Value {
            self.read_op(FindInput::new(self.db.clone(), coll.to_string(), filter.map(DocumentFunction::from_document), None)).await
        }

        pub async fn find_with_options(&mut self, coll: &str, filter: Option<Document>, options: FindOptionsWrapper) -> Value {
            self.read_op(FindInput::new(
                self.db.clone(),
                coll.to_string(),
                filter.map(DocumentFunction::from_document),
                Some(options),
            ))
            .await
        }

        pub async fn find_one(&mut self, coll: &str, filter: Option<Document>) -> Value {
            self.read_op(FindOneInput::new(self.db.clone(), coll.to_string(), filter.map(DocumentWrapper::from), None)).await
        }

        pub async fn find_one_with_options(&mut self, coll: &str, filter: Option<Document>, options: FindOneOptionsWrapper) -> Value {
            self.read_op(FindOneInput::new(
                self.db.clone(),
                coll.to_string(),
                filter.map(DocumentWrapper::from),
                Some(options),
            ))
            .await
        }

        // --- Update operations ---

        pub async fn update_one(&mut self, coll: &str, filter: Document, update: Document) -> Value {
            self.write_op(UpdateOneInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                UpdateModificationsWrapper::Document(DocumentWrapper::from(update)),
                None,
            ))
            .await
        }

        pub async fn update_one_with_options(
            &mut self,
            coll: &str,
            filter: Document,
            update: Document,
            options: UpdateOptionsWrapper,
        ) -> Value {
            self.write_op(UpdateOneInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                UpdateModificationsWrapper::Document(DocumentWrapper::from(update)),
                Some(options),
            ))
            .await
        }

        pub async fn update_many(&mut self, coll: &str, filter: Document, update: Document) -> Value {
            self.write_op(UpdateManyInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                UpdateModificationsWrapper::Document(DocumentWrapper::from(update)),
                None,
            ))
            .await
        }

        pub async fn update_many_with_options(
            &mut self,
            coll: &str,
            filter: Document,
            update: Document,
            options: UpdateOptionsWrapper,
        ) -> Value {
            self.write_op(UpdateManyInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                UpdateModificationsWrapper::Document(DocumentWrapper::from(update)),
                Some(options),
            ))
            .await
        }

        // --- Delete operations ---

        pub async fn delete_one(&mut self, coll: &str, filter: Document) -> Value {
            self.write_op(DeleteOneInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                None,
            ))
            .await
        }

        pub async fn delete_many(&mut self, coll: &str, filter: Document) -> Value {
            self.write_op(DeleteManyInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                None,
            ))
            .await
        }

        // --- Find and modify operations ---

        pub async fn find_one_and_update(
            &mut self,
            coll: &str,
            filter: Document,
            update: Document,
            options: Option<FindOneAndUpdateOptionsWrapper>,
        ) -> Value {
            self.write_op(FindOneAndUpdateInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                UpdateModificationsWrapper::Document(DocumentWrapper::from(update)),
                options,
            ))
            .await
        }

        pub async fn find_one_and_delete(
            &mut self,
            coll: &str,
            filter: Document,
            options: Option<FindOneAndDeleteOptionsWrapper>,
        ) -> Value {
            self.write_op(FindOneAndDeleteInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                options,
            ))
            .await
        }

        pub async fn find_one_and_replace(
            &mut self,
            coll: &str,
            filter: Document,
            replacement: Document,
            options: Option<FindOneAndReplaceOptionsWrapper>,
        ) -> Value {
            self.write_op(FindOneAndReplaceInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                DocumentFunction::from_document(replacement),
                options,
            ))
            .await
        }

        // --- Replace operations ---

        pub async fn replace_one(&mut self, coll: &str, filter: Document, replacement: Document) -> Value {
            self.write_op(ReplaceOneInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                DocumentFunction::from_document(replacement),
                None,
            ))
            .await
        }

        pub async fn replace_one_with_options(
            &mut self,
            coll: &str,
            filter: Document,
            replacement: Document,
            options: ReplaceOptionsWrapper,
        ) -> Value {
            self.write_op(ReplaceOneInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                DocumentFunction::from_document(replacement),
                Some(options),
            ))
            .await
        }

        // --- Aggregation ---

        pub async fn aggregate(&mut self, coll: &str, pipeline: Vec<Document>) -> Value {
            let wrapper_pipeline: Vec<DocumentWrapperType> = pipeline.into_iter().map(DocumentFunction::from_document).collect();
            self.write_op(CollectionAggregateInput::new(self.db.clone(), coll.to_string(), wrapper_pipeline, None)).await
        }

        // --- Count and distinct ---

        pub async fn count_documents(&mut self, coll: &str, filter: Option<Document>) -> Value {
            self.read_op(CountDocumentsInput::new(
                self.db.clone(),
                coll.to_string(),
                filter.map(DocumentFunction::from_document),
                None,
            ))
            .await
        }

        pub async fn distinct(&mut self, coll: &str, field_name: &str, filter: Option<Document>) -> Value {
            self.read_op(DistinctInput::new(
                self.db.clone(),
                coll.to_string(),
                field_name.to_string(),
                filter.map(DocumentFunction::from_document),
                None,
            ))
            .await
        }

        pub async fn estimated_document_count(&mut self, coll: &str) -> Value {
            self.read_op(EstimateDocumentCountInput::new(self.db.clone(), coll.to_string(), None)).await
        }

        // --- Index operations ---

        pub async fn create_index(&mut self, coll: &str, keys: Document, options: Option<IndexOptionsWrapper>) -> Value {
            self.write_op(CreateIndexInput::new(
                self.db.clone(),
                coll.to_string(),
                IndexModelWrapper { keys: DocumentWrapper::from(keys), options },
                None,
            ))
            .await
        }

        pub async fn create_indexes(&mut self, coll: &str, models: Vec<IndexModelWrapper>) -> Value {
            self.write_op(CreateIndexesInput::new(self.db.clone(), coll.to_string(), models, None)).await
        }

        pub async fn list_indexes(&mut self, coll: &str) -> Value {
            self.read_op(ListIndexesInput::new(self.db.clone(), coll.to_string(), None)).await
        }

        pub async fn list_index_names(&mut self, coll: &str) -> Value {
            self.read_op(ListIndexNamesInput::new(self.db.clone(), coll.to_string())).await
        }

        pub async fn drop_index(&mut self, coll: &str, name: &str) -> Value {
            self.write_op(DropIndexInput::new(self.db.clone(), coll.to_string(), name.to_string(), None)).await
        }

        pub async fn drop_indexes(&mut self, coll: &str) -> Value {
            self.write_op(DropIndexesInput::new(self.db.clone(), coll.to_string(), None)).await
        }

        // --- Collection operations ---

        pub async fn drop_collection(&mut self, coll: &str) -> Value {
            self.write_op(CollectionDropInput::new(self.db.clone(), coll.to_string(), None)).await
        }

        pub async fn collection_name(&mut self, coll: &str) -> Value {
            self.read_op(CollectionNameInput::new(self.db.clone(), coll.to_string())).await
        }

        pub async fn collection_namespace(&mut self, coll: &str) -> Value {
            self.read_op(NamespaceInput::new(self.db.clone(), coll.to_string())).await
        }

        // --- Database operations ---

        pub async fn create_collection(&mut self, name: &str) -> Value {
            self.write_op(CreateCollectionInput::new(self.db.clone(), name.to_string(), None)).await
        }

        pub async fn create_collection_with_options(&mut self, name: &str, options: CreateCollectionOptionsWrapper) -> Value {
            self.write_op(CreateCollectionInput::new(self.db.clone(), name.to_string(), Some(options))).await
        }

        pub async fn list_collection_names(&mut self) -> Value {
            self.read_op(ListCollectionNamesInput::new(self.db.clone(), None)).await
        }

        pub async fn run_command(&mut self, command: Document) -> Value {
            self.write_op(RunCommandInput::new(self.db.clone(), DocumentFunction::from_document(command), None)).await
        }

        // --- Client operations ---

        pub async fn list_database_names(&mut self) -> Value {
            self.read_op(ListDatabaseNamesInput::new(None, None)).await
        }

        pub async fn list_databases(&mut self) -> Value {
            self.read_op(ListDatabasesInput::new(None, None)).await
        }

        // --- Database-level operations ---

        pub async fn database_aggregate(&mut self, pipeline: Vec<Document>) -> Value {
            let wrapper_pipeline: Vec<DocumentWrapperType> = pipeline.into_iter().map(DocumentFunction::from_document).collect();
            self.read_op(DatabaseAggregateInput::new(self.db.clone(), wrapper_pipeline, None)).await
        }

        pub async fn database_drop(&mut self) -> Value {
            self.write_op(DatabaseDropInput::new(self.db.clone(), None)).await
        }

        pub async fn database_name(&mut self) -> Value {
            self.read_op(DatabaseNameInput::new(self.db.clone())).await
        }

        pub async fn list_collections(&mut self) -> Value {
            self.read_op(ListCollectionsInput::new(self.db.clone(), None, None)).await
        }

        pub async fn list_collections_with_filter(&mut self, filter: Document) -> Value {
            self.read_op(ListCollectionsInput::new(self.db.clone(), Some(DocumentFunction::from_document(filter)), None)).await
        }

        pub async fn run_cursor_command(&mut self, command: Document) -> Value {
            self.write_op(RunCursorCommandInput::new(self.db.clone(), DocumentFunction::from_document(command), None)).await
        }

        // --- Database-level config getters ---

        pub async fn database_read_concern(&mut self) -> Value {
            self.read_op(DatabaseReadConcernInput::new(self.db.clone())).await
        }

        pub async fn database_write_concern(&mut self) -> Value {
            self.read_op(DatabaseWriteConcernInput::new(self.db.clone())).await
        }

        pub async fn database_selection_criteria(&mut self) -> Value {
            self.read_op(DatabaseSelectionCriteriaInput::new(self.db.clone())).await
        }

        // --- Collection-level config getters ---

        pub async fn collection_read_concern(&mut self, coll: &str) -> Value {
            self.read_op(CollectionReadConcernInput::new(self.db.clone(), coll.to_string())).await
        }

        pub async fn collection_write_concern(&mut self, coll: &str) -> Value {
            self.read_op(CollectionWriteConcernInput::new(self.db.clone(), coll.to_string())).await
        }

        pub async fn collection_selection_criteria(&mut self, coll: &str) -> Value {
            self.read_op(CollectionSelectionCriteriaInput::new(self.db.clone(), coll.to_string())).await
        }

        // --- GridFS operations ---

        pub async fn gridfs_find(&mut self, filter: Document) -> Value {
            self.read_op(GridfsFindInput::new(self.db.clone(), None, DocumentFunction::from_document(filter), None)).await
        }

        pub async fn gridfs_delete(&mut self, id: BsonWrapper) -> Value {
            self.write_op(GridfsDeleteInput::new(self.db.clone(), None, id)).await
        }

        pub async fn gridfs_delete_result(&mut self, id: BsonWrapper) -> Result<Value, error::EpError> {
            self.write_op_result(GridfsDeleteInput::new(self.db.clone(), None, id)).await
        }

        pub async fn gridfs_drop(&mut self) -> Value {
            self.write_op(GridfsDropInput::new(self.db.clone(), None)).await
        }

        pub async fn gridfs_rename(&mut self, id: BsonWrapper, new_filename: String) -> Value {
            self.write_op(GridfsRenameInput::new(self.db.clone(), None, id, new_filename)).await
        }

        pub async fn gridfs_rename_result(&mut self, id: BsonWrapper, new_filename: String) -> Result<Value, error::EpError> {
            self.write_op_result(GridfsRenameInput::new(self.db.clone(), None, id, new_filename)).await
        }

        // --- GridFS-level config getters ---

        pub async fn gridfs_read_concern(&mut self) -> Value {
            self.read_op(GridfsReadConcernInput::new(self.db.clone(), None)).await
        }

        pub async fn gridfs_write_concern(&mut self) -> Value {
            self.read_op(GridfsWriteConcernInput::new(self.db.clone(), None)).await
        }

        pub async fn gridfs_selection_criteria(&mut self) -> Value {
            self.read_op(GridfsSelectionCriteriaInput::new(self.db.clone(), None)).await
        }

        // --- Error-returning variants ---

        pub async fn insert_one_err(&mut self, coll: &str, doc: Document) -> error::EpError {
            self.write_op_result(InsertOneInput::new(self.db.clone(), coll.to_string(), DocumentWrapper::from(doc), None))
                .await
                .unwrap_err()
        }

        pub async fn update_one_err(&mut self, coll: &str, filter: Document, update: Document) -> error::EpError {
            self.write_op_result(UpdateOneInput::new(
                self.db.clone(),
                coll.to_string(),
                DocumentFunction::from_document(filter),
                UpdateModificationsWrapper::Document(DocumentWrapper::from(update)),
                None,
            ))
            .await
            .unwrap_err()
        }
    }
}
