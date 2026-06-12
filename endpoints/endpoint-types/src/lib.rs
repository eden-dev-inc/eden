#![cfg_attr(test, allow(clippy::unwrap_used))]
// Suppress async_fn_in_trait warning because we don't need to specify auto trait bounds for these traits.
#![allow(async_fn_in_trait)]

use std::{any::Any, borrow::Cow, fmt::Debug, io, pin::Pin};

use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{
    ep::{EpConfig, RWPool},
    settings::EdenSettings,
};
use error::{EpError, ResultEP};
pub use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

use crate::metadata::{EpMetadata, SyncMetadata};
pub use crate::{request::EpRequest, transaction::EpTransaction};
pub use ep_core::{EndpointType, EpOutput, ReqType, ToOutput};

pub mod analytics;
#[cfg(feature = "runtime")]
pub mod ep;
pub mod metadata;
pub mod protocol;
pub mod request;
pub mod transaction;

#[cfg(feature = "runtime")]
pub use ep::{EP, EpLifecycleRouter, EpLifecycleSpec};

pub trait EndpointOperation: Any + Send + Sync + Debug {}

/// Metadata describing an endpoint API operation.
///
/// Used for generating documentation and API schemas.
#[derive(Debug, Clone)]
pub struct ApiInfo<K, T>
where
    T: Clone + 'static,
{
    pub endpoint: EpKind,
    pub api: K,
    pub description: &'static str,
    pub request_type: ReqType,
    pub safe: bool,
    //TODO add versioning, using a vector in-case there are different deployment types (Open-Source)
    // enterprise, etc
    // pub minimum_versions: Vec<&'static str>,
    // pub maximum_versions: Vec<&'static str>,
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
        safe: bool,
        // examples: &'static [ApiExample<T>],
    ) -> Self {
        Self {
            api,
            endpoint,
            description,
            request_type,
            safe,
            examples: Vec::new(),
        }
    }

    pub const fn with_safe(mut self, safe: bool) -> Self {
        self.safe = safe;
        self
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

    pub fn safe(&self) -> bool {
        self.safe
    }

    pub fn examples(&self) -> &[ApiExample<T>] {
        &self.examples
    }
}

pub type RunOutput<'a> = Pin<Box<dyn Future<Output = ResultEP<Box<dyn EpOutput>>> + Send + 'a>>;
/// Returns the RAW binary output from the database
pub type RunOutputRaw = Pin<Box<dyn Future<Output = ResultEP<Vec<u8>>>>>;

/// Executes requests against database connections or transactions.
pub trait RunRequest<A: Send + 'static, K: 'static, X: 'static>: EndpointType + Send + Sync {
    /// Returns the wrapped operation.
    fn operation(&self) -> &dyn Operation<A, K, X>;
    /// Runs request against database connection.
    #[named]
    fn run_request(
        &self,
        context: A,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> impl Future<Output = ResultEP<Box<dyn EpOutput>>> + Send {
        async move {
            let mut span = telemetry_wrapper.server_tracer(format!("{}.{}", Self::r#type(), function_name!()));

            match self.operation().as_exec() {
                Some(op) => op.run_operation_request(context, telemetry_wrapper.clone()).await,
                None => {
                    span.set_status(FastSpanStatus::Error {
                        message: Cow::Owned(format!("Operation {:?} does not implement OperationExecutor", self.operation())),
                    });
                    Err(EpError::database("Operation does not implement OperationExecutor"))
                }
            }
        }
    }
    /// Runs request within a transaction.
    fn run_transaction(&self, tx_context: &mut X, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        match self.operation().as_exec() {
            Some(op) => {
                op.run_operation_transaction(tx_context, telemetry_wrapper);
                Ok(())
            }
            None => Err(EpError::database("Operation does not implement OperationExecutor")),
        }
    }
}

pub fn downcast_config<A: Send, C: EpConfig + RWPool<A> + Clone + ToSchema + 'static>(
    config: Box<dyn EpConfig>,
    span: &mut telemetry::FastSpan,
) -> ResultEP<C> {
    match config.as_any().downcast_ref::<C>() {
        Some(config) => Ok(config.to_owned()),
        None => {
            let error = "failed to downcast config";
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
            Err(EpError::connect(error))
        }
    }
}

pub fn downcast_request<'req, A: Send + 'static, K: 'static, X: 'static, Req: EpRequest + EndpointType + RunRequest<A, K, X> + 'static>(
    request: &'req dyn EpRequest,
    span: &mut telemetry::FastSpan,
) -> ResultEP<&'req Req> {
    match request.as_any().downcast_ref::<Req>() {
        Some(request) => Ok(request),
        None => {
            let error = "failed to downcast config";
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
            Err(EpError::connect(error))
        }
    }
}

pub fn downcast_metadata<
    A: Clone + Send + Sync + 'static,
    K: 'static,
    X: 'static,
    M: EpMetadata + SyncMetadata<A> + Clone + Serialize + 'static,
>(
    metadata: &dyn EpMetadata,
    span: &mut telemetry::FastSpan,
) -> ResultEP<M> {
    match metadata.as_any().downcast_ref::<M>() {
        Some(metadata) => Ok(metadata.to_owned()),
        None => {
            let error = "failed to downcast metadata";
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
            Err(EpError::metadata(error))
        }
    }
}

/// Example request/response pair for API documentation.
#[derive(Debug, Clone)]
pub struct ApiExample<T>
where
    T: Clone + 'static,
{
    pub name: &'static str,
    pub description: &'static str,
    pub request: T,
    pub response: Result<Option<Value>, Option<Value>>,
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

/// Maps operation types to their API kind enum variant.
pub trait OperationKind<K> {
    fn operation_kind() -> K;
}

pub trait Operation<A, K, X>: Any + Send + Sync + Debug + EndpointOperation {
    fn as_any(&self) -> &dyn Any;
    fn kind(&self) -> K;
    fn request_type(&self) -> ReqType;
    fn as_operation(self: Box<Self>) -> Box<dyn Operation<A, K, X>>;
    fn as_exec(&self) -> Option<&dyn OperationExecutor<A, K, X>>;
    fn clone_box(&self) -> Box<dyn Operation<A, K, X>>;
}

impl<A: 'static, K: 'static, X: 'static> Clone for Box<dyn Operation<A, K, X>> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Executor for operations that can run against connections or transactions.
pub trait OperationExecutor<A: 'static, K: 'static, X: 'static>: Operation<A, K, X> + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    // fn run_sync(&self, context: R, telemetry_context: TelemetryWrapper) -> RunOutput;
    fn run_operation_request<'a>(&'a self, context: A, telemetry_context: TelemetryWrapper) -> RunOutput<'a>;
    /// Executes operation within a transaction context.
    fn run_operation_transaction(&self, tx_context: &mut X, telemetry_context: &mut TelemetryWrapper);
}

pub trait ComplexExecutor<'a, T: 'static, A, K, X>: Operation<A, K, X> + Send + Sync {
    // fn kind(&self) -> MongoApiKind;
    fn as_any(&self) -> &dyn Any;
    // fn run_sync(
    //     &self,
    //     input: &'a Box<dyn EpOutput>,
    //     telemetry_wrapper: &mut TelemetryWrapper,
    // ) -> RunOutput;
    fn run_complex_request<'b>(&'b self, input: &'a dyn EpOutput, telemetry_wrapper: &mut TelemetryWrapper) -> RunOutput<'b>;
    fn downcast(input: &'a dyn EpOutput) -> ResultEP<&'a T>;
}

/// Multi-operation ACID transaction for a single endpoint.
///
/// Contains a vector of operations that execute atomically within
/// a database transaction.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone)]
pub struct Transaction<R: EpRequest + EndpointType + Serialize + 'static>(pub Vec<R>);

impl<R: EpRequest + EndpointType + Debug + Serialize + DeserializeOwned + BorshSerialize + Send + Sync + 'static> EpTransaction
    for Transaction<R>
{
    fn kind(&self) -> EpKind {
        R::r#type()
    }

    fn as_request(self: Box<Self>) -> Box<dyn EpTransaction> {
        self
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn borsh_serialize(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        struct WriteWrapper<'a>(&'a mut dyn io::Write);

        impl<'a> io::Write for WriteWrapper<'a> {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.write(buf)
            }

            fn flush(&mut self) -> io::Result<()> {
                self.0.flush()
            }
        }

        BorshSerialize::serialize(self, &mut WriteWrapper(writer))
    }
}

/// Constructs endpoint-specific request wrappers from operations.
pub trait RequestConstructor {
    type AsyncType;
    type ApiKindType;
    type TxType;
    type OperationType: Operation<Self::AsyncType, Self::ApiKindType, Self::TxType> + ?Sized;

    fn new(op: Box<Self::OperationType>) -> Self
    where
        Self: EpRequest;
}
