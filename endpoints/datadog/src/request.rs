use crate::EpRequest;
use crate::api::lib::DatadogApi;
use crate::{DatadogOperation, Operation};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;

define_request!(EpKind::Datadog => Datadog, DatadogOperation, DatadogAsync, DatadogApi, DatadogTx);

define_request_serializer_stuff!(EpKind::Datadog => DatadogRequest);
