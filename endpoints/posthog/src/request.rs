use crate::EpRequest;
use crate::api::lib::PosthogApi;
use crate::{Operation, PosthogOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use posthog_core::{PosthogAsync, PosthogTx};

define_request!(EpKind::Posthog => Posthog, PosthogOperation, PosthogAsync, PosthogApi, PosthogTx);

define_request_serializer_stuff!(EpKind::Posthog => PosthogRequest);
