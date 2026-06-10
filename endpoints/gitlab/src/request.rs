use crate::EpRequest;
use crate::api::lib::GitlabApi;
use crate::{GitlabOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use gitlab_core::{GitlabAsync, GitlabTx};

define_request!(EpKind::Gitlab => Gitlab, GitlabOperation, GitlabAsync, GitlabApi, GitlabTx);

define_request_serializer_stuff!(EpKind::Gitlab => GitlabRequest);
