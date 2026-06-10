use crate::api::lib::GitlabApi;
use ep_core::{define_operation_types, implement_operation_registry};
use gitlab_core::{GitlabAsync, GitlabTx};

define_operation_types!();

implement_operation_registry!(GitlabOperation<GitlabAsync, GitlabApi, GitlabTx>);
