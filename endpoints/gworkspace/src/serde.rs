use crate::api::lib::GoogleWorkspaceApi;
use ep_core::{define_operation_types, implement_operation_registry};
use gworkspace_core::{GoogleWorkspaceAsync, GoogleWorkspaceTx};

define_operation_types!();

implement_operation_registry!(GoogleWorkspaceOperation<GoogleWorkspaceAsync, GoogleWorkspaceApi, GoogleWorkspaceTx>);
