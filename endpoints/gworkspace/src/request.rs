use crate::EpRequest;
use crate::api::lib::GoogleWorkspaceApi;
use crate::{GoogleWorkspaceOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use gworkspace_core::{GoogleWorkspaceAsync, GoogleWorkspaceTx};

define_request!(EpKind::GoogleWorkspace => GoogleWorkspace, GoogleWorkspaceOperation, GoogleWorkspaceAsync, GoogleWorkspaceApi, GoogleWorkspaceTx);

define_request_serializer_stuff!(EpKind::GoogleWorkspace => GoogleWorkspaceRequest);
