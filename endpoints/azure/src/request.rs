use crate::EpRequest;
use crate::api::lib::AzureApi;
use crate::{AzureOperation, Operation};
use azure_core::{AzureAsync, AzureTx};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;

define_request!(EpKind::Azure => Azure, AzureOperation, AzureAsync, AzureApi, AzureTx);

define_request_serializer_stuff!(EpKind::Azure => AzureRequest);
