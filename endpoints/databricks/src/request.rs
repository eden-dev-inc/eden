use crate::EpRequest;
use crate::api::lib::DatabricksApi;
use crate::{DatabricksOperation, Operation};
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;

define_request!(EpKind::Databricks => Databricks, DatabricksOperation, DatabricksAsync, DatabricksApi, DatabricksTx);

define_request_serializer_stuff!(EpKind::Databricks => DatabricksRequest);
