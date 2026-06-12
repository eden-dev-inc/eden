use crate::api::lib::OracleApi;
use crate::{EpRequest, Operation, OracleOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use oracle_core::{OracleAsync, OracleTx};

define_request!(EpKind::Oracle => Oracle, OracleOperation, OracleAsync, OracleApi, OracleTx);

define_request_serializer_stuff!(EpKind::Oracle => OracleRequest);
