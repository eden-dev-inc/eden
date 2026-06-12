use crate::api::lib::MssqlApi;
use crate::{EpRequest, MssqlOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use mssql_core::{MssqlAsync, MssqlTx};

define_request!(EpKind::Mssql => Mssql, MssqlOperation, MssqlAsync, MssqlApi, MssqlTx);

define_request_serializer_stuff!(EpKind::Mssql => MssqlRequest);
