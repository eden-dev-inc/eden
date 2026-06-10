use crate::api::lib::MysqlApi;
use crate::{EpRequest, MysqlOperation, Operation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use mysql_core::{MysqlAsync, MysqlTx};

define_request!(EpKind::Mysql => Mysql, MysqlOperation, MysqlAsync, MysqlApi, MysqlTx);

define_request_serializer_stuff!(EpKind::Mysql => MysqlRequest);
