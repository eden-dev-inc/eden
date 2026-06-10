use crate::api::lib::TavilyApi;
use ep_core::{define_operation_types, implement_operation_registry};
use tavily_core::{TavilyAsync, TavilyTx};

define_operation_types!();

implement_operation_registry!(TavilyOperation<TavilyAsync, TavilyApi, TavilyTx>);
