use crate::EpRequest;
use crate::api::lib::TavilyApi;
use crate::{Operation, TavilyOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use tavily_core::{TavilyAsync, TavilyTx};

define_request!(EpKind::Tavily => Tavily, TavilyOperation, TavilyAsync, TavilyApi, TavilyTx);

define_request_serializer_stuff!(EpKind::Tavily => TavilyRequest);
