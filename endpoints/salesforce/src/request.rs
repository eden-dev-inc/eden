use crate::EpRequest;
use crate::api::lib::SalesforceApi;
use crate::{Operation, SalesforceOperation};
use ep_core::{define_request, define_request_serializer_stuff};
use format::endpoint::EpKind;
use salesforce_core::{SalesforceAsync, SalesforceTx};

define_request!(EpKind::Salesforce => Salesforce, SalesforceOperation, SalesforceAsync, SalesforceApi, SalesforceTx);

define_request_serializer_stuff!(EpKind::Salesforce => SalesforceRequest);
