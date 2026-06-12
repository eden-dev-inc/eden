use std::collections::HashMap;
use std::fmt;

use eden_logger_internal::{ctx_with_trace, log_trace};
use serde::{Deserialize, Serialize};

use crate::PubKey;
use function_name::named;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ServiceKind {
    Relay,
    Block,
    Consensus,
    Engine,
    AddressBook,
}

impl fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default().replace('"', ""))
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Service {
    pub kind: ServiceKind,
    pub public_key: PubKey,
    pub host: String,
}

#[derive(Clone, Default)]
pub struct AddressBook {
    pub services: HashMap<ServiceKind, Service>,
}

impl AddressBook {
    #[named]
    pub fn add(&mut self, service: &Service) {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.services.insert(service.kind, service.clone()) {
            #[allow(unused_variables)] // Used in log_trace! when log features enabled
            Some(old_service) => {
                log_trace!(
                    _ctx,
                    "Service replaced",
                    audience = eden_logger_internal::LogAudience::Internal,
                    old_service = old_service.to_string(),
                    new_service = service.to_string()
                );
            }
            None => {
                log_trace!(
                    _ctx,
                    "New service added",
                    audience = eden_logger_internal::LogAudience::Internal,
                    service = service.to_string()
                );
            }
        }
    }

    #[named]
    pub fn service(&self, service_kind: ServiceKind) -> Option<Service> {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.services.get(&service_kind) {
            Some(s) => {
                log_trace!(
                    _ctx,
                    "Service found",
                    audience = eden_logger_internal::LogAudience::Internal,
                    service = s.to_string()
                );
                Some(s.clone())
            }
            None => {
                log_trace!(
                    _ctx,
                    "Service not found",
                    audience = eden_logger_internal::LogAudience::Internal,
                    service_kind = service_kind.to_string()
                );
                None
            }
        }
    }
}

impl fmt::Display for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}@{}", self.kind, self.public_key, self.host)
    }
}
