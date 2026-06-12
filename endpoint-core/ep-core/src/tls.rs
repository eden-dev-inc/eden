use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;
use utoipa::ToSchema;

mod global_bundle;
pub use global_bundle::GLOBAL_BUNDLE_PEM;

#[derive(Serialize, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, ToSchema, Default)]
pub struct TlsData {
    pub tls_cert: String,
    pub tls_key: String,
    pub ca_cert: String,
    pub domain: String,
}
