#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::SalesforceClient;
use deadpool::unmanaged::Pool;

/// Type alias for Salesforce async client pool (read operations).
pub type SalesforceAsync = Pool<SalesforceClient>;

/// Type alias for Salesforce client pool (write operations).
pub type SalesforceTx = Pool<SalesforceClient>;
