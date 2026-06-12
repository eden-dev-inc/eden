use crate::api::lib::query::QueryInput;
use crate::metadata::stc::common::ratio_percentage;
use crate::metadata::stc::utils::{RowExt, map_rows, run_named_query, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use oracle_client::Row;
use oracle_core::OracleAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use telemetry::TelemetryWrapper;

mod collection;
mod collector;
mod methods;
mod models;

pub use models::*;
