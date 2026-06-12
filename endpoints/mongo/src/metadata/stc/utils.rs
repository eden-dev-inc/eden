use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::DocumentWrapper;
use chrono::{DateTime, Utc};
use eden_logger_internal::{log_debug, trace_context};
use error::{EpError, ResultEP};
use futures_util::TryStreamExt;
use mongo_core::MongoAsync;
use mongodb::bson::{Bson, Document, doc};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

use format::timestamp::DateTimeWrapper;

/// Execute a MongoDB admin command and format result like profiling data.
/// This allows parsers expecting profiled command results to work unchanged.
pub async fn execute_admin_command_as_profiled(
    command: Document,
    context: MongoAsync,
    timeout_duration: Duration,
    label: &str,
) -> ResultEP<Vec<Document>> {
    let _ctx = trace_context();
    log_debug!(
        _ctx.clone(),
        "Executing admin command",
        audience = eden_logger_internal::LogAudience::Internal,
        command = label
    );

    let mongo_client = match context.get().await {
        Ok(client) => {
            log_debug!(
                _ctx.clone(),
                "Got MongoDB client",
                audience = eden_logger_internal::LogAudience::Internal,
                command = label
            );
            client
        }
        Err(e) => {
            log_debug!(
                _ctx.clone(),
                "Failed to get MongoDB client",
                audience = eden_logger_internal::LogAudience::Internal,
                command = label,
                error = format!("{:?}", e)
            );
            return Err(EpError::connect(e));
        }
    };

    let admin_db = mongo_client.database("admin");
    log_debug!(
        _ctx.clone(),
        "Got admin database",
        audience = eden_logger_internal::LogAudience::Internal,
        command = label
    );

    let result = match timeout(timeout_duration, admin_db.run_command(command.clone(), None)).await {
        Ok(Ok(doc)) => {
            log_debug!(
                _ctx.clone(),
                "Admin command succeeded",
                audience = eden_logger_internal::LogAudience::Internal,
                command = label,
                field_count = doc.len()
            );
            doc
        }
        Ok(Err(e)) => {
            log_debug!(
                _ctx.clone(),
                "Admin command failed",
                audience = eden_logger_internal::LogAudience::Internal,
                command = label,
                error = format!("{:?}", e)
            );
            return Err(EpError::database(e));
        }
        Err(_) => {
            log_debug!(
                _ctx.clone(),
                "Admin command timed out",
                audience = eden_logger_internal::LogAudience::Internal,
                command = label,
                timeout = format!("{:?}", timeout_duration)
            );
            return Err(EpError::metadata(format!("Query timeout for {label}")));
        }
    };

    // Wrap result like profiling data so existing parsers work unchanged
    let wrapped = vec![doc! {
        "result": result,
        "ts": mongodb::bson::DateTime::from_chrono(chrono::Utc::now())
    }];

    log_debug!(
        _ctx,
        "Wrapped admin command result",
        audience = eden_logger_internal::LogAudience::Internal,
        command = label,
        doc_count = wrapped.len()
    );
    Ok(wrapped)
}

/// Execute currentOp aggregation command and format like profiling data.
pub async fn execute_current_op_as_profiled(filter: Document, context: MongoAsync, timeout_duration: Duration) -> ResultEP<Vec<Document>> {
    execute_admin_command_as_profiled(doc! { "currentOp": 1, "filter": filter }, context, timeout_duration, "currentOp").await
}

/// Execute a prepared `FindInput` against MongoDB and hydrate the full result set.
pub async fn execute_find(requests: &HashMap<String, FindInput>, key: &str, context: MongoAsync) -> ResultEP<Vec<Document>> {
    let find_input = requests.get(key).ok_or_else(|| EpError::metadata(format!("Missing find operation: {key}")))?;

    let mongo_client = context.get().await.map_err(EpError::connect)?;
    let database = mongo_client.database(find_input.database());
    let collection = database.collection::<Document>(find_input.collection());

    let filter = find_input.filter().as_ref().map(|f| DocumentWrapper(f.clone()).into());

    let find_options = find_input.options().as_ref().map(|opts| opts.clone().into());

    let mut cursor = collection.find(filter, find_options).await.map_err(EpError::database)?;

    let mut results = Vec::new();
    while let Some(doc) = cursor.try_next().await.map_err(EpError::request)? {
        results.push(doc);
    }

    Ok(results)
}

/// Execute a metadata query with a timeout and consistent error message.
async fn fetch_with_timeout(
    requests: &HashMap<String, FindInput>,
    key: &str,
    context: MongoAsync,
    timeout_duration: Duration,
    label: &str,
) -> ResultEP<Vec<Document>> {
    timeout(timeout_duration, execute_find(requests, key, context))
        .await
        .map_err(|_| EpError::metadata(format!("Query timeout for {label}")))?
}

/// Convenience wrapper around [`fetch_with_timeout`] that uses `key` as the
/// timeout-error label, avoiding the duplicated argument at every call site.
pub async fn fetch(
    requests: &HashMap<String, FindInput>,
    key: &str,
    context: MongoAsync,
    timeout_duration: Duration,
) -> ResultEP<Vec<Document>> {
    fetch_with_timeout(requests, key, context, timeout_duration, key).await
}

/// Lightweight accessor around a BSON document to centralize type coercion.
pub struct DocAccessor<'a> {
    doc: &'a Document,
}

#[allow(dead_code)]
impl<'a> DocAccessor<'a> {
    pub fn new(doc: &'a Document) -> Self {
        Self { doc }
    }

    pub fn opt_u64(&self, key: &str) -> Option<u64> {
        self.doc.get(key).and_then(|v| match v {
            Bson::Int64(v) => Some((*v).max(0) as u64),
            Bson::Int32(v) => Some((*v).max(0) as u64),
            Bson::Double(v) => Some((*v as i64).max(0) as u64),
            _ => None,
        })
    }

    pub fn req_u64(&self, key: &str) -> ResultEP<u64> {
        self.opt_u64(key).ok_or_else(|| EpError::metadata(format!("Missing numeric field: {key}")))
    }

    pub fn opt_i64(&self, key: &str) -> Option<i64> {
        self.doc
            .get_i64(key)
            .ok()
            .or_else(|| self.doc.get_i32(key).map(|v| v as i64).ok())
            .or_else(|| self.doc.get_f64(key).map(|v| v as i64).ok())
    }

    pub fn req_i64(&self, key: &str) -> ResultEP<i64> {
        self.opt_i64(key).ok_or_else(|| EpError::metadata(format!("Missing i64 field: {key}")))
    }

    pub fn opt_i32(&self, key: &str) -> Option<i32> {
        self.doc
            .get_i32(key)
            .ok()
            .or_else(|| self.doc.get_i64(key).map(|v| v as i32).ok())
            .or_else(|| self.doc.get_f64(key).map(|v| v as i32).ok())
    }

    pub fn req_i32(&self, key: &str) -> ResultEP<i32> {
        self.opt_i32(key).ok_or_else(|| EpError::metadata(format!("Missing i32 field: {key}")))
    }

    pub fn opt_f64(&self, key: &str) -> Option<f64> {
        self.doc.get(key).and_then(|v| match v {
            Bson::Double(v) => Some(*v),
            Bson::Int64(v) => Some(*v as f64),
            Bson::Int32(v) => Some(*v as f64),
            _ => None,
        })
    }

    pub fn req_f64(&self, key: &str) -> ResultEP<f64> {
        self.opt_f64(key).ok_or_else(|| EpError::metadata(format!("Missing float field: {key}")))
    }

    pub fn opt_string(&self, key: &str) -> Option<String> {
        self.doc.get_str(key).map(|s| s.to_string()).or_else(|_| self.doc.get_object_id(key).map(|id| id.to_hex())).ok()
    }

    pub fn req_string(&self, key: &str) -> ResultEP<String> {
        self.opt_string(key).ok_or_else(|| EpError::metadata(format!("Missing string field: {key}")))
    }

    pub fn opt_bool(&self, key: &str) -> Option<bool> {
        self.doc.get_bool(key).ok()
    }

    pub fn req_bool(&self, key: &str) -> ResultEP<bool> {
        self.opt_bool(key).ok_or_else(|| EpError::metadata(format!("Missing bool field: {key}")))
    }

    pub fn opt_datetime(&self, key: &str) -> Option<DateTimeWrapper> {
        self.doc.get_datetime(key).map(|dt| DateTimeWrapper::from(DateTime::<Utc>::from(*dt))).ok()
    }

    pub fn req_datetime(&self, key: &str) -> ResultEP<DateTimeWrapper> {
        self.opt_datetime(key).ok_or_else(|| EpError::metadata(format!("Missing datetime field: {key}")))
    }

    pub fn child(&self, key: &str) -> Option<DocAccessor<'_>> {
        self.doc.get_document(key).ok().map(DocAccessor::new)
    }

    pub fn array(&self, key: &str) -> Option<Vec<DocAccessor<'_>>> {
        self.doc.get_array(key).ok().map(|arr| arr.iter().filter_map(|v| v.as_document().map(DocAccessor::new)).collect())
    }

    pub fn raw(&self) -> &Document {
        self.doc
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MongoErrorClass {
    TopologyMismatch,
    PermissionDenied,
    Transient,
    Permanent,
}

#[allow(dead_code)]
pub fn classify_mongo_error(err: &EpError) -> MongoErrorClass {
    if is_topology_error(err) {
        MongoErrorClass::TopologyMismatch
    } else if is_permission_error(err) {
        MongoErrorClass::PermissionDenied
    } else if is_transient_error(err) {
        MongoErrorClass::Transient
    } else {
        MongoErrorClass::Permanent
    }
}

#[allow(dead_code)]
pub fn is_topology_error(err: &EpError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("not running with --replset")
        || msg.contains("noreplicationenabled")
        || msg.contains("not primary")
        || msg.contains("node is recovering")
        || msg.contains("sharding not enabled")
        || msg.contains("not master")
        || msg.contains("replicasetnotfound")
}

#[allow(dead_code)]
fn is_permission_error(err: &EpError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("not authorized") || msg.contains("unauthorized") || msg.contains("authentication failed")
}

#[allow(dead_code)]
fn is_transient_error(err: &EpError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("timeout") || msg.contains("connection reset") || msg.contains("network error")
}
