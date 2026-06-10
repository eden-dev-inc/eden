//! Eden-specific wrapper around [`eden_logger`].
//!
//! Defines [`EdenRequestFields`] — the application-specific identity fields
//! attached to every log (tenant, user, endpoint, etc.) — and re-exports the
//! `eden_logger` public API with `EdenRequestFields` plugged in as the
//! request schema.
//!
//! Downstream Eden code should depend on this crate, **not** `eden_logger`
//! directly. The plain `LogContext` exported here is an alias for
//! `eden_logger::LogContext<EdenRequestFields>`, so the API surface looks
//! identical to the pre-generic logger.

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

pub use eden_logger::{FieldWriter, RequestFields};

/// Application-specific request-context fields attached to every Eden log.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EdenRequestFields {
    #[serde(skip_serializing_if = "Option::is_none", rename = "eden_node_uuid")]
    pub eden_node_uuid: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "org_uuid")]
    pub organization_uuid: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "org_id")]
    pub organization_id: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "user_uuid")]
    pub user_uuid: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "user_id")]
    pub user_id: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "endpoint_uuid")]
    pub endpoint_uuid: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "endpoint_id")]
    pub endpoint_id: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "endpoint_kind")]
    pub endpoint_kind: Option<SmolStr>,
}

impl RequestFields for EdenRequestFields {
    fn write_display(&self, w: &mut dyn FieldWriter) {
        if let Some(v) = &self.organization_uuid {
            w.write_str("org", v);
        }
        if let Some(v) = &self.user_uuid {
            w.write_str("user", v);
        }
        if let Some(v) = &self.endpoint_uuid {
            w.write_str("endpoint", v);
        }
    }

    fn write_json(&self, w: &mut dyn FieldWriter) {
        if let Some(v) = &self.eden_node_uuid {
            w.write_str("eden_node_uuid", v);
        }
        if let Some(v) = &self.organization_uuid {
            w.write_str("org_uuid", v);
        }
        if let Some(v) = &self.organization_id {
            w.write_str("org_id", v);
        }
        if let Some(v) = &self.user_uuid {
            w.write_str("user_uuid", v);
        }
        if let Some(v) = &self.user_id {
            w.write_str("user_id", v);
        }
        if let Some(v) = &self.endpoint_uuid {
            w.write_str("endpoint_uuid", v);
        }
        if let Some(v) = &self.endpoint_id {
            w.write_str("endpoint_id", v);
        }
        if let Some(v) = &self.endpoint_kind {
            w.write_str("endpoint_kind", v);
        }
    }

    fn merge(&mut self, other: Self) {
        if other.eden_node_uuid.is_some() {
            self.eden_node_uuid = other.eden_node_uuid;
        }
        if other.organization_uuid.is_some() {
            self.organization_uuid = other.organization_uuid;
        }
        if other.organization_id.is_some() {
            self.organization_id = other.organization_id;
        }
        if other.user_uuid.is_some() {
            self.user_uuid = other.user_uuid;
        }
        if other.user_id.is_some() {
            self.user_id = other.user_id;
        }
        if other.endpoint_uuid.is_some() {
            self.endpoint_uuid = other.endpoint_uuid;
        }
        if other.endpoint_id.is_some() {
            self.endpoint_id = other.endpoint_id;
        }
        if other.endpoint_kind.is_some() {
            self.endpoint_kind = other.endpoint_kind;
        }
    }
}

/// Concrete Eden log context: `eden_logger::LogContext<EdenRequestFields>`.
pub type LogContext = eden_logger::LogContext<EdenRequestFields>;
/// Concrete Eden log record: `eden_logger::EdenLog<EdenRequestFields>`.
pub type EdenLog = eden_logger::EdenLog<EdenRequestFields>;

/// Newtype wrapper around `eden_logger::LogContext<EdenRequestFields>` that
/// hosts the Eden-specific setter methods (`with_organization_uuid`,
/// `with_user_uuid`, etc.). Existing call sites use these as inherent methods
/// rather than via a trait import.
#[derive(Debug, Clone, Default)]
pub struct EdenLogContext(pub eden_logger::LogContext<EdenRequestFields>);

impl std::ops::Deref for EdenLogContext {
    type Target = eden_logger::LogContext<EdenRequestFields>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for EdenLogContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Inherent methods on the concrete Eden `LogContext` type.
///
/// Rust allows inherent impls only on types defined in the same crate, so we
/// implement these on a type alias here. Because `LogContext` is a type alias
/// the methods land on the underlying generic struct — but only for this
/// concrete `R = EdenRequestFields` instantiation.
pub mod ext {
    use super::{EdenRequestFields, SmolStr};

    pub trait LogContextEdenExt {
        fn with_eden_node_uuid(self, uuid: impl Into<SmolStr>) -> Self;
        fn with_organization_uuid(self, uuid: impl Into<SmolStr>) -> Self;
        fn with_organization_id(self, id: impl Into<SmolStr>) -> Self;
        fn with_user_uuid(self, uuid: impl Into<SmolStr>) -> Self;
        fn with_user_id(self, id: impl Into<SmolStr>) -> Self;
        fn with_endpoint_uuid(self, uuid: impl Into<SmolStr>) -> Self;
        fn with_endpoint_id(self, id: impl Into<SmolStr>) -> Self;
        fn with_endpoint_kind(self, kind: impl Into<SmolStr>) -> Self;
    }

    impl LogContextEdenExt for eden_logger::LogContext<EdenRequestFields> {
        fn with_eden_node_uuid(mut self, uuid: impl Into<SmolStr>) -> Self {
            self.request.eden_node_uuid = Some(uuid.into());
            self
        }
        fn with_organization_uuid(mut self, uuid: impl Into<SmolStr>) -> Self {
            self.request.organization_uuid = Some(uuid.into());
            self
        }
        fn with_organization_id(mut self, id: impl Into<SmolStr>) -> Self {
            self.request.organization_id = Some(id.into());
            self
        }
        fn with_user_uuid(mut self, uuid: impl Into<SmolStr>) -> Self {
            self.request.user_uuid = Some(uuid.into());
            self
        }
        fn with_user_id(mut self, id: impl Into<SmolStr>) -> Self {
            self.request.user_id = Some(id.into());
            self
        }
        fn with_endpoint_uuid(mut self, uuid: impl Into<SmolStr>) -> Self {
            self.request.endpoint_uuid = Some(uuid.into());
            self
        }
        fn with_endpoint_id(mut self, id: impl Into<SmolStr>) -> Self {
            self.request.endpoint_id = Some(id.into());
            self
        }
        fn with_endpoint_kind(mut self, kind: impl Into<SmolStr>) -> Self {
            self.request.endpoint_kind = Some(kind.into());
            self
        }
    }
}

pub use ext::LogContextEdenExt;

/// Convenient glob import: brings the trait into scope so the inherent-looking
/// setters work without an explicit `use`. Downstream call sites that do
/// `use eden_logger_internal::*` get them automatically.
pub mod prelude {
    pub use super::ext::LogContextEdenExt;
}

// Re-export the public eden_logger API so downstream `use eden_logger_internal::*`
// is a drop-in replacement for `use eden_logger::*`.
pub use eden_logger::{
    LogAudience, LogFormat, LogLevel, LogTarget, TraceContextExt, TraceSource, WriterConfig, clear_filter, disable_levels, emit_direct,
    enable_levels, extract_trace_context, init, init_from_env, init_from_value, install_sink, set_trace_source, should_log, trace_source,
    write_display_direct, write_json_direct,
};

// `ctx_with_trace!()` defaults to `()` for its type parameter, so re-export
// our own macro that fills in `EdenRequestFields`.
#[macro_export]
macro_rules! ctx_with_trace {
    () => {
        $crate::__eden_logger_macros::ctx_with_trace!($crate::EdenRequestFields)
    };
}

// Re-export the upstream log_* proc-macros directly. They expand to calls into
// `::eden_logger::emit_direct` which works for any concrete `R`.
pub use eden_logger::{log_debug, log_error, log_info, log_trace, log_warn};

// Internal re-export so the `ctx_with_trace!` macro can resolve the upstream
// proc macro path from user crates that only depend on `eden_logger_internal`.
#[doc(hidden)]
pub mod __eden_logger_macros {
    pub use eden_logger::ctx_with_trace;
}

// A standalone `trace_context()` helper that returns the Eden-typed context.
pub fn trace_context() -> LogContext {
    eden_logger::trace_context::<EdenRequestFields>()
}
