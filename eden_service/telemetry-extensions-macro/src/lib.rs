#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Telemetry Extensions Macro
//!
//! Provides the `#[with_telemetry]` procedural macro for automatic OpenTelemetry
//! instrumentation of Actix-web handler functions.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use telemetry_extensions_macro::with_telemetry;
//!
//! #[with_telemetry]
//! async fn create_user(body: web::Json<User>) -> Result<HttpResponse, Error> {
//!     // `span` and `telemetry_wrapper` are automatically available
//!     span.add_event("creating user", vec![]);
//!     Ok(HttpResponse::Ok().json(user))
//! }
//! ```
//!
//! The macro automatically:
//! - Injects telemetry parameters (metrics, metadata, labels, durations)
//! - Initializes OpenTelemetry span and telemetry wrapper
//! - Attaches telemetry data to response extensions for middleware
//!
//! ## Feature Flags
//!
//! Span instrumentation is always enabled.

// lib.rs - Simple telemetry macro
use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ItemFn, Pat, PatType, Signature, parse_macro_input, parse_quote};

/// Automatically instruments Actix-web handlers with OpenTelemetry tracing.
///
/// Injects telemetry parameters and initializes `span` and `telemetry_wrapper` variables.
///
#[proc_macro_attribute]
pub fn with_telemetry(_args: TokenStream, input: TokenStream) -> TokenStream {
    let mut input_fn = parse_macro_input!(input as ItemFn);

    // Get the function name for the span
    let fn_name = input_fn.sig.ident.to_string();

    // Add telemetry parameters to function signature
    add_telemetry_params(&mut input_fn.sig);

    // Get the original function body
    let original_body = &input_fn.block;

    let new_body = quote! {
        {
            use eden_core::telemetry::{TraceContext, TelemetryWrapper};

            // Initialize telemetry wrapper - temporary value is kept alive in this scope
            let mut telemetry_wrapper_value = TelemetryWrapper::new_with_telemetry(
                TraceContext::from(metadata.metadata().clone()),
                metrics.into_inner(),
                labels,
                durations
            );
            // Reference is valid for the entire function scope since temporary is still alive
            let mut telemetry_wrapper = &mut telemetry_wrapper_value;

            // Create span using fast-telemetry (no OTel Context overhead)
            let mut span = telemetry_wrapper.server_tracer(#fn_name.to_string());

            // Create tracing span to enable ctx_with_trace!() to extract trace IDs
            let _tracing_span = tracing::info_span!(#fn_name).entered();

            let response = {
                #original_body
            };

            match response {
                ::core::result::Result::Ok(mut resp) => {
                    {
                        let mut ext = resp.extensions_mut();
                        ext.insert(telemetry_wrapper.labels().clone());
                        ext.insert(telemetry_wrapper.durations().clone());
                    }
                    ::core::result::Result::Ok(resp)
                }
                other => other,
            }
        }
    };

    // Replace the function body
    input_fn.block = Box::new(parse_quote!({ #new_body }));

    TokenStream::from(quote! { #input_fn })
}

/// Adds telemetry parameters to function signature if not already present.
fn add_telemetry_params(sig: &mut Signature) {
    // Define the telemetry parameters to add
    let telemetry_params: Vec<FnArg> = vec![
        parse_quote! { metrics: actix_web::web::Data<eden_core::telemetry::AllMetrics> },
        parse_quote! { metadata: eden_core::telemetry::MetadataMapWrapper },
        parse_quote! { labels: eden_core::telemetry::TelemetryLabels },
        parse_quote! { durations: eden_core::telemetry::TelemetryDurations },
        // parse_quote! { telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper },
        // parse_quote! { span: &mut opentelemetry::trace::SpanRef<'_> },
    ];

    // Get existing parameter names to avoid duplicates
    let existing_param_names: std::collections::HashSet<String> = sig
        .inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(PatType { pat, .. }) = arg
                && let Pat::Ident(ident) = pat.as_ref()
            {
                return Some(ident.ident.to_string());
            }
            None
        })
        .collect();

    // Add telemetry parameters if they don't already exist
    for param in telemetry_params {
        if let FnArg::Typed(PatType { pat, .. }) = &param
            && let Pat::Ident(ident) = pat.as_ref()
        {
            let param_name = ident.ident.to_string();
            if !existing_param_names.contains(&param_name) {
                sig.inputs.push(param);
            }
        }
    }
}
