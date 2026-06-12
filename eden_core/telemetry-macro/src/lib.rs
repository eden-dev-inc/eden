#![cfg_attr(test, allow(clippy::unwrap_used))]
// // telemetry_macro/src/lib.rs
// use proc_macro::TokenStream;
// use quote::{quote, ToTokens};
// use syn::{
//     parse_macro_input, parse_quote, Block, FnArg, Ident, ItemFn, Pat, PatType, Type, TypePath,
// };
//
// /// A procedural macro that automatically implements telemetry span logic for async functions.
// ///
// /// This macro requires that:
// /// 1. The function has a parameter named `telemetry_wrapper` of type `TelemetryWrapper`, `&mut TelemetryWrapper`, or `Option<&mut TelemetryWrapper>`
// /// 2. The function is also marked with the `#[named]` attribute
// ///
// /// # Example
// ///
// /// ```rust
// /// #[named]
// /// #[telemetry]
// /// async fn set(&self, key: U, value: T, telemetry_wrapper: Option<&mut TelemetryWrapper>) -> ResultEP<()> {
// ///     let mut redis_conn = self.cache_connection().await?;
// ///
// ///     // If telemetry_wrapper is Some, a span will be created and this code will run within it
// ///     // If telemetry_wrapper is None, this code will run without telemetry
// ///     span.add_event(
// ///         "setting data to cache with expiration",
// ///         vec![FastSpanAttribute::new("key", key.to_string())],
// ///     );
// ///
// ///     redis_conn.set::<_, _, ()>(key, value).await.map_err(
// ///         EpError::cache)?;
// ///
// ///     Ok(())
// /// }
// /// ```
// #[proc_macro_attribute]
// pub fn telemetry(_attr: TokenStream, item: TokenStream) -> TokenStream {
//     // Parse the input function
//     let mut input_fn = parse_macro_input!(item as ItemFn);
//
//     // Check if function is async
//     if input_fn.sig.asyncness.is_none() {
//         return syn::Error::new_spanned(
//             input_fn.sig.fn_token,
//             "#[telemetry] can only be applied to async functions",
//         )
//         .to_compile_error()
//         .into();
//     }
//
//     // Find and analyze telemetry_wrapper parameter
//     let telemetry_param_info = find_telemetry_wrapper_param(&input_fn);
//     if telemetry_param_info.is_none() {
//         return syn::Error::new_spanned(
//             input_fn.sig.fn_token,
//             "#[telemetry] requires a parameter named 'telemetry_wrapper' of type 'TelemetryWrapper', '&mut TelemetryWrapper', or 'Option<&mut TelemetryWrapper>'",
//         )
//             .to_compile_error()
//             .into();
//     }
//
//     let (is_optional, is_mutable_ref) = telemetry_param_info.unwrap_or_default();
//
//     // Extract the original function body
//     let original_body = input_fn.block;
//
//     // Get function name
//     let fn_name = &input_fn.sig.ident;
//
//     // Create the new function body with telemetry instrumentation
//     let new_body =
//         create_telemetry_body(original_body, fn_name, false, is_optional, is_mutable_ref);
//
//     // Update the function with the new body
//     input_fn.block = new_body;
//
//     // Return the transformed function
//     TokenStream::from(input_fn.to_token_stream())
// }
//
// // Find the telemetry_wrapper parameter in the function signature
// // Returns Some((is_optional, is_mutable_ref)) if found
// fn find_telemetry_wrapper_param(func: &ItemFn) -> Option<(bool, bool)> {
//     func.sig.inputs.iter().find_map(|arg| {
//         if let FnArg::Typed(PatType { pat, ty, .. }) = arg {
//             if let Pat::Ident(pat_ident) = &**pat {
//                 if pat_ident.ident == "telemetry_wrapper" {
//                     // Check for &mut TelemetryWrapper
//                     if is_type_mut_ref_telemetry_wrapper(ty) {
//                         return Some((false, true)); // &mut TelemetryWrapper
//                     }
//                     // Check for TelemetryWrapper
//                     else if is_type_telemetry_wrapper(ty) {
//                         return Some((false, false)); // Direct TelemetryWrapper
//                     }
//                     // Check for Option<&mut TelemetryWrapper>
//                     else if is_type_option_mut_ref_telemetry_wrapper(ty) {
//                         return Some((true, true)); // Option<&mut TelemetryWrapper>
//                     }
//                     // Check for Option<TelemetryWrapper>
//                     else if is_type_option_telemetry_wrapper(ty) {
//                         return Some((true, false)); // Option<TelemetryWrapper>
//                     }
//                 }
//             }
//         }
//         None
//     })
// }
//
// // Check if a type is TelemetryWrapper
// fn is_type_telemetry_wrapper(ty: &Type) -> bool {
//     if let Type::Path(TypePath { path, .. }) = ty {
//         if let Some(segment) = path.segments.last() {
//             return segment.ident == "TelemetryWrapper";
//         }
//     }
//     false
// }
//
// // Check if a type is &mut TelemetryWrapper
// fn is_type_mut_ref_telemetry_wrapper(ty: &Type) -> bool {
//     if let Type::Reference(type_ref) = ty {
//         if type_ref.mutability.is_some() {
//             return is_type_telemetry_wrapper(&type_ref.elem);
//         }
//     }
//     false
// }
//
// // Check if a type is Option<TelemetryWrapper>
// fn is_type_option_telemetry_wrapper(ty: &Type) -> bool {
//     if let Type::Path(TypePath { path, .. }) = ty {
//         if let Some(segment) = path.segments.last() {
//             if segment.ident == "Option" {
//                 // Check the type parameter
//                 if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
//                     if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
//                         return is_type_telemetry_wrapper(inner_type);
//                     }
//                 }
//             }
//         }
//     }
//     false
// }
//
// // Check if a type is Option<&mut TelemetryWrapper>
// fn is_type_option_mut_ref_telemetry_wrapper(ty: &Type) -> bool {
//     if let Type::Path(TypePath { path, .. }) = ty {
//         if let Some(segment) = path.segments.last() {
//             if segment.ident == "Option" {
//                 // Check the type parameter
//                 if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
//                     if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
//                         return is_type_mut_ref_telemetry_wrapper(inner_type);
//                     }
//                 }
//             }
//         }
//     }
//     false
// }
//
// // Create the new function body with telemetry instrumentation
// fn create_telemetry_body(
//     original_body: Box<Block>,
//     fn_name: &Ident,
//     skip_error_handling: bool,
//     is_optional: bool,
//     is_mutable_ref: bool,
// ) -> Box<Block> {
//     if is_optional {
//         if is_mutable_ref {
//             // Handle Option<&mut TelemetryWrapper>
//             if skip_error_handling {
//                 // Create body for optional mutable ref telemetry without error handling
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//
//                         // Check if telemetry_wrapper is Some or None
//                         match telemetry_wrapper {
//                             Some(telemetry_wrapper) => {
//                                 let span_context: opentelemetry::Context = telemetry_wrapper
//                                     .client_tracer(fn_name.to_string());
//
//                                 // Make span available to the original function body
//                                 {
//                                     #original_body
//                                 }
//                             },
//                             None => {
//                                 // Run without telemetry
//                                 #original_body
//                             }
//                         }
//                     }
//                 }
//             } else {
//                 // Create body for optional mutable ref telemetry with error handling
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//
//                         // Check if telemetry_wrapper is Some or None
//                         match telemetry_wrapper {
//                             Some(telemetry_wrapper) => {
//                                 let span_context: opentelemetry::Context = telemetry_wrapper
//                                     .client_tracer(fn_name.to_string());
//
//                                 // Make span available to the original function body
//                                 let result = {
//                                     #original_body
//                                 };
//
//                                 // Handle errors
//                                 result.inspect_err(|e| {
//                                     span.set_status(opentelemetry::trace::Status::Error {
//                                         description: std::borrow::Cow::Owned(e.to_string()),
//                                     });
//                                 })
//                             },
//                             None => {
//                                 // Run without telemetry and error handling for span
//                                 #original_body
//                             }
//                         }
//                     }
//                 }
//             }
//         } else {
//             // Handle Option<TelemetryWrapper> (original implementation)
//             if skip_error_handling {
//                 // Create body for optional telemetry without error handling
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//
//                         // Check if telemetry_wrapper is Some or None
//                         match telemetry_wrapper {
//                             Some(telemetry_wrapper) => {
//                                 let span_context: opentelemetry::Context = telemetry_wrapper
//                                     .client_tracer(fn_name.to_string());
//
//                                 // Make span available to the original function body
//                                 {
//                                     #original_body
//                                 }
//                             },
//                             None => {
//                                 // Run without telemetry
//                                 #original_body
//                             }
//                         }
//                     }
//                 }
//             } else {
//                 // Create body for optional telemetry with error handling
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//
//                         // Check if telemetry_wrapper is Some or None
//                         match telemetry_wrapper {
//                             Some(telemetry_wrapper) => {
//                                 let span_context: opentelemetry::Context = telemetry_wrapper
//                                     .client_tracer(fn_name.to_string());
//
//                                 // Make span available to the original function body
//                                 let result = {
//                                     #original_body
//                                 };
//
//                                 // Handle errors
//                                 result.inspect_err(|e| {
//                                     span.set_status(opentelemetry::trace::Status::Error {
//                                         description: std::borrow::Cow::Owned(e.to_string()),
//                                     });
//                                 })
//                             },
//                             None => {
//                                 // Run without telemetry and error handling for span
//                                 #original_body
//                             }
//                         }
//                     }
//                 }
//             }
//         }
//     } else {
//         if is_mutable_ref {
//             // Handle &mut TelemetryWrapper
//             if skip_error_handling {
//                 // Create body without error handling for mutable ref
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly rather than the macro
//                         let fn_name = stringify!(#fn_name);
//                         let span_context: opentelemetry::Context = telemetry_wrapper
//                             .client_tracer(fn_name.to_string());
//
//                         // Make span available to the original function body
//                         {
//                             #original_body
//                         }
//                     }
//                 }
//             } else {
//                 // Create body with error handling for mutable ref
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly rather than the macro
//                         let fn_name = stringify!(#fn_name);
//                         let span_context: opentelemetry::Context = telemetry_wrapper
//                             .client_tracer(fn_name.to_string());
//
//                         // Make span available to the original function body
//                         let result = {
//                             #original_body
//                         };
//
//                         // Handle errors
//                         result.inspect_err(|e| {
//                             span.set_status(opentelemetry::trace::Status::Error {
//                                 description: std::borrow::Cow::Owned(e.to_string()),
//                             });
//                         })
//                     }
//                 }
//             }
//         } else {
//             // Original implementation for non-optional TelemetryWrapper
//             if skip_error_handling {
//                 // Create body without error handling
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly rather than the macro
//                         let fn_name = stringify!(#fn_name);
//                         let span_context: opentelemetry::Context = telemetry_wrapper
//                             .client_tracer(fn_name.to_string());
//
//                         // Make span available to the original function body
//                         {
//                             #original_body
//                         }
//                     }
//                 }
//             } else {
//                 // Create body with error handling
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly rather than the macro
//                         let fn_name = stringify!(#fn_name);
//                         let span_context: opentelemetry::Context = telemetry_wrapper
//                             .client_tracer(fn_name.to_string());
//
//                         // Make span available to the original function body
//                         let result = {
//                             #original_body
//                         };
//
//                         // Handle errors
//                         result.inspect_err(|e| {
//                             span.set_status(opentelemetry::trace::Status::Error {
//                                 description: std::borrow::Cow::Owned(e.to_string()),
//                             });
//                         })
//                     }
//                 }
//             }
//         }
//     }
// }
//
// // Helper macro to generate specialized error handling for specific error types
// #[proc_macro_attribute]
// pub fn telemetry_with_error(attr: TokenStream, item: TokenStream) -> TokenStream {
//     // Parse the attribute input as a stream of tokens
//     let attr_tokens = proc_macro2::TokenStream::from(attr.clone());
//     let attr_str = attr.to_string();
//     let mut input_fn = parse_macro_input!(item as ItemFn);
//
//     // Check if skip_error_handling flag is provided
//     let skip_error_handling = attr_str.contains("skip_error_handling");
//
//     // Get error mapping function if provided (and not a directive)
//     let error_mapper = if !attr_tokens.is_empty() && !skip_error_handling {
//         Some(attr_tokens)
//     } else {
//         None
//     };
//
//     // Find and analyze telemetry_wrapper parameter
//     let telemetry_param_info = find_telemetry_wrapper_param(&input_fn);
//     if telemetry_param_info.is_none() {
//         return syn::Error::new_spanned(
//             input_fn.sig.fn_token,
//             "#[telemetry_with_error] requires a parameter named 'telemetry_wrapper' of type 'TelemetryWrapper', '&mut TelemetryWrapper', or 'Option<&mut TelemetryWrapper>'",
//         )
//             .to_compile_error()
//             .into();
//     }
//
//     let (is_optional, is_mutable_ref) = telemetry_param_info.unwrap_or_default();
//
//     // Extract the original function body
//     let original_body = input_fn.block;
//
//     // Get function name
//     let fn_name = &input_fn.sig.ident;
//
//     // Create new body based on error handling preferences and whether telemetry is optional
//     let new_body = if skip_error_handling {
//         // Skip error handling
//         create_telemetry_body(original_body, fn_name, true, is_optional, is_mutable_ref)
//     } else if let Some(mapper) = error_mapper {
//         // Apply custom error mapping and handle errors
//         if is_optional {
//             if is_mutable_ref {
//                 // Option<&mut TelemetryWrapper> with custom error mapping
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//
//                         // Check if telemetry_wrapper is Some or None
//                         match telemetry_wrapper {
//                             Some(telemetry_wrapper) => {
//                                 let span_context: opentelemetry::Context = telemetry_wrapper
//                                     .client_tracer(fn_name.to_string());
//                                 let span = TraceContextExt::span(&span_context);
//
//                                 // Make span available to the original function body
//                                 let result = {
//                                     #original_body
//                                 };
//
//                                 // Apply custom error mapping and handle errors
//                                 result
//                                     .map_err(#mapper)
//                                     .inspect_err(|e| {
//                                         span.set_status(opentelemetry::trace::Status::Error {
//                                             description: std::borrow::Cow::Owned(e.to_string()),
//                                         });
//                                     })
//                             },
//                             None => {
//                                 // Run without telemetry, but still apply error mapping
//                                 let result = {
//                                     #original_body
//                                 };
//
//                                 // Apply custom error mapping without telemetry
//                                 result.map_err(#mapper)
//                             }
//                         }
//                     }
//                 }
//             } else {
//                 // Option<TelemetryWrapper> with custom error mapping (original)
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//
//                         // Check if telemetry_wrapper is Some or None
//                         match &mut telemetry_wrapper {
//                             Some(telemetry_wrapper) => {
//                                 let span_context: opentelemetry::Context = telemetry_wrapper
//                                     .client_tracer(fn_name.to_string());
//                                 let span = TraceContextExt::span(&span_context);
//
//                                 // Make span available to the original function body
//                                 let result = {
//                                     #original_body
//                                 };
//
//                                 // Apply custom error mapping and handle errors
//                                 result
//                                     .map_err(#mapper)
//                                     .inspect_err(|e| {
//                                         span.set_status(opentelemetry::trace::Status::Error {
//                                             description: std::borrow::Cow::Owned(e.to_string()),
//                                         });
//                                     })
//                             },
//                             None => {
//                                 // Run without telemetry, but still apply error mapping
//                                 let result = {
//                                     #original_body
//                                 };
//
//                                 // Apply custom error mapping without telemetry
//                                 result.map_err(#mapper)
//                             }
//                         }
//                     }
//                 }
//             }
//         } else {
//             if is_mutable_ref {
//                 // &mut TelemetryWrapper with custom error mapping
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//                         let span_context: opentelemetry::Context = telemetry_wrapper
//                             .client_tracer(fn_name.to_string());
//                         let span = TraceContextExt::span(&span_context);
//
//                         // Make span available to the original function body
//                         let result = {
//                             #original_body
//                         };
//
//                         // Apply custom error mapping and handle errors
//                         result
//                             .map_err(#mapper)
//                             .inspect_err(|e| {
//                                 span.set_status(opentelemetry::trace::Status::Error {
//                                     description: std::borrow::Cow::Owned(e.to_string()),
//                                 });
//                             })
//                     }
//                 }
//             } else {
//                 // TelemetryWrapper with custom error mapping (original)
//                 parse_quote! {
//                     {
//                         // Use the function name identifier directly
//                         let fn_name = stringify!(#fn_name);
//                         let span_context: opentelemetry::Context = telemetry_wrapper
//                             .client_tracer(fn_name.to_string());
//                         let span = TraceContextExt::span(&span_context);
//
//                         // Make span available to the original function body
//                         let result = {
//                             #original_body
//                         };
//
//                         // Apply custom error mapping and handle errors
//                         result
//                             .map_err(#mapper)
//                             .inspect_err(|e| {
//                                 span.set_status(opentelemetry::trace::Status::Error {
//                                     description: std::borrow::Cow::Owned(e.to_string()),
//                                 });
//                             })
//                     }
//                 }
//             }
//         }
//     } else {
//         // Use default error handling
//         create_telemetry_body(original_body, fn_name, false, is_optional, is_mutable_ref)
//     };
//
//     // Update the function with the new body
//     input_fn.block = new_body;
//
//     // Return the transformed function
//     TokenStream::from(input_fn.to_token_stream())
// }
//
// // New macro that skips error handling
// #[proc_macro_attribute]
// pub fn telemetry_no_errors(_attr: TokenStream, item: TokenStream) -> TokenStream {
//     let mut input_fn = parse_macro_input!(item as ItemFn);
//
//     // Find and analyze telemetry_wrapper parameter
//     let telemetry_param_info = find_telemetry_wrapper_param(&input_fn);
//     if telemetry_param_info.is_none() {
//         return syn::Error::new_spanned(
//             input_fn.sig.fn_token,
//             "#[telemetry_no_errors] requires a parameter named 'telemetry_wrapper' of type 'TelemetryWrapper', '&mut TelemetryWrapper', or 'Option<&mut TelemetryWrapper>'",
//         )
//             .to_compile_error()
//             .into();
//     }
//
//     let (is_optional, is_mutable_ref) = telemetry_param_info.unwrap_or_default();
//
//     // Extract the original function body
//     let original_body = input_fn.block;
//
//     // Get function name
//     let fn_name = &input_fn.sig.ident;
//
//     // Create new body without error handling
//     let new_body = create_telemetry_body(original_body, fn_name, true, is_optional, is_mutable_ref);
//
//     // Update the function with the new body
//     input_fn.block = new_body;
//
//     // Return the transformed function
//     TokenStream::from(input_fn.to_token_stream())
// }
//
// /// Macro to skip error handling for specific operations within a telemetry function
// ///
// /// # Example
// ///
// /// ```rust
// /// #[telemetry_with_error]
// /// async fn set(&self, key: U, value: T, telemetry_wrapper: Option<&mut TelemetryWrapper>) -> ResultEP<()> {
// ///     // This operation will have error handling
// ///     self.cache_connection()
// ///         .await?
// ///         .set::<_, _, ()>(key, value)
// ///         .await
// ///         .map_err(EpError::cache)?;
// ///
// ///     // This operation will skip error handling
// ///     telemetry_skip_error! {
// ///         self.local_cache_connection()
// ///             .insert(key.to_string(), value)
// ///             .await
// ///     }
// /// }
// /// ```
// #[proc_macro]
// pub fn telemetry_skip_error(input: TokenStream) -> TokenStream {
//     let expr = parse_macro_input!(input as syn::Block);
//
//     let expanded = quote! {
//         {
//             // This forwards the block expression with its semicolons
//             #expr
//         }
//     };
//
//     TokenStream::from(expanded)
// }
//
// #[cfg(feature = "visit-mut")]
// #[proc_macro_attribute]
// pub fn simple_auto_map_err(_args: TokenStream, input: TokenStream) -> TokenStream {
//     use syn::visit_mut::{visit_expr_mut, VisitMut};
//
//     let input_fn = parse_macro_input!(input as ItemFn);
//     let mut new_fn = input_fn.clone();
//
//     println!("=== ORIGINAL FUNCTION ===");
//     println!("{}", quote! { #input_fn });
//
//     let mut transformer = SimpleErrorTransformer;
//     transformer.visit_item_fn_mut(&mut new_fn);
//
//     println!("=== TRANSFORMED FUNCTION ===");
//     println!("{}", quote! { #new_fn });
//
//     TokenStream::from(quote! { #new_fn })
// }
//
// #[cfg(not(feature = "visit-mut"))]
// #[proc_macro_attribute]
// pub fn simple_auto_map_err(_args: TokenStream, input: TokenStream) -> TokenStream {
//     // Return the original function unchanged if visit-mut feature is not enabled
//     let input_fn = parse_macro_input!(input as ItemFn);
//     TokenStream::from(quote! { #input_fn })
// }
//
// #[cfg(feature = "visit-mut")]
// struct SimpleErrorTransformer;
//
// #[cfg(feature = "visit-mut")]
// impl syn::visit_mut::VisitMut for SimpleErrorTransformer {
//     fn visit_expr_mut(&mut self, expr: &mut Expr) {
//         use syn::visit_mut::visit_expr_mut;
//
//         // First recursively visit children
//         visit_expr_mut(self, expr);
//
//         // Then check if this is a Try expression containing an Await
//         if let Expr::Try(try_expr) = expr {
//             if let Expr::Await(await_expr) = try_expr.expr.as_ref() {
//                 // Replace: something.await?
//                 // With: something.await.map_err(|e| endpoint_error_handling(e, &mut span, telemetry_wrapper, &labels))?
//                 let base_expr = &await_expr.base;
//
//                 *expr = parse_quote! {
//                     #base_expr.await.map_err(|e| endpoint_error_handling(e, &mut span, telemetry_wrapper, &labels))?
//                 };
//             }
//         }
//     }
// }
