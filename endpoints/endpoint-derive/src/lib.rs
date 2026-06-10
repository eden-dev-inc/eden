#![cfg_attr(test, allow(clippy::unwrap_used))]
mod docs;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Attribute, Fields, Variant};
use syn::{
    Data, DeriveInput, Expr, GenericArgument, Ident, ItemEnum, ItemStruct, Lit, Meta, PathArguments, Result, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

// Parser for the endpoint kind attribute
struct EndpointKind {
    kind: Ident,
}

impl Parse for EndpointKind {
    fn parse(input: ParseStream) -> Result<Self> {
        Ok(EndpointKind { kind: input.parse()? })
    }
}

#[proc_macro_attribute]
pub fn mongo_endpoint(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let register_ident = format_ident!("__REGISTER_{}_OPERATION_{}", "MONGO", name);

    let impl_block = generate_mongo_impl(name);

    let expanded = quote! {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder)]
        #[builder(setter(into))]
        #input

        // Common EndpointOperation implementation
        impl crate::EndpointOperation for #name {}

        #impl_block

        // Automatic registration
        #[ctor::ctor]
        fn #register_ident() {
            crate::mongo::serde::register_operation::<#name>();
        }
    };

    TokenStream::from(expanded)
}

fn generate_mongo_impl(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl #name {
            #[allow(dead_code)]
            fn mongo_operation_todo() {
                todo!("Determine if we want to derive here or in the mongo crate")
            }
        }
    }
}

#[proc_macro_attribute]
pub fn pinecone_endpoint(args: TokenStream, input: TokenStream) -> TokenStream {
    let EndpointKind { kind } = parse_macro_input!(args as EndpointKind);
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let register_ident = format_ident!("__REGISTER_{}_OPERATION_{}", kind, name);

    let impl_block = crate::generate_pinecone_impl(name);

    let expanded = quote! {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder)]
        #[builder(setter(into))]
        #input

        // Common EndpointOperation implementation
        impl crate::EndpointOperation for #name {}

        #impl_block

        // Automatic registration
        #[ctor::ctor]
        fn #register_ident() {
            crate::pinecone::serde::register_operation::<#name>();
        }
    };

    TokenStream::from(expanded)
}

fn generate_pinecone_impl(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl #name {
            #[allow(dead_code)]
            fn pinecone_operation_todo() {
                todo!("Determine if we want to derive here or in the pinecone crate")
            }
        }
    }
}

#[proc_macro_attribute]
pub fn redis_endpoint(args: TokenStream, input: TokenStream) -> TokenStream {
    let EndpointKind { kind } = parse_macro_input!(args as EndpointKind);
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let register_ident = format_ident!("__REGISTER_{}_OPERATION_{}", kind, name);

    let impl_block = crate::generate_redis_impl(name);

    let expanded = quote! {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder)]
        #[builder(setter(into))]
        #input

        // Common EndpointOperation implementation
        impl crate::EndpointOperation for #name {}

        #impl_block

        // Automatic registration
        #[ctor::ctor]
        fn #register_ident() {
            crate::redis::serde::register_operation::<#name>();
        }
    };

    TokenStream::from(expanded)
}

fn generate_redis_impl(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl #name {
            #[allow(dead_code)]
            fn redis_operation_todo() {
                todo!("Determine if we want to derive here or in the redis crate")
            }
        }
    }
}

#[proc_macro_derive(DocumentAPI, attributes(noinput, simple_complex, simple))]
pub fn document_api(input: TokenStream) -> TokenStream {
    let res = if let Ok(input) = syn::parse::<ItemEnum>(input.clone()) {
        docs::document_api_enum(&input)
    } else {
        panic!("DocumentAPI can only be defined on enums")
    };

    TokenStream::from(match res {
        Ok(res) => res,
        Err(err) => err.to_compile_error(),
    })
}

#[proc_macro_derive(DocumentInput)]
pub fn document_input_struct(input: TokenStream) -> TokenStream {
    let res = if let Ok(input) = syn::parse::<ItemStruct>(input.clone()) {
        docs::document_input(&input)
    } else {
        // Derive macros can only be defined on structs and enums
        unreachable!()
    };

    TokenStream::from(match res {
        Ok(res) => res,
        Err(err) => err.to_compile_error(),
    })
}

#[proc_macro_derive(ApiBuilder, attributes(api_builder, noinput))]
pub fn derive_api_builder(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match generate_api_builder_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_api_builder_impl(input: &DeriveInput) -> syn::Result<TokenStream2> {
    // Parse builder name from attributes
    let builder_name = parse_builder_name(&input.attrs)?;
    let builder_ident = format_ident!("{}", builder_name);
    let enum_name = &input.ident;

    // Extract enum data
    let data = match &input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(input, "ApiBuilder can only be derived for enums"));
        }
    };

    // Generate builder methods
    let builder_methods = generate_methods(&data.variants, enum_name)?;

    let expanded = quote! {
        #[derive(Default)]
        pub struct #builder_ident {}

        impl #builder_ident {
            #(#builder_methods)*
        }
    };

    Ok(expanded)
}

fn parse_builder_name(attrs: &[Attribute]) -> syn::Result<String> {
    for attr in attrs {
        if attr.path().is_ident("api_builder")
            && let Meta::List(list) = &attr.meta
        {
            let nested = list.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)?;

            for meta in nested {
                if let Meta::NameValue(nv) = meta
                    && nv.path.is_ident("builder_name")
                    && let Expr::Lit(expr_lit) = &nv.value
                    && let Lit::Str(lit_str) = &expr_lit.lit
                {
                    return Ok(lit_str.value());
                }
            }
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "Missing or invalid #[api_builder(builder_name = \"...\")] attribute",
    ))
}

fn generate_methods(variants: &syn::punctuated::Punctuated<Variant, syn::Token![,]>, _enum_name: &Ident) -> syn::Result<Vec<TokenStream2>> {
    let mut methods = Vec::new();

    for variant in variants {
        // Skip variants with #[noinput] or #[strum(disabled)]
        if should_skip_variant(variant) {
            continue;
        }

        let variant_name = &variant.ident;
        let method_name = to_snake_case(&variant_name.to_string());
        let method_ident = format_ident!("{}", method_name);

        match &variant.fields {
            // Handle unit variants (no nested enum)
            Fields::Unit => {
                let input_builder_type = format_ident!("{}InputBuilder", variant_name);

                let method = quote! {
                    pub fn #method_ident(self) -> #input_builder_type {
                        #input_builder_type::default()
                    }
                };

                methods.push(method);
            }

            // Handle variants with nested enums like Database(Option<DatabaseApi>)
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field = &fields.unnamed[0];

                // Check if it's Option<SomeEnum>
                if let Some(inner_enum) = extract_option_inner_type(&field.ty) {
                    // Generate a nested builder that returns the appropriate type
                    let nested_builder_type = format_ident!("{}Builder", inner_enum);

                    let method = quote! {
                        pub fn #method_ident(self) -> #nested_builder_type {
                            #nested_builder_type::default()
                        }
                    };

                    methods.push(method);
                } else {
                    // For non-Option types, try the standard approach
                    let input_builder_type = format_ident!("{}InputBuilder", variant_name);

                    let method = quote! {
                        pub fn #method_ident(self) -> #input_builder_type {
                            #input_builder_type::default()
                        }
                    };

                    methods.push(method);
                }
            }

            // Skip named fields and multiple unnamed fields for now
            _ => continue,
        }
    }

    Ok(methods)
}

fn extract_option_inner_type(ty: &Type) -> Option<Ident> {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && segment.ident == "Option"
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(Type::Path(inner_path))) = args.args.first()
        && let Some(inner_segment) = inner_path.path.segments.last()
    {
        return Some(inner_segment.ident.clone());
    }
    None
}

fn should_skip_variant(variant: &Variant) -> bool {
    for attr in &variant.attrs {
        // Check for #[noinput]
        if attr.path().is_ident("noinput") {
            return true;
        }

        // Check for #[strum(disabled)]
        if attr.path().is_ident("strum")
            && let Meta::List(list) = &attr.meta
            && let Ok(nested) = list.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        {
            for meta in nested {
                if let Meta::Path(path) = meta
                    && path.is_ident("disabled")
                {
                    return true;
                }
            }
        }
    }
    false
}

fn to_snake_case(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_uppercase() {
            if !result.is_empty() && chars.peek().is_some_and(|&next| next.is_lowercase()) {
                result.push('_');
            }
            result.extend(ch.to_lowercase());
        } else {
            result.push(ch);
        }
    }

    result
}
