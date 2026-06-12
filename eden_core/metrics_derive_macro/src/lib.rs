#![cfg_attr(test, allow(clippy::unwrap_used))]
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{Data, DeriveInput, Expr, Field, Fields, Ident, Lit, Meta, Variant, parse::Parser, parse_macro_input};

/// This generates:
/// - A new metrics struct with OpenTelemetry instruments
/// - Recording methods for each event variant
#[proc_macro_attribute]
pub fn metric_events(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Parse args
    let parser = syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated;
    let args = parser.parse(args).expect("Failed to parse attribute arguments");

    let category = extract_arg(&args, "category").unwrap_or_else(|| "unknown".to_string());
    let prefix = extract_arg(&args, "prefix").unwrap_or_else(|| category.clone());

    // Get enum name and create metrics struct name
    let _enum_name = &input.ident;
    let metrics_name = format_ident!("{}Metrics", capitalize(&category));

    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => panic!("metric_events can only be used on enums"),
    };

    // Parse all variants and collect metrics
    let mut all_metrics = Vec::new();
    let mut recording_methods = Vec::new();
    let mut cleaned_variants = Vec::new();

    for variant in variants {
        let result = process_variant(variant, &prefix);
        all_metrics.extend(result.metrics);
        recording_methods.push(result.recording_method);

        // Create cleaned variant without custom attributes
        let variant_name = &variant.ident;
        let variant_attrs: Vec<_> = variant.attrs.iter().filter(|a| !is_metric_attr(a)).collect();

        let cleaned_fields = if let Fields::Named(fields) = &variant.fields {
            let cleaned: Vec<_> = fields
                .named
                .iter()
                .map(|f| {
                    let field_name = &f.ident;
                    let field_type = &f.ty;
                    let field_vis = &f.vis;
                    let kept_attrs: Vec<_> = f.attrs.iter().filter(|a| !is_metric_attr(a)).collect();
                    quote! {
                        #(#kept_attrs)*
                        #field_vis #field_name: #field_type
                    }
                })
                .collect();
            quote! { { #(#cleaned),* } }
        } else if let Fields::Unnamed(fields) = &variant.fields {
            let cleaned: Vec<_> = fields
                .unnamed
                .iter()
                .map(|f| {
                    let field_type = &f.ty;
                    let field_vis = &f.vis;
                    let kept_attrs: Vec<_> = f.attrs.iter().filter(|a| !is_metric_attr(a)).collect();
                    quote! {
                        #(#kept_attrs)*
                        #field_vis #field_type
                    }
                })
                .collect();
            quote! { ( #(#cleaned),* ) }
        } else {
            quote! {}
        };

        cleaned_variants.push(quote! {
            #(#variant_attrs)*
            #variant_name #cleaned_fields
        });
    }

    // Deduplicate metrics based on field_name and instrument_type
    // This allows multiple events to share the same underlying metric
    let mut unique_metrics: HashMap<String, MetricDef> = HashMap::new();
    for metric in all_metrics {
        let key = format!("{}:{}", metric.field_name, metric.instrument_type);

        // Only add if not already present, or if the new one has more details
        unique_metrics.entry(key).or_insert_with(|| metric);
    }

    // Convert back to Vec for consistent ordering
    let mut deduplicated_metrics: Vec<_> = unique_metrics.into_values().collect();
    // Sort by field name for consistent output
    deduplicated_metrics.sort_by(|a, b| a.field_name.to_string().cmp(&b.field_name.to_string()));

    // Generate metric struct fields
    let metric_fields: Vec<_> = deduplicated_metrics
        .iter()
        .map(|m| {
            let name = &m.field_name;
            let ty_str = &m.instrument_type;
            // Parse the instrument type string into a TokenStream
            let ty: proc_macro2::TokenStream = ty_str.parse().expect("Invalid instrument type");
            quote! { #name: #ty }
        })
        .collect();

    // Generate metric initialization using deduplicated metrics
    let metric_inits: Vec<_> = deduplicated_metrics
        .iter()
        .map(|m| {
            let name = &m.field_name;
            let metric_name = &m.metric_name;
            let description = &m.description;
            let instrument_type = &m.instrument_type;
            let unit = &m.unit;

            if instrument_type == "Counter<u64>" {
                if let Some(u) = unit {
                    quote! {
                        #name: meter
                            .u64_counter(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .with_unit(#u)
                            .build()
                    }
                } else {
                    quote! {
                        #name: meter
                            .u64_counter(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .build()
                    }
                }
            } else if instrument_type == "Histogram<f64>" {
                if let Some(u) = unit {
                    quote! {
                        #name: meter
                            .f64_histogram(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .with_unit(#u)
                            .build()
                    }
                } else {
                    quote! {
                        #name: meter
                            .f64_histogram(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .build()
                    }
                }
            } else if instrument_type == "Gauge<u64>" {
                if let Some(u) = unit {
                    quote! {
                        #name: meter
                            .u64_gauge(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .with_unit(#u)
                            .build()
                    }
                } else {
                    quote! {
                        #name: meter
                            .u64_gauge(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .build()
                    }
                }
            } else if instrument_type == "Gauge<i64>" {
                if let Some(u) = unit {
                    quote! {
                        #name: meter
                            .i64_gauge(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .with_unit(#u)
                            .build()
                    }
                } else {
                    quote! {
                        #name: meter
                            .i64_gauge(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .build()
                    }
                }
            } else if instrument_type == "Gauge<f64>" {
                if let Some(u) = unit {
                    quote! {
                        #name: meter
                            .f64_gauge(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .with_unit(#u)
                            .build()
                    }
                } else {
                    quote! {
                        #name: meter
                            .f64_gauge(format!("{}.{}", prefix, #metric_name))
                            .with_description(#description)
                            .build()
                    }
                }
            } else {
                quote! {
                    #name: meter
                        .i64_up_down_counter(format!("{}.{}", prefix, #metric_name))
                        .with_description(#description)
                        .build()
                }
            }
        })
        .collect();

    // Build cleaned enum
    let enum_name = &input.ident;
    let enum_vis = &input.vis;
    let enum_attrs: Vec<_> = input.attrs.iter().filter(|a| !a.path().is_ident("metric_events")).collect();

    let output = quote! {
        use opentelemetry::metrics::{Counter, Histogram, Gauge, UpDownCounter};

        // Output the enum without custom metric attributes
        #(#enum_attrs)*
        #enum_vis enum #enum_name {
            #(#cleaned_variants),*
        }

        // Generate metrics struct
        pub struct #metrics_name {
            #(#metric_fields,)*
        }

        impl #metrics_name {
            /// Create new metrics instance with OpenTelemetry meter
            pub fn new(meter: opentelemetry::metrics::Meter) -> Self {
                let prefix = #prefix;

                Self {
                    #(#metric_inits,)*
                }
            }

            #(#recording_methods)*
        }
    };

    // Return both the cleaned enum and generated code
    TokenStream::from(output)
}

#[derive(Debug)]
struct MetricDef {
    field_name: Ident,
    metric_name: String,
    description: String,
    instrument_type: String,
    unit: Option<String>,
}

#[derive(Debug)]
struct VariantResult {
    metrics: Vec<MetricDef>,
    recording_method: proc_macro2::TokenStream,
}

#[derive(Debug, Default)]
struct VariantAttrs {
    flat: bool,
    no_counter: bool,
}

fn parse_variant_attrs(variant: &Variant) -> VariantAttrs {
    let mut attrs = VariantAttrs::default();

    for attr in &variant.attrs {
        if attr.path().is_ident("variant")
            && let Ok(list) = attr.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        {
            for meta in list {
                if let Meta::Path(path) = meta {
                    if path.is_ident("flat") {
                        attrs.flat = true;
                    } else if path.is_ident("no_counter") {
                        attrs.no_counter = true;
                    }
                }
            }
        }
    }

    attrs
}

fn process_variant(variant: &Variant, _prefix: &str) -> VariantResult {
    let variant_name = &variant.ident;
    let variant_snake = to_snake_case(&variant_name.to_string());
    let method_name = format_ident!("record_{}", variant_snake);
    let variant_attrs = parse_variant_attrs(variant);

    let mut metrics = Vec::new();
    let mut label_fields = Vec::new();
    let mut metric_recordings = Vec::new();

    if let Fields::Named(fields) = &variant.fields {
        for field in &fields.named {
            let field_name = field.ident.as_ref().unwrap();
            let field_type = &field.ty;
            let attr_info = parse_field_attr(field);

            match attr_info.attr_type.as_str() {
                "label" => {
                    label_fields.push((field_name.clone(), field_type.clone()));
                }
                "counter" => {
                    // Use custom metric_name if provided, otherwise generate based on flat attribute
                    let (metric_field_name, metric_name_str) = if let Some(custom_name) = &attr_info.metric_name {
                        (format_ident!("{}", custom_name), custom_name.clone())
                    } else if variant_attrs.flat {
                        (format_ident!("{}_total", field_name), format!("{}_total", field_name))
                    } else {
                        (
                            format_ident!("{}_{}_total", variant_snake, field_name),
                            format!("{}_{}_total", variant_snake, field_name),
                        )
                    };

                    let description = attr_info.description.unwrap_or_else(|| format!("Total {} for {}", field_name, variant_snake));

                    metrics.push(MetricDef {
                        field_name: metric_field_name.clone(),
                        metric_name: metric_name_str,
                        description,
                        instrument_type: "Counter<u64>".to_string(),
                        unit: attr_info.unit,
                    });

                    metric_recordings.push(quote! {
                        self.#metric_field_name.add(#field_name, &labels);
                    });
                }
                "histogram" => {
                    let (metric_field_name, metric_name_str) = if let Some(custom_name) = &attr_info.metric_name {
                        (format_ident!("{}", custom_name), custom_name.clone())
                    } else if variant_attrs.flat {
                        (format_ident!("{}", field_name), field_name.to_string())
                    } else {
                        (format_ident!("{}_{}", variant_snake, field_name), format!("{}_{}", variant_snake, field_name))
                    };

                    let description = attr_info.description.unwrap_or_else(|| format!("{} for {}", field_name, variant_snake));

                    metrics.push(MetricDef {
                        field_name: metric_field_name.clone(),
                        metric_name: metric_name_str,
                        description,
                        instrument_type: "Histogram<f64>".to_string(),
                        unit: attr_info.unit,
                    });

                    metric_recordings.push(quote! {
                        self.#metric_field_name.record(#field_name, &labels);
                    });
                }
                "gauge" => {
                    let (metric_field_name, metric_name_str) = if let Some(custom_name) = &attr_info.metric_name {
                        (format_ident!("{}", custom_name), custom_name.clone())
                    } else if variant_attrs.flat {
                        (format_ident!("{}", field_name), field_name.to_string())
                    } else {
                        (format_ident!("{}_{}", variant_snake, field_name), format!("{}_{}", variant_snake, field_name))
                    };

                    let description = attr_info.description.unwrap_or_else(|| format!("{} for {}", field_name, variant_snake));

                    // Determine gauge type from field type
                    let ty_str = quote!(#field_type).to_string();
                    let instrument_type = if ty_str.contains("i64") {
                        "Gauge<i64>".to_string()
                    } else if ty_str.contains("f64") {
                        "Gauge<f64>".to_string()
                    } else {
                        "Gauge<u64>".to_string()
                    };

                    metrics.push(MetricDef {
                        field_name: metric_field_name.clone(),
                        metric_name: metric_name_str,
                        description,
                        instrument_type,
                        unit: attr_info.unit,
                    });

                    metric_recordings.push(quote! {
                        self.#metric_field_name.record(#field_name, &labels);
                    });
                }
                "updown" => {
                    let (metric_field_name, metric_name_str) = if let Some(custom_name) = &attr_info.metric_name {
                        (format_ident!("{}", custom_name), custom_name.clone())
                    } else if variant_attrs.flat {
                        (format_ident!("{}", field_name), field_name.to_string())
                    } else {
                        (format_ident!("{}_{}", variant_snake, field_name), format!("{}_{}", variant_snake, field_name))
                    };

                    let description = attr_info.description.unwrap_or_else(|| format!("{} for {}", field_name, variant_snake));

                    metrics.push(MetricDef {
                        field_name: metric_field_name.clone(),
                        metric_name: metric_name_str,
                        description,
                        instrument_type: "UpDownCounter<i64>".to_string(),
                        unit: None,
                    });

                    metric_recordings.push(quote! {
                        self.#metric_field_name.add(#field_name, &labels);
                    });
                }
                _ => {}
            }
        }
    }

    // Generate method parameters
    let method_params: Vec<_> = if let Fields::Named(fields) = &variant.fields {
        fields
            .named
            .iter()
            .map(|f| {
                let name = f.ident.as_ref().unwrap();
                let ty = &f.ty;
                quote! { #name: #ty }
            })
            .collect()
    } else {
        Vec::new()
    };

    // Generate label building code
    let label_building: Vec<_> = label_fields
        .iter()
        .map(|(name, ty)| {
            let name_str = name.to_string();
            let ty_str = quote!(#ty).to_string();

            if ty_str.contains("Option") {
                quote! {
                    if let Some(val) = #name {
                        labels.push(opentelemetry::FastSpanAttribute::new(#name_str, val.to_string()));
                    }
                }
            } else {
                quote! {
                    labels.push(opentelemetry::FastSpanAttribute::new(#name_str, #name.to_string()));
                }
            }
        })
        .collect();

    // Add counter for total events (unless no_counter is set)
    let recording_method = if !variant_attrs.no_counter {
        let requests_field_name = format_ident!("{}_total", variant_snake);
        let requests_metric_name = format!("{}_total", variant_snake);
        metrics.insert(
            0,
            MetricDef {
                field_name: requests_field_name.clone(),
                metric_name: requests_metric_name,
                description: format!("Total {} events", variant_snake),
                instrument_type: "Counter<u64>".to_string(),
                unit: None,
            },
        );

        let doc_comment = format!("Record {} event", variant_snake);

        quote! {
            #[doc = #doc_comment]
            pub fn #method_name(&self, #(#method_params),*) {
                let mut labels = Vec::new();
                #(#label_building)*

                // Increment total counter
                self.#requests_field_name.add(1, &labels);

                // Record specific metrics
                #(#metric_recordings)*
            }
        }
    } else {
        let doc_comment = format!("Record {} event", variant_snake);

        quote! {
            #[doc = #doc_comment]
            pub fn #method_name(&self, #(#method_params),*) {
                let mut labels = Vec::new();
                #(#label_building)*

                // Record specific metrics
                #(#metric_recordings)*
            }
        }
    };

    VariantResult { metrics, recording_method }
}

#[derive(Debug)]
struct FieldAttrInfo {
    attr_type: String,
    description: Option<String>,
    unit: Option<String>,
    metric_name: Option<String>,
}

fn parse_field_attr(field: &Field) -> FieldAttrInfo {
    for attr in &field.attrs {
        // Check if it's a simple path attribute like #[label], #[counter]
        if attr.path().is_ident("label") {
            return FieldAttrInfo {
                attr_type: "label".to_string(),
                description: None,
                unit: None,
                metric_name: None,
            };
        }

        if attr.path().is_ident("counter") {
            let mut info = FieldAttrInfo {
                attr_type: "counter".to_string(),
                description: None,
                unit: None,
                metric_name: None,
            };

            // Try to parse arguments
            if let Ok(list) = attr.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated) {
                for meta in list {
                    if let Meta::NameValue(nv) = meta {
                        if nv.path.is_ident("description") {
                            if let Expr::Lit(expr_lit) = &nv.value
                                && let Lit::Str(lit) = &expr_lit.lit
                            {
                                info.description = Some(lit.value());
                            }
                        } else if nv.path.is_ident("unit") {
                            if let Expr::Lit(expr_lit) = &nv.value
                                && let Lit::Str(lit) = &expr_lit.lit
                            {
                                info.unit = Some(lit.value());
                            }
                        } else if nv.path.is_ident("metric_name")
                            && let Expr::Lit(expr_lit) = &nv.value
                            && let Lit::Str(lit) = &expr_lit.lit
                        {
                            info.metric_name = Some(lit.value());
                        }
                    }
                }
            }

            return info;
        }

        if attr.path().is_ident("histogram") {
            let mut info = FieldAttrInfo {
                attr_type: "histogram".to_string(),
                description: None,
                unit: None,
                metric_name: None,
            };

            if let Ok(list) = attr.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated) {
                for meta in list {
                    if let Meta::NameValue(nv) = meta {
                        if nv.path.is_ident("description") {
                            if let Expr::Lit(expr_lit) = &nv.value
                                && let Lit::Str(lit) = &expr_lit.lit
                            {
                                info.description = Some(lit.value());
                            }
                        } else if nv.path.is_ident("unit") {
                            if let Expr::Lit(expr_lit) = &nv.value
                                && let Lit::Str(lit) = &expr_lit.lit
                            {
                                info.unit = Some(lit.value());
                            }
                        } else if nv.path.is_ident("metric_name")
                            && let Expr::Lit(expr_lit) = &nv.value
                            && let Lit::Str(lit) = &expr_lit.lit
                        {
                            info.metric_name = Some(lit.value());
                        }
                    }
                }
            }

            return info;
        }

        if attr.path().is_ident("gauge") {
            let mut info = FieldAttrInfo {
                attr_type: "gauge".to_string(),
                description: None,
                unit: None,
                metric_name: None,
            };

            if let Ok(list) = attr.parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated) {
                for meta in list {
                    if let Meta::NameValue(nv) = meta {
                        if nv.path.is_ident("description") {
                            if let Expr::Lit(expr_lit) = &nv.value
                                && let Lit::Str(lit) = &expr_lit.lit
                            {
                                info.description = Some(lit.value());
                            }
                        } else if nv.path.is_ident("unit") {
                            if let Expr::Lit(expr_lit) = &nv.value
                                && let Lit::Str(lit) = &expr_lit.lit
                            {
                                info.unit = Some(lit.value());
                            }
                        } else if nv.path.is_ident("metric_name")
                            && let Expr::Lit(expr_lit) = &nv.value
                            && let Lit::Str(lit) = &expr_lit.lit
                        {
                            info.metric_name = Some(lit.value());
                        }
                    }
                }
            }

            return info;
        }

        if attr.path().is_ident("updown") {
            return FieldAttrInfo {
                attr_type: "updown".to_string(),
                description: None,
                unit: None,
                metric_name: None,
            };
        }
    }

    FieldAttrInfo {
        attr_type: "label".to_string(),
        description: None,
        unit: None,
        metric_name: None,
    }
}

fn extract_arg(args: &syn::punctuated::Punctuated<Meta, syn::Token![,]>, name: &str) -> Option<String> {
    for arg in args {
        if let Meta::NameValue(nv) = arg
            && nv.path.is_ident(name)
            && let Expr::Lit(expr_lit) = &nv.value
            && let Lit::Str(lit) = &expr_lit.lit
        {
            return Some(lit.value());
        }
    }
    None
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

/// Check if an attribute is a metric-related custom attribute
fn is_metric_attr(attr: &syn::Attribute) -> bool {
    attr.path().is_ident("label")
        || attr.path().is_ident("counter")
        || attr.path().is_ident("histogram")
        || attr.path().is_ident("gauge")
        || attr.path().is_ident("updown")
        || attr.path().is_ident("variant")
}
