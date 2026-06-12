use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, format_ident, quote};
use syn::{Attribute, Fields, ItemEnum, ItemStruct, PathArguments, Type, WhereClause};

pub fn document_api_enum(input: &ItemEnum) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let where_clause = where_clause.map_or_else(
        || WhereClause {
            where_token: Default::default(),
            predicates: Default::default(),
        },
        Clone::clone,
    );

    let mut commands_body = TokenStream2::new();
    for variant in input.variants.iter() {
        let variant_ident = &variant.ident;
        let variant_ident_str = variant_ident.to_string().to_lowercase();
        let variant_inputs = if simple_complex(&variant.attrs) {
            commands_body.extend(quote! { let mut oneof_variant = utoipa::openapi::OneOfBuilder::new(); });
            vec![
                format_ident!("Simple{}Input", &variant.ident),
                format_ident!("Complex{}Input", &variant.ident),
            ]
        } else if simple(&variant.attrs) {
            vec![format_ident!("Simple{}Input", &variant.ident)]
        } else {
            vec![format_ident!("{}Input", &variant.ident)]
        };

        for variant_input in &variant_inputs {
            commands_body.extend(quote! {let mut obj = utoipa::openapi::ObjectBuilder::new();});
            if let Some(variant_field) = variant.fields.iter().next() {
                commands_body.extend(quote! {
                    obj = obj.title(Some(#variant_ident_str.to_string()))
                    .property(
                        "kind",
                        utoipa::openapi::ObjectBuilder::new()
                            .schema_type(utoipa::openapi::schema::SchemaType::Type(utoipa::openapi::Type::String))
                            .enum_values(Some([<#name>::db_kind()].to_vec()))
                            .build(),
                    )

                });
                let mut type_schema = quote! {};
                let mut _is_optional = false;
                // if the variant has a field, e.g. Datavase(Option<DatabaseApi>), show the schema for this field
                if let Type::Path(p) = &variant_field.ty {
                    let ty_path = &p.path;
                    if let Some(first_segment) = ty_path.segments.first() {
                        if &first_segment.ident == "Option" {
                            _is_optional = true;
                            if let PathArguments::AngleBracketed(ab_args) = &first_segment.arguments {
                                for arg in ab_args.args.iter() {
                                    if let syn::GenericArgument::Type(inner_ty) = arg {
                                        type_schema = quote! { #inner_ty::schema() };
                                        break;
                                    }
                                }
                            }
                        } else {
                            type_schema = quote! { #ty_path::schema() };
                        }
                    } else {
                        type_schema = quote! { #ty_path::schema() };
                    }
                }
                commands_body.extend(quote! {
                    .property(
                        "type",
                        #type_schema,
                    )
                    .required("kind")
                    .required("type");
                });
            } else {
                commands_body.extend(quote! {
                    obj = obj.title(Some((#name::#variant_ident).to_string()))
                    .property(
                        "kind",
                        utoipa::openapi::ObjectBuilder::new()
                            .schema_type(utoipa::openapi::schema::SchemaType::Type(utoipa::openapi::Type::String))
                            .enum_values(Some([<#name>::db_kind()].to_vec()))
                            .build(),
                    )
                    .property(
                        "type",
                        utoipa::openapi::ObjectBuilder::new()
                            .schema_type(utoipa::openapi::schema::SchemaType::Type(utoipa::openapi::Type::String))
                            .enum_values(Some(vec![(#name::#variant_ident).to_string()]))
                            .build(),
                    )
                    .required("kind")
                    .required("type");
                });
                if parse_input_struct(&variant.attrs) {
                    // if simple_complex(&variant.attrs) {
                    //     commands_body.extend(quote! {
                    //         let simple_complex_inputs = utoipa::openapi::OneOfBuilder::new();
                    //     });
                    //     for variant_input in &[
                    //         format_ident!("Simple{}Input", &variant.ident),
                    //         format_ident!("Complex{}Input", &variant.ident),
                    //     ] {
                    //         commands_body.extend(quote! {
                    //             let mut sc_obj = utoipa::openapi::ObjectBuilder::new();
                    //             for (field_name, field_schema, is_optional) in #variant_input::fields() {
                    //                 sc_obj = sc_obj.property(field_name.to_string(), field_schema);
                    //                 if !is_optional {
                    //                     obj = obj.required(field_name);
                    //                 }
                    //             }
                    //             let simple_complex_inputs = simple_complex_inputs.item(sc_obj.build());
                    //         });
                    //     }
                    //     commands_body.extend(quote! {
                    //         let simple_complex_inputs = simple_complex_inputs.build();
                    //         obj = obj.p
                    //     });
                    // } else {
                    commands_body.extend(quote! {
                        for (field_name, field_schema, is_optional) in #variant_input::fields() {
                            obj = obj.property(field_name.to_string(), field_schema);
                            if !is_optional {
                                obj = obj.required(field_name);
                            }
                        }
                    });
                    // };
                }
            }
            if variant_inputs.len() > 1 {
                commands_body.extend(quote! {oneof_variant = oneof_variant.item(obj.build());})
            }
        }
        if variant_inputs.len() > 1 {
            commands_body.extend(quote! {
                let objects = objects.item(utoipa::openapi::RefOr::T(utoipa::openapi::Schema::OneOf(oneof_variant.build())));
            });
        } else {
            commands_body.extend(quote! {
                let objects = objects.item(obj.build());
            });
        }
    }

    Ok(quote! {
        impl #impl_generics utoipa::ToSchema for #name #ty_generics #where_clause {}
        impl #impl_generics utoipa::PartialSchema for #name #ty_generics #where_clause {
            fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
                let objects = utoipa::openapi::OneOfBuilder::new();
                #commands_body
                utoipa::openapi::RefOr::T(utoipa::openapi::Schema::OneOf(objects.title(Some(<#name>::name())).build()))
            }
        }
    })
}

pub fn document_input(input: &ItemStruct) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let where_clause = where_clause.map_or_else(
        || WhereClause {
            where_token: Default::default(),
            predicates: Default::default(),
        },
        Clone::clone,
    );
    let mut vec_body = TokenStream2::new();
    if let Fields::Named(fields) = &input.fields {
        for field in &fields.named {
            // Named fields should always have an ident, but handle None gracefully
            let field_name = match &field.ident {
                Some(ident) => ident,
                None => continue,
            };

            let mut is_optional = false;
            let mut schema = TokenStream2::new();
            if let Type::Path(p) = &field.ty
                && let Some(option_ident) = p.path.segments.first()
                && &option_ident.ident == "Option"
            {
                is_optional = true;
                if let PathArguments::AngleBracketed(ab_args) = &option_ident.arguments {
                    ab_args.args.to_tokens(&mut schema);
                }
            }
            if !is_optional {
                field.ty.to_tokens(&mut schema);
            }
            let key = format!("{}", format_ident!("{}", field_name));
            vec_body.extend(quote! {
                (#key.to_string(), <#schema>::schema(), #is_optional),
            });
        }
    }
    Ok(quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            pub fn fields() -> Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::Schema>, bool)> {
                vec![
                    #vec_body
                ]
            }
        }
    })
}

// parse_input_struct - check if there's no #[noinput] attribute for the variant
fn parse_input_struct(attr: &[Attribute]) -> bool {
    !attr.iter().any(|attr| {
        if let Some(id) = attr.path().get_ident() {
            id == "noinput"
        } else {
            false
        }
    })
}

// simple_complex - check for #[simple_complex] attribute for the variant
fn simple_complex(attr: &[Attribute]) -> bool {
    attr.iter().any(|attr| {
        if let Some(id) = attr.path().get_ident() {
            id == "simple_complex"
        } else {
            false
        }
    })
}

// simple - check for #[simple] attribute for the variant
fn simple(attr: &[Attribute]) -> bool {
    attr.iter().any(|attr| {
        if let Some(id) = attr.path().get_ident() {
            id == "simple"
        } else {
            false
        }
    })
}
