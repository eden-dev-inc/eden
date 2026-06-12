// TODO: Prefer $crate:: for macro paths where appropriate; keep crate:: only when intentional.
#![allow(clippy::crate_in_macro_def)]

#[macro_export]
macro_rules! mongo_endpoint {
    (
        $api_info:expr,
        struct $name:ident {
            $(
                $(#[$attr:meta])*
                $field:ident: $type:ty
            ),* $(,)?
        }
    ) => {
        #[derive(Debug, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, endpoint_derive::DocumentInput, schemars::JsonSchema)]
        #[builder(setter(into))]
        pub struct $name {
            $(
                $(#[$attr])*
                $field: $type
            ),*
        }

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeStruct;
                let field_count = 1 $(+ {stringify!($field); 1})*;
                let mut state = serializer.serialize_struct(stringify!($name), field_count)?;
                state.serialize_field("type", &$api_info.api.to_string())?;
                $(state.serialize_field(stringify!($field), &self.$field)?;)*
                state.end()
            }
        }

        impl crate::EndpointOperation for $name {}

        impl $name {
            #[allow(dead_code)]
            fn mongo_operation_todo() {
                todo!("Determine if we want to derive here or in the mongo crate")
            }

            pub fn new($($field: $type),*) -> Self {
                Self {
                    $($field),*
                }
            }

            $(
                pub fn $field(&self) -> &$type {
                    &self.$field
                }
            )*
        }

        paste::paste! {
            #[ctor::ctor]
            fn [<__register_mongo_operation_for_ $name:snake>]() {
                $crate::serde::register_operation::<$name>();
            }
        }
    }
}
