// TODO: Prefer $crate:: for macro paths where appropriate; keep crate:: only when intentional.
#![allow(clippy::crate_in_macro_def)]

#[macro_export]
macro_rules! snowflake_endpoint {
    (
        $kind:ident,
        $api_info:expr,
        struct {
            $($field:ident: $type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, endpoint_derive::DocumentInput)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {
                $($field: $type),*
            }

            type SimpleInput = [<$kind Input>];

            impl $crate::EndpointOperation for [<$kind Input>] {}

            impl [<$kind Input>] {
                #[allow(dead_code)]
                fn snowflake_operation_todo() {
                    todo!("Determine if we want to derive here or in the snowflake crate")
                }

                // Generate getter methods for each field
                $(
                    pub fn $field(&self) -> &$type {
                        &self.$field
                    }
                )*
            }

            // Generated name includes PascalCase operation variant from $kind.
            #[allow(non_snake_case)]
            #[ctor::ctor]
            fn [<__register_snowflake_operation_for_ $kind>]() {
                crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}
