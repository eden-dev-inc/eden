// TODO: Prefer $crate:: for macro paths where appropriate; keep crate:: only when intentional.
#![allow(clippy::crate_in_macro_def)]

#[macro_export]
macro_rules! function_endpoint {
    (
        $kind:ident,
        $api_info:expr,
        struct {
            $( $(#[$meta:meta])* $field:ident: $type:ty ),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, endpoint_derive::DocumentInput)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {
                $( $(#[$meta])* $field: $type ),*
            }

            type SimpleInput = [<$kind Input>];

            impl $crate::EndpointOperation for [<$kind Input>] {}

            impl [<$kind Input>] {
                #[allow(dead_code)]
                fn function_operation_todo() {
                    todo!("Determine if we want to derive here or in the function crate")
                }

                $(
                    pub fn $field(&self) -> &$type {
                        &self.$field
                    }
                )*
            }

            #[allow(non_snake_case)]
            #[ctor::ctor]
            fn [<__register_function_operation_for_ $kind>]() {
                crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}
