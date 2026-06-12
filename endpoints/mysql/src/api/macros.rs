#[macro_export]
macro_rules! mysql_endpoint {
    (
        $kind:ident,
        $api_info:expr,
        struct {
            $($field:ident: $type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, endpoint_derive::DocumentInput, schemars::JsonSchema)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {
                $($field: $type),*
            }

            type SimpleInput = [<$kind Input>];

            impl $crate::EndpointOperation for [<$kind Input>] {}

            #[allow(dead_code)]
            impl [<$kind Input>] {
                fn mysql_operation_todo() {
                    todo!("Determine if we want to derive here or in the mysql crate")
                }


                // Add a constructor function
                pub fn new($($field: $type),*) -> Self {
                    Self {
                        $($field),*
                    }
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
            fn [<__register_mysql_operation_for_ $kind>]() {
                $crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}
