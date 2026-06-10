#[macro_export]
macro_rules! oracle_endpoint {
    (
        struct $name:ident {
            $($field:ident: $type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, endpoint_derive::DocumentInput, schemars::JsonSchema)]
            #[builder(setter(into))]
            pub struct $name {
                $($field: $type),*
            }
            type SimpleInput = $name;
            impl $crate::EndpointOperation for $name {}
            impl $name {
                #[allow(dead_code)]
                fn oracle_operation_todo() {
                    todo!("Determine if we want to derive here or in the oracle crate")
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
            #[ctor::ctor]
            fn [<__register_oracle_operation_for_ $name:snake>]() {
                $crate::serde::register_operation::<$name>();
            }
        }
    }
}
