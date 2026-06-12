#[macro_export]
macro_rules! weaviate_endpoint {
    // Empty struct variant (no fields)
    (
        $kind:ident,
        $api_info:expr,
        struct {}
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, endpoint_derive::DocumentInput)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {}

            type SimpleInput = [<$kind Input>];

            impl $crate::EndpointOperation for [<$kind Input>] {}

            #[allow(non_snake_case)]
            #[ctor::ctor]
            fn [<__register_weaviate_operation_for_ $kind>]() {
                $crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
    // Struct with fields
    (
        $kind:ident,
        $api_info:expr,
        struct {
            $($field:ident: $type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, endpoint_derive::DocumentInput)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {
                $($field: $type),*
            }

            type SimpleInput = [<$kind Input>];

            impl $crate::EndpointOperation for [<$kind Input>] {}

            impl [<$kind Input>] {
                $(
                    pub fn $field(&self) -> &$type {
                        &self.$field
                    }
                )*
            }

            #[allow(non_snake_case)]
            #[ctor::ctor]
            fn [<__register_weaviate_operation_for_ $kind>]() {
                $crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}
