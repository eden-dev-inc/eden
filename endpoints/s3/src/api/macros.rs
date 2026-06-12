#[macro_export]
macro_rules! s3_endpoint {
    (
        $kind:ident,
        $api_info:expr,
        struct {
            $( $(#[$meta:meta])* $field:ident: $type:ty ),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, schemars::JsonSchema)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {
                $( $(#[$meta])* $field: $type ),*
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
            fn [<__register_s3_operation_for_ $kind>]() {
                $crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}
