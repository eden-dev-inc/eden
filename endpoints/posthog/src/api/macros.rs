#[macro_export]
macro_rules! posthog_endpoint {
    (
        $kind:ident,
        $api_info:expr,
        struct {
            $($field:ident: $type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema, schemars::JsonSchema)]
            #[builder(setter(into))]
            pub struct [<$kind Input>] {
                $($field: $type),*
            }

            impl serde::Serialize for [<$kind Input>] {
                fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    use serde::ser::SerializeStruct;
                    let field_count = 1 $(+ {stringify!($field); 1})*;
                    let mut state = serializer.serialize_struct(stringify!([<$kind Input>]), field_count)?;
                    state.serialize_field("type", &$api_info.api.to_string())?;
                    $(state.serialize_field(stringify!($field), &self.$field)?;)*
                    state.end()
                }
            }

            type SimpleInput = [<$kind Input>];

            impl $crate::EndpointOperation for [<$kind Input>] {}

            #[allow(dead_code)]
            impl [<$kind Input>] {
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

            #[allow(non_snake_case)]
            #[ctor::ctor]
            fn [<__register_posthog_operation_for_ $kind>]() {
                $crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}
