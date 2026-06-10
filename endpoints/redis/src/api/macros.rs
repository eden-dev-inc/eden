// TODO: Prefer $crate:: for macro paths where appropriate; keep crate:: only when intentional.
#![allow(clippy::crate_in_macro_def)]

#[macro_export]
macro_rules! redis_endpoint {
    (
        $kind:ident,
        $api_info:expr,
        $(#[doc = $doc:expr])*
        struct {
            $($field:ident: $type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            $(#[doc = $doc])*
            #[derive(Debug, serde::Deserialize, Clone, Default, derive_builder::Builder, utoipa::ToSchema)]
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
                    state.serialize_field("type", &$api_info.api)?;
                    $(state.serialize_field(stringify!($field), &self.$field)?;)*
                    state.end()
                }
            }

            type SimpleInput = [<$kind Input>];

            impl crate::EndpointOperation for [<$kind Input>] {}

            impl [<$kind Input>] {
                #[allow(dead_code)]
                fn redis_operation_todo() {
                    todo!("Determine if we want to derive here or in the redis crate")
                }

                // Generate getter methods for each field
                $(
                    #[allow(dead_code)]
                    pub fn $field(&self) -> &$type {
                        &self.$field
                    }
                )*
            }

            #[ctor::ctor]
            #[allow(non_snake_case)]
            #[allow(dead_code)]
            fn __register_redis_operation() {
                // Register operation using the string-based registry
                $crate::serde::register_operation::<[<$kind Input>]>();
            }
        }
    };
}

#[macro_export]
macro_rules! impl_redis_operation {
    // Pattern for unit structs (no fields, no generics)
    ($type:ty, $api_info:expr) => {
        use endpoint_types::{EpRequest, RequestConstructor};

        use endpoint_types::request::EndpointRequestInput;
        use ::error::EpError;

        impl crate::EndpointOperation for $type {}

        impl TryInto<EndpointRequestInput> for $type {
            type Error = ::error::EpError;
            fn try_into(self) -> Result<EndpointRequestInput, Self::Error> {
                Ok(EndpointRequestInput::new(
                    serde_json::to_value(
                        Box::new($crate::request::RedisRequest::new(Box::new(self))).as_request()
                    ).map_err(::error::EpError::serde)?
                ))
            }
        }

        impl crate::OperationKind<$crate::api::lib::RedisApi> for $type {
            fn operation_kind() -> $crate::api::lib::RedisApi {
                $api_info.api
            }
        }

        impl $type {
            /// Returns the request type (Read or Write) for this command without requiring an instance.
            pub fn static_request_type() -> crate::ReqType {
                $api_info.request_type
            }

            /// Returns whether this command can safely use a shared backend connection.
            pub fn safe() -> bool {
                $api_info.safe()
            }
        }

        impl crate::Operation<redis_core::RedisAsync, $crate::api::lib::RedisApi, redis_core::RedisTx> for $type {
            fn kind(&self) -> $crate::api::lib::RedisApi {
                $api_info.api
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn request_type(&self) -> crate::ReqType {
                $api_info.request_type
            }

            fn as_operation(self: Box<Self>) -> Box<dyn crate::Operation<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx>> {
                self
            }

            fn as_exec(&self) -> Option<&dyn crate::OperationExecutor<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx>> {
                Some(self)
            }

            fn clone_box(&self) -> Box<dyn crate::Operation<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx>> {
                Box::new(self.clone())
            }
        }

        impl crate::OperationExecutor<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx> for $type {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn run_operation_request(&self, context: $crate::ep::RedisAsync, mut telemetry_wrapper: telemetry::TelemetryWrapper) -> crate::RunOutput<'_> {
                Box::pin(async move {
                  self.run_async_generic(context, &mut telemetry_wrapper).await
                })
            }

            fn run_operation_transaction(&self, tx_context: &mut $crate::ep::RedisTx, telemetry_wrapper: &mut telemetry::TelemetryWrapper) {
                self.run_transaction_generic(tx_context, telemetry_wrapper);
            }
        }

        impl borsh::BorshDeserialize for $type {
            fn deserialize(_buf: &mut &[u8]) -> std::io::Result<Self> {
                Ok(Self {})
            }

            fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer)?;
                borsh::BorshDeserialize::deserialize(&mut buffer.as_slice())
            }
        }

        impl borsh::BorshSerialize for $type {
            fn serialize<W: std::io::Write>(&self, _writer: &mut W) -> std::io::Result<()> {
                Ok(())
            }
        }

        #[ctor::ctor]
        #[allow(non_snake_case)]
        #[allow(dead_code)]
        fn __register_redis_operation() {
            // Register operation using the string-based registry
            $crate::serde::register_operation::<$type>();
        }
    };

    // Pattern for structs with fields but no generics
    ($type:ty, $api_info:expr, {$($field:ident),+}) => {
        use endpoint_types::{EpRequest, RequestConstructor};

        use endpoint_types::request::EndpointRequestInput;
        use ::error::EpError;

        impl crate::EndpointOperation for $type {}

        impl TryInto<EndpointRequestInput> for $type {
            type Error = ::error::EpError;
            fn try_into(self) -> Result<EndpointRequestInput, Self::Error> {
                Ok(EndpointRequestInput::new(
                    serde_json::to_value(
                        Box::new($crate::request::RedisRequest::new(Box::new(self))).as_request()
                    ).map_err(::error::EpError::serde)?
                ))
            }
        }

        impl crate::OperationKind<$crate::api::lib::RedisApi> for $type {
            fn operation_kind() -> $crate::api::lib::RedisApi {
                $api_info.api
            }
        }

        impl $type {
            /// Returns the request type (Read or Write) for this command without requiring an instance.
            pub fn static_request_type() -> crate::ReqType {
                $api_info.request_type
            }

            /// Returns whether this command can safely use a shared backend connection.
            pub fn safe() -> bool {
                $api_info.safe()
            }
        }

        impl crate::Operation<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx> for $type {
            fn kind(&self) -> $crate::api::lib::RedisApi {
                $api_info.api
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn request_type(&self) -> crate::ReqType {
                $api_info.request_type
            }

            fn as_operation(self: Box<Self>) -> Box<dyn crate::Operation<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx>> {
                self
            }

            fn as_exec(&self) -> Option<&dyn crate::OperationExecutor<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx>> {
                Some(self)
            }

            fn clone_box(&self) -> Box<dyn crate::Operation<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx>> {
                Box::new(self.clone())
            }
        }

        impl crate::OperationExecutor<$crate::ep::RedisAsync, $crate::api::lib::RedisApi, $crate::ep::RedisTx> for $type {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn run_operation_request(&self, context: $crate::ep::RedisAsync, mut telemetry_wrapper: telemetry::TelemetryWrapper) -> crate::RunOutput<'_> {
                Box::pin(async move {
                    self.run_async_generic(context, &mut telemetry_wrapper).await
                })
            }

             fn run_operation_transaction(&self, tx_context: &mut $crate::ep::RedisTx, telemetry_wrapper: &mut telemetry::TelemetryWrapper) {
                self.run_transaction_generic(tx_context, telemetry_wrapper);
            }
        }

        #[allow(deprecated)]
        impl borsh::BorshDeserialize for $type {
            fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
                Ok(Self {
                    $($field: borsh::BorshDeserialize::deserialize(buf)?),+
                })
            }

            fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer)?;
                borsh::BorshDeserialize::deserialize(&mut buffer.as_slice())
            }
        }

        #[allow(deprecated)]
        impl borsh::BorshSerialize for $type {
            fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
                $(borsh::BorshSerialize::serialize(&self.$field, writer)?;)+
                Ok(())
            }
        }

        #[ctor::ctor]
        #[allow(non_snake_case)]
        #[allow(dead_code)]
        fn __register_redis_operation() {
            // Register operation using the string-based registry
            $crate::serde::register_operation::<$type>();
        }
    };
}

#[macro_export]
macro_rules! redis_api_commands {
    (
        $(
            $(#[$attr:meta])?
            ($variant:ident, $str_rep:expr $(, $input_type:ident)?)
        ),* $(,)?
    ) => {
        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, DocumentAPI, ApiBuilder)]
        #[api_builder(builder_name = "RedisApiBuilder")]
        pub enum RedisApi {
            $(
                $(#[$attr])?
                $variant,
            )*
        }

        impl Display for RedisApi {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl TryFrom<&str> for RedisApi {
            type Error = ::error::EpError;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                Self::try_from_case_insensitive(s)
            }
        }

        impl TryFrom<&[u8]> for RedisApi {
            type Error = ::error::EpError;
            fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
                Self::try_from_case_insensitive_bytes(bytes)
            }
        }

        impl RedisApi {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $str_rep,
                    )*
                }
            }

            #[inline]
            fn try_hot_from_case_insensitive_bytes(bytes: &[u8]) -> Option<Self> {
                match bytes.len() {
                    3 if bytes.eq_ignore_ascii_case(b"GET") => Some(Self::Get),
                    3 if bytes.eq_ignore_ascii_case(b"SET") => Some(Self::Set),
                    3 if bytes.eq_ignore_ascii_case(b"DEL") => Some(Self::Del),
                    3 if bytes.eq_ignore_ascii_case(b"TTL") => Some(Self::Ttl),
                    4 if bytes.eq_ignore_ascii_case(b"MGET") => Some(Self::Mget),
                    4 if bytes.eq_ignore_ascii_case(b"HGET") => Some(Self::Hget),
                    4 if bytes.eq_ignore_ascii_case(b"HSET") => Some(Self::Hset),
                    4 if bytes.eq_ignore_ascii_case(b"INCR") => Some(Self::Incr),
                    4 if bytes.eq_ignore_ascii_case(b"DECR") => Some(Self::Decr),
                    4 if bytes.eq_ignore_ascii_case(b"PING") => Some(Self::Ping),
                    4 if bytes.eq_ignore_ascii_case(b"SADD") => Some(Self::Sadd),
                    4 if bytes.eq_ignore_ascii_case(b"ZADD") => Some(Self::Zadd),
                    5 if bytes.eq_ignore_ascii_case(b"LPUSH") => Some(Self::Lpush),
                    5 if bytes.eq_ignore_ascii_case(b"RPUSH") => Some(Self::Rpush),
                    5 if bytes.eq_ignore_ascii_case(b"EXISTS") => Some(Self::Exists),
                    6 if bytes.eq_ignore_ascii_case(b"EXPIRE") => Some(Self::Expire),
                    6 if bytes.eq_ignore_ascii_case(b"LRANGE") => Some(Self::Lrange),
                    _ => None,
                }
            }

            pub fn try_from_case_insensitive(s: &str) -> Result<Self, ::error::EpError> {
                Self::try_from_case_insensitive_bytes(s.as_bytes())
            }

            pub fn try_from_case_insensitive_bytes(bytes: &[u8]) -> Result<Self, ::error::EpError> {
                if let Some(api) = Self::try_hot_from_case_insensitive_bytes(bytes) {
                    return Ok(api);
                }

                match bytes.len() {
                    $(
                        len if len == $str_rep.len() && bytes.eq_ignore_ascii_case($str_rep.as_bytes()) => Ok(Self::$variant),
                    )*
                    _ => Err(::error::EpError::parse(format!(
                        "failed to parse RedisAPI: {}",
                        String::from_utf8_lossy(bytes)
                    ))),
                }
            }

            pub fn try_from_command_words(command: &str, subcommand: Option<&str>) -> Result<(Self, usize), ::error::EpError> {
                Self::try_from_command_words_bytes(command.as_bytes(), subcommand.map(str::as_bytes))
            }

            pub fn try_from_command_words_bytes(command: &[u8], subcommand: Option<&[u8]>) -> Result<(Self, usize), ::error::EpError> {
                if let Some(api) = Self::try_hot_from_case_insensitive_bytes(command) {
                    return Ok((api, 1));
                }

                if let Some(subcommand) = subcommand {
                    $(
                        if Self::command_words_match($str_rep.as_bytes(), command, subcommand) {
                            return Ok((Self::$variant, 2));
                        }
                    )*
                }

                if let Ok(api) = Self::try_from_case_insensitive_bytes(command) {
                    return Ok((api, 1));
                }

                Err(::error::EpError::parse(format!(
                    "failed to parse RedisAPI: {}",
                    String::from_utf8_lossy(command)
                )))
            }

            #[inline]
            fn command_words_match(candidate: &[u8], command: &[u8], subcommand: &[u8]) -> bool {
                let Some(space) = candidate.iter().position(|byte| *byte == b' ') else {
                    return false;
                };
                command.eq_ignore_ascii_case(&candidate[..space]) && subcommand.eq_ignore_ascii_case(&candidate[space + 1..])
            }

            pub fn name() -> String {
                "RedisApi".to_string()
            }

            pub fn db_kind() -> String {
                "redis".to_string()
            }

            #[allow(deprecated)]
            pub fn keys_from_args(&self, args: &[RedisJsonValue]) -> ResultEP<Vec<RedisKey>> {
                Ok(match self {
                    $(
                        $(
                            RedisApi::$variant => $input_type::decode(args.to_vec())?.keys(),
                        )?
                    )*
                    #[allow(unreachable_patterns)]
                    _ => return Err(::error::EpError::parse("Command not implemented for parsing")),
                })
            }

            #[allow(deprecated)]
            pub fn decode_from_args(
                &self,
                args: Vec<RedisJsonValue>,
            ) -> ResultEP<Box<dyn RedisOperation>> {
                Ok(match self {
                    $(
                        $(
                            RedisApi::$variant => Box::new($input_type::decode(args)?),
                        )?
                    )*
                    #[allow(unreachable_patterns)]
                    _ => return Err(::error::EpError::parse("Command not implemented for parsing")),
                })
            }

            /// Returns whether this command is a read or write operation.
            #[allow(deprecated)]
            pub fn request_type(&self) -> crate::ReqType {
                match self {
                    $($(RedisApi::$variant => $input_type::static_request_type(),)?)*
                    #[allow(unreachable_patterns)]
                    _ => crate::ReqType::Write, // Default for safety (commands without input types)
                }
            }

            /// Returns whether this command can safely use a shared
            /// backend connection in the direct Redis lane pool.
            #[allow(deprecated)]
            pub fn safe(&self) -> bool {
                match self {
                    $($(RedisApi::$variant => $input_type::safe(),)?)*
                    #[allow(unreachable_patterns)]
                    _ => false,
                }
            }
        }
    };
}
