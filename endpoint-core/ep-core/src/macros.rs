#[macro_export]
macro_rules! impl_endpoint {
    ($kind:expr => $variant:ident, $async:ty) => {
        $crate::paste::paste! {
            #[derive(Debug, Clone, Default)]
            pub struct [<$variant Ep>](pub EpPool<$async>);

            impl GetPool<$async> for [<$variant Ep>] {
                fn pool(&self) -> &EpPool<$async> {
                    &self.0
                }
                fn mut_pool(&mut self) -> &mut EpPool<$async> {
                    &mut self.0
                }
            }

            impl EpRouter for [<$variant Ep>] {
                fn as_router(self: Box<Self>) -> Box<dyn EpRouter> {
                    self
                }
                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }
                fn any_mut(&mut self) -> &mut dyn std::any::Any {
                    self
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_endpoint_lifecycle_spec {
    ($ep:ty, $async:ty, $config:ty, $request:ty, $metadata:ty, $api:ty, $tx:ty) => {
        impl ::endpoint_types::EpLifecycleSpec for $ep {
            type Async = $async;
            type Config = $config;
            type Request = $request;
            type Metadata = $metadata;
            type Api = $api;
            type Tx = $tx;
        }
    };
}

#[macro_export]
macro_rules! define_request {
    ($kind:expr => $variant:ident, $trait:ty, $async:ty, $api_kind:ty, $tx:ty) => {
        $crate::paste::paste! {
            #[derive(Debug, serde::Serialize, serde::Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize)]
            pub struct [<$variant Request>](pub Box<dyn $trait>);

            // Use `::endpoint_types::` absolute paths instead of `crate::` in
            // #[macro_export] macros. `crate::` resolves to the invoking crate, which
            // relies on a glob re-export (`pub use endpoint_types::*`). clippy --fix
            // cannot trace `crate::` usage through macro expansions and will
            // incorrectly remove traits from the glob, breaking compilation.
            impl ::endpoint_types::RequestConstructor for [<$variant Request>] {
                // Define the associated types
                type AsyncType = $async;
                type ApiKindType = $api_kind;
                type TxType = $tx;
                type OperationType = dyn $trait;

                // Use the associated types in the implementation
                fn new(op: Box<Self::OperationType>) -> Self {
                    Self(op)
                }
            }

            impl $crate::EndpointType for [<$variant Request>] {
                fn r#type() -> EpKind {
                    $kind
                }
            }

            impl ::endpoint_types::RunRequest<$async, $api_kind, $tx> for [<$variant Request>] {
                fn operation(&self) -> &dyn Operation<$async, $api_kind, $tx> {
                    &*self.0
                }
            }

            impl EpRequest for [<$variant Request>] {
                fn kind(&self) -> EpKind {
                    $kind
                }

                fn as_request(self: Box<Self>) -> Box<dyn EpRequest> {
                    self
                }

                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }

                fn to_value(&self) -> $crate::serde_json::Result<$crate::serde_json::Value> {
                    $crate::serde_json::to_value(self)
                }

                fn borsh_serialize(&self, writer: &mut dyn ::std::io::Write) -> ::std::io::Result<()> {
                    $crate::borsh::to_writer(writer, self)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! define_request_serializer_stuff {
    ($epkind:ident :: $variant:ident => $reqtype:ty) => {
        #[::linkme::distributed_slice(::endpoint_types::request::REQUEST_SERIALIZERS)]
        static MY_REQUEST_SERIALIZER: (EpKind, fn(&Box<dyn ::endpoint_types::EpRequest>) -> Result<::serde_json::Value, Box<dyn ::std::error::Error>>) = ($epkind::$variant, my_request_serializer);

        fn my_request_serializer(req: &Box<dyn ::endpoint_types::EpRequest>) -> Result<::serde_json::Value, Box<dyn ::std::error::Error>> {
            let req = req.as_any().downcast_ref::<$reqtype>().ok_or(concat!("failed to downcast to ", stringify!($reqtype)))?;
            ::serde_json::to_value(req).map_err(|e| Box::new(e) as Box<dyn ::std::error::Error>)
        }

        #[::linkme::distributed_slice(::endpoint_types::request::REQUEST_BORSH_SERIALIZERS)]
        static MY_REQUEST_BORSH_SERIALIZER: (EpKind, fn(&Box<dyn ::endpoint_types::EpRequest>, &mut dyn ::std::io::Write) -> ::std::io::Result<()>) = ($epkind::$variant, my_request_borsh_serializer);

        fn my_request_borsh_serializer(req: &Box<dyn ::endpoint_types::EpRequest>, write: &mut dyn ::std::io::Write) -> ::std::io::Result<()> {
            let req = req.as_any().downcast_ref::<$reqtype>().ok_or_else(|| ::std::io::Error::new(::std::io::ErrorKind::InvalidInput, concat!("failed to downcast to ", stringify!($reqtype))))?;
            ::borsh::to_writer(write, req)
        }

        #[::linkme::distributed_slice(::endpoint_types::request::REQUEST_DESERIALIZERS)]
        static MY_REQUEST_DESERIALIZER: (EpKind, fn(::serde_json::Value) -> Result<Box<dyn ::endpoint_types::EpRequest>, Box<dyn ::std::error::Error>>) = ($epkind::$variant, my_request_deserializer);

        fn my_request_deserializer(value: serde_json::Value) -> Result<Box<dyn ::endpoint_types::EpRequest>, Box<dyn ::std::error::Error>> {
            let req: $reqtype = ::serde_json::from_value(value)?;
            Ok(Box::new(req))
        }

        #[::linkme::distributed_slice(::endpoint_types::request::REQUEST_BORSH_DESERIALIZERS)]
        static MY_REQUEST_BORSH_DESERIALIZER: (EpKind, fn(&mut dyn ::std::io::Read) -> ::std::io::Result<Box<dyn ::endpoint_types::EpRequest>>) = ($epkind::$variant, my_request_borsh_deserializer);

        fn my_request_borsh_deserializer(read: &mut dyn ::std::io::Read) -> ::std::io::Result<Box<dyn ::endpoint_types::EpRequest>> {
            struct ReadHelper<'a>(&'a mut dyn ::std::io::Read);

            impl<'a> ::std::io::Read for ReadHelper<'a> {
                fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
                    self.0.read(buf)
                }
            }

            let req: $reqtype = ::borsh::from_reader(&mut ReadHelper(read))?;
            Ok(Box::new(req))
        }

        $crate::define_transaction_serializer_stuff!($epkind :: $variant => ::endpoint_types::transaction::Transaction<$reqtype>);
    }
}

#[macro_export]
macro_rules! define_metadata_serializer_stuff {
    ($epkind:ident :: $variant:ident => $metadatatype:ty) => {
        #[::linkme::distributed_slice(::endpoint_types::metadata::METADATA_DESERIALIZERS)]
        static MY_METADATA_DESERIALIZER: (
            EpKind,
            fn(::serde_json::Value) -> Result<Box<dyn ::endpoint_types::metadata::EpMetadata>, Box<dyn ::std::error::Error>>,
        ) = ($epkind::$variant, my_metadata_deserializer);

        fn my_metadata_deserializer(
            value: serde_json::Value,
        ) -> Result<Box<dyn ::endpoint_types::metadata::EpMetadata>, Box<dyn ::std::error::Error>> {
            let req: $metadatatype = ::serde_json::from_value(value)?;
            Ok(Box::new(req))
        }

        #[::linkme::distributed_slice(::endpoint_types::metadata::METADATA_BORSH_DESERIALIZERS)]
        static MY_METADATA_BORSH_DESERIALIZER: (
            EpKind,
            fn(&mut dyn ::std::io::Read) -> ::std::io::Result<Box<dyn ::endpoint_types::metadata::EpMetadata>>,
        ) = ($epkind::$variant, my_metadata_borsh_deserializer);

        fn my_metadata_borsh_deserializer(
            read: &mut dyn ::std::io::Read,
        ) -> ::std::io::Result<Box<dyn ::endpoint_types::metadata::EpMetadata>> {
            struct ReadHelper<'a>(&'a mut dyn ::std::io::Read);

            impl<'a> ::std::io::Read for ReadHelper<'a> {
                fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
                    self.0.read(buf)
                }
            }

            let req: $metadatatype = ::borsh::from_reader(&mut ReadHelper(read))?;
            Ok(Box::new(req))
        }
    };
}

#[macro_export]
macro_rules! define_transaction_serializer_stuff {
    ($epkind:ident :: $variant:ident => $transactiontype:ty) => {
        #[::linkme::distributed_slice(::endpoint_types::transaction::TRANSACTION_DESERIALIZERS)]
        static MY_TRANSACTION_DESERIALIZER: (
            EpKind,
            fn(::serde_json::Value) -> Result<Box<dyn ::endpoint_types::EpTransaction>, Box<dyn ::std::error::Error>>,
        ) = ($epkind::$variant, my_transaction_deserializer);

        fn my_transaction_deserializer(
            value: serde_json::Value,
        ) -> Result<Box<dyn ::endpoint_types::EpTransaction>, Box<dyn ::std::error::Error>> {
            let req: $transactiontype = ::serde_json::from_value(value)?;
            Ok(Box::new(req))
        }

        #[::linkme::distributed_slice(::endpoint_types::transaction::TRANSACTION_BORSH_DESERIALIZERS)]
        static MY_TRANSACTION_BORSH_DESERIALIZER: (
            EpKind,
            fn(&mut dyn ::std::io::Read) -> ::std::io::Result<Box<dyn ::endpoint_types::EpTransaction>>,
        ) = ($epkind::$variant, my_transaction_borsh_deserializer);

        fn my_transaction_borsh_deserializer(
            read: &mut dyn ::std::io::Read,
        ) -> ::std::io::Result<Box<dyn ::endpoint_types::EpTransaction>> {
            struct ReadHelper<'a>(&'a mut dyn ::std::io::Read);

            impl<'a> ::std::io::Read for ReadHelper<'a> {
                fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
                    self.0.read(buf)
                }
            }

            let req: $transactiontype = ::borsh::from_reader(&mut ReadHelper(read))?;
            Ok(Box::new(req))
        }
    };
}

#[macro_export]
macro_rules! impl_simple_operation {
    ($simple_input_type:ty, $async_ctx:ty, $tx_ctx:ty, $kind_type:ty, $request_type:ty) => {
        use ::endpoint_types::{
            Operation, RequestConstructor,
            request::{EndpointRequestInput, EpRequest},
        };
        use $crate::database::schema::endpoint::EpRequestWrapper;

        impl $crate::borsh::BorshDeserialize for $simple_input_type {
            fn deserialize(buf: &mut &[u8]) -> ::std::io::Result<Self> {
                $crate::serde_json::from_slice(buf).map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::InvalidData, e.to_string()))
            }
            fn deserialize_reader<R: $crate::borsh::io::Read>(reader: &mut R) -> ::std::io::Result<Self> {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer)?;
                $crate::borsh::BorshDeserialize::deserialize(&mut buffer.as_slice())
            }
        }

        impl TryInto<EndpointRequestInput> for $simple_input_type {
            type Error = EpError;
            fn try_into(self) -> Result<EndpointRequestInput, Self::Error> {
                Ok(EndpointRequestInput::new(
                    $crate::serde_json::to_value(Box::new(<$request_type>::new(Box::new(self))).as_request()).map_err(EpError::serde)?,
                ))
            }
        }
        //
        // impl TryInto<EndpointRequestInput> for $simple_input_type {
        //     type Error = EpError;
        //     fn try_into(self) -> Result<EndpointRequestInput, Self::Error> {
        //         Ok(EndpointRequestInput::new(
        //             serde_json::to_value(Box::new($request_type::new(Box::new(self).as_operation())).as_request()).map_err(EpError::serde)?
        //         ))
        //     }
        // }

        impl ::endpoint_types::OperationKind<$kind_type> for $simple_input_type {
            fn operation_kind() -> $kind_type {
                API_INFO.api
            }
        }

        impl ::endpoint_types::Operation<$async_ctx, $kind_type, $tx_ctx> for $simple_input_type {
            fn kind(&self) -> $kind_type {
                API_INFO.api
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn request_type(&self) -> ReqType {
                API_INFO.request_type
            }

            fn as_operation(self: Box<Self>) -> Box<dyn ::endpoint_types::Operation<$async_ctx, $kind_type, $tx_ctx>> {
                self
            }

            fn as_exec(&self) -> Option<&dyn ::endpoint_types::OperationExecutor<$async_ctx, $kind_type, $tx_ctx>> {
                Some(self)
            }

            fn clone_box(&self) -> Box<dyn ::endpoint_types::Operation<$async_ctx, $kind_type, $tx_ctx>> {
                Box::new(self.clone())
            }
        }

        impl ::endpoint_types::OperationExecutor<$async_ctx, $kind_type, $tx_ctx> for $simple_input_type {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn run_operation_request(&self, context: $async_ctx, mut telemetry_wrapper: TelemetryWrapper) -> RunOutput {
                Box::pin(async move {
                    use $crate::telemetry::guards::EndpointGuard;

                    let labels = telemetry_wrapper.labels_low_cardinality();
                    let labels_refs: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    let metrics = telemetry_wrapper.metrics().clone();

                    let _endpoint_guard = EndpointGuard::new(&metrics.endpoint(), &labels_refs);

                    let output = self.run_async_generic(context, &mut telemetry_wrapper).await;

                    output
                })
            }

            fn run_operation_transaction(&self, tx_context: &mut $tx_ctx, telemetry_wrapper: &mut TelemetryWrapper) {
                let t0 = $crate::chrono::Utc::now();

                // let labels = telemetry_wrapper.labels().await;

                // telemetry_wrapper
                //     .mut_metrics(|metrics| metrics.endpoint().start_endpoint_request(&[]));

                self.run_transaction_generic(tx_context, telemetry_wrapper);

                // telemetry_wrapper
                //     .mut_durations(|metrics, durations| {
                //         metrics
                //             .endpoint()
                //             .finish_endpoint_request(durations, t0, &[])
                //     })
                //     .await;
            }
        }
    };
}
//
// #[macro_export]
// macro_rules! impl_complex_operation {
//     ($complex_input_type:ty, $expected_input_type:ty, $sync_ctx:ty, $async_ctx:ty, $kind_type:ty, $kind:expr) => {
//         impl borsh::BorshDeserialize for $complex_input_type {
//             fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
//                 serde_json::from_slice(buf).map_err(|e| {
//                     std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
//                 })
//             }
//             fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
//                 let mut buffer = Vec::new();
//                 reader.read_to_end(&mut buffer)?;
//                 borsh::BorshDeserialize::deserialize(&mut buffer.as_slice())
//             }
//         }
//
//         impl OperationKind<$kind_type> for $complex_input_type {
//             fn operation_kind() -> $kind_type {
//                 $kind
//             }
//         }
//
//         impl Operation<$sync_ctx, $async_ctx, $kind_type> for $complex_input_type {
//             fn as_any(&self) -> &dyn Any {
//                 self
//             }
//
//             fn kind(&self) -> $kind_type {
//                 $kind
//             }
//
//             fn as_operation(
//                 self: Box<Self>,
//             ) -> Box<dyn Operation<$sync_ctx, $async_ctx, $kind_type>> {
//                 self
//             }
//
//             fn as_exec(&self) -> Option<&dyn OperationExecutor<$sync_ctx, $async_ctx, $kind_type>> {
//                 None
//             }
//
//             fn clone_box(&self) -> Box<dyn Operation<$sync_ctx, $async_ctx, $kind_type>> {
//                 Box::new(self.clone())
//             }
//         }
//
//         impl<'a> ComplexExecutor<'a, $expected_input_type, $sync_ctx, $async_ctx, $kind_type>
//             for $complex_input_type
//         {
//             fn as_any(&self) -> &dyn Any {
//                 self
//             }
//
//             fn run_sync(
//                 &self,
//                 input: &'a Box<dyn EpOutput>,
//                 telemetry_wrapper: &mut TelemetryWrapper,
//             ) -> RunOutput {
//                 self.run_sync_generic(input, telemetry_wrapper)
//             }
//
//             fn run_async(
//                 &self,
//                 input: &'a Box<dyn EpOutput>,
//                 telemetry_wrapper: &mut TelemetryWrapper,
//             ) -> RunOutput {
//                 self.run_async_generic(input, telemetry_wrapper)
//             }
//
//             fn downcast(input: &'a Box<dyn EpOutput>) -> ResultEP<&'a $expected_input_type> {
//                 let any_ref = input.as_any();
//                 match any_ref.downcast_ref::<$expected_input_type>() {
//                     Some(r) => Ok(r),
//                     None => Err(EpError::database(format!(
//                         "failed to downcast input for {}",
//                         stringify!($kind)
//                     ))),
//                 }
//             }
//         }
//     };
// }

#[macro_export]
macro_rules! impl_connection {
    ($conn_type:ty, $kind:expr) => {
        impl EpConnection for $conn_type {
            fn as_connection(self: Box<Self>) -> Box<dyn EpConnection> {
                self
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn kind(&self) -> EpKind {
                $kind
            }

            fn clone_box(&self) -> Box<dyn EpConnection> {
                Box::new(self.clone())
            }
        }
    };
}

#[macro_export]
macro_rules! impl_ep_config_generic {
    ($config_type:ty, $conn_type:ty, $kind:expr) => {
        impl EpConfig for $config_type {
            fn as_config(&self) -> Box<dyn EpConfig> {
                Box::new(self.to_owned())
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn kind(&self) -> EpKind {
                $kind
            }

            fn clone_box(&self) -> Box<dyn EpConfig> {
                Box::new(self.clone())
            }

            fn read_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.read_conn.as_ref().map(|conn| Box::new(conn.clone()) as Box<dyn EpConnection>)
            }

            fn write_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.write_conn.as_ref().map(|conn| Box::new(conn.clone()) as Box<dyn EpConnection>)
            }

            fn admin_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.admin_conn.as_ref().map(|conn| Box::new(conn.clone()) as Box<dyn EpConnection>)
            }

            fn system_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.system_conn.as_ref().map(|conn| Box::new(conn.clone()) as Box<dyn EpConnection>)
            }

            fn update_read_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        self.read_conn.replace(conn.to_owned());
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast read connection for {}", stringify!($config_type)))),
                }
            }

            fn update_write_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        self.write_conn.replace(conn.to_owned());
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast write connection for {}", stringify!($config_type)))),
                }
            }

            fn update_admin_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        self.admin_conn.replace(conn.to_owned());
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast admin connection for {}", stringify!($config_type)))),
                }
            }

            fn update_system_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        self.system_conn.replace(conn.to_owned());
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast system connection for {}", stringify!($config_type)))),
                }
            }

            fn serialize(&self) -> ResultEP<serde_json::Value> {
                serde_json::to_value(self).map_err(EpError::serde)
            }
        }

        impl $crate::postgres::types::ToSql for $config_type {
            fn to_sql(
                &self,
                ty: &$crate::postgres::types::Type,
                out: &mut $crate::bytes::BytesMut,
            ) -> Result<$crate::postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
            where
                Self: Sized,
            {
                // Serialize the struct to JSON or bytes and write to the buffer
                let serialized = serde_json::to_vec(self).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;

                out.extend_from_slice(&serialized);
                Ok($crate::postgres::types::IsNull::No)
            }

            fn accepts(ty: &$crate::postgres::types::Type) -> bool
            where
                Self: Sized,
            {
                // Accept JSONB, JSON, or BYTEA types
                matches!(
                    ty,
                    &$crate::postgres::types::Type::JSONB | &$crate::postgres::types::Type::JSON | &$crate::postgres::types::Type::BYTEA
                )
            }

            fn to_sql_checked(
                &self,
                ty: &$crate::postgres::types::Type,
                out: &mut $crate::bytes::BytesMut,
            ) -> Result<$crate::postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                // Check if the type is acceptable first
                if !Self::accepts(ty) {
                    return Err(format!("Type {:?} not supported", ty).into());
                }

                match ty {
                    &$crate::postgres::types::Type::JSONB | &$crate::postgres::types::Type::JSON => {
                        // Serialize as JSON for JSON/JSONB types
                        let json_str = serde_json::to_string(self).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
                        out.extend_from_slice(json_str.as_bytes());
                        Ok($crate::postgres::types::IsNull::No)
                    }
                    &$crate::postgres::types::Type::BYTEA => {
                        // For BYTEA, you could use a binary format like borsh or bincode
                        let serialized = borsh::to_vec(self).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
                        out.extend_from_slice(&serialized);
                        Ok($crate::postgres::types::IsNull::No)
                    }
                    _ => Err("Unsupported type".into()),
                }
            }
        }
    };
}

/// Generate an `EpConfig` implementation for a config struct that uses the
/// **target + credentials** model (one shared connection target with per-tier
/// credentials) instead of the legacy 4-independent-connections model.
///
/// The config struct must have fields:
/// - `target: $target_type`
/// - `read_credentials: Option<$creds_type>`
/// - `write_credentials: Option<$creds_type>`
/// - `admin_credentials: Option<$creds_type>`
/// - `system_credentials: Option<$creds_type>`
///
/// The `$conn_type` must implement:
/// - `from_target_and_credentials(target: &$target_type, creds: &$creds_type) -> Self`
/// - `split(&self) -> ResultEP<($target_type, $creds_type)>`
#[macro_export]
macro_rules! impl_ep_config_target_auth {
    ($config_type:ty, $conn_type:ty, $target_type:ty, $creds_type:ty, $kind:expr) => {
        impl EpConfig for $config_type {
            fn as_config(&self) -> Box<dyn EpConfig> {
                Box::new(self.to_owned())
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn kind(&self) -> EpKind {
                $kind
            }

            fn clone_box(&self) -> Box<dyn EpConfig> {
                Box::new(self.clone())
            }

            fn read_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.read_credentials
                    .as_ref()
                    .map(|creds| Box::new(<$conn_type>::from_target_and_credentials(&self.target, creds)) as Box<dyn EpConnection>)
            }

            fn write_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.write_credentials
                    .as_ref()
                    .map(|creds| Box::new(<$conn_type>::from_target_and_credentials(&self.target, creds)) as Box<dyn EpConnection>)
            }

            fn admin_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.admin_credentials
                    .as_ref()
                    .map(|creds| Box::new(<$conn_type>::from_target_and_credentials(&self.target, creds)) as Box<dyn EpConnection>)
            }

            fn system_conn(&self) -> Option<Box<dyn EpConnection>> {
                self.system_credentials
                    .as_ref()
                    .map(|creds| Box::new(<$conn_type>::from_target_and_credentials(&self.target, creds)) as Box<dyn EpConnection>)
            }

            fn update_read_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        let (target, creds) = conn.split()?;
                        if target != self.target {
                            return Err(EpError::connect("provider/config mismatch on connection update; targets must match"));
                        }
                        self.read_credentials.replace(creds);
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast read connection for {}", stringify!($config_type)))),
                }
            }

            fn update_write_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        let (target, creds) = conn.split()?;
                        if target != self.target {
                            return Err(EpError::connect("provider/config mismatch on connection update; targets must match"));
                        }
                        self.write_credentials.replace(creds);
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast write connection for {}", stringify!($config_type)))),
                }
            }

            fn update_admin_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        let (target, creds) = conn.split()?;
                        if target != self.target {
                            return Err(EpError::connect("provider/config mismatch on connection update; targets must match"));
                        }
                        self.admin_credentials.replace(creds);
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast admin connection for {}", stringify!($config_type)))),
                }
            }

            fn update_system_conn(&mut self, conn: Box<dyn EpConnection>) -> ResultEP<()> {
                match conn.as_any().downcast_ref::<$conn_type>() {
                    Some(conn) => {
                        let (target, creds) = conn.split()?;
                        if target != self.target {
                            return Err(EpError::connect("provider/config mismatch on connection update; targets must match"));
                        }
                        self.system_credentials.replace(creds);
                        Ok(())
                    }
                    None => Err(EpError::connect(format!("failed to downcast system connection for {}", stringify!($config_type)))),
                }
            }

            fn serialize(&self) -> ResultEP<serde_json::Value> {
                serde_json::to_value(self).map_err(EpError::serde)
            }

            fn connection_with_auth(
                &self,
                _tier: $crate::ep::ConnectionTier,
                auth: &dyn $crate::ep_auth::EpAuth,
            ) -> Option<Box<dyn EpConnection>> {
                // Try direct downcast first (if caller passes credentials type directly).
                if let Some(creds) = auth.as_any().downcast_ref::<$creds_type>() {
                    return Some(Box::new(<$conn_type>::from_target_and_credentials(&self.target, creds)) as Box<dyn EpConnection>);
                }

                // Fall back to JSON round-trip: serialize the ELS auth, then
                // deserialize as the endpoint's credentials type. Works because
                // ELS auth types (e.g. MysqlAuth) and credentials types (e.g.
                // MysqlCredentials) share the same field structure.
                let json = auth.to_json().ok()?;
                let creds: $creds_type = serde_json::from_value(json).ok()?;
                Some(Box::new(<$conn_type>::from_target_and_credentials(&self.target, &creds)) as Box<dyn EpConnection>)
            }
        }

        impl $crate::postgres::types::ToSql for $config_type {
            fn to_sql(
                &self,
                ty: &$crate::postgres::types::Type,
                out: &mut $crate::bytes::BytesMut,
            ) -> Result<$crate::postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
            where
                Self: Sized,
            {
                let serialized = serde_json::to_vec(self).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
                out.extend_from_slice(&serialized);
                Ok($crate::postgres::types::IsNull::No)
            }

            fn accepts(ty: &$crate::postgres::types::Type) -> bool
            where
                Self: Sized,
            {
                matches!(
                    ty,
                    &$crate::postgres::types::Type::JSONB | &$crate::postgres::types::Type::JSON | &$crate::postgres::types::Type::BYTEA
                )
            }

            fn to_sql_checked(
                &self,
                ty: &$crate::postgres::types::Type,
                out: &mut $crate::bytes::BytesMut,
            ) -> Result<$crate::postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                if !Self::accepts(ty) {
                    return Err(format!("Type {:?} not supported", ty).into());
                }

                match ty {
                    &$crate::postgres::types::Type::JSONB | &$crate::postgres::types::Type::JSON => {
                        let json_str = serde_json::to_string(self).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
                        out.extend_from_slice(json_str.as_bytes());
                        Ok($crate::postgres::types::IsNull::No)
                    }
                    &$crate::postgres::types::Type::BYTEA => {
                        let serialized = borsh::to_vec(self).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
                        out.extend_from_slice(&serialized);
                        Ok($crate::postgres::types::IsNull::No)
                    }
                    _ => Err("Unsupported type".into()),
                }
            }
        }
    };
}

#[macro_export]
macro_rules! implement_operation_registry {
    ($trait:ident < $async:ty, $api:ty, $tx:ty >) => {
        pub trait $trait: ::endpoint_types::Operation<$async, $api, $tx> + 'static {}
        impl<T: ::endpoint_types::Operation<$async, $api, $tx> + 'static> $trait for T {}

        impl<T: $trait + 'static> From<T> for Box<dyn $trait> {
            fn from(value: T) -> Self {
                Box::new(value)
            }
        }

        $crate::lazy_static::lazy_static! {
            static ref SERIALIZERS: ::std::sync::RwLock<
                ::std::collections::HashMap<
                    String,
                    Box<dyn Fn(&dyn $trait) -> Result<$crate::serde_json::Value, $crate::serde_json::Error> + Send + Sync>,
                >,
            > = {
                let m = ::std::collections::HashMap::new();
                ::std::sync::RwLock::new(m)
            };
            static ref DESERIALIZERS: ::std::sync::RwLock<
                ::std::collections::HashMap<
                    String,
                    Box<dyn Fn($crate::serde_json::Value) -> Result<Box<dyn $trait>, $crate::serde_json::Error> + Send + Sync>,
                >,
            > = {
                let m = ::std::collections::HashMap::new();
                ::std::sync::RwLock::new(m)
            };
        }

        pub fn register_operation<T>()
        where
            T: ::endpoint_types::Operation<$async, $api, $tx>
                + ::endpoint_types::OperationKind<$api>
                + $crate::serde::Serialize
                + for<'de> $crate::serde::Deserialize<'de>
                + 'static,
        {
            let operation_type = T::operation_kind().to_string().to_lowercase();

            // Register deserializer
            if let Ok(mut deserializers) = DESERIALIZERS.write() {
                deserializers.insert(
                    operation_type.clone(),
                    Box::new(|value| $crate::serde_json::from_value::<T>(value).map(|op| Box::new(op) as Box<dyn $trait>)),
                );
            }

            // Register serializer
            if let Ok(mut serializers) = SERIALIZERS.write() {
                serializers.insert(
                    operation_type,
                    Box::new(|op: &dyn $trait| {
                        let concrete =
                            op.as_any().downcast_ref::<T>().ok_or_else(|| serde::ser::Error::custom("Failed to downcast operation"))?;
                        $crate::serde_json::to_value(concrete)
                    }),
                );
            }
        }

        impl $crate::serde::Serialize for Box<dyn $trait> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: $crate::serde::Serializer,
            {
                let operation_type = self.kind().to_string().to_lowercase();

                let serializers = SERIALIZERS.read().map_err(serde::ser::Error::custom)?;
                let serializer_fn = serializers
                    .get(&operation_type)
                    .ok_or_else(|| serde::ser::Error::custom(format!("Unknown operation type: {}", operation_type)))?;

                let value = serializer_fn(self.as_ref()).map_err(serde::ser::Error::custom)?;

                let wrapper = SerdeOperationWrapper { operation_type, data: value };

                wrapper.serialize(serializer)
            }
        }

        impl $crate::borsh::BorshSerialize for Box<dyn $trait> {
            fn serialize<W: $crate::borsh::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
                let operation_type = self.kind().to_string().to_lowercase();

                let serializers =
                    SERIALIZERS.read().map_err(|_| ::std::io::Error::new(::std::io::ErrorKind::Other, "Registry lock is poisoned!"))?;

                let serializer_fn = serializers.get(&operation_type).ok_or_else(|| {
                    ::std::io::Error::new(::std::io::ErrorKind::InvalidData, format!("Unknown operation type: {}", operation_type))
                })?;

                let value =
                    serializer_fn(self.as_ref()).map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::InvalidData, e.to_string()))?;

                let wrapper = BorshOperationWrapper { operation_type, data: OperationData::from(value) };

                wrapper.serialize(writer)
            }
        }

        impl borsh::BorshDeserialize for Box<dyn $trait> {
            fn deserialize(buf: &mut &[u8]) -> std::io::Result<Self> {
                let wrapper = <BorshOperationWrapper as $crate::borsh::BorshDeserialize>::deserialize(buf)?;
                let value: $crate::serde_json::Value = wrapper.data.try_into()?;
                let operation_type = wrapper.operation_type.to_string().to_lowercase();

                match DESERIALIZERS.read() {
                    Ok(deserializers) => {
                        if let Some(deserializer) = deserializers.get(&operation_type) {
                            deserializer(value).map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::InvalidData, e.to_string()))
                        } else {
                            Err(::std::io::Error::new(
                                ::std::io::ErrorKind::InvalidData,
                                format!("Unknown operation type: {}", operation_type),
                            ))
                        }
                    }
                    Err(_) => Err(::std::io::Error::new(::std::io::ErrorKind::Other, "Registry lock is poisoned!")),
                }
            }

            fn deserialize_reader<R: $crate::borsh::io::Read>(reader: &mut R) -> std::io::Result<Self> {
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer)?;
                $crate::borsh::BorshDeserialize::deserialize(&mut buffer.as_slice())
            }
        }

        impl<'de> $crate::serde::Deserialize<'de> for Box<dyn $trait> {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: $crate::serde::Deserializer<'de>,
            {
                let wrapper = SerdeOperationWrapper::deserialize(deserializer)?;
                let value: $crate::serde_json::Value = wrapper.data.try_into().map_err(serde::de::Error::custom)?;

                match DESERIALIZERS.read() {
                    Ok(deserializers) => {
                        if let Some(deserializer) = deserializers.get(&wrapper.operation_type.to_lowercase()) {
                            deserializer(value).map_err(serde::de::Error::custom)
                        } else {
                            Err($crate::serde::de::Error::custom(format!("Unknown operation type: {}", wrapper.operation_type)))
                        }
                    }
                    Err(_) => Err($crate::serde::de::Error::custom("Registry lock is poisoned!")),
                }
            }
        }
    };
}

#[macro_export]
macro_rules! define_operation_types {
    () => {
        #[derive($crate::serde::Serialize, $crate::serde::Deserialize, $crate::borsh::BorshSerialize, $crate::borsh::BorshDeserialize)]
        struct OperationData {
            raw_data: Vec<u8>,
        }

        impl From<$crate::serde_json::Value> for OperationData {
            fn from(value: $crate::serde_json::Value) -> Self {
                Self {
                    raw_data: $crate::serde_json::to_vec(&value).unwrap_or_default(),
                }
            }
        }

        impl TryInto<$crate::serde_json::Value> for OperationData {
            type Error = $crate::serde_json::Error;

            fn try_into(self) -> Result<$crate::serde_json::Value, Self::Error> {
                $crate::serde_json::from_slice(&self.raw_data)
            }
        }

        #[derive($crate::borsh::BorshSerialize, $crate::borsh::BorshDeserialize)]
        struct BorshOperationWrapper {
            operation_type: String,
            data: OperationData,
        }

        #[derive($crate::serde::Serialize, $crate::serde::Deserialize)]
        struct SerdeOperationWrapper {
            #[serde(rename = "type")]
            operation_type: String,
            #[serde(flatten)]
            data: $crate::serde_json::Value,
        }
    };
}
