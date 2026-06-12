// use crate::api::lib::RedisApi;
// use crate::api::traits::RedisData;
// use redis_core::{RedisAsync, RedisSync};
// use crate::output::RedisValueOutput;
// use crate::{EndpointOperation, EpOutput, Operation, OperationExecutor, OperationKind, RunOutput};
// use error::EpError;
// use function_name::named;
// use redis::{AsyncCommands, Commands};
// use serde::de::{DeserializeOwned, MapAccess, Visitor};
// use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
// use std::any::Any;
// use std::fmt;
// use std::fmt::Debug;
// use std::marker::PhantomData;
// use tonic::metadata::MetadataMap;
// use trace::client_tracer_config;
//
// const KIND: RedisApi = RedisApi::ZrangebylexLimit;
//
// type OutputWrapper = RedisValueOutput;
//
// pub trait Register {
//     fn register();
// }
//
// impl<KY, MN, MX> Register for ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + Serialize + DeserializeOwned + Debug,
//     MN: RedisData + Serialize + DeserializeOwned + Debug,
//     MX: RedisData + Serialize + DeserializeOwned + Debug,
// {
//     fn register() {
//         crate::serde::register_operation::<ZrangebylexLimitInput<KY, MN, MX>>();
//     }
// }
//
// #[ctor::ctor]
// fn register_zrangebylex_limit() {
//     ZrangebylexLimitInput::<String, String, String>::register();
// }
//
// #[derive(Debug, Clone, Default, derive_builder::Builder)]
// #[builder(setter(into))]
// pub struct ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData,
//     MN: RedisData,
//     MX: RedisData,
// {
//     key: KY,
//     min: MN,
//     max: MX,
//     offset: isize,
//     count: isize,
// }
//
// impl<KY, MN, MX> Serialize for ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + Serialize,
//     MN: RedisData + Serialize,
//     MX: RedisData + Serialize,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         use serde::ser::SerializeStruct;
//
//         let mut state = serializer.serialize_struct("ZrangebylexLimitInput", 5)?;
//
//         state.serialize_field("key", &self.key)?;
//         state.serialize_field("min", &self.min)?;
//         state.serialize_field("max", &self.max)?;
//         state.serialize_field("offset", &self.offset)?;
//         state.serialize_field("count", &self.count)?;
//
//         state.end()
//     }
// }
//
// impl<'de, KY, MN, MX> Deserialize<'de> for ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + DeserializeOwned,
//     MN: RedisData + DeserializeOwned,
//     MX: RedisData + DeserializeOwned,
// {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         #[derive(Deserialize)]
//         #[serde(field_identifier, rename_all = "lowercase")]
//         enum Field {
//             Key,
//             Min,
//             Max,
//             Offset,
//             Count,
//         }
//
//         struct VisitorImpl<KY, MN, MX> {
//             marker: PhantomData<(KY, MN, MX)>,
//         }
//
//         impl<'de, KY, MN, MX> Visitor<'de> for VisitorImpl<KY, MN, MX>
//         where
//             KY: RedisData + DeserializeOwned,
//             MN: RedisData + DeserializeOwned,
//             MX: RedisData + DeserializeOwned,
//         {
//             type Value = ZrangebylexLimitInput<KY, MN, MX>;
//
//             fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//                 formatter.write_str("struct ZrangebylexLimitInput")
//             }
//
//             fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
//             where
//                 V: MapAccess<'de>,
//             {
//                 let mut key = None;
//                 let mut min = None;
//                 let mut max = None;
//                 let mut offset = None;
//                 let mut count = None;
//
//                 while let Some(k) = map.next_key()? {
//                     match k {
//                         Field::Key => {
//                             if key.is_some() {
//                                 return Err(de::Error::duplicate_field("key"));
//                             }
//                             key = Some(map.next_value()?);
//                         }
//                         Field::Min => {
//                             if min.is_some() {
//                                 return Err(de::Error::duplicate_field("min"));
//                             }
//                             min = Some(map.next_value()?);
//                         }
//                         Field::Max => {
//                             if max.is_some() {
//                                 return Err(de::Error::duplicate_field("max"));
//                             }
//                             max = Some(map.next_value()?);
//                         }
//                         Field::Offset => {
//                             if offset.is_some() {
//                                 return Err(de::Error::duplicate_field("offset"));
//                             }
//                             offset = Some(map.next_value()?);
//                         }
//                         Field::Count => {
//                             if count.is_some() {
//                                 return Err(de::Error::duplicate_field("count"));
//                             }
//                             count = Some(map.next_value()?);
//                         }
//                     }
//                 }
//
//                 let key = key.ok_or_else(|| de::Error::missing_field("key"))?;
//                 let min = min.ok_or_else(|| de::Error::missing_field("min"))?;
//                 let max = max.ok_or_else(|| de::Error::missing_field("max"))?;
//                 let offset = offset.ok_or_else(|| de::Error::missing_field("offset"))?;
//                 let count = count.ok_or_else(|| de::Error::missing_field("count"))?;
//
//                 Ok(ZrangebylexLimitInput {
//                     key,
//                     min,
//                     max,
//                     offset,
//                     count,
//                 })
//             }
//         }
//
//         const FIELDS: &[&str] = &["key", "min", "max", "offset", "count"];
//         deserializer.deserialize_struct(
//             "ZrangebylexLimitInput",
//             FIELDS,
//             VisitorImpl {
//                 marker: PhantomData,
//             },
//         )
//     }
// }
//
// impl<KY, MN, MX> ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + DeserializeOwned + Debug,
//     MN: RedisData + DeserializeOwned + Debug,
//     MX: RedisData + DeserializeOwned + Debug,
// {
//     fn key(&self) -> &KY {
//         &self.key
//     }
//     fn min(&self) -> &MN {
//         &self.min
//     }
//     fn max(&self) -> &MX {
//         &self.max
//     }
//     fn offset(&self) -> isize {
//         self.offset
//     }
//     fn count(&self) -> isize {
//         self.count
//     }
// }
//
// impl<KY, MN, MX> EndpointOperation for ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + Debug,
//     MN: RedisData + Debug,
//     MX: RedisData + Debug,
// {
// }
//
// impl<KY, MN, MX> OperationKind<RedisApi> for ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + DeserializeOwned + Debug,
//     MN: RedisData + DeserializeOwned + Debug,
//     MX: RedisData + DeserializeOwned + Debug,
// {
//     fn operation_kind() -> RedisApi {
//         KIND
//     }
// }
//
// impl<KY, MN, MX> Operation<RedisSync, RedisAsync, RedisApi> for ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + DeserializeOwned + Debug,
//     MN: RedisData + DeserializeOwned + Debug,
//     MX: RedisData + DeserializeOwned + Debug,
// {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//     fn kind(&self) -> RedisApi {
//         KIND
//     }
//     fn as_operation(self: Box<Self>) -> Box<dyn Operation<RedisSync, RedisAsync, RedisApi>> {
//         self
//     }
//     fn as_exec(&self) -> Option<&dyn OperationExecutor<RedisSync, RedisAsync, RedisApi>> {
//         Some(self)
//     }
//     fn clone_box(&self) -> Box<dyn Operation<RedisSync, RedisAsync, RedisApi>> {
//         Box::new(self.clone())
//     }
// }
//
// impl<KY, MN, MX> ZrangebylexLimitInput<KY, MN, MX>
// where
//     KY: RedisData + DeserializeOwned + Clone + Debug,
//     MN: RedisData + DeserializeOwned + Clone + Debug,
//     MX: RedisData + DeserializeOwned + Clone + Debug,
// {
//     #[named]
//     fn run_sync_generic(&self, context: RedisSync, telemetry_context: TelemetryWrapper) -> RunOutput {
//         let mut span = client_tracer_config(
//             format!("redis.{}.{}", self.kind(), function_name!()),
//             &metadata_map,
//         );
//
//         Box::pin(async move {
//             let context = context
//                 .get()
//                 .await
//                 .map_err(|e| EpError::request_with_span(e, &mut span))?;
//
//             let input = self.clone();
//
//             let result = context
//                 .interact(move |client| {
//                     client
//                         .zrangebylex_limit(
//                             input.key(),
//                             input.min(),
//                             input.max(),
//                             input.offset(),
//                             input.count(),
//                         )
//                         .map_err(EpError::request)
//                 })
//                 .await
//                 .map_err(|e| EpError::request_with_span(e, &mut span))??;
//
//             Ok(Box::new(RedisValueOutput(result).to_output()) as Box<dyn EpOutput>)
//         })
//     }
//
//     #[named]
//     fn run_async_generic(&self, context: RedisAsync, telemetry_context: TelemetryWrapper) -> RunOutput {
//         let mut span = client_tracer_config(
//             format!("redis.{}.{}", self.kind(), function_name!()),
//             &metadata_map,
//         );
//
//         Box::pin(async move {
//             let mut context = context
//                 .get()
//                 .await
//                 .map_err(|e| EpError::request_with_span(e, &mut span))?;
//
//             let result = context
//                 .zrangebylex_limit(
//                     self.key(),
//                     self.min(),
//                     self.max(),
//                     self.offset(),
//                     self.count(),
//                 )
//                 .await
//                 .map_err(|e| EpError::request_with_span(e, &mut span))?;
//
//             Ok(Box::new(RedisValueOutput(result).to_output()) as Box<dyn EpOutput>)
//         })
//     }
// }
