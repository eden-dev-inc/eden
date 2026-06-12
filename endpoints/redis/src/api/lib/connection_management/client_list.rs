use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Type, key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClientListInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClientList, "Lists open connections", ReqType::Read, false);

/// See official Redis documentation for `CLIENT LIST`
/// https://redis.io/docs/latest/commands/client-list/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientListInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    r#type: Option<Type>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ids: Option<Vec<RedisJsonValue>>,
}

impl Serialize for ClientListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.r#type.is_some() {
            fields += 1;
        }
        if self.ids.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ClientListInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(client_type) = &self.r#type {
            state.serialize_field("client_type", client_type)?;
        }
        if let Some(ids) = &self.ids {
            state.serialize_field("ids", ids)?;
        }
        state.end()
    }
}

impl_redis_operation!(ClientListInput, API_INFO, { r#type, ids });

impl RedisCommandInput for ClientListInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(r#type) = &self.r#type {
            command.arg("TYPE");
            match r#type {
                Type::NORMAL => command.arg("NORMAL"),
                Type::MASTER => command.arg("MASTER"),
                Type::REPLICA => command.arg("REPLICA"),
                Type::PUBSUB => command.arg("PUBSUB"),
                _ => &mut command,
            };
        }

        if let Some(ids) = &self.ids {
            command.arg("ID");
            for id in ids {
                command.arg(id);
            }
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut client_type = None;
        let mut ids = None;
        let mut i = 0;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "TYPE" => {
                        if i + 1 < args.len() {
                            if let RedisJsonValue::String(type_str) = &args[i + 1] {
                                client_type = match type_str.to_uppercase().as_str() {
                                    "NORMAL" => Some(Type::NORMAL),
                                    "MASTER" => Some(Type::MASTER),
                                    "REPLICA" => Some(Type::REPLICA),
                                    "PUBSUB" => Some(Type::PUBSUB),
                                    _ => None,
                                };
                            }
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    "ID" => {
                        if i + 1 < args.len() {
                            ids = Some(args[i + 1..].to_vec());
                            break;
                        } else {
                            i += 1;
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { r#type: client_type, ids })
    }
}

/// Output for Redis CLIENT LIST command
///
/// Returns information about client connections.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientListOutput {
    /// Raw client list output as a string
    clients: String,
}

impl ClientListOutput {
    pub fn new(clients: String) -> Self {
        Self { clients }
    }

    /// Get the raw client list string
    pub fn clients(&self) -> &str {
        &self.clients
    }

    /// Get the number of clients (by counting lines)
    pub fn count(&self) -> usize {
        if self.clients.is_empty() { 0 } else { self.clients.lines().count() }
    }

    /// Check if the response is empty (no clients)
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// Parse client entries into a list of key-value maps
    pub fn parse_clients(&self) -> Vec<std::collections::HashMap<String, String>> {
        self.clients
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                line.split_whitespace()
                    .filter_map(|pair| {
                        let mut parts = pair.splitn(2, '=');
                        match (parts.next(), parts.next()) {
                            (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                            _ => None,
                        }
                    })
                    .collect()
            })
            .collect()
    }

    /// Decode the Redis protocol response into a ClientListOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let clients = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT LIST response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLIENT LIST response: {:?}", other)));
                }
            },
        };

        Ok(Self { clients })
    }
}

impl Serialize for ClientListOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientListOutput", 1)?;
        state.serialize_field("clients", &self.clients)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_args() {
            let input = ClientListInput::default();
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"CLIENT"));
            assert!(cmd.windows(4).any(|w| w == b"LIST"));
        }

        #[test]
        fn test_encode_command_with_type() {
            let input = ClientListInput { r#type: Some(Type::NORMAL), ids: None };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"TYPE"));
            assert!(cmd.windows(6).any(|w| w == b"NORMAL"));
        }

        #[test]
        fn test_encode_command_with_ids() {
            let input = ClientListInput {
                r#type: None,
                ids: Some(vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)]),
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"ID"));
        }

        #[test]
        fn test_decode_bulk_string_response() {
            let response = b"$43\r\nid=1 addr=127.0.0.1:6379 fd=5 name= age=10\n\r\n";
            let output = ClientListOutput::decode(response).unwrap();
            assert!(!output.is_empty());
            assert!(output.count() >= 1);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClientListOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_parse_clients() {
            let output = ClientListOutput::new("id=1 addr=127.0.0.1:6379 name=test\nid=2 addr=127.0.0.1:6380 name=other\n".to_string());
            let clients = output.parse_clients();
            assert_eq!(clients.len(), 2);
            assert_eq!(clients[0].get("id"), Some(&"1".to_string()));
            assert_eq!(clients[1].get("name"), Some(&"other".to_string()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClientListInput::decode(args).unwrap();
            assert!(input.r#type.is_none());
            assert!(input.ids.is_none());
        }

        #[test]
        fn test_decode_input_with_type() {
            let args = vec![RedisJsonValue::String("TYPE".into()), RedisJsonValue::String("NORMAL".into())];
            let input = ClientListInput::decode(args).unwrap();
            assert!(matches!(input.r#type, Some(Type::NORMAL)));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClientListInput::default();
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_list_returns_clients() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClientListInput::default().command()).await.expect("raw failed");

                    let output = ClientListOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty(), "Should have at least our connection");
                    assert!(output.count() >= 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_list_contains_connection_info() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClientListInput::default().command()).await.expect("raw failed");

                    let output = ClientListOutput::decode(&result).expect("decode failed");
                    let clients = output.parse_clients();

                    assert!(!clients.is_empty());
                    // Each client should have an id and addr
                    for client in clients {
                        assert!(client.contains_key("id"));
                        assert!(client.contains_key("addr"));
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_list_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ClientListInput::default().command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = ClientListOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_client_list_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ClientListInput::default().command()).await.expect("raw failed");

            let output = ClientListOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }
    }
}
