use crate::ApiExample;
use crate::api::SetInput;
use crate::api::lib::string::set::args::*;
use crate::api::{key::RedisKey, value::RedisJsonValue};
use tokio::sync::OnceCell;

static EXAMPLES: OnceCell<Vec<ApiExample<SetInput>>> = OnceCell::const_new();

pub async fn examples() -> &'static [ApiExample<SetInput>] {
    EXAMPLES
        .get_or_init(|| async {
            vec![
                // Example 1: Basic SET operation
                ApiExample {
                    name: "Basic SET",
                    description: "Set a simple string value for a key",
                    request: SetInput {
                        key: RedisKey::from("mykey"),
                        value: RedisJsonValue::from("Hello, Redis!"),
                        rule: None,
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 2: SET with expiration (EX option)
                ApiExample {
                    name: "SET with expiration",
                    description: "Set a key with an expiration time in seconds",
                    request: SetInput {
                        key: RedisKey::from("session:token"),
                        value: RedisJsonValue::from("xyz123abc"),
                        rule: None,
                        get: None,
                        options: Some(Options::EX(EX {
                            seconds: RedisJsonValue::Integer(3600), // Expire in 1 hour
                        })),
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 3: SET with NX option (set only if key doesn't exist)
                ApiExample {
                    name: "SET with NX",
                    description: "Set a key only if it doesn't already exist (for creating new keys)",
                    request: SetInput {
                        key: RedisKey::from("user:123"),
                        value: RedisJsonValue::from("{\"username\":\"johndoe\",\"email\":\"john@example.com\"}"),
                        rule: Some(Rule::NX),
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 4: SET with XX option (set only if key exists)
                ApiExample {
                    name: "SET with XX",
                    description: "Set a key only if it already exists (for updating existing keys)",
                    request: SetInput {
                        key: RedisKey::from("counter"),
                        value: RedisJsonValue::Integer(42),
                        rule: Some(Rule::XX),
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::Null)), // Nil if key doesn't exist
                },
                // Example 5: SET with GET option (return previous value)
                ApiExample {
                    name: "SET with GET",
                    description: "Set a key and return its previous value",
                    request: SetInput {
                        key: RedisKey::from("status"),
                        value: RedisJsonValue::from("online"),
                        rule: None,
                        get: Some(true),
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::Null)), // Nil if key didn't exist
                },
                // Example 6: SET with PX option (expiration in milliseconds)
                ApiExample {
                    name: "SET with millisecond expiration",
                    description: "Set a key with an expiration time in milliseconds",
                    request: SetInput {
                        key: RedisKey::from("temporary:data"),
                        value: RedisJsonValue::from("short-lived"),
                        rule: None,
                        get: None,
                        options: Some(Options::PX(PX {
                            milliseconds: RedisJsonValue::Integer(5000), // Expire in 5 seconds
                        })),
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 7: SET with KEEPTTL option
                ApiExample {
                    name: "SET with KEEPTTL",
                    description: "Set a new value for a key while retaining its existing TTL",
                    request: SetInput {
                        key: RedisKey::from("cached:item"),
                        value: RedisJsonValue::from("updated-value"),
                        rule: None,
                        get: None,
                        options: Some(Options::KEEPTTL),
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 8: SET with EXAT option (expire at specific Unix timestamp)
                ApiExample {
                    name: "SET with EXAT",
                    description: "Set a key with expiration at a specific Unix timestamp (in seconds)",
                    request: SetInput {
                        key: RedisKey::from("event:reminder"),
                        value: RedisJsonValue::from("Annual meeting"),
                        rule: None,
                        get: None,
                        options: Some(Options::EXAT(EXAT {
                            unix_time_seconds: RedisJsonValue::Integer(1717027200), // May 30, 2024
                        })),
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 9: SET with PXAT option (expire at specific Unix timestamp in milliseconds)
                ApiExample {
                    name: "SET with PXAT",
                    description: "Set a key with expiration at a specific Unix timestamp (in milliseconds)",
                    request: SetInput {
                        key: RedisKey::from("auth:token"),
                        value: RedisJsonValue::from("oauth2token456"),
                        rule: None,
                        get: None,
                        options: Some(Options::PXAT(PXAT {
                            unix_time_milliseconds: RedisJsonValue::Integer(1717027200000), // May 30, 2024
                        })),
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 10: SET with combined options (NX and expiration)
                ApiExample {
                    name: "SET with combined options",
                    description: "Create a new key with expiration only if it doesn't exist (atomic creation with TTL)",
                    request: SetInput {
                        key: RedisKey::from("lock:resource"),
                        value: RedisJsonValue::from("process-id-789"),
                        rule: Some(Rule::NX),
                        get: None,
                        options: Some(Options::PX(PX {
                            milliseconds: RedisJsonValue::Integer(10000), // Lock for 10 seconds
                        })),
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 11: SET with numeric value
                ApiExample {
                    name: "SET with numeric value",
                    description: "Store a numeric value",
                    request: SetInput {
                        key: RedisKey::from("score"),
                        value: RedisJsonValue::Float(95.5),
                        rule: None,
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 12: Failed SET with NX condition (key already exists)
                ApiExample {
                    name: "Failed SET with NX",
                    description: "Attempt to create a key that already exists with NX option returns nil",
                    request: SetInput {
                        key: RedisKey::from("existing:key"),
                        value: RedisJsonValue::from("new value"),
                        rule: Some(Rule::NX),
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::Null)),
                },
                // Example 13: Failed SET with XX condition (key does not exist)
                ApiExample {
                    name: "Failed SET with XX",
                    description: "Attempt to update a non-existent key with XX option returns nil",
                    request: SetInput {
                        key: RedisKey::from("nonexistent:key"),
                        value: RedisJsonValue::from("update value"),
                        rule: Some(Rule::XX),
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::Null)),
                },
                // Example 14: SET with GET on a new key
                ApiExample {
                    name: "SET with GET on new key",
                    description: "Set a new key with GET option (which returns nil for non-existent keys)",
                    request: SetInput {
                        key: RedisKey::from("new:counter"),
                        value: RedisJsonValue::Integer(1),
                        rule: None,
                        get: Some(true),
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::Null)),
                },
                // Example 15: SET with Boolean value
                ApiExample {
                    name: "SET with Boolean value",
                    description: "Store a boolean value",
                    request: SetInput {
                        key: RedisKey::from("feature:enabled"),
                        value: RedisJsonValue::Bool(true),
                        rule: None,
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
                // Example 16: SET with Integer value
                ApiExample {
                    name: "SET with Integer value",
                    description: "Store an integer value",
                    request: SetInput {
                        key: RedisKey::from("count"),
                        value: RedisJsonValue::Integer(8),
                        rule: None,
                        get: None,
                        options: None,
                    },
                    response: Ok(Some(serde_json::Value::from("OK"))),
                },
            ]
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_examples_load() {
        let exs = examples().await;
        assert!(!exs.is_empty());
        assert!(exs.len() >= 10);
    }

    #[tokio::test]
    async fn test_basic_example_structure() {
        let exs = examples().await;
        let basic = &exs[0];
        assert_eq!(basic.name, "Basic SET");
        assert!(basic.response.is_ok());
    }

    #[tokio::test]
    async fn test_all_examples_have_names() {
        let exs = examples().await;
        for ex in exs {
            assert!(!ex.name.is_empty());
            assert!(!ex.description.is_empty());
        }
    }
}
