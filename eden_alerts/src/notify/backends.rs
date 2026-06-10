//! Notification backend implementations.

use async_trait::async_trait;
use reqwest::Client;
use serde::Serialize;

use super::config::{SlackConfig, WebhookConfig};
use super::{Notification, NotifyError};

/// Notification backend interface.
#[async_trait]
pub trait NotificationBackend: Send + Sync {
    /// Send a notification to this backend.
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError>;

    /// Backend name for logging.
    fn name(&self) -> &'static str;
}

/// Slack webhook backend.
pub struct SlackBackend {
    client: Client,
    config: SlackConfig,
}

impl SlackBackend {
    pub fn new(client: Client, config: SlackConfig) -> Self {
        Self { client, config }
    }
}

/// Generic webhook backend.
pub struct WebhookBackend {
    client: Client,
    config: WebhookConfig,
}

impl WebhookBackend {
    pub fn new(client: Client, config: WebhookConfig) -> Self {
        Self { client, config }
    }
}

#[derive(Debug, Serialize)]
struct SlackPayload {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon_emoji: Option<String>,
}

#[async_trait]
impl NotificationBackend for SlackBackend {
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError> {
        let payload = SlackPayload {
            text: notification.plain_text(),
            channel: self.config.channel.clone(),
            username: self.config.username.clone(),
            icon_emoji: self.config.icon_emoji.clone(),
        };

        let response = self.client.post(&self.config.webhook_url).json(&payload).send().await?;

        if !response.status().is_success() {
            return Err(NotifyError::Backend(format!("slack webhook returned {}", response.status())));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "slack"
    }
}

#[async_trait]
impl NotificationBackend for WebhookBackend {
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError> {
        let mut request = self.client.post(&self.config.url).json(notification);
        for header in &self.config.headers {
            request = request.header(&header.name, &header.value);
        }

        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(NotifyError::Backend(format!("webhook returned {}", response.status())));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "webhook"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::{NotificationKind, WebhookHeader};
    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_notification() -> Notification {
        Notification::new(
            NotificationKind::PeriodicSummary { rule_id: "rule-1".to_string() },
            "Test Title".to_string(),
            "Test Body".to_string(),
        )
    }

    #[tokio::test]
    async fn slack_backend_formats_payload() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let notification = make_notification();
        let expected_text = notification.plain_text();
        let config = SlackConfig {
            webhook_url: server.uri(),
            channel: Some("#alerts".to_string()),
            username: Some("bot".to_string()),
            icon_emoji: Some(":zap:".to_string()),
        };
        let backend = SlackBackend::new(reqwest::Client::new(), config);

        backend.send(&notification).await.unwrap();

        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body.get("text").and_then(Value::as_str), Some(expected_text.as_str()));
        assert_eq!(body.get("channel").and_then(Value::as_str), Some("#alerts"));
    }

    #[tokio::test]
    async fn webhook_backend_includes_headers_and_payload() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/notify")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let notification = make_notification();
        let config = WebhookConfig {
            url: format!("{}/notify", server.uri()),
            headers: vec![WebhookHeader { name: "X-Test".to_string(), value: "123".to_string() }],
        };
        let backend = WebhookBackend::new(reqwest::Client::new(), config);

        backend.send(&notification).await.unwrap();

        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(requests.len(), 1);
        let header = requests[0].headers.get("X-Test").and_then(|value| value.to_str().ok());
        assert_eq!(header, Some("123"));
    }

    #[tokio::test]
    async fn webhook_backend_returns_error_on_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/")).respond_with(ResponseTemplate::new(500)).mount(&server).await;

        let notification = make_notification();
        let config = WebhookConfig { url: server.uri(), headers: Vec::new() };
        let backend = WebhookBackend::new(reqwest::Client::new(), config);

        let result = backend.send(&notification).await;
        assert!(matches!(result, Err(NotifyError::Backend(_))));
    }
}
