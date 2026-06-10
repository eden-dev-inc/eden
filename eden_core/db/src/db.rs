use error::{DBError, ResultDB};

use crate::{ConnectionParameters, DBKind};

// use serde::{Deserialize, Serialize};

// use eden_core::format::Nonce;

// use crate::stc::node::Node;

// pub use self::redis::Redis;
// pub use postgres::PGDB;

pub trait DB: Send + Sync {
    fn store(&mut self, key: &str, value: &str) -> ResultDB<Option<String>>;
    fn load(&mut self, key: &str) -> ResultDB<String>;
    fn delete(&mut self, key: &str) -> Result<(), DBError>;
    fn execute(&mut self, command: &str) -> Result<String, DBError>;
    fn query(&mut self, command: &str) -> Result<String, DBError>;
    fn connect(&mut self, params: &ConnectionParameters) -> Result<(), DBError>;
    fn disconnect(&mut self) -> Result<(), DBError>;
    fn rename(&mut self, old_name: &str, new_name: &str) -> Result<(), DBError>;
    fn request_secret(&mut self) -> Result<String, DBError>;
    fn kind(&self) -> DBKind;
}

// #[async_trait]
// pub trait DB<N>: Send + Sync
// where
//     N: Node,
// {
//     async fn register_node(&self, self_node: &N) -> Result<(), Box<dyn Error>>;
//     async fn get_active_nodes(&self) -> Result<Vec<N>, Box<dyn Error>>;
//     async fn deactivate_node(&self, self_node: &N) -> Result<(), Box<dyn Error>>;
//     async fn health(&self) -> Result<(), Box<dyn Error>>;
//     async fn get_all_users(&self) -> Result<Vec<String>, Box<dyn Error>>;
//     async fn create_user(
//         &self,
//         user_id: &str,
//         content: &str,
//         x_public_key: &str,
//         data: &str,
//     ) -> Result<(), Box<dyn Error>>;
//     async fn get_user(&self, user_id: &str) -> Result<String, Box<dyn Error>>;
//     async fn set_nickname(
//         &self,
//         user_id: &str,
//         nickname_pub_key: &str,
//         nickname: &str,
//     ) -> Result<(), Box<dyn Error>>;
//     async fn key_from_nickname(
//         &self,
//         user_id: &str,
//         nickname: &str,
//     ) -> Result<Option<String>, Box<dyn Error>>;
//     async fn subscribe_user(
//         &self,
//         user_id: &str,
//         subscription: &PushSubscription,
//     ) -> Result<(), Box<dyn Error>>;
//     async fn unsubscribe_user(&self, user_id: &str) -> Result<(), Box<dyn Error>>;
//     // fn subscriptions(&self) -> Result<Vec<(String, Vec<PushSubscription>)>, Box<dyn Error>>;
//     async fn pop_node(&self) -> Result<Option<(String, Option<String>)>, Box<dyn Error>>;
//     async fn store_epoch(&self, epoch: &str, nonce: Nonce) -> Result<(), Box<dyn Error>>;
//     async fn load_blockchain(&self) -> Result<Vec<String>, Box<dyn Error>>;
// }

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub struct Nickname {
//     pub public_key: String,
//     pub nickname: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub struct UserData {
//     pub secret: String,
//     pub x_public_key: String,
//     pub data: String,
//     #[serde(default)]
//     pub nicknames: Vec<Nickname>,
//     #[serde(default)]
//     pub subscriptions: Vec<PushSubscription>,
// }

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// pub struct PushSubscription {
//     pub endpoint: String,
//     pub keys: SubscriptionKeys,
// }

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// pub struct SubscriptionKeys {
//     pub p256dh: String,
//     pub auth: String,
// }
