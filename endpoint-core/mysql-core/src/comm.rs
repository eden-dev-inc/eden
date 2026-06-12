use crate::connection::MysqlConnection;
use error::EpError;
use reqwest::Client;
//
// #[derive(Debug, Clone, BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
// pub struct HttpRequest {
//     pub method: String,
//     pub body: Option<String>,
//     pub headers: Option<HashMap<String, String>>,
// }
//
// #[cfg(test)]
// mod tests {
//     use serde_json::json;
//
//     use crate::http::comm::HttpRequest;
//
//     #[test]
//     fn json_output() {
//         let req = HttpRequest {
//         method: "post".to_string(),
//         body: Some(serde_json::to_string(&json!({
//           "q": "The Great Pyramid of Giza (also known as the Pyramid of Khufu or the Pyramid of Cheops) is the oldest and largest of the three pyramids in the Giza pyramid complex.",
//           "source": "en",
//           "target": "es",
//           "format": "text"
//         })).unwrap_or_default()),
//         headers: None,
//     };
//
//         print!("{}", serde_json::to_string(&req).unwrap_or_default())
//     }
// }
//
// impl HttpRequest {
//     pub async fn read(self, client: &MysqlClient) -> Result<Value, EpError> {
//         match self.method.to_uppercase().as_str() {
//             "GET" => client.get(self.body, self.headers).await,
//             _ => Err(EpError::request(
//                 "request does not have propper permissions",
//             )),
//         }
//     }
//     pub async fn write(self, client: &MysqlClient) -> Result<Value, EpError> {
//         match self.method.to_uppercase().as_str() {
//             "DELETE" => client.delete(self.body, self.headers).await,
//             "GET" => client.get(self.body, self.headers).await,
//             "POST" => client.post(self.body, self.headers).await,
//             "PUT" => client.put(self.body, self.headers).await,
//             _ => Err(EpError::request(&format!(
//                 "Unsupported HTTP method {}",
//                 self.method
//             ))),
//         }
//     }
// }

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct MysqlClient {
    client: Client, // reqwest client
    url: String,    // base url
}

impl MysqlClient {
    pub async fn new(_conn: &MysqlConnection) -> Result<Self, EpError> {
        todo!("Connectint Mysql client not implemented");
    }
}
