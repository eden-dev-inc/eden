// use super::comm::{ClickhouseClient, ClickhouseRequests, ReqData};
//
// #[cfg(test)]
// mod tests {
//     use super::{ClickhouseClient, ClickhouseRequests, ReqData};
//     use crate::connection::ClickhouseConnection;
//
//     #[test]
//     fn config_serde() {
//         let conn = ClickhouseConnection {
//             url: "https://cfyax6k29q.us-east-1.aws.clickhouse.cloud:8443".to_string(),
//             // username: Some("DFUQKIgWjOq7j3AI6m0m".to_string()),
//             // password: Some("4b1djMRnnsJ7tdZVG7YSqFwsjoyz0WYd7pwoOGCBXn".to_string()),
//             username: Some("default".to_string()),
//             password: Some("DX~qc6Wn70Mig".to_string()),
//             database: None,
//         };
//
//         print!("{}", serde_json::to_string(&conn).unwrap_or_default());
//     }
//
//     #[test]
//     fn req_serde() {
//         let request = "SHOW databases".to_string();
//         let body = ReqData::new(request, None, None, None, vec![]);
//
//         print!("{}", serde_json::to_string(&body).unwrap_or_default());
//     }
//
//     #[tokio::test]
//     async fn test_conn() {
//         let conn = ClickhouseConnection {
//             url: "https://cfyax6k29q.us-east-1.aws.clickhouse.cloud:8443".to_string(),
//             // username: Some("DFUQKIgWjOq7j3AI6m0m".to_string()),
//             // password: Some("4b1djMRnnsJ7tdZVG7YSqFwsjoyz0WYd7pwoOGCBXn".to_string()),
//             username: Some("default".to_string()),
//             password: Some("DX~qc6Wn70Mig".to_string()),
//             database: None,
//         };
//
//         let client = ClickhouseClient::build(&conn)
//             .await
//             .expect("failed to make connection");
//
//         let request = "SHOW databases".to_string();
//         let body = &ReqData::new(request, None, None, None, vec![]);
//         let res = client.read(body).await.expect("expected result");
//         println!("{res}");
//
//         let request = "SELECT * FROM columns LIMIT 1 FORMAT json".to_string();
//         let body = &ReqData::new(
//             request,
//             Some("information_schema".to_string()),
//             None,
//             None,
//             vec![],
//         );
//         let res = client.read(body).await.expect("expected result");
//         println!("{res}");
//     }
// }
