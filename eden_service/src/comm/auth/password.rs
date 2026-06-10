// use actix_web::{web, HttpRequest, Responder};
// use deadpool::unmanaged::Pool;
// use database::db::lib::{DatabaseManager, ClickhouseConn, PgConn, RedisConn};
// use serde::{Deserialize, Serialize};
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use utoipa::ToSchema;
//
// /// Request password reset
// #[utoipa::path(
//     post,
//     tags = ["Authorization"],
//     path="/auth/password/reset",
//     request_body = PasswordResetRequest,
//     responses((status = OK, body = String))
// )]
// pub async fn reset(
//     req: HttpRequest,
//     database: web::Data<EdenDb>, // auth database
//     client: web::Data<Arc<EdenClient>>,                      // engine client
//     pool: web::Data<Pool<Mutex<Wrapper>>>,                   // engine pool
//     input: web::Json<PasswordResetRequest>,                  // request
// ) -> impl Responder {
//     Ok::<&str, actix_web::error::Error>("TODO PASSWORD RESET")
// }
//
// #[derive(Serialize, Deserialize)]
// pub struct ReqPwdResetRequest {
//     username: String,
// }
//
// #[derive(Serialize, Deserialize, ToSchema)]
// pub struct PasswordResetRequest {
//     username: String,
//     token: String,
//     new_password: String,
// }
