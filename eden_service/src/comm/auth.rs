use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub mod login;
pub mod password;
pub mod robot_login;

pub use login::check_user_rbac_access;

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct JwtResponse {
    pub token: String,
}
