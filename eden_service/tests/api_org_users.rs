#![cfg(external_db)]

mod common;
mod request;
mod util;

#[path = "api_admin_auth/org_transfer.rs"]
mod org_transfer;
#[path = "api_admin_auth/organization_crud.rs"]
mod organization_crud;
#[path = "api_admin_auth/organizations.rs"]
mod organizations;
#[path = "api_admin_auth/users.rs"]
mod users;
#[path = "api_admin_auth/users_me.rs"]
mod users_me;
