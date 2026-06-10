use super::AuthDocs;
use utoipa::{Modify, OpenApi, openapi};

pub struct ApiDocs;

impl OpenApi for ApiDocs {
    fn openapi() -> openapi::OpenApi {
        let mut openapi = openapi::OpenApi::default();
        AuthDocs.modify(&mut openapi);
        openapi
    }
}
