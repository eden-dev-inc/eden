pub mod create_bucket;
pub mod delete_bucket;
pub mod delete_object;
pub mod get_object;
pub mod head_object;
pub mod list_buckets;
pub mod list_objects;
pub mod put_object;

#[allow(unused_imports)]
use create_bucket::*;
#[allow(unused_imports)]
use delete_bucket::*;
#[allow(unused_imports)]
use delete_object::*;
#[allow(unused_imports)]
use get_object::*;
#[allow(unused_imports)]
use head_object::*;
#[allow(unused_imports)]
use list_buckets::*;
#[allow(unused_imports)]
use list_objects::*;
#[allow(unused_imports)]
use put_object::*;

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub enum S3Api {
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    ListObjects,
    CreateBucket,
    DeleteBucket,
    ListBuckets,
}

impl S3Api {
    pub fn name() -> String {
        "S3Api".to_string()
    }

    pub fn db_kind() -> String {
        "s3".to_string()
    }
}

impl Display for S3Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PutObject => f.write_str("put_object"),
            Self::GetObject => f.write_str("get_object"),
            Self::HeadObject => f.write_str("head_object"),
            Self::DeleteObject => f.write_str("delete_object"),
            Self::ListObjects => f.write_str("list_objects"),
            Self::CreateBucket => f.write_str("create_bucket"),
            Self::DeleteBucket => f.write_str("delete_bucket"),
            Self::ListBuckets => f.write_str("list_buckets"),
        }
    }
}
