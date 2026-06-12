#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::S3Client;
use deadpool::unmanaged::Pool;

pub use comm::{
    S3GetObjectResponse, S3HeadObjectResponse, S3ListBucketEntry, S3ListBucketsResponse, S3ListObjectEntry, S3ListObjectsResponse,
    S3ObjectBody, S3PutObjectRequest, S3PutObjectResponse, normalize_payload,
};
pub use connection::S3Provider;

pub type S3Async = Pool<S3Client>;
pub type S3Tx = Pool<S3Client>;
