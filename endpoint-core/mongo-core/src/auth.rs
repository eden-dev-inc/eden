use borsh::{BorshDeserialize, BorshSerialize};
use mongodb::options::AuthMechanism;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MongoUap {
    pub username: String,
    pub password: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_mechanism: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_key: Option<String>,
}

impl MongoUap {
    pub fn as_authmechanism(&self) -> AuthMechanism {
        match &self.auth_mechanism {
            Some(auth) => match auth.to_lowercase().as_str() {
                "gssapi" => AuthMechanism::Gssapi,
                "mongodbcr" => AuthMechanism::MongoDbCr,
                "mongodbaws" => AuthMechanism::MongoDbAws,
                "mongodboidc" => AuthMechanism::MongoDbOidc,
                "mongodbx509" => AuthMechanism::MongoDbX509,
                "scramsha1" => AuthMechanism::ScramSha1,
                "scramsha256" => AuthMechanism::ScramSha256,
                _ => AuthMechanism::ScramSha1,
            },
            None => AuthMechanism::MongoDbX509,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct MongoTls {
    pub ca_file: String,
    pub cert_file: String,
    pub auth_mechanism: Option<String>,
    pub aws_key: Option<String>,
}

impl MongoTls {
    pub fn as_authmechanism(&self) -> AuthMechanism {
        match &self.auth_mechanism {
            Some(auth) => match auth.to_lowercase().as_str() {
                "gssapi" => AuthMechanism::Gssapi,
                "mongodbcr" => AuthMechanism::MongoDbCr,
                "mongodbaws" => AuthMechanism::MongoDbAws,
                "mongodboidc" => AuthMechanism::MongoDbOidc,
                "mongodbx509" => AuthMechanism::MongoDbX509,
                "scramsha1" => AuthMechanism::ScramSha1,
                "scramsha256" => AuthMechanism::ScramSha256,
                _ => AuthMechanism::ScramSha1,
            },
            None => AuthMechanism::MongoDbX509,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub enum MongoAuth {
    UAP(MongoUap),
    TLS(MongoTls),
}
