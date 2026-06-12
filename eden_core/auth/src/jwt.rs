use super::auth::{JwToken, SubjectType};
use crate::ParsedJwt;
use error::{AuthError, EpError};
use format::{EdenUuid, OrganizationId, OrganizationUuid, RobotId, RobotUuid, UserId, UserUuid};
use hmac::{Hmac, Mac};
use jwt::{Header, SignWithKey, Token, VerifyWithKey};
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashMap},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

/// JWT token generator and validator using HMAC-SHA256.
#[derive(Debug)]
pub struct Jwt {
    key: Hmac<Sha256>,
    expiry_seconds: u64,
}

impl Jwt {
    pub fn new(secret: &[u8], expiry_seconds: u64) -> Self {
        let key = Hmac::new_from_slice(secret).unwrap_or_else(|_| match Hmac::new_from_slice(&[0u8; 32]) {
            Ok(k) => k,
            Err(_) => unreachable!("32 bytes is valid HMAC key size"),
        });
        Self { key, expiry_seconds }
    }

    pub fn create(&self, user_id: &UserId, user_uuid: &UserUuid, org_id: &OrganizationId, org_uuid: &OrganizationUuid) -> JwToken {
        self.create_with_jti(user_id, user_uuid, org_id, org_uuid).0
    }

    /// Create a JWT token and return both the token and the jti (JWT ID).
    /// The jti can be used to record the session for later revocation.
    pub fn create_with_jti(
        &self,
        user_id: &UserId,
        user_uuid: &UserUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> (JwToken, String) {
        let jti = Uuid::new_v4().to_string();
        let mut claims = HashMap::new();
        claims.insert("sub".to_owned(), user_id.to_string());
        claims.insert("org".to_owned(), org_id.to_string());
        claims.insert("user_uuid".to_owned(), user_uuid.uuid().to_string());
        claims.insert("org_uuid".to_owned(), org_uuid.uuid().to_string());
        claims.insert("subject_type".to_owned(), "user".to_owned());
        // jti (JWT ID) - unique token identifier for per-session revocation
        claims.insert("jti".to_owned(), jti.clone());

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        claims.insert("nbf".to_string(), format!("{now}"));
        claims.insert("exp".to_string(), format!("{}", now + self.expiry_seconds));
        let token = claims.sign_with_key(&self.key).map(|t| JwToken::from(t.as_str())).unwrap_or_default();
        (token, jti)
    }

    pub fn create_robot(
        &self,
        robot_id: &RobotId,
        robot_uuid: &RobotUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> JwToken {
        self.create_robot_with_jti(robot_id, robot_uuid, org_id, org_uuid).0
    }

    /// Create a robot JWT token and return both the token and the jti (JWT ID).
    /// The jti can be used to record the session for later revocation.
    pub fn create_robot_with_jti(
        &self,
        robot_id: &RobotId,
        robot_uuid: &RobotUuid,
        org_id: &OrganizationId,
        org_uuid: &OrganizationUuid,
    ) -> (JwToken, String) {
        let jti = Uuid::new_v4().to_string();
        let mut claims = HashMap::new();
        claims.insert("sub".to_owned(), robot_id.to_string());
        claims.insert("org".to_owned(), org_id.to_string());
        claims.insert("user_uuid".to_owned(), robot_uuid.uuid().to_string());
        claims.insert("org_uuid".to_owned(), org_uuid.uuid().to_string());
        claims.insert("subject_type".to_owned(), "robot".to_owned());
        // jti (JWT ID) - unique token identifier for per-session revocation
        claims.insert("jti".to_owned(), jti.clone());

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        claims.insert("nbf".to_string(), format!("{now}"));
        claims.insert("exp".to_string(), format!("{}", now + self.expiry_seconds));
        let token = claims.sign_with_key(&self.key).map(|t| JwToken::from(t.as_str())).unwrap_or_default();
        (token, jti)
    }

    pub fn validate(&self, token: &JwToken) -> Result<ParsedJwt, EpError> {
        let token_result: Result<Token<Header, BTreeMap<String, String>, _>, _> = VerifyWithKey::verify_with_key(&**token, &self.key);
        match token_result {
            Ok(t) => {
                let subject_type = t
                    .claims()
                    .get("subject_type")
                    .and_then(|s| SubjectType::from_claim(s))
                    .ok_or(EpError::Auth(AuthError::TokenMalformed))?;

                let user_id = UserId::from(t.claims()["sub"].as_str());
                let user_uuid = UserUuid::from(Uuid::parse_str(t.claims()["user_uuid"].as_str()).map_err(EpError::parse)?);
                let org_id = OrganizationId::from(t.claims()["org"].as_str());
                let org_uuid = OrganizationUuid::from(Uuid::parse_str(t.claims()["org_uuid"].as_str()).map_err(EpError::parse)?);
                // jti is optional for backwards compatibility with existing tokens
                let jti = t.claims().get("jti").cloned();

                match subject_type {
                    SubjectType::User => Ok(ParsedJwt::new_with_jti(user_id, user_uuid, org_id, org_uuid, jti)),
                    SubjectType::Robot => {
                        let robot_id = RobotId::from(user_id.to_string().as_str());
                        let robot_uuid = RobotUuid::from(user_uuid.uuid());
                        Ok(ParsedJwt::new_robot_with_jti(robot_id, robot_uuid, org_id, org_uuid, jti))
                    }
                }
            }
            Err(_) => Err(EpError::Auth(AuthError::TokenMalformed)),
        }
    }
}
