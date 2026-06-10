use bytes::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Password {
    salt: [u8; 8],
    hash: [u8; 32],
}

impl ToSql for Password {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // We expect the password to be stored as bytea
        if *ty != Type::BYTEA {
            return Err("Password can only be serialized to bytea".into());
        }

        // Write salt followed by hash
        out.extend_from_slice(&self.salt);
        out.extend_from_slice(&self.hash);

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA
    }

    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

impl<'a> FromSql<'a> for Password {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if *ty != Type::BYTEA {
            return Err("Expected bytea type for Password".into());
        }

        // Validate length
        if raw.len() != 40 {
            // 8 bytes salt + 32 bytes hash
            return Err("Invalid password data length".into());
        }

        let mut password = Password::default();

        // Copy salt (first 8 bytes)
        password.salt.copy_from_slice(&raw[0..8]);

        // Copy hash (next 32 bytes)
        password.hash.copy_from_slice(&raw[8..40]);

        Ok(password)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA
    }
}

impl Password {
    pub fn new(password: String) -> Self {
        let mut rng = rng();
        let salt: [u8; 8] = rng.random();

        let hash = hash([salt.as_ref(), password.as_bytes()].concat());

        Self { salt, hash }
    }

    pub fn verify(&self, password: String) -> bool {
        self.hash == hash([self.salt.as_ref(), password.as_bytes()].concat())
    }
}

fn hash(bytes: Vec<u8>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod test {
    use super::Password;

    #[test]
    fn test_password() {
        let pass = Password::new("password".to_string());

        println!("{pass:#?}");

        assert!(pass.verify("password".to_string()));
        assert!(!pass.verify("bad_password".to_string()));
    }
}
