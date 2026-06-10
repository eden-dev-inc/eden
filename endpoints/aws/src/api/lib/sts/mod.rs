pub mod assume_role;
pub mod assume_role_with_saml;
pub mod assume_role_with_web_identity;
pub mod decode_authorization_message;
pub mod get_access_key_info;
pub mod get_caller_identity;
pub mod get_session_token;

#[allow(unused_imports)]
pub use assume_role::*;
#[allow(unused_imports)]
pub use assume_role_with_saml::*;
#[allow(unused_imports)]
pub use assume_role_with_web_identity::*;
#[allow(unused_imports)]
pub use decode_authorization_message::*;
#[allow(unused_imports)]
pub use get_access_key_info::*;
#[allow(unused_imports)]
pub use get_caller_identity::*;
#[allow(unused_imports)]
pub use get_session_token::*;
