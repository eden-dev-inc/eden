pub mod create_contact_list;
pub mod create_email_identity;
pub mod delete_email_identity;
pub mod get_account;
pub mod get_email_identity;
pub mod list_contact_lists;
pub mod list_email_identities;
pub mod send_email;

#[allow(unused_imports)]
pub use create_contact_list::*;
#[allow(unused_imports)]
pub use create_email_identity::*;
#[allow(unused_imports)]
pub use delete_email_identity::*;
#[allow(unused_imports)]
pub use get_account::*;
#[allow(unused_imports)]
pub use get_email_identity::*;
#[allow(unused_imports)]
pub use list_contact_lists::*;
#[allow(unused_imports)]
pub use list_email_identities::*;
#[allow(unused_imports)]
pub use send_email::*;
