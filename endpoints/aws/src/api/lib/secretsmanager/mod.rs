pub mod create_secret;
pub mod delete_secret;
pub mod describe_secret;
pub mod get_random_password;
pub mod get_secret_value;
pub mod list_secret_version_ids;
pub mod list_secrets;
pub mod put_secret_value;
pub mod restore_secret;
pub mod rotate_secret;
pub mod tag_resource;
pub mod untag_resource;
pub mod update_secret;

#[allow(unused_imports)]
pub use create_secret::*;
#[allow(unused_imports)]
pub use delete_secret::*;
#[allow(unused_imports)]
pub use describe_secret::*;
#[allow(unused_imports)]
pub use get_random_password::*;
#[allow(unused_imports)]
pub use get_secret_value::*;
#[allow(unused_imports)]
pub use list_secret_version_ids::*;
#[allow(unused_imports)]
pub use list_secrets::*;
#[allow(unused_imports)]
pub use put_secret_value::*;
#[allow(unused_imports)]
pub use restore_secret::*;
#[allow(unused_imports)]
pub use rotate_secret::*;
#[allow(unused_imports)]
pub use tag_resource::*;
#[allow(unused_imports)]
pub use untag_resource::*;
#[allow(unused_imports)]
pub use update_secret::*;
