pub mod change_resource_record_sets;
pub mod create_health_check;
pub mod create_hosted_zone;
pub mod delete_health_check;
pub mod delete_hosted_zone;
pub mod get_health_check;
pub mod get_hosted_zone;
pub mod get_hosted_zone_count;
pub mod list_health_checks;
pub mod list_hosted_zones;
pub mod list_hosted_zones_by_name;
pub mod list_resource_record_sets;
pub mod test_dns_answer;

#[allow(unused_imports)]
pub use change_resource_record_sets::*;
#[allow(unused_imports)]
pub use create_health_check::*;
#[allow(unused_imports)]
pub use create_hosted_zone::*;
#[allow(unused_imports)]
pub use delete_health_check::*;
#[allow(unused_imports)]
pub use delete_hosted_zone::*;
#[allow(unused_imports)]
pub use get_health_check::*;
#[allow(unused_imports)]
pub use get_hosted_zone::*;
#[allow(unused_imports)]
pub use get_hosted_zone_count::*;
#[allow(unused_imports)]
pub use list_health_checks::*;
#[allow(unused_imports)]
pub use list_hosted_zones::*;
#[allow(unused_imports)]
pub use list_hosted_zones_by_name::*;
#[allow(unused_imports)]
pub use list_resource_record_sets::*;
#[allow(unused_imports)]
pub use test_dns_answer::*;
