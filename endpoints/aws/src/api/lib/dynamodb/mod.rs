pub mod batch_get_item;
pub mod batch_write_item;
pub mod create_backup;
pub mod create_global_table;
pub mod create_table;
pub mod delete_backup;
pub mod delete_table;
pub mod describe_backup;
pub mod describe_continuous_backups;
pub mod describe_endpoints;
pub mod describe_global_table;
pub mod describe_limits;
pub mod describe_time_to_live;
pub mod export_table_to_point_in_time;
pub mod list_backups;
pub mod list_global_tables;
pub mod list_tags_of_resource;
pub mod restore_table_from_backup;
pub mod restore_table_to_point_in_time;
pub mod tag_resource;
pub mod transact_get_items;
pub mod transact_write_items;
pub mod untag_resource;
pub mod update_continuous_backups;
pub mod update_item;
pub mod update_table;
pub mod update_time_to_live;

#[allow(unused_imports)]
pub use batch_get_item::*;
#[allow(unused_imports)]
pub use batch_write_item::*;
#[allow(unused_imports)]
pub use create_backup::*;
#[allow(unused_imports)]
pub use create_global_table::*;
#[allow(unused_imports)]
pub use create_table::*;
#[allow(unused_imports)]
pub use delete_backup::*;
#[allow(unused_imports)]
pub use delete_table::*;
#[allow(unused_imports)]
pub use describe_backup::*;
#[allow(unused_imports)]
pub use describe_continuous_backups::*;
#[allow(unused_imports)]
pub use describe_endpoints::*;
#[allow(unused_imports)]
pub use describe_global_table::*;
#[allow(unused_imports)]
pub use describe_limits::*;
#[allow(unused_imports)]
pub use describe_time_to_live::*;
#[allow(unused_imports)]
pub use export_table_to_point_in_time::*;
#[allow(unused_imports)]
pub use list_backups::*;
#[allow(unused_imports)]
pub use list_global_tables::*;
#[allow(unused_imports)]
pub use list_tags_of_resource::*;
#[allow(unused_imports)]
pub use restore_table_from_backup::*;
#[allow(unused_imports)]
pub use restore_table_to_point_in_time::*;
#[allow(unused_imports)]
pub use tag_resource::*;
#[allow(unused_imports)]
pub use transact_get_items::*;
#[allow(unused_imports)]
pub use transact_write_items::*;
#[allow(unused_imports)]
pub use untag_resource::*;
#[allow(unused_imports)]
pub use update_continuous_backups::*;
#[allow(unused_imports)]
pub use update_item::*;
#[allow(unused_imports)]
pub use update_table::*;
#[allow(unused_imports)]
pub use update_time_to_live::*;
