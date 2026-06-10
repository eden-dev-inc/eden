pub mod create_auto_scaling_group;
pub mod create_launch_configuration;
pub mod delete_auto_scaling_group;
pub mod describe_auto_scaling_groups;
pub mod describe_launch_configurations;
pub mod describe_policies;
pub mod describe_scaling_activities;
pub mod execute_policy;
pub mod put_scaling_policy;
pub mod set_desired_capacity;
pub mod update_auto_scaling_group;

#[allow(unused_imports)]
pub use create_auto_scaling_group::*;
#[allow(unused_imports)]
pub use create_launch_configuration::*;
#[allow(unused_imports)]
pub use delete_auto_scaling_group::*;
#[allow(unused_imports)]
pub use describe_auto_scaling_groups::*;
#[allow(unused_imports)]
pub use describe_launch_configurations::*;
#[allow(unused_imports)]
pub use describe_policies::*;
#[allow(unused_imports)]
pub use describe_scaling_activities::*;
#[allow(unused_imports)]
pub use execute_policy::*;
#[allow(unused_imports)]
pub use put_scaling_policy::*;
#[allow(unused_imports)]
pub use set_desired_capacity::*;
#[allow(unused_imports)]
pub use update_auto_scaling_group::*;
