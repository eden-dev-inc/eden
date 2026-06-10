pub mod accept_vpc_peering_connection;
pub mod allocate_address;
pub mod associate_address;
pub mod associate_route_table;
pub mod attach_internet_gateway;
pub mod attach_network_interface;
pub mod attach_volume;
pub mod attach_vpn_gateway;
pub mod authorize_security_group_egress;
pub mod authorize_security_group_ingress;
pub mod cancel_spot_instance_requests;
pub mod copy_image;
pub mod copy_snapshot;
pub mod create_customer_gateway;
pub mod create_flow_logs;
pub mod create_internet_gateway;
pub mod create_key_pair;
pub mod create_launch_template;
pub mod create_nat_gateway;
pub mod create_network_acl;
pub mod create_network_acl_entry;
pub mod create_network_interface;
pub mod create_placement_group;
pub mod create_route;
pub mod create_route_table;
pub mod create_security_group;
pub mod create_snapshot;
pub mod create_subnet;
pub mod create_tags;
pub mod create_transit_gateway;
pub mod create_volume;
pub mod create_vpc;
pub mod create_vpc_endpoint;
pub mod create_vpc_peering_connection;
pub mod create_vpn_connection;
pub mod create_vpn_gateway;
pub mod delete_customer_gateway;
pub mod delete_flow_logs;
pub mod delete_key_pair;
pub mod delete_launch_template;
pub mod delete_nat_gateway;
pub mod delete_network_acl;
pub mod delete_network_acl_entry;
pub mod delete_network_interface;
pub mod delete_placement_group;
pub mod delete_route;
pub mod delete_route_table;
pub mod delete_security_group;
pub mod delete_snapshot;
pub mod delete_subnet;
pub mod delete_tags;
pub mod delete_transit_gateway;
pub mod delete_volume;
pub mod delete_vpc;
pub mod delete_vpc_endpoints;
pub mod delete_vpc_peering_connection;
pub mod delete_vpn_connection;
pub mod delete_vpn_gateway;
pub mod deregister_image;
pub mod describe_addresses;
pub mod describe_availability_zones;
pub mod describe_customer_gateways;
pub mod describe_flow_logs;
pub mod describe_images;
pub mod describe_instance_status;
pub mod describe_instance_types;
pub mod describe_instances;
pub mod describe_internet_gateways;
pub mod describe_key_pairs;
pub mod describe_launch_templates;
pub mod describe_nat_gateways;
pub mod describe_network_acls;
pub mod describe_network_interfaces;
pub mod describe_placement_groups;
pub mod describe_regions;
pub mod describe_reserved_instances;
pub mod describe_route_tables;
pub mod describe_security_groups;
pub mod describe_snapshots;
pub mod describe_spot_instance_requests;
pub mod describe_subnets;
pub mod describe_tags;
pub mod describe_transit_gateway_attachments;
pub mod describe_transit_gateways;
pub mod describe_volumes;
pub mod describe_vpc_endpoints;
pub mod describe_vpc_peering_connections;
pub mod describe_vpcs;
pub mod describe_vpn_connections;
pub mod describe_vpn_gateways;
pub mod detach_internet_gateway;
pub mod detach_network_interface;
pub mod detach_volume;
pub mod detach_vpn_gateway;
pub mod disassociate_address;
pub mod disassociate_route_table;
pub mod get_console_output;
pub mod import_key_pair;
pub mod modify_instance_attribute;
pub mod modify_instance_metadata_options;
pub mod modify_subnet_attribute;
pub mod modify_volume;
pub mod modify_vpc_attribute;
pub mod reboot_instances;
pub mod register_image;
pub mod release_address;
pub mod request_spot_instances;
pub mod revoke_security_group_egress;
pub mod revoke_security_group_ingress;
pub mod run_instances;
pub mod start_instances;
pub mod stop_instances;
pub mod terminate_instances;

#[allow(unused_imports)]
pub use accept_vpc_peering_connection::*;
#[allow(unused_imports)]
pub use allocate_address::*;
#[allow(unused_imports)]
pub use associate_address::*;
#[allow(unused_imports)]
pub use associate_route_table::*;
#[allow(unused_imports)]
pub use attach_internet_gateway::*;
#[allow(unused_imports)]
pub use attach_network_interface::*;
#[allow(unused_imports)]
pub use attach_volume::*;
#[allow(unused_imports)]
pub use attach_vpn_gateway::*;
#[allow(unused_imports)]
pub use authorize_security_group_egress::*;
#[allow(unused_imports)]
pub use authorize_security_group_ingress::*;
#[allow(unused_imports)]
pub use cancel_spot_instance_requests::*;
#[allow(unused_imports)]
pub use copy_image::*;
#[allow(unused_imports)]
pub use copy_snapshot::*;
#[allow(unused_imports)]
pub use create_customer_gateway::*;
#[allow(unused_imports)]
pub use create_flow_logs::*;
#[allow(unused_imports)]
pub use create_internet_gateway::*;
#[allow(unused_imports)]
pub use create_key_pair::*;
#[allow(unused_imports)]
pub use create_launch_template::*;
#[allow(unused_imports)]
pub use create_nat_gateway::*;
#[allow(unused_imports)]
pub use create_network_acl::*;
#[allow(unused_imports)]
pub use create_network_acl_entry::*;
#[allow(unused_imports)]
pub use create_network_interface::*;
#[allow(unused_imports)]
pub use create_placement_group::*;
#[allow(unused_imports)]
pub use create_route::*;
#[allow(unused_imports)]
pub use create_route_table::*;
#[allow(unused_imports)]
pub use create_security_group::*;
#[allow(unused_imports)]
pub use create_snapshot::*;
#[allow(unused_imports)]
pub use create_subnet::*;
#[allow(unused_imports)]
pub use create_tags::*;
#[allow(unused_imports)]
pub use create_transit_gateway::*;
#[allow(unused_imports)]
pub use create_volume::*;
#[allow(unused_imports)]
pub use create_vpc::*;
#[allow(unused_imports)]
pub use create_vpc_endpoint::*;
#[allow(unused_imports)]
pub use create_vpc_peering_connection::*;
#[allow(unused_imports)]
pub use create_vpn_connection::*;
#[allow(unused_imports)]
pub use create_vpn_gateway::*;
#[allow(unused_imports)]
pub use delete_customer_gateway::*;
#[allow(unused_imports)]
pub use delete_flow_logs::*;
#[allow(unused_imports)]
pub use delete_key_pair::*;
#[allow(unused_imports)]
pub use delete_launch_template::*;
#[allow(unused_imports)]
pub use delete_nat_gateway::*;
#[allow(unused_imports)]
pub use delete_network_acl::*;
#[allow(unused_imports)]
pub use delete_network_acl_entry::*;
#[allow(unused_imports)]
pub use delete_network_interface::*;
#[allow(unused_imports)]
pub use delete_placement_group::*;
#[allow(unused_imports)]
pub use delete_route::*;
#[allow(unused_imports)]
pub use delete_route_table::*;
#[allow(unused_imports)]
pub use delete_security_group::*;
#[allow(unused_imports)]
pub use delete_snapshot::*;
#[allow(unused_imports)]
pub use delete_subnet::*;
#[allow(unused_imports)]
pub use delete_tags::*;
#[allow(unused_imports)]
pub use delete_transit_gateway::*;
#[allow(unused_imports)]
pub use delete_volume::*;
#[allow(unused_imports)]
pub use delete_vpc::*;
#[allow(unused_imports)]
pub use delete_vpc_endpoints::*;
#[allow(unused_imports)]
pub use delete_vpc_peering_connection::*;
#[allow(unused_imports)]
pub use delete_vpn_connection::*;
#[allow(unused_imports)]
pub use delete_vpn_gateway::*;
#[allow(unused_imports)]
pub use deregister_image::*;
#[allow(unused_imports)]
pub use describe_addresses::*;
#[allow(unused_imports)]
pub use describe_availability_zones::*;
#[allow(unused_imports)]
pub use describe_customer_gateways::*;
#[allow(unused_imports)]
pub use describe_flow_logs::*;
#[allow(unused_imports)]
pub use describe_images::*;
#[allow(unused_imports)]
pub use describe_instance_status::*;
#[allow(unused_imports)]
pub use describe_instance_types::*;
#[allow(unused_imports)]
pub use describe_instances::*;
#[allow(unused_imports)]
pub use describe_internet_gateways::*;
#[allow(unused_imports)]
pub use describe_key_pairs::*;
#[allow(unused_imports)]
pub use describe_launch_templates::*;
#[allow(unused_imports)]
pub use describe_nat_gateways::*;
#[allow(unused_imports)]
pub use describe_network_acls::*;
#[allow(unused_imports)]
pub use describe_network_interfaces::*;
#[allow(unused_imports)]
pub use describe_placement_groups::*;
#[allow(unused_imports)]
pub use describe_regions::*;
#[allow(unused_imports)]
pub use describe_reserved_instances::*;
#[allow(unused_imports)]
pub use describe_route_tables::*;
#[allow(unused_imports)]
pub use describe_security_groups::*;
#[allow(unused_imports)]
pub use describe_snapshots::*;
#[allow(unused_imports)]
pub use describe_spot_instance_requests::*;
#[allow(unused_imports)]
pub use describe_subnets::*;
#[allow(unused_imports)]
pub use describe_tags::*;
#[allow(unused_imports)]
pub use describe_transit_gateway_attachments::*;
#[allow(unused_imports)]
pub use describe_transit_gateways::*;
#[allow(unused_imports)]
pub use describe_volumes::*;
#[allow(unused_imports)]
pub use describe_vpc_endpoints::*;
#[allow(unused_imports)]
pub use describe_vpc_peering_connections::*;
#[allow(unused_imports)]
pub use describe_vpcs::*;
#[allow(unused_imports)]
pub use describe_vpn_connections::*;
#[allow(unused_imports)]
pub use describe_vpn_gateways::*;
#[allow(unused_imports)]
pub use detach_internet_gateway::*;
#[allow(unused_imports)]
pub use detach_network_interface::*;
#[allow(unused_imports)]
pub use detach_volume::*;
#[allow(unused_imports)]
pub use detach_vpn_gateway::*;
#[allow(unused_imports)]
pub use disassociate_address::*;
#[allow(unused_imports)]
pub use disassociate_route_table::*;
#[allow(unused_imports)]
pub use get_console_output::*;
#[allow(unused_imports)]
pub use import_key_pair::*;
#[allow(unused_imports)]
pub use modify_instance_attribute::*;
#[allow(unused_imports)]
pub use modify_instance_metadata_options::*;
#[allow(unused_imports)]
pub use modify_subnet_attribute::*;
#[allow(unused_imports)]
pub use modify_volume::*;
#[allow(unused_imports)]
pub use modify_vpc_attribute::*;
#[allow(unused_imports)]
pub use reboot_instances::*;
#[allow(unused_imports)]
pub use register_image::*;
#[allow(unused_imports)]
pub use release_address::*;
#[allow(unused_imports)]
pub use request_spot_instances::*;
#[allow(unused_imports)]
pub use revoke_security_group_egress::*;
#[allow(unused_imports)]
pub use revoke_security_group_ingress::*;
#[allow(unused_imports)]
pub use run_instances::*;
#[allow(unused_imports)]
pub use start_instances::*;
#[allow(unused_imports)]
pub use stop_instances::*;
#[allow(unused_imports)]
pub use terminate_instances::*;
