pub mod delete_alarms;
pub mod describe_alarm_history;
pub mod describe_alarms;
pub mod describe_anomaly_detectors;
pub mod disable_alarm_actions;
pub mod enable_alarm_actions;
pub mod get_dashboard;
pub mod get_metric_data;
pub mod get_metric_statistics;
pub mod list_dashboards;
pub mod list_metrics;
pub mod list_tags_for_resource;
pub mod put_dashboard;
pub mod put_metric_alarm;
pub mod put_metric_data;
pub mod set_alarm_state;
pub mod tag_resource;
pub mod untag_resource;

#[allow(unused_imports)]
pub use delete_alarms::*;
#[allow(unused_imports)]
pub use describe_alarm_history::*;
#[allow(unused_imports)]
pub use describe_alarms::*;
#[allow(unused_imports)]
pub use describe_anomaly_detectors::*;
#[allow(unused_imports)]
pub use disable_alarm_actions::*;
#[allow(unused_imports)]
pub use enable_alarm_actions::*;
#[allow(unused_imports)]
pub use get_dashboard::*;
#[allow(unused_imports)]
pub use get_metric_data::*;
#[allow(unused_imports)]
pub use get_metric_statistics::*;
#[allow(unused_imports)]
pub use list_dashboards::*;
#[allow(unused_imports)]
pub use list_metrics::*;
#[allow(unused_imports)]
pub use list_tags_for_resource::*;
#[allow(unused_imports)]
pub use put_dashboard::*;
#[allow(unused_imports)]
pub use put_metric_alarm::*;
#[allow(unused_imports)]
pub use put_metric_data::*;
#[allow(unused_imports)]
pub use set_alarm_state::*;
#[allow(unused_imports)]
pub use tag_resource::*;
#[allow(unused_imports)]
pub use untag_resource::*;
