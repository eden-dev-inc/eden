pub mod change_message_visibility;
pub mod create_queue;
pub mod delete_message;
pub mod delete_queue;
pub mod get_queue_attributes;
pub mod get_queue_url;
pub mod list_queue_tags;
pub mod list_queues;
pub mod purge_queue;
pub mod receive_message;
pub mod send_message;
pub mod set_queue_attributes;
pub mod tag_queue;
pub mod untag_queue;

#[allow(unused_imports)]
pub use change_message_visibility::*;
#[allow(unused_imports)]
pub use create_queue::*;
#[allow(unused_imports)]
pub use delete_message::*;
#[allow(unused_imports)]
pub use delete_queue::*;
#[allow(unused_imports)]
pub use get_queue_attributes::*;
#[allow(unused_imports)]
pub use get_queue_url::*;
#[allow(unused_imports)]
pub use list_queue_tags::*;
#[allow(unused_imports)]
pub use list_queues::*;
#[allow(unused_imports)]
pub use purge_queue::*;
#[allow(unused_imports)]
pub use receive_message::*;
#[allow(unused_imports)]
pub use send_message::*;
#[allow(unused_imports)]
pub use set_queue_attributes::*;
#[allow(unused_imports)]
pub use tag_queue::*;
#[allow(unused_imports)]
pub use untag_queue::*;
