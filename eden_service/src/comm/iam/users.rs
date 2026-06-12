pub mod analytics_prefs;
pub mod delete;
pub mod get;
pub mod me;
pub mod patch;
pub mod post;

#[allow(ambiguous_glob_reexports)]
pub use self::{delete::*, get::*, me::*, patch::*, post::*};
