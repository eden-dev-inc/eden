//! Redis command dispatch classification.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreDispatchHandling {
    ExplicitLocalState,
    GenericForward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandDispatchPath {
    PolicyOverride,
    PinnedConnection,
    RoutedConnection,
}

pub(crate) struct RedisDispatch;

impl RedisDispatch {
    #[inline]
    pub(crate) fn pre_dispatch_handling(command: &RedisApi) -> PreDispatchHandling {
        match command {
            RedisApi::Watch
            | RedisApi::Unwatch
            | RedisApi::Multi
            | RedisApi::Exec
            | RedisApi::Discard
            | RedisApi::Psync
            | RedisApi::Subscribe
            | RedisApi::Psubscribe
            | RedisApi::Ssubscribe
            | RedisApi::Unsubscribe
            | RedisApi::Punsubscribe
            | RedisApi::Sunsubscribe
            | RedisApi::Auth
            | RedisApi::Select => PreDispatchHandling::ExplicitLocalState,
            _ => PreDispatchHandling::GenericForward,
        }
    }

    #[inline]
    pub(crate) fn command_path(has_policy_override: bool, has_pinned_connection: bool) -> CommandDispatchPath {
        if has_policy_override {
            CommandDispatchPath::PolicyOverride
        } else if has_pinned_connection {
            CommandDispatchPath::PinnedConnection
        } else {
            CommandDispatchPath::RoutedConnection
        }
    }

    #[inline]
    pub(crate) fn should_capture_replication_bytes(
        is_write: bool,
        was_policy_blocked: bool,
        has_replication_manager: bool,
        allow_replication_stream: bool,
    ) -> bool {
        is_write && !was_policy_blocked && has_replication_manager && allow_replication_stream
    }
}
