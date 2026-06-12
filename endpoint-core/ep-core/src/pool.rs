use deadpool::managed::{Manager, Object};
use std::ops::{Deref, DerefMut};

/// Wraps a pooled connection and removes it from the pool on drop unless
/// explicitly disarmed via [`disarm`](PoisonGuard::disarm). This prevents a
/// cancelled future from returning a connection that still has an unread
/// backend response buffered on the socket.
pub struct PoisonGuard<M: Manager> {
    conn: Option<Object<M>>,
}

impl<M: Manager> PoisonGuard<M> {
    pub fn new(conn: Object<M>) -> Self {
        Self { conn: Some(conn) }
    }

    pub fn disarm(mut self) {
        drop(self.conn.take());
    }
}

impl<M: Manager> Deref for PoisonGuard<M> {
    type Target = M::Type;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("PoisonGuard already consumed")
    }
}

impl<M: Manager> DerefMut for PoisonGuard<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("PoisonGuard already consumed")
    }
}

impl<M: Manager> Drop for PoisonGuard<M> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _dropped = Object::take(conn);
        }
    }
}

/// Like [`PoisonGuard`] but for connections that persist across multiple
/// operations (e.g. WATCH/MULTI/EXEC sequences). Poisons on drop; call
/// [`release`](PinnedGuard::release) to return cleanly to the pool.
pub struct PinnedGuard<M: Manager> {
    conn: Option<Object<M>>,
}

impl<M: Manager> PinnedGuard<M> {
    pub fn empty() -> Self {
        Self { conn: None }
    }

    pub fn insert(&mut self, conn: Object<M>) {
        self.poison();
        self.conn = Some(conn);
    }

    pub fn is_some(&self) -> bool {
        self.conn.is_some()
    }

    pub fn as_deref_mut(&mut self) -> Option<&mut M::Type> {
        self.conn.as_deref_mut()
    }

    /// Return the connection to the pool without poisoning.
    pub fn release(&mut self) {
        drop(self.conn.take());
    }

    /// Remove from pool (do not recycle).
    pub fn poison(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _dropped = Object::take(conn);
        }
    }

    /// Take the raw Object out (caller assumes responsibility).
    pub fn take(&mut self) -> Option<Object<M>> {
        self.conn.take()
    }
}

impl<M: Manager> Drop for PinnedGuard<M> {
    fn drop(&mut self) {
        self.poison();
    }
}
