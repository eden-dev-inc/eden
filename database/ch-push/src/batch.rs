use std::mem;

#[derive(Debug)]
pub struct BatchBuffer<T> {
    items: Vec<T>,
    flush_at: usize,
    max_size: usize,
}

impl<T> BatchBuffer<T> {
    pub fn new(flush_at: usize, max_size: usize) -> Self {
        debug_assert!(flush_at > 0);
        debug_assert!(flush_at <= max_size || max_size == usize::MAX);
        Self { items: Vec::with_capacity(flush_at), flush_at, max_size }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn should_flush(&self) -> bool {
        self.items.len() >= self.flush_at
    }

    pub fn can_fit(&self, incoming: usize) -> bool {
        self.items.len().saturating_add(incoming) <= self.max_size
    }

    pub fn push(&mut self, item: T) -> bool {
        if self.items.len() < self.max_size {
            self.items.push(item);
            true
        } else {
            false
        }
    }

    pub fn extend(&mut self, mut items: Vec<T>) {
        self.items.append(&mut items);
    }

    pub fn take(&mut self) -> Vec<T> {
        mem::take(&mut self.items)
    }

    pub fn restore(&mut self, items: Vec<T>) {
        self.items = items;
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn as_slice(&self) -> &[T] {
        &self.items
    }
}
