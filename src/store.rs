//! This module contains the Redis store.
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// --- Store entry ---
/// An entry in the Redis store.
pub struct Entry {
    pub value: String,
    pub deletion_time: Option<tokio::time::Instant>,
}

impl Entry {
    /// Creates a new Redis entry.
    pub fn new<T: Into<String>>(value: T) -> Self {
        let value = value.into();
        Self {
            value,
            deletion_time: None,
        }
    }

    /// Adds a deletion timer to the entry.
    pub fn with_deletion(mut self, deletion_time: tokio::time::Instant) -> Self {
        self.deletion_time = Some(deletion_time);
        self
    }
}

// --- Redis store ---
pub type Store = Arc<Mutex<Box<HashMap<String, Entry>>>>;

/// Creates a new Redis store.
pub fn new() -> Store {
    Arc::new(Mutex::new(Box::new(HashMap::new())))
}
