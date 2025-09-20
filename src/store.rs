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
    pub fn with_deletion<T: Into<u64>>(mut self, delete_timer_duration_ms: T) -> Self {
        let delete_timer_duration_ms = delete_timer_duration_ms.into();
        let deletion_time = tokio::time::Instant::now()
            + tokio::time::Duration::from_millis(delete_timer_duration_ms);
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
