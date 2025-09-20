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
    pub fn new<T: Into<String>>(value: T, deletion_time: Option<tokio::time::Instant>) -> Self {
        let value = value.into();
        Self {
            value,
            deletion_time,
        }
    }
}

// --- Redis store ---
pub type Store = Arc<Mutex<Box<HashMap<String, Entry>>>>;

/// Creates a new Redis store.
pub fn new() -> Store {
    Arc::new(Mutex::new(Box::new(HashMap::new())))
}
