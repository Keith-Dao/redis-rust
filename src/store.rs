//! This module contains the Redis store.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

// === Redis store ===
pub type Store = Arc<Mutex<Box<HashMap<String, (String, Option<Instant>)>>>>;

/// Creates a new Redis store.
pub fn new() -> Store {
    Arc::new(Mutex::new(Box::new(HashMap::new())))
}
