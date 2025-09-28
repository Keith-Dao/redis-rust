//! This module contains the Redis store.
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// --- Store entry ---
#[derive(PartialEq, Debug)]
/// An entry value.
pub enum EntryValue {
    String(String),
}

impl EntryValue {}

#[derive(PartialEq, Debug)]
/// An entry in the Redis store.
pub struct Entry {
    pub value: EntryValue,
    pub deletion_time: Option<tokio::time::Instant>,
}

impl Entry {
    /// Creates a new Redis entry for a string.
    pub fn new_string<T: Into<String>>(value: T) -> Self {
        let value = EntryValue::String(value.into());
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

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    // --- Tests ---
    #[rstest]
    fn test_new() {
        let value = "value";
        let expected = Entry {
            value: EntryValue::String(value.into()),
            deletion_time: None,
        };
        assert_eq!(expected, Entry::new_string(value));
    }

    #[rstest]
    #[tokio::test]
    async fn test_with_deletion() {
        tokio::time::pause();
        let value = "value";
        let duration = 100;
        let expected = Entry {
            value: EntryValue::String(value.into()),
            deletion_time: Some(
                tokio::time::Instant::now() + tokio::time::Duration::from_millis(duration),
            ),
        };
        assert_eq!(expected, Entry::new_string(value).with_deletion(duration));
    }
}
