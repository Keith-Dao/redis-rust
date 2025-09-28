//! This module contains the Redis store.
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// --- Store entry ---
#[derive(PartialEq, Debug, Clone)]
/// An entry value.
pub enum EntryValue {
    List(Vec<String>),
    String(String),
}

impl EntryValue {}

#[derive(PartialEq, Debug, Clone)]
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

    /// Creates a new Redis entry for a list.
    pub fn new_list() -> Self {
        let value = EntryValue::List(Vec::new());
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
#[derive(Debug, PartialEq)]
/// The Redis store.
pub struct Store {
    store: HashMap<String, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    /// Removes an entry from the store if it has expired.
    fn remove_if_expired<T: std::borrow::Borrow<str> + ?Sized>(&mut self, key: &T) {
        let key = key.borrow();
        match self.store.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                if let Some(deletion_time) = entry.get().deletion_time {
                    if deletion_time <= tokio::time::Instant::now() {
                        entry.remove_entry();
                    }
                }
            }
            _ => (),
        }
    }

    /// Gets the given key's entry and removes the entry if it has expired.
    pub fn entry(&mut self, key: String) -> std::collections::hash_map::Entry<String, Entry> {
        self.remove_if_expired(&key);
        self.store.entry(key)
    }

    /// Inserts a key-value pair irrespective of the key already existing.
    pub fn insert(&mut self, key: String, value: Entry) -> Option<Entry> {
        self.remove_if_expired(&key);
        self.store.insert(key, value)
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<T>(&mut self, key: &T) -> Option<&Entry>
    where
        T: std::hash::Hash + Eq + ?Sized,
        T: std::borrow::Borrow<str>,
        String: std::borrow::Borrow<T>,
    {
        self.remove_if_expired(key);
        self.store.get(key)
    }
}

pub type SharedStore = Arc<Mutex<Box<Store>>>;

/// Creates a new Redis store.
pub fn new() -> SharedStore {
    Arc::new(Mutex::new(Box::new(Store::new())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // --- Fixtures ---
    #[rstest::fixture]
    fn store() -> Store {
        Store::new()
    }

    #[rstest::fixture]
    fn key() -> String {
        "key".into()
    }

    #[rstest::fixture]
    fn value() -> Entry {
        Entry::new_string("value")
    }

    // --- Tests ---
    // ---- Entry ----
    #[rstest]
    fn test_entry_string() {
        let value = "value";
        let expected = Entry {
            value: EntryValue::String(value.into()),
            deletion_time: None,
        };
        assert_eq!(expected, Entry::new_string(value));
    }

    #[rstest]
    fn test_entry_list() {
        let expected = Entry {
            value: EntryValue::List(vec![]),
            deletion_time: None,
        };
        assert_eq!(expected, Entry::new_list());
    }

    #[rstest]
    #[tokio::test]
    async fn test_entry_with_deletion() {
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

    // ---- Store ----
    #[rstest]
    fn test_store_new() {
        let expected = Store {
            store: std::collections::HashMap::new(),
        };
        assert_eq!(expected, Store::new());
    }

    #[rstest]
    fn test_store_insert(mut store: Store, key: String, value: Entry) {
        store.insert(key.clone(), value.clone());
        let result = store.store.get(&key).expect("Entry should be insterted.");
        assert_eq!(value, *result);
    }

    #[rstest]
    fn test_store_insert_overwrite_existing(mut store: Store, key: String, value: Entry) {
        store
            .store
            .insert(key.clone(), Entry::new_string("old value"));
        store.insert(key.clone(), value.clone());
        let result = store.store.get(&key).expect("Entry should be insterted.");
        assert_eq!(value, *result);
    }

    #[rstest]
    #[tokio::test]
    async fn test_store_insert_overwrite_expired(mut store: Store, key: String, value: Entry) {
        tokio::time::pause();
        let duration = 100u64;
        store.store.insert(
            key.clone(),
            Entry::new_string("old value").with_deletion(duration),
        );

        tokio::time::advance(tokio::time::Duration::from_millis(duration)).await;
        store.insert(key.clone(), value.clone());
        let result = store.store.get(&key).expect("Entry should be insterted.");
        assert_eq!(value, *result);
    }

    #[rstest]
    fn test_store_entry_occupied(mut store: Store, key: String, value: Entry) {
        store.store.insert(key.clone(), value.clone());
        match store.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                assert_eq!(value, *entry.get());
            }
            _ => panic!("Entry should be occupied."),
        }
    }

    #[rstest]
    fn test_store_entry_vacant(mut store: Store, key: String) {
        match store.entry(key) {
            std::collections::hash_map::Entry::Vacant(_) => {}
            _ => panic!("Entry should be vacant."),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_store_entry_with_deletion(mut store: Store, key: String, mut value: Entry) {
        tokio::time::pause();
        let duration = 10;

        value = value.with_deletion(duration);
        store.store.insert(key.clone(), value.clone());
        match store.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                assert_eq!(value, *entry.get());
            }
            _ => panic!("Entry should be occupied."),
        }

        tokio::time::advance(tokio::time::Duration::from_millis(duration)).await;
        match store.entry(key) {
            std::collections::hash_map::Entry::Vacant(_) => {}
            _ => panic!("Entry should be vacant."),
        }
    }

    #[rstest]
    fn test_store_get_occupied(mut store: Store, key: String, value: Entry) {
        store.store.insert(key.clone(), value.clone());
        match store.get(&key) {
            Some(result) => {
                assert_eq!(value, *result);
            }
            _ => panic!("Entry should exist."),
        }
    }

    #[rstest]
    fn test_store_get_vacant(mut store: Store, key: String) {
        match store.get(&key) {
            None => {}
            _ => panic!("Entry should not exist."),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_store_get_with_deletion(mut store: Store, key: String, mut value: Entry) {
        tokio::time::pause();
        let duration = 10;

        value = value.with_deletion(duration);
        store.store.insert(key.clone(), value.clone());
        match store.get(&key) {
            Some(result) => {
                assert_eq!(value, *result);
            }
            _ => panic!("Entry should exist."),
        }

        tokio::time::advance(tokio::time::Duration::from_millis(duration)).await;
        match store.get(&key) {
            None => {}
            _ => panic!("Entry should not exist."),
        }
    }

    // ---- Shared store ----
    #[rstest]
    #[tokio::test]
    async fn test_shared_store() {
        let shared_store = new();
        let store = shared_store.try_lock().expect("Should acquire lock");
        assert_eq!(HashMap::new(), store.store);
    }
}
