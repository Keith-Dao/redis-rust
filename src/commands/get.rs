//! This module contains the GET command.
use crate::{resp, store};

/// Handles the GET command.
pub async fn handle(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
    match args.as_slice() {
        [key] | [key, ..] => {
            let key = match resp::extract_string(key) {
                Ok(key) => key,
                _ => panic!("Invalid GET arguments: {:?}", args),
            };

            let mut store = store.lock().await;
            match store.get(&key) {
                Some(store::Entry {
                    value,
                    deletion_time,
                }) => match deletion_time {
                    Some(deletion_time) if deletion_time <= &tokio::time::Instant::now() => {
                        store.remove(&key);
                        resp::RespType::BulkString(None)
                    }
                    _ => resp::RespType::BulkString(Some(value.clone())),
                },
                None => resp::RespType::BulkString(None),
            }
        }

        _ => panic!("Invalid"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};

    // --- Fixtures ---
    #[fixture]
    fn store() -> crate::store::Store {
        crate::store::new()
    }

    #[fixture]
    fn key() -> String {
        "key".into()
    }

    #[fixture]
    fn value() -> String {
        "value".into()
    }

    // --- Tests ---
    #[rstest]
    #[tokio::test]
    async fn test_handle_existing(store: crate::store::Store, key: String, value: String) {
        store
            .lock()
            .await
            .insert(key.clone(), crate::store::Entry::new(value.clone()));

        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value)));
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_non_existing(store: crate::store::Store, key: String) {
        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(None));
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_expired_key(store: crate::store::Store, key: String, value: String) {
        let deletion_time = 0u32;
        store.lock().await.insert(
            key.clone(),
            crate::store::Entry::new(value.clone()).with_deletion(deletion_time),
        );

        let args = vec![resp::RespType::SimpleString(key.clone())];
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(None));

        assert!(store.lock().await.get(&key).is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_expiry(store: crate::store::Store, key: String, value: String) {
        let deletion_time = 300;
        store.lock().await.insert(
            key.clone(),
            crate::store::Entry::new(value.clone()).with_deletion(deletion_time),
        );

        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle(args.clone(), &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value)));

        tokio::time::sleep(tokio::time::Duration::from_millis(deletion_time)).await;
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(None));
        assert!(store.lock().await.get("expiredkey").is_none());
    }
}
