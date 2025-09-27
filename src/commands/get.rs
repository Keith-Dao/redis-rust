//! This module contains the GET command.
use crate::{resp, store};
use anyhow::{Context, Result};

/// Parses the GET options.
fn parse_get_options<I: IntoIterator<Item = resp::RespType>>(iter: I) -> Result<String> {
    let mut iter = iter.into_iter();
    let key = resp::extract_string(&iter.next().ok_or(anyhow::anyhow!("Missing key option."))?)
        .context("Failed to extract key.")?;
    Ok(key)
}

/// Handles the GET command.
pub async fn handle(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
    let key = match parse_get_options(args.into_iter()) {
        Ok(result) => result,
        Err(err) => {
            log::error!("{err}");
            return resp::RespType::BulkError(format!("ERR {err} for 'GET' command"));
        }
    };

    let mut store = store.lock().await;
    let missing_value = resp::RespType::Null();
    match store.entry(key) {
        std::collections::hash_map::Entry::Occupied(entry) => {
            if let Some(deletion_time) = entry.get().deletion_time {
                if deletion_time <= tokio::time::Instant::now() {
                    entry.remove_entry();
                    return missing_value;
                }
            }

            resp::RespType::BulkString(Some(entry.get().value.clone()))
        }
        _ => missing_value,
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
        assert_eq!(response, resp::RespType::Null());
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
        assert_eq!(response, resp::RespType::Null());

        assert!(store.lock().await.get(&key).is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_expiry(store: crate::store::Store, key: String, value: String) {
        tokio::time::pause();
        let deletion_time = 300;
        store.lock().await.insert(
            key.clone(),
            crate::store::Entry::new(value.clone()).with_deletion(deletion_time),
        );

        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle(args.clone(), &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value)));

        tokio::time::advance(tokio::time::Duration::from_millis(deletion_time)).await;
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::Null());
        assert!(store.lock().await.get("expiredkey").is_none());
    }
}
