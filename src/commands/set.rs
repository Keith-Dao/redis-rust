//! This module contains the SET command.
use crate::{resp, store};
use anyhow::{Context, Result};

/// Parses the SET options and returns the entry if successful.
fn parse_set_options<I: IntoIterator<Item = resp::RespType>>(iter: I) -> Result<store::Entry> {
    let mut iter = iter.into_iter();
    let value = resp::extract_string(&iter.next().ok_or(anyhow::anyhow!("Missing value"))?)
        .context("Failed to extract value")?;

    let mut entry = store::Entry::new(value);
    while let Some(token) = &iter.next() {
        let option = resp::extract_string(token).context("Failed to extract option")?;

        match option.to_lowercase().as_str() {
            "px" => {
                let duration = resp::extract_string(
                    &iter
                        .next()
                        .ok_or(anyhow::anyhow!("Missing milliseconds for PX option"))?,
                )
                .context("Failed to extract duration string")?
                .parse::<u64>()
                .context("Failed to convert PX duration string to a number")?;
                entry = entry.with_deletion(duration);
            }
            _ => {
                return Err(anyhow::anyhow!("{option} is not a valid option"));
            }
        }
    }

    Ok(entry)
}
/// Handles the SET command.
pub async fn handle(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
    let mut args = args.into_iter();

    let key = match args
        .next()
        .context("Missing key")
        .and_then(|key_token| resp::extract_string(&key_token).context("Failed to extract key"))
    {
        Ok(result) => result,
        Err(err) => {
            log::error!("Failed to extract the key. Err: {err}");
            return resp::RespType::BulkError(format!("ERR {err} for 'SET' command"));
        }
    };

    let entry = match parse_set_options(args) {
        Ok(result) => result,
        Err(err) => {
            log::error!("{err}");
            return resp::RespType::BulkError(format!("ERR {err} for 'SET' command"));
        }
    };

    store.lock().await.insert(key, entry);
    resp::RespType::SimpleString("OK".into())
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
    async fn test_handle_basic(store: crate::store::Store, key: String, value: String) {
        let args = vec![
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
        ];
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::SimpleString("OK".into()));

        let stored_value = store.lock().await.get(&key).unwrap().value.clone();
        assert_eq!(stored_value, value);
    }

    #[rstest]
    #[case::px_upper("PX")]
    #[case::px_lower("px")]
    #[tokio::test]
    async fn test_handle_with_px(
        store: crate::store::Store,
        key: String,
        value: String,
        #[case] px: String,
    ) {
        tokio::time::pause();
        let duration = 100;
        let args = vec![
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
            resp::RespType::SimpleString(px),
            resp::RespType::SimpleString(duration.to_string()), // 100 milliseconds
        ];
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::SimpleString("OK".into()));

        let store = store.lock().await;
        let entry = store.get(&key).unwrap();
        assert_eq!(value, value);
        assert!(entry.deletion_time.is_some());

        tokio::time::advance(tokio::time::Duration::from_millis(duration)).await;
        assert_eq!(
            entry.deletion_time.expect("Checked it is some."),
            tokio::time::Instant::now()
        );
    }

    // --- Errors ---
    #[rstest]
    #[tokio::test]
    async fn test_handle_missing_key(store: crate::store::Store) {
        let args = vec![];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError("ERR Missing key for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_key(store: crate::store::Store) {
        let args = vec![resp::RespType::Array(vec![])];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError("ERR Failed to extract key for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_missing_value(store: crate::store::Store, key: String) {
        let args = vec![resp::RespType::BulkString(Some(key))];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError("ERR Missing value for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_value(store: crate::store::Store, key: String) {
        let args = vec![
            resp::RespType::BulkString(Some(key)),
            resp::RespType::Array(vec![]),
        ];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError("ERR Failed to extract value for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_option(store: crate::store::Store, key: String, value: String) {
        let args = vec![
            resp::RespType::BulkString(Some(key)),
            resp::RespType::BulkString(Some(value)),
            resp::RespType::BulkString(Some("invalid option".into())),
        ];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError(
                "ERR invalid option is not a valid option for 'SET' command".into()
            ),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_option_type(
        store: crate::store::Store,
        key: String,
        value: String,
    ) {
        let args = vec![
            resp::RespType::BulkString(Some(key)),
            resp::RespType::BulkString(Some(value)),
            resp::RespType::Array(vec![]),
        ];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError("ERR Failed to extract option for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_missing_px_value(store: crate::store::Store, key: String, value: String) {
        let args = vec![
            resp::RespType::BulkString(Some(key)),
            resp::RespType::BulkString(Some(value)),
            resp::RespType::BulkString(Some("px".into())),
        ];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError(
                "ERR Missing milliseconds for PX option for 'SET' command".into()
            ),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_px_value(store: crate::store::Store, key: String, value: String) {
        let args = vec![
            resp::RespType::BulkString(Some(key)),
            resp::RespType::BulkString(Some(value)),
            resp::RespType::BulkString(Some("px".into())),
            resp::RespType::BulkString(Some("abc".into())),
        ];
        let response = handle(args, &store).await;
        assert_eq!(
            resp::RespType::BulkError(
                "ERR Failed to convert PX duration string to a number for 'SET' command".into()
            ),
            response
        );
    }
}
