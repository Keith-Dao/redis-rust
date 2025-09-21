//! This module contains the SET command.
use crate::{resp, store};
use anyhow::{Context, Result};

/// Parses the SET options and returns the entry if successful.
fn parse_set_options<I: IntoIterator<Item = resp::RespType>>(iter: I) -> Result<store::Entry> {
    let mut iter = iter.into_iter();
    let value = resp::extract_string(
        &iter
            .next()
            .ok_or(anyhow::anyhow!("Missing value option."))?,
    )
    .context("Failed to extract value.")?;
    let mut entry = store::Entry::new(value);

    while let Some(token) = &iter.next() {
        let option = resp::extract_string(token).context("Failed to extract option.")?;

        match option.to_lowercase().as_str() {
            "px" => {
                let duration = resp::extract_string(
                    &iter
                        .next()
                        .ok_or(anyhow::anyhow!("Missing milliseconds for PX option."))?,
                )
                .context("Failed to extract duration string.")?
                .parse::<u64>()
                .context("Failed to convert duration string to a number.")?;
                entry = entry.with_deletion(duration);
            }
            _ => {
                return Err(anyhow::anyhow!("An invalid option was provided: {option}."));
            }
        }
    }

    Ok(entry)
}
/// Handles the SET command.
pub async fn handle(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
    let failure_result = resp::RespType::Null();
    let mut args = args.into_iter();

    let key;
    if let Some(key_token) = &args.next() {
        if let Ok(result) = resp::extract_string(key_token) {
            key = result;
        } else {
            log::error!("Failed to extract key string from: {:?}", key_token);
            return failure_result;
        }
    } else {
        log::error!("Key was not provided.");
        return failure_result;
    }

    let entry;
    match parse_set_options(args) {
        Ok(result) => {
            entry = result;
        }
        Err(err) => {
            log::error!("{err}");
            return failure_result;
        }
    }

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
    #[tokio::test]
    async fn test_handle_with_px(store: crate::store::Store, key: String, value: String) {
        let duration = 100;
        let args = vec![
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
            resp::RespType::SimpleString("PX".into()),
            resp::RespType::SimpleString(duration.to_string()), // 100 milliseconds
        ];
        let response = handle(args, &store).await;
        assert_eq!(response, resp::RespType::SimpleString("OK".into()));

        let store = store.lock().await;
        let entry = store.get(&key).unwrap();
        assert_eq!(value, value);
        assert!(entry.deletion_time.is_some());

        tokio::time::sleep(tokio::time::Duration::from_millis(duration)).await;
        assert!(
            entry.deletion_time.expect("Checked it is some.") <= tokio::time::Instant::now(),
            "Deletion timestamp should be before now."
        );
    }
}
