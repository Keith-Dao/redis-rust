//! This module contains the SET command.
use crate::commands::Command;
use anyhow::{Context, Result};

/// Parses the SET options.
fn parse_set_options<I: IntoIterator<Item = crate::resp::RespType>>(
    iter: I,
) -> Result<(String, crate::store::Entry)> {
    let mut iter = iter.into_iter();

    let key = crate::resp::extract_string(&iter.next().context("Missing key")?)
        .context("Failed to extract key")?;

    let value = crate::resp::extract_string(&iter.next().ok_or(anyhow::anyhow!("Missing value"))?)
        .context("Failed to extract value")?;
    let mut entry = crate::store::Entry::new_string(value);
    while let Some(token) = &iter.next() {
        let option = crate::resp::extract_string(token).context("Failed to extract option")?;

        match option.to_lowercase().as_str() {
            "px" => {
                let duration = crate::resp::extract_string(
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

    Ok((key, entry))
}

pub struct Set;

#[async_trait::async_trait]
impl Command for Set {
    fn name(&self) -> String {
        "SET".into()
    }

    /// Handles the SET command.
    async fn handle(
        &self,
        args: Vec<crate::resp::RespType>,
        store: &crate::store::SharedStore,
        _: &mut crate::state::State,
    ) -> crate::resp::RespType {
        let (key, entry) = match parse_set_options(args) {
            Ok(result) => result,
            Err(err) => {
                log::error!("{err}");
                return crate::resp::RespType::BulkError(format!("ERR {err} for 'SET' command"));
            }
        };

        store.lock().await.insert(key, entry);
        crate::resp::RespType::SimpleString("OK".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};

    // --- Fixtures ---
    #[fixture]
    fn store() -> crate::store::SharedStore {
        crate::store::new()
    }

    #[fixture]
    fn state() -> crate::state::State {
        crate::state::State::new(0)
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
    fn test_name() {
        assert_eq!("SET", Set.name());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_basic(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
    ) {
        let args = vec![
            crate::resp::RespType::SimpleString(key.clone()),
            crate::resp::RespType::SimpleString(value.clone()),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(response, crate::resp::RespType::SimpleString("OK".into()));

        let mut store = store.lock().await;
        let entry = store.get(&key).unwrap();
        let expected = crate::store::Entry::new_string(value.clone());
        assert_eq!(expected, *entry);
    }

    #[rstest]
    #[case::px_upper("PX")]
    #[case::px_lower("px")]
    #[tokio::test]
    async fn test_handle_with_px(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
        #[case] px: String,
    ) {
        tokio::time::pause();
        let duration = 100;
        let args = vec![
            crate::resp::RespType::SimpleString(key.clone()),
            crate::resp::RespType::SimpleString(value.clone()),
            crate::resp::RespType::SimpleString(px),
            crate::resp::RespType::SimpleString(duration.to_string()), // 100 milliseconds
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(response, crate::resp::RespType::SimpleString("OK".into()));

        let mut store = store.lock().await;
        let entry = store.get(&key).unwrap();
        let expected =
            crate::store::Entry::new_string(value.clone()).with_deletion(duration as u64);
        assert_eq!(expected, *entry);
    }

    #[rstest]
    #[case::string(crate::store::Entry::new_string("old value"))]
    #[case::list(crate::store::Entry::new_list())]
    #[tokio::test]
    async fn test_handle_replace(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
        #[case] old_entry: crate::store::Entry,
    ) {
        store.lock().await.insert(key.clone(), old_entry);

        let args = vec![
            crate::resp::RespType::SimpleString(key.clone()),
            crate::resp::RespType::SimpleString(value.clone()),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(response, crate::resp::RespType::SimpleString("OK".into()));

        let mut store = store.lock().await;
        let entry = store.get(&key).unwrap();
        let expected = crate::store::Entry::new_string(value.clone());
        assert_eq!(expected, *entry);
    }

    // --- Errors ---
    #[rstest]
    #[tokio::test]
    async fn test_handle_missing_key(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
    ) {
        let args = vec![];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError("ERR Missing key for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_key(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
    ) {
        let args = vec![crate::resp::RespType::Array(vec![])];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError("ERR Failed to extract key for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_missing_value(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
    ) {
        let args = vec![crate::resp::RespType::BulkString(Some(key))];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError("ERR Missing value for 'SET' command".into()),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_value(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
    ) {
        let args = vec![
            crate::resp::RespType::BulkString(Some(key)),
            crate::resp::RespType::Array(vec![]),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError(
                "ERR Failed to extract value for 'SET' command".into()
            ),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_option(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
    ) {
        let args = vec![
            crate::resp::RespType::BulkString(Some(key)),
            crate::resp::RespType::BulkString(Some(value)),
            crate::resp::RespType::BulkString(Some("invalid option".into())),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError(
                "ERR invalid option is not a valid option for 'SET' command".into()
            ),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_option_type(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
    ) {
        let args = vec![
            crate::resp::RespType::BulkString(Some(key)),
            crate::resp::RespType::BulkString(Some(value)),
            crate::resp::RespType::Array(vec![]),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError(
                "ERR Failed to extract option for 'SET' command".into()
            ),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_missing_px_value(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
    ) {
        let args = vec![
            crate::resp::RespType::BulkString(Some(key)),
            crate::resp::RespType::BulkString(Some(value)),
            crate::resp::RespType::BulkString(Some("px".into())),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError(
                "ERR Missing milliseconds for PX option for 'SET' command".into()
            ),
            response
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_invalid_px_value(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        key: String,
        value: String,
    ) {
        let args = vec![
            crate::resp::RespType::BulkString(Some(key)),
            crate::resp::RespType::BulkString(Some(value)),
            crate::resp::RespType::BulkString(Some("px".into())),
            crate::resp::RespType::BulkString(Some("abc".into())),
        ];
        let response = Set.handle(args, &store, &mut state).await;
        assert_eq!(
            crate::resp::RespType::BulkError(
                "ERR Failed to convert PX duration string to a number for 'SET' command".into()
            ),
            response
        );
    }
}
