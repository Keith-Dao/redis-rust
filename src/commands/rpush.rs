//! This module contains the RPUSH command.
use crate::{commands::Command, resp, store};
use anyhow::{Context, Result};

/// Parses the RPUSH options.
fn parse_options<I: IntoIterator<Item = resp::RespType>>(iter: I) -> Result<(String, Vec<String>)> {
    let mut iter = iter.into_iter();

    let key = resp::extract_string(&iter.next().context("Missing key")?)
        .context("Failed to extract key")?;

    let mut result = vec![];
    while let Some(token) = iter.next() {
        let value = resp::extract_string(&token).context("Failed to extract value")?;
        result.push(value);
    }
    if result.is_empty() {
        return Err(anyhow::anyhow!("At least one value must be provided"));
    }

    Ok((key, result))
}

pub struct Rpush;

#[async_trait::async_trait]
impl Command for Rpush {
    fn static_name() -> String {
        "RPUSH".into()
    }

    fn name(&self) -> String {
        Self::static_name()
    }

    /// Handles the RPUSH command.
    async fn handle(
        &self,
        args: Vec<resp::RespType>,
        store: &store::SharedStore,
    ) -> resp::RespType {
        let (key, values) = match parse_options(args) {
            Ok(result) => result,
            Err(err) => {
                log::error!("{err}");
                return resp::RespType::BulkError(format!("ERR {err} for 'RPUSH' command"));
            }
        };

        let mut store = store.lock().await;
        let entry_ref = store.entry(key.clone()).or_insert(store::Entry::new_list());
        let length = match &mut entry_ref.value {
            store::EntryValue::List(list) => {
                list.extend(values.into_iter());
                list.len()
            }
            _ => {
                return resp::RespType::BulkError(format!(
                    "WRONGTYPE Entry at key {key} is not a list"
                ))
            }
        };

        resp::RespType::Integer(length as i64)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::{fixture, rstest};

    // --- Fixtures ---
    #[fixture]
    fn store() -> crate::store::SharedStore {
        crate::store::new()
    }

    #[fixture]
    fn key() -> String {
        "key".into()
    }

    fn value() -> Vec<String> {
        vec!["value".into()]
    }

    fn values() -> Vec<String> {
        (0..5).map(|i| format!("value {i}")).collect()
    }

    #[fixture]
    fn existing_values() -> Vec<String> {
        (0..10).map(|i| format!("existing {i}")).collect()
    }

    fn make_args(key: &String, values: &Vec<String>) -> Vec<resp::RespType> {
        vec![resp::RespType::SimpleString(key.clone())]
            .into_iter()
            .chain(
                values
                    .iter()
                    .map(|value| resp::RespType::SimpleString(value.clone())),
            )
            .collect()
    }

    // --- Tests ---
    #[rstest]
    #[case::single(value())]
    #[case::multiple(values())]
    #[tokio::test]
    async fn test_handle_not_existing(
        store: crate::store::SharedStore,
        key: String,
        #[case] values: Vec<String>,
    ) {
        let args = make_args(&key, &values);
        let response = Rpush.handle(args, &store).await;
        let expected_length = values.len();
        let expected = resp::RespType::Integer(expected_length as i64);
        assert_eq!(expected, response);

        let mut store = store.lock().await;
        let list = match &store.get(&key).unwrap().value {
            crate::store::EntryValue::List(list) => list,
            _ => panic!("Unexpected type"),
        };

        assert_eq!(expected_length, list.len());
        for (expected, value) in values.into_iter().zip(list.into_iter()) {
            assert_eq!(expected, *value);
        }
    }

    #[rstest]
    #[case::single(value())]
    #[case::multiple(values())]
    #[tokio::test]
    async fn test_handle_existing(
        store: crate::store::SharedStore,
        key: String,
        #[case] values: Vec<String>,
        existing_values: Vec<String>,
    ) {
        let mut entry = crate::store::Entry::new_list();
        let list = match &mut entry.value {
            crate::store::EntryValue::List(list) => list,
            _ => unreachable!(),
        };
        list.extend(existing_values.clone());
        store.lock().await.insert(key.clone(), entry);

        let args = make_args(&key, &values);
        let mut expected = existing_values;
        expected.extend(values);

        let response = Rpush.handle(args, &store).await;
        let expected_response = resp::RespType::Integer(expected.len() as i64);
        assert_eq!(expected_response, response);

        let mut store = store.lock().await;
        let list = match &store.get(&key).unwrap().value {
            crate::store::EntryValue::List(list) => list,
            _ => panic!("Unexpected type"),
        };
        assert_eq!(expected.len(), list.len());
        for (expected, value) in expected.into_iter().zip(list.into_iter()) {
            assert_eq!(expected, *value);
        }
    }

    // --- Errors ---
    #[rstest]
    #[tokio::test]
    async fn text_missing_key(store: crate::store::SharedStore) {
        let args = vec![];
        let expected = resp::RespType::BulkError("ERR Missing key for 'RPUSH' command".into());
        let response = Rpush.handle(args, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[tokio::test]
    async fn text_invalid_key(store: crate::store::SharedStore) {
        let args = vec![resp::RespType::Array(vec![])];
        let expected =
            resp::RespType::BulkError("ERR Failed to extract key for 'RPUSH' command".into());
        let response = Rpush.handle(args, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[tokio::test]
    async fn text_missing_value(store: crate::store::SharedStore, key: String) {
        let args = vec![resp::RespType::SimpleString(key)];
        let expected = resp::RespType::BulkError(
            "ERR At least one value must be provided for 'RPUSH' command".into(),
        );
        let response = Rpush.handle(args, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid_value(store: crate::store::SharedStore, key: String) {
        let args = vec![
            resp::RespType::SimpleString(key),
            resp::RespType::Array(vec![]),
        ];
        let expected =
            resp::RespType::BulkError("ERR Failed to extract value for 'RPUSH' command".into());
        let response = Rpush.handle(args, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[case::single(value())]
    #[case::multiple(values())]
    #[tokio::test]
    async fn test_existing_invalid_value_type(
        store: crate::store::SharedStore,
        key: String,
        #[case] values: Vec<String>,
    ) {
        store.lock().await.insert(
            key.clone(),
            crate::store::Entry::new_string("existing value"),
        );

        let args = make_args(&key, &values);
        let expected =
            resp::RespType::BulkError(format!("WRONGTYPE Entry at key {key} is not a list"));
        let response = Rpush.handle(args, &store).await;
        assert_eq!(expected, response);
    }
}
