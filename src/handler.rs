//! This module contains the handler.
use crate::{resp, store};
use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Handles the ECHO command.
fn handle_echo(args: Vec<resp::RespType>) -> resp::RespType {
    if let Some(message_token) = args.first() {
        let message = resp::extract_string(message_token).ok();
        resp::RespType::BulkString(message)
    } else {
        log::trace!("No message provided.");
        resp::RespType::BulkString(None)
    }
}

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
async fn handle_set(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
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
    resp::RespType::SimpleString("OK".to_string())
}

/// Handles the GET command.
async fn handle_get(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
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

async fn get_response(message: resp::RespType, store: &store::Store) -> resp::RespType {
    let (command, args) = resp::extract_command(message).unwrap();
    match command.to_lowercase().as_str() {
        "ping" => resp::RespType::SimpleString("PONG".to_string()),
        "echo" => handle_echo(args),
        "set" => handle_set(args, &store).await,
        "get" => handle_get(args, &store).await,
        _ => panic!("Invalid redis command: {:?}", command),
    }
}

/// Handles reading and writing RESP messages over a TCP stream.
pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    /// Creates a new RESP handler.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    /// Reads a RESP message from the TCP stream.
    pub async fn read_stream(&mut self) -> Result<Option<resp::RespType>> {
        let bytes = self.stream.read_buf(&mut self.buffer).await?;
        if bytes == 0 {
            Ok(None)
        } else {
            Ok(Some(resp::RespType::from_bytes(&mut self.buffer)?))
        }
    }

    /// Writes a RESP message to the TCP stream.
    pub async fn write_stream(&mut self, value: resp::RespType) -> Result<()> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }

    /// Runs the handler.
    pub async fn run(&mut self, store: store::Store) {
        while let Ok(Some(message)) = self.read_stream().await {
            let response = get_response(message, &store).await;
            self.write_stream(response).await.unwrap();
        }
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
        "key".to_string()
    }

    #[fixture]
    fn value() -> String {
        "value".to_string()
    }

    // --- Handle SET ---
    #[rstest]
    #[tokio::test]
    async fn test_handle_set_basic(store: crate::store::Store, key: String, value: String) {
        let args = vec![
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
        ];
        let response = handle_set(args, &store).await;
        assert_eq!(response, resp::RespType::SimpleString("OK".to_string()));

        let stored_value = store.lock().await.get(&key).unwrap().value.clone();
        assert_eq!(stored_value, value);
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_set_with_px(store: crate::store::Store, key: String, value: String) {
        let duration = 100;
        let args = vec![
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
            resp::RespType::SimpleString("PX".to_string()),
            resp::RespType::SimpleString(duration.to_string()), // 100 milliseconds
        ];
        let response = handle_set(args, &store).await;
        assert_eq!(response, resp::RespType::SimpleString("OK".to_string()));

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

    // --- Handle GET ---
    #[rstest]
    #[tokio::test]
    async fn test_handle_get_existing(store: crate::store::Store, key: String, value: String) {
        store
            .lock()
            .await
            .insert(key.clone(), crate::store::Entry::new(value.clone()));

        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle_get(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value)));
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_get_non_existing(store: crate::store::Store, key: String) {
        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle_get(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(None));
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_get_expired_key(store: crate::store::Store, key: String, value: String) {
        let deletion_time = 0u32;
        store.lock().await.insert(
            key.clone(),
            crate::store::Entry::new(value.clone()).with_deletion(deletion_time),
        );

        let args = vec![resp::RespType::SimpleString(key.clone())];
        let response = handle_get(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(None));

        assert!(store.lock().await.get(&key).is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle_get_expiry_handling(
        store: crate::store::Store,
        key: String,
        value: String,
    ) {
        let deletion_time = 300;
        store.lock().await.insert(
            key.clone(),
            crate::store::Entry::new(value.clone()).with_deletion(deletion_time),
        );

        let args = vec![resp::RespType::SimpleString(key)];
        let response = handle_get(args.clone(), &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value)));

        tokio::time::sleep(tokio::time::Duration::from_millis(deletion_time)).await;
        let response = handle_get(args, &store).await;
        assert_eq!(response, resp::RespType::BulkString(None));
        assert!(store.lock().await.get("expiredkey").is_none());
    }

    // --- Get Response ---
    #[rstest]
    #[tokio::test]
    async fn test_get_response_ping(store: crate::store::Store) {
        let message = resp::RespType::Array(vec![resp::RespType::SimpleString("PING".to_string())]);
        let response = get_response(message, &store).await;
        assert_eq!(response, resp::RespType::SimpleString("PONG".to_string()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_response_echo(store: crate::store::Store) {
        let expected = "Hello".to_string();
        let message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("ECHO".to_string()),
            resp::RespType::SimpleString(expected.clone()),
        ]);
        let response = get_response(message, &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(expected)));
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_response_set_get_flow(
        store: crate::store::Store,
        key: String,
        value: String,
    ) {
        // SET
        let set_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("SET".to_string()),
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
        ]);
        let set_response = get_response(set_message, &store).await;
        assert_eq!(set_response, resp::RespType::SimpleString("OK".to_string()));

        // GET
        let get_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("GET".to_string()),
            resp::RespType::SimpleString(key.clone()),
        ]);
        let response = get_response(get_message.clone(), &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value.clone())));

        // SET with PX and GET after expiration
        let expired_key = "expired_key".to_string();
        let expired_value = "expired_value".to_string();
        let set_px_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("SET".to_string()),
            resp::RespType::SimpleString(expired_key.clone()),
            resp::RespType::SimpleString(expired_value.clone()),
            resp::RespType::SimpleString("PX".to_string()),
            resp::RespType::SimpleString("10".to_string()), // 10 milliseconds
        ]);
        let set_px_response = get_response(set_px_message, &store).await;
        assert_eq!(
            set_px_response,
            resp::RespType::SimpleString("OK".to_string())
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let get_exp_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("GET".to_string()),
            resp::RespType::SimpleString(expired_key.clone()),
        ]);
        let get_exp_response = get_response(get_exp_message, &store).await;
        assert_eq!(get_exp_response, resp::RespType::BulkString(None));
        let response = get_response(get_message, &store).await;
        assert_eq!(response, resp::RespType::BulkString(Some(value.clone())));
    }
}
