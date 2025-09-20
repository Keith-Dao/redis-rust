//! This module contains the handler.
use crate::{resp, store};
use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Extracts the string from the message.
fn extract_string(message: &resp::RespType) -> Result<String> {
    match message {
        resp::RespType::BulkString(Some(s)) | resp::RespType::SimpleString(s) => Ok(s.clone()),
        _ => Err(anyhow::anyhow!("Cannot unpack: {:?}", message)),
    }
}

/// Extracts the command and its arguments.
fn extract_command(message: resp::RespType) -> Result<(String, Vec<resp::RespType>)> {
    match message {
        resp::RespType::Array(vec) => Ok((
            extract_string(&vec[0]).unwrap(),
            vec.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Invalid command: {:?}", message)),
    }
}

/// Handles the set command.
async fn handle_set(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
    match args.as_slice() {
        [key, value] | [key, value, ..] => {
            let (key, value) = match (extract_string(key), extract_string(value)) {
                (Ok(key), Ok(value)) => (key, value),
                _ => panic!("Invalid SET arguments: {:?}", args),
            };

            let deletion_instant = match args.get(2..4) {
                Some([param, duration]) => {
                    match (extract_string(param), extract_string(duration)) {
                        (Ok(param), Ok(duration)) if param.to_lowercase() == "px" => {
                            match duration.parse::<u64>() {
                                Ok(milliseconds) => Some(milliseconds),
                                _ => None,
                            }
                        }
                        _ => None,
                    }
                }
                _ => None,
            };

            let mut store = store.lock().await;
            let mut entry = store::Entry::new(value);
            if let Some(deletion_time) = deletion_instant {
                entry = entry.with_deletion(deletion_time);
            }
            store.insert(key, entry);

            resp::RespType::SimpleString("OK".to_string())
        }

        _ => panic!("Invalid"),
    }
}

/// Handles the get command.
async fn handle_get(args: Vec<resp::RespType>, store: &store::Store) -> resp::RespType {
    match args.as_slice() {
        [key] | [key, ..] => {
            let key = match extract_string(key) {
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
    let (command, args) = extract_command(message).unwrap();
    match command.to_lowercase().as_str() {
        "ping" => resp::RespType::SimpleString("PONG".to_string()),
        "echo" => args[0].clone(),
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

    // --- Extract string ---
    #[rstest]
    #[case::bulk_string(resp::RespType::BulkString(Some("Test".to_string())), "Test")]
    #[case::simple_string(resp::RespType::SimpleString("Test".to_string()), "Test")]
    fn test_extract_string(#[case] message: resp::RespType, #[case] expected: String) {
        let result = extract_string(&message);
        if let Ok(result) = result {
            assert_eq!(result, expected);
        } else {
            panic!("Result should have been successful.");
        }
    }

    #[rstest]
    #[case::array(resp::RespType::Array(vec![]))]
    #[case::null(resp::RespType::Null())]
    fn test_extract_string_fail(#[case] message: resp::RespType) {
        let result = extract_string(&message);
        assert!(result.is_err());
    }

    // --- Extract command ---
    #[rstest]
    #[case::set_command(
        resp::RespType::Array(vec![
            resp::RespType::BulkString(Some("SET".to_string())),
            resp::RespType::BulkString(Some("key".to_string())),
            resp::RespType::BulkString(Some("value".to_string())),
        ]),
        "SET",
        vec![
            resp::RespType::BulkString(Some("key".to_string())),
            resp::RespType::BulkString(Some("value".to_string())),
        ]
    )]
    #[case::get_command(
        resp::RespType::Array(vec![
            resp::RespType::BulkString(Some("GET".to_string())),
            resp::RespType::BulkString(Some("key".to_string())),
        ]),
        "GET",
        vec![resp::RespType::BulkString(Some("key".to_string()))]
    )]
    #[case::no_args(
        resp::RespType::Array(vec![resp::RespType::SimpleString("Test".to_string())]),
        "Test",
        vec![],
    )]
    fn test_extract_command(
        #[case] message: resp::RespType,
        #[case] expected_command: String,
        #[case] expected_args: Vec<resp::RespType>,
    ) {
        let (command, args) = extract_command(message).unwrap();
        assert_eq!(command, expected_command);
        assert_eq!(args, expected_args);
    }

    #[rstest]
    #[case::simple_string(resp::RespType::SimpleString("SET".to_string()))]
    #[case::bulk_string(resp::RespType::BulkString(Some("SET".to_string())))]
    fn test_extract_command_fail(#[case] message: resp::RespType) {
        let result = extract_command(message);
        assert!(result.is_err());
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
        assert_eq!(response, resp::RespType::SimpleString(expected));
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
