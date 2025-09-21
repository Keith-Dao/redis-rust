//! This module contains the handler.
use crate::{commands, resp, store};
use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn get_response(message: resp::RespType, store: &store::Store) -> resp::RespType {
    let (command, args) = resp::extract_command(message).unwrap();
    match command.to_lowercase().as_str() {
        "ping" => commands::ping::handle(),
        "echo" => commands::echo::handle(args),
        "set" => commands::set::handle(args, &store).await,
        "get" => commands::get::handle(args, &store).await,
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

    // --- Tests ---
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
