//! This module contains the handler.
use crate::{commands, resp, store};
use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn get_response(message: resp::RespType, store: &store::SharedStore) -> resp::RespType {
    let (command, args) = resp::extract_command(message).unwrap();
    match command.to_lowercase().as_str() {
        "ping" => commands::ping::handle(),
        "echo" => commands::echo::handle(args),
        "set" => commands::set::handle(args, &store).await,
        "get" => commands::get::handle(args, &store).await,
        "rpush" => commands::rpush::handle(args, &store).await,
        _ => resp::RespType::BulkError(format!("ERR Command ({command}) is not valid")),
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
    pub async fn run(&mut self, store: store::SharedStore) {
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
    fn store() -> crate::store::SharedStore {
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
    async fn test_get_response_ping(store: crate::store::SharedStore) {
        let message = resp::RespType::Array(vec![resp::RespType::SimpleString("PING".into())]);
        let response = get_response(message, &store).await;
        assert_eq!(resp::RespType::SimpleString("PONG".into()), response);
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_response_echo(store: crate::store::SharedStore) {
        let expected = "Hello";
        let message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("ECHO".into()),
            resp::RespType::SimpleString(expected.into()),
        ]);
        let response = get_response(message, &store).await;
        assert_eq!(resp::RespType::BulkString(Some(expected.into())), response);
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_response_set_get_flow(
        store: crate::store::SharedStore,
        key: String,
        value: String,
    ) {
        tokio::time::pause();
        // SET
        let set_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("SET".into()),
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
        ]);
        let set_response = get_response(set_message, &store).await;
        assert_eq!(resp::RespType::SimpleString("OK".into()), set_response);

        // GET
        let get_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("GET".into()),
            resp::RespType::SimpleString(key.clone()),
        ]);
        let response = get_response(get_message.clone(), &store).await;
        assert_eq!(resp::RespType::BulkString(Some(value.clone())), response);

        // SET with PX and GET after expiration
        let expired_key = "expired_key";
        let expired_value = "expired_value";
        let set_px_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("SET".into()),
            resp::RespType::SimpleString(expired_key.into()),
            resp::RespType::SimpleString(expired_value.into()),
            resp::RespType::SimpleString("PX".into()),
            resp::RespType::SimpleString("10".into()), // 10 milliseconds
        ]);

        let set_px_response = get_response(set_px_message, &store).await;
        assert_eq!(resp::RespType::SimpleString("OK".into()), set_px_response);

        // Key still valid
        tokio::time::advance(tokio::time::Duration::from_millis(9)).await;
        let get_exp_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("GET".into()),
            resp::RespType::SimpleString(expired_key.into()),
        ]);
        let get_exp_response = get_response(get_exp_message, &store).await;
        assert_eq!(
            get_exp_response,
            resp::RespType::BulkString(Some(expired_value.into()))
        );

        // Key expired now
        tokio::time::advance(tokio::time::Duration::from_millis(1)).await;
        let get_exp_message = resp::RespType::Array(vec![
            resp::RespType::SimpleString("GET".into()),
            resp::RespType::SimpleString(expired_key.into()),
        ]);
        let get_exp_response = get_response(get_exp_message, &store).await;
        assert_eq!(resp::RespType::Null(), get_exp_response);
        let response = get_response(get_message, &store).await;
        assert_eq!(resp::RespType::BulkString(Some(value.clone())), response);
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid_command(store: crate::store::SharedStore) {
        let message = resp::RespType::Array(vec![resp::RespType::SimpleString("Invalid".into())]);
        let response = get_response(message, &store).await;
        let expected = resp::RespType::BulkError("ERR Command (Invalid) is not valid".into());
        assert_eq!(expected, response);
    }
}
