//! This module contains the handler.
use crate::{commands, commands::Command, resp, store};
use anyhow::Result;
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

async fn get_response(message: resp::RespType, store: &store::SharedStore) -> resp::RespType {
    let (command, args) = resp::extract_command(message).unwrap();
    match command.to_lowercase().as_str() {
        "ping" => commands::ping::Ping().handle(args, &store).await,
        "echo" => commands::echo::Echo().handle(args, &store).await,
        "set" => commands::set::handle(args, &store).await,
        "get" => commands::get::Get().handle(args, &store).await,
        "rpush" => commands::rpush::Rpush().handle(args, &store).await,
        _ => resp::RespType::SimpleError(format!("ERR Command ({command}) is not valid")),
    }
}

/// Handles reading and writing RESP messages over a TCP stream.
pub struct RespHandler<T> {
    stream: T,
    buffer: BytesMut,
}

impl<T> RespHandler<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Creates a new RESP handler.
    pub fn new(stream: T) -> Self {
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

    #[fixture]
    fn stream_and_handler() -> (
        tokio::io::DuplexStream,
        RespHandler<tokio::io::DuplexStream>,
    ) {
        let (client_stream, server_stream) = tokio::io::duplex(512);
        (client_stream, RespHandler::new(server_stream))
    }

    fn make_handle_args(args: &Vec<resp::RespType>) -> Vec<resp::RespType> {
        args.clone().into_iter().skip(1).collect()
    }

    // --- Tests ---
    // ---- Commands ----
    #[rstest]
    #[case::lower("ping")]
    #[case::upper("PING")]
    #[case::mixed("PinG")]
    #[tokio::test]
    async fn test_get_response_ping(store: crate::store::SharedStore, #[case] command: String) {
        let message = resp::RespType::Array(vec![resp::RespType::SimpleString(command)]);
        let expected = commands::ping::Ping().handle(vec![], &store).await;
        let response = get_response(message, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[case::lower("echo")]
    #[case::upper("ECHO")]
    #[case::mixed("EchO")]
    #[tokio::test]
    async fn test_get_response_echo(
        store: crate::store::SharedStore,
        #[case] command: String,
        value: String,
    ) {
        let args = vec![
            resp::RespType::SimpleString(command),
            resp::RespType::SimpleString(value),
        ];
        let expected = commands::echo::Echo()
            .handle(make_handle_args(&args), &store)
            .await;

        let message = resp::RespType::Array(args);
        let response = get_response(message, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[case::lower("get")]
    #[case::upper("GET")]
    #[case::mixed("GeT")]
    #[tokio::test]
    async fn test_get_response_get(
        store: crate::store::SharedStore,
        #[case] command: String,
        key: String,
        value: String,
    ) {
        store
            .lock()
            .await
            .insert(key.clone(), crate::store::Entry::new_string(value.clone()));
        let args = vec![
            resp::RespType::SimpleString(command),
            resp::RespType::SimpleString(key.clone()),
        ];
        let expected = commands::get::Get()
            .handle(make_handle_args(&args), &store)
            .await;

        let get_message = resp::RespType::Array(args);
        let response = get_response(get_message, &store).await;
        assert_eq!(expected, response);
    }

    #[rstest]
    #[case::lower("set")]
    #[case::upper("SET")]
    #[case::mixed("SeT")]
    #[tokio::test]
    async fn test_get_response_set(
        store: crate::store::SharedStore,
        #[case] command: String,
        key: String,
        value: String,
    ) {
        let expected_store = crate::store::new();
        let args = vec![
            resp::RespType::SimpleString(command),
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
        ];
        let expected = commands::set::handle(make_handle_args(&args), &expected_store).await;

        let set_message = resp::RespType::Array(args);
        let response = get_response(set_message, &store).await;
        assert_eq!(expected, response);
        assert_eq!(*expected_store.lock().await, *store.lock().await);
    }

    #[rstest]
    #[case::lower("rpush")]
    #[case::upper("RPUSH")]
    #[case::mixed("RPusH")]
    #[tokio::test]
    async fn test_get_response_rpush(
        store: crate::store::SharedStore,
        #[case] command: String,
        key: String,
        value: String,
    ) {
        let expected_store = crate::store::new();
        let args = vec![
            resp::RespType::SimpleString(command),
            resp::RespType::SimpleString(key.clone()),
            resp::RespType::SimpleString(value.clone()),
        ];
        let expected = commands::rpush::Rpush()
            .handle(make_handle_args(&args), &expected_store)
            .await;

        let set_message = resp::RespType::Array(args);
        let response = get_response(set_message, &store).await;
        assert_eq!(expected, response);
        assert_eq!(*expected_store.lock().await, *store.lock().await);
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid_command(store: crate::store::SharedStore) {
        let message = resp::RespType::Array(vec![resp::RespType::SimpleString("Invalid".into())]);
        let response = get_response(message, &store).await;
        let expected = resp::RespType::SimpleError("ERR Command (Invalid) is not valid".into());
        assert_eq!(expected, response);
    }

    // ---- Handler ----
    #[rstest]
    fn test_handler_new() {
        let (_, server_stream) = tokio::io::duplex(512);
        let handler = RespHandler::new(server_stream);
        assert_eq!(handler.buffer.capacity(), 512);
        assert!(handler.buffer.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handler_read_empty(
        stream_and_handler: (
            tokio::io::DuplexStream,
            RespHandler<tokio::io::DuplexStream>,
        ),
    ) -> Result<()> {
        let (mut client_stream, mut handler) = stream_and_handler;
        client_stream.write(b"").await?;
        client_stream.shutdown().await?;

        match handler.read_stream().await {
            Ok(None) => (),
            _ => panic!("Incorrect read."),
        };

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_handler_read(
        stream_and_handler: (
            tokio::io::DuplexStream,
            RespHandler<tokio::io::DuplexStream>,
        ),
        value: String,
    ) -> Result<()> {
        let (mut client_stream, mut handler) = stream_and_handler;

        let expected = resp::RespType::SimpleString(value);
        client_stream.write(expected.serialize().as_bytes()).await?;
        client_stream.shutdown().await?;

        match handler.read_stream().await {
            Ok(Some(message)) => assert_eq!(expected, message),
            _ => panic!("Incorrect read."),
        };

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_handler_write(
        stream_and_handler: (
            tokio::io::DuplexStream,
            RespHandler<tokio::io::DuplexStream>,
        ),
        value: String,
    ) -> Result<()> {
        let (mut client_stream, mut handler) = stream_and_handler;

        let expected = resp::RespType::SimpleString(value);
        handler.write_stream(expected.clone()).await?;

        let mut buffer = BytesMut::with_capacity(512);
        client_stream.read_buf(&mut buffer).await?;
        assert_eq!(expected.serialize(), buffer);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_handler_run(
        stream_and_handler: (
            tokio::io::DuplexStream,
            RespHandler<tokio::io::DuplexStream>,
        ),
        store: crate::store::SharedStore,
    ) -> Result<()> {
        let (mut client_stream, mut handler) = stream_and_handler;

        let message = resp::RespType::Array(vec![resp::RespType::SimpleString("PING".into())]);
        client_stream
            .write_all(message.serialize().as_bytes())
            .await?;
        client_stream.shutdown().await?;

        handler.run(store).await;

        let mut buffer = BytesMut::with_capacity(512);
        client_stream.read_buf(&mut buffer).await?;
        let expected = resp::RespType::SimpleString("PONG".into());
        assert_eq!(expected.serialize(), buffer);

        Ok(())
    }
}
