//! This module contains the handler.
use crate::{resp, store};
use anyhow::Result;
use bytes::BytesMut;
use std::time::{Duration, Instant};
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpStream};

/// Extracts the string from the message.
fn extract_string(message: &resp::RespType) -> Result<String> {
    match message {
        resp::RespType::BulkString(s) | resp::RespType::SimpleString(s) => Ok(s.clone()),
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
                                Ok(milliseconds) => {
                                    Some(Instant::now() + Duration::from_millis(milliseconds))
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    }
                }
                _ => None,
            };

            let mut store = store.lock().await;
            store.insert(key, (value, deletion_instant));

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
                Some((value, deletion_time)) => match deletion_time {
                    Some(deletion_time) if deletion_time <= &Instant::now() => {
                        store.remove(&key);
                        resp::RespType::NullArray()
                    }
                    _ => resp::RespType::BulkString(value.clone()),
                },
                None => resp::RespType::NullArray(),
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
            Ok(Some(resp::parse_message(&mut self.buffer)?))
        }
    }

    /// Writes a RESP message to the TCP stream.
    pub async fn write_stream(&mut self, value: resp::RespType) -> Result<()> {
        self.stream.write_all(value.serialise().as_bytes()).await?;
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
