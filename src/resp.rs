use anyhow::Result;
use bytes::BytesMut;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpStream};

#[derive(Debug, Clone)]
/// Represents a RESP (Redis Serialization Protocol) data type.
pub enum RespType {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RespType>),
    Null(),
}

impl RespType {
    /// Serialises the RESP into a RESP-compliant string.
    pub fn serialise(&self) -> String {
        match self {
            Self::SimpleString(s) => format!("+{}\r\n", s),
            Self::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Self::Null() => "$-1\r\n".to_string(),
            _ => panic!("Invalid type to serialise."),
        }
    }
}

/// Reads bytes from a buffer until a `\r\n` sequence is found.
/// Returns the slice before `\r\n` and the total bytes consumed including `\r\n`.
fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[..i - 1], i + 1));
        }
    }
    None
}

/// Parses a byte slice into an integer.
fn parse_num(buffer: &[u8]) -> Result<i64> {
    String::from_utf8(buffer.to_vec())?
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!(e))
}

/// Parses the buffer for a simple string.
fn parse_simple_string(buffer: BytesMut) -> Result<(RespType, usize)> {
    if let Some((message, bytes_used)) = read_until_crlf(&buffer[1..]) {
        return Ok((
            RespType::SimpleString(String::from_utf8(message.to_vec())?),
            bytes_used,
        ));
    }

    Err(anyhow::anyhow!("Invalid simple string: {:?}.", buffer))
}

/// Parses a buffer for a bulk string.
fn parse_bulk_string(buffer: BytesMut) -> Result<(RespType, usize)> {
    let (string_length, bytes_consumed) =
        if let Some((message, bytes_used)) = read_until_crlf(&buffer[1..]) {
            (parse_num(message)? as usize, bytes_used + 1)
        } else {
            return Err(anyhow::anyhow!(
                "Bulk string missing length segment: {:?}.",
                buffer
            ));
        };

    Ok((
        RespType::BulkString(String::from_utf8(
            buffer[bytes_consumed..bytes_consumed + string_length].to_vec(),
        )?),
        bytes_consumed + string_length + 2,
    ))
}

/// Parses a buffer for an array.
fn parse_array(buffer: BytesMut) -> Result<(RespType, usize)> {
    let (array_length, mut bytes_consumed) =
        if let Some((message, bytes_used)) = read_until_crlf(&buffer[1..]) {
            (parse_num(message)?, bytes_used + 1)
        } else {
            return Err(anyhow::anyhow!(
                "Array missing length segment: {:?}.",
                buffer
            ));
        };

    let mut messages = vec![];
    for _ in 0..array_length {
        let (message, bytes_used) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        bytes_consumed += bytes_used;
        messages.push(message);
    }

    Ok((RespType::Array(messages), bytes_consumed))
}

/// Parses a buffer for the message.
fn parse_message(buffer: BytesMut) -> Result<(RespType, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(anyhow::anyhow!("Invalid message type.")),
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
    pub async fn read_stream(&mut self) -> Result<Option<RespType>> {
        let bytes = self.stream.read_buf(&mut self.buffer).await?;
        if bytes == 0 {
            Ok(None)
        } else {
            Ok(Some(parse_message(self.buffer.split())?.0))
        }
    }

    /// Writes a RESP message to the TCP stream.
    pub async fn write_stream(&mut self, value: RespType) -> Result<()> {
        self.stream.write_all(value.serialise().as_bytes()).await?;
        Ok(())
    }
}
