//! This module contains the RESP (Redis Serialization Protocol) data types.
use anyhow::Result;
use bytes::{Buf, BytesMut};
use log::trace;

#[derive(Debug, Clone)]
/// Represents a RESP (Redis Serialization Protocol) data type.
pub enum RespType {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RespType>),
    NullArray(),
    Null(),
}

impl RespType {
    /// Serialises the RESP into a RESP-compliant string.
    pub fn serialise(&self) -> String {
        match self {
            Self::SimpleString(s) => format!("+{}\r\n", s),
            Self::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Self::NullArray() => "$-1\r\n".to_string(),
            Self::Null() => "_\r\n".to_string(),
            _ => panic!("Invalid type to serialise."),
        }
    }
}

/// Reads bytes from a buffer until a `\r\n` sequence is found.
/// Returns the slice before `\r\n` and the total bytes consumed including `\r\n`.
fn read_until_crlf(buffer: &mut BytesMut) -> Option<BytesMut> {
    trace!("Reading buffer until first CRLF: {:?}.", buffer);
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            let result = buffer.split_to(i - 1);
            buffer.advance(2); // Skip CRLF
            return Some(result);
        }
    }
    None
}

/// Parses a byte slice into an integer.
fn parse_num(buffer: BytesMut) -> Result<i64> {
    trace!("Attempting to parse number from buffer: {:?}.", buffer);
    String::from_utf8(buffer.to_vec())?
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!(e))
}

/// Parses the buffer for a simple string.
fn parse_simple_string(buffer: &mut BytesMut) -> Result<RespType> {
    trace!("Parsing simple string: {:?}.", buffer);
    if let Some(message) = read_until_crlf(buffer) {
        return Ok(RespType::SimpleString(String::from_utf8(message.to_vec())?));
    }

    Err(anyhow::anyhow!("Invalid simple string: {:?}.", buffer))
}

/// Parses a buffer for a bulk string.
fn parse_bulk_string(buffer: &mut BytesMut) -> Result<RespType> {
    trace!("Parsing bulk string: {:?}", buffer);
    let expected_message_length = parse_num(read_until_crlf(buffer).expect(&format!(
        "Bulk string missing length segment: {:?}.",
        buffer
    )))? as usize;

    let message =
        read_until_crlf(buffer).expect(&format!("Bulk string message missing: {:?}.", buffer));
    if expected_message_length != message.len() {
        return Err(anyhow::anyhow!(
            "Message did not match the expected length. Expected: {}, got {}.",
            expected_message_length,
            message.len()
        ));
    }

    Ok(RespType::BulkString(String::from_utf8(message.to_vec())?))
}

/// Parses a buffer for an array.
fn parse_array(buffer: &mut BytesMut) -> Result<RespType> {
    trace!("Parsing array: {:?}", buffer);
    let array_length = if let Some(message) = read_until_crlf(buffer) {
        parse_num(message)? as usize
    } else {
        return Err(anyhow::anyhow!(
            "Array missing length segment: {:?}.",
            buffer
        ));
    };

    let mut messages = vec![];
    for _ in 0..array_length {
        let message = parse_message(buffer)?;
        messages.push(message);
    }

    Ok(RespType::Array(messages))
}

/// Parses a buffer for the message.
pub fn parse_message(buffer: &mut BytesMut) -> Result<RespType> {
    trace!("Parsing message: {:?}.", buffer);
    match buffer.split_to(1)[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(anyhow::anyhow!("Invalid message type.")),
    }
}
