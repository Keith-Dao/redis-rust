//! This module contains the RESP (Redis Serialization Protocol) data types.
use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use log::trace;

// TODO: Add proper error handling to expects
///
/// Extracts the string from the message.
pub fn extract_string(message: &RespType) -> Result<String> {
    match message {
        RespType::BulkString(Some(s)) | RespType::SimpleString(s) => Ok(s.clone()),
        _ => Err(anyhow::anyhow!("Cannot unpack: {:?}", message)),
    }
}

/// Extracts the command and its arguments.
pub fn extract_command(message: RespType) -> Result<(String, Vec<RespType>)> {
    match message {
        RespType::Array(vec) => Ok((
            extract_string(&vec[0]).unwrap(),
            vec.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Invalid command: {:?}", message)),
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

#[derive(Debug, Clone, PartialEq)]
/// Represents a RESP (Redis Serialization Protocol) data type.
pub enum RespType {
    SimpleString(String),
    SimpleError(String),
    BulkString(Option<String>),
    Array(Vec<RespType>),
    BulkError(String),
    Integer(i64),
    Null(),
}

impl RespType {
    /// Parses the buffer for a simple string.
    fn parse_simple_string(buffer: &mut BytesMut) -> Result<RespType> {
        trace!("Parsing simple string: {:?}.", buffer);
        if let Some(message) = read_until_crlf(buffer) {
            return Ok(RespType::SimpleString(String::from_utf8(message.to_vec())?));
        }

        Err(anyhow::anyhow!("Invalid simple string: {:?}.", buffer))
    }

    /// Parses the buffer for a simple error.
    fn parse_simple_error(buffer: &mut BytesMut) -> Result<RespType> {
        trace!("Parsing simple error: {:?}.", buffer);
        if let Some(message) = read_until_crlf(buffer) {
            return Ok(RespType::SimpleError(String::from_utf8(message.to_vec())?));
        }

        Err(anyhow::anyhow!("Invalid simple error: {:?}.", buffer))
    }

    /// Parses a buffer for a bulk string.
    fn parse_bulk_string(buffer: &mut BytesMut) -> Result<RespType> {
        trace!("Parsing bulk string: {:?}", buffer);
        let expected_message_length = parse_num(
            read_until_crlf(buffer)
                .context(format!("Bulk string missing length segment: {:?}.", buffer))?,
        )? as usize;

        if buffer.len() < expected_message_length {
            return Err(anyhow::anyhow!(
                "Message did not match the expected length. Expected: {}, got: {}.",
                expected_message_length,
                buffer.len()
            ));
        }

        let message = String::from_utf8(buffer.split_to(expected_message_length).to_vec())?;
        if buffer.len() < 2 || buffer.split_to(2).as_ref() != b"\r\n" {
            return Err(anyhow::anyhow!("Expected CRLF."));
        }
        Ok(RespType::BulkString(Some(message)))
    }

    /// Parses a buffer for a bulk error.
    fn parse_bulk_error(buffer: &mut BytesMut) -> Result<RespType> {
        trace!("Parsing bulk error: {:?}", buffer);
        let expected_message_length = parse_num(
            read_until_crlf(buffer)
                .context(format!("Bulk error missing length segment: {:?}.", buffer))?,
        )? as usize;

        if buffer.len() < expected_message_length {
            return Err(anyhow::anyhow!(
                "Message did not match the expected length. Expected: {}, got: {}.",
                expected_message_length,
                buffer.len()
            ));
        }

        let message = String::from_utf8(buffer.split_to(expected_message_length).to_vec())?;
        if buffer.len() < 2 || buffer.split_to(2).as_ref() != b"\r\n" {
            return Err(anyhow::anyhow!("Expected CRLF."));
        }
        Ok(RespType::BulkError(message))
    }

    /// Parses a buffer for a bulk error.
    fn parse_integer(buffer: &mut BytesMut) -> Result<RespType> {
        trace!("Parsing integer: {:?}", buffer);

        let number =
            parse_num(read_until_crlf(buffer).context(format!("Number missing: {:?}.", buffer))?)
                .context("Failed to parse number.")?;

        Ok(RespType::Integer(number))
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
            let message = RespType::from_bytes(buffer).context(format!(
                "Message did not match expected length. Expected: {}, got: {}.",
                array_length,
                messages.len()
            ))?;
            messages.push(message);
        }

        Ok(RespType::Array(messages))
    }

    /// Parses a buffer for the message.
    pub fn from_bytes(buffer: &mut BytesMut) -> Result<Self> {
        trace!("Parsing message: {:?}.", buffer);
        if let Some((&first_byte, _)) = buffer.split_first() {
            _ = buffer.split_to(1);
            match first_byte as char {
                '+' => Self::parse_simple_string(buffer),
                '-' => Self::parse_simple_error(buffer),
                '$' => Self::parse_bulk_string(buffer),
                '!' => Self::parse_bulk_error(buffer),
                ':' => Self::parse_integer(buffer),
                '*' => Self::parse_array(buffer),
                _ => Err(anyhow::anyhow!("Invalid message type.")),
            }
        } else {
            Err(anyhow::anyhow!("Buffer empty."))
        }
    }

    /// Serializes the RESP into a RESP-compliant string.
    pub fn serialize(&self) -> String {
        match self {
            Self::SimpleString(s) => format!("+{s}\r\n"),
            Self::SimpleError(s) => format!("-{s}\r\n"),
            Self::BulkString(Some(s)) => format!("${}\r\n{s}\r\n", s.len()),
            Self::BulkString(None) => "$-1\r\n".into(),
            Self::BulkError(s) => format!("!{}\r\n{s}\r\n", s.len()),
            Self::Integer(num) => format!(":{num}\r\n"),
            Self::Null() => "_\r\n".into(),
            _ => panic!("Invalid type to serialise."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // --- Helpers ---
    // --- Extract string ---
    #[rstest]
    #[case::bulk_string(RespType::BulkString(Some("Test".into())), "Test")]
    #[case::simple_string(RespType::SimpleString("Test".into()), "Test")]
    fn test_extract_string(#[case] message: RespType, #[case] expected: String) {
        let result = extract_string(&message);
        if let Ok(result) = result {
            assert_eq!(result, expected);
        } else {
            panic!("Result should have been successful.");
        }
    }

    #[rstest]
    #[case::array(RespType::Array(vec![]))]
    #[case::null(RespType::Null())]
    fn test_extract_string_fail(#[case] message: RespType) {
        let result = extract_string(&message);
        assert!(result.is_err());
    }

    // --- Extract command ---
    #[rstest]
    #[case::set_command(
        RespType::Array(vec![
            RespType::BulkString(Some("SET".into())),
            RespType::BulkString(Some("key".into())),
            RespType::BulkString(Some("value".into())),
        ]),
        "SET",
        vec![
            RespType::BulkString(Some("key".into())),
            RespType::BulkString(Some("value".into())),
        ]
    )]
    #[case::get_command(
        RespType::Array(vec![
            RespType::BulkString(Some("GET".into())),
            RespType::BulkString(Some("key".into())),
        ]),
        "GET",
        vec![RespType::BulkString(Some("key".into()))]
    )]
    #[case::no_args(
        RespType::Array(vec![RespType::SimpleString("Test".into())]),
        "Test",
        vec![],
    )]
    fn test_extract_command(
        #[case] message: RespType,
        #[case] expected_command: String,
        #[case] expected_args: Vec<RespType>,
    ) {
        let (command, args) = extract_command(message).unwrap();
        assert_eq!(command, expected_command);
        assert_eq!(args, expected_args);
    }

    #[rstest]
    #[case::simple_string(RespType::SimpleString("SET".into()))]
    #[case::bulk_string(RespType::BulkString(Some("SET".into())))]
    fn test_extract_command_fail(#[case] message: RespType) {
        let result = extract_command(message);
        assert!(result.is_err());
    }

    // --- Read until CRLF ---
    #[rstest]
    #[case::empty_buffer("", &[], "")]
    #[case::no_crlf("No CRLF here", &[], "No CRLF here")]
    #[case::partial_crlf_at_end("Data\r", &[], "Data\r")]
    #[case::crlf_at_beginning("\r\nTest", &[""], "Test")]
    #[case::crlf_at_beginning_and_end("\r\nTest\r\n", &["", "Test"], "")]
    #[case::empty_token("\r\n", &[""], "")]
    #[case::mixed_data_no_final_crlf("Test\r\nPartial", &["Test"], "Partial")]
    #[case::contains_cr_but_not_crlf("Line\rBreak\n", &[], "Line\rBreak\n")]
    /// Tests the read_until_crlf function.
    fn test_read_until_clrf(
        #[case] input: &str,
        #[case] expected: &[&str],
        #[case] remaining: &str,
    ) {
        let mut bytes = input.bytes().collect();
        let mut actual_tokens = vec![];

        while let Some(result) = read_until_crlf(&mut bytes) {
            actual_tokens.push(result);
        }

        assert_eq!(expected, actual_tokens);
        assert!(read_until_crlf(&mut bytes).is_none());
        // After all tokens are read, the remaining buffer should be empty,
        // unless the last part of the input didn't end with CRLF.
        assert_eq!(remaining.as_bytes(), bytes);
    }

    // --- Parse number ---
    #[rstest]
    #[case::zero(b"0", Ok(0))]
    #[case::standard(b"123", Ok(123))]
    #[case::leading_zero(b"0123", Ok(123))]
    #[case::positive(b"+123", Ok(123))]
    #[case::negative(b"-123", Ok(-123))]
    #[case::invalid_num(b"123a", Err(anyhow::anyhow!("invalid digit found in string")))]
    #[case::float(b"123.0", Err(anyhow::anyhow!("invalid digit found in string")))]
    #[case::empty(b"", Err(anyhow::anyhow!("cannot parse integer from empty string")))]
    #[case::only_sign(b"-", Err(anyhow::anyhow!("invalid digit found in string")))]
    #[case::whitespace_before(b" 123", Err(anyhow::anyhow!("invalid digit found in string")))]
    #[case::max_i64(b"9223372036854775807", Ok(i64::MAX))]
    #[case::min_i64(b"-9223372036854775808", Ok(i64::MIN))]
    #[case::overflow_pos(b"9223372036854775808", Err(anyhow::anyhow!("number too large to fit in target type")))]
    #[case::overflow_neg(b"-9223372036854775809", Err(anyhow::anyhow!("number too small to fit in target type")))]
    /// Tests the parse number function.
    fn test_parse_num(#[case] buffer: &[u8], #[case] expected: Result<i64>) {
        let result = parse_num(buffer.into());
        assert_eq!(expected.is_ok(), result.is_ok());
        if expected.is_ok() {
            assert_eq!(expected.unwrap(), result.unwrap());
        } else {
            assert_eq!(
                expected.unwrap_err().root_cause().to_string(),
                result.unwrap_err().root_cause().to_string()
            );
        }
    }

    // --- Parsers ---
    #[rstest]
    // Simple strings
    #[case::simple_string(b"+Test\r\n", Ok(RespType::SimpleString("Test".into())))]
    #[case::simple_string_empty(b"+\r\n", Ok(RespType::SimpleString("".into())))]
    #[case::simple_string_multiple_elements(
        b"+Test\r\n+Another\r\n",
        Ok(RespType::SimpleString("Test".into()))
    )]
    #[case::simple_string_multiple_words(
        b"+Test with more than one word\r\n+Another\r\n",
        Ok(RespType::SimpleString("Test with more than one word".into()))
    )]
    #[case::simple_string_missing_clrf(
        b"+Test",
        Err(anyhow::anyhow!("Invalid simple string: b\"Test\"."))
    )]
    // Simple error
    #[case::simple_error(b"-Test\r\n", Ok(RespType::SimpleError("Test".into())))]
    #[case::simple_error_empty(b"-\r\n", Ok(RespType::SimpleError("".into())))]
    #[case::simple_error_multiple_elements(
        b"-Test\r\n+Another\r\n",
        Ok(RespType::SimpleError("Test".into()))
    )]
    #[case::simple_error_multiple_words(
        b"-Test with more than one word\r\n+Another\r\n",
        Ok(RespType::SimpleError("Test with more than one word".into()))
    )]
    #[case::simple_error_missing_clrf(
        b"-Test",
        Err(anyhow::anyhow!("Invalid simple error: b\"Test\"."))
    )]
    // Bulk strings
    #[case::bulk_string(b"$4\r\nTest\r\n", Ok(RespType::BulkString(Some("Test".into()))))]
    #[case::bulk_string_empty(b"$0\r\n\r\n", Ok(RespType::BulkString(Some("".into()))))]
    #[case::bulk_string_long(
        b"$21\r\nReally long text here\r\n",
        Ok(RespType::BulkString(Some("Really long text here".into())))
    )]
    #[case::bulk_string_with_crlf(
        b"$13\r\nTest\r\nAnother\r\n",
        Ok(RespType::BulkString(Some("Test\r\nAnother".into())))
    )]
    #[case::bulk_string_mismatch_length(
        b"$7\r\nTest\r\n",
        Err(anyhow::anyhow!("Message did not match the expected length. Expected: 7, got: 6."))
    )]
    #[case::bulk_string_invalid_length(
        b"$4a\r\nTest\r\n",
        Err(anyhow::anyhow!("invalid digit found in string"))
    )]
    #[case::bulk_string_missing_crlf(
        b"$4\r\nTest",
        Err(anyhow::anyhow!("Expected CRLF."))
    )]
    #[case::bulk_string_missing_lf(
        b"$4\r\nTest\r",
        Err(anyhow::anyhow!("Expected CRLF."))
    )]
    #[case::bulk_string_expected_crlf(
        b"$4\r\nTestab",
        Err(anyhow::anyhow!("Expected CRLF."))
    )]
    #[case::bulk_string_missing_length(
        b"$4",
        Err(anyhow::anyhow!("Bulk string missing length segment: b\"4\"."))
    )]
    // Bulk errors
    #[case::bulk_error(b"!4\r\nTest\r\n", Ok(RespType::BulkError("Test".into())))]
    #[case::bulk_error_empty(b"!0\r\n\r\n", Ok(RespType::BulkError("".into())))]
    #[case::bulk_error_long(
        b"!21\r\nReally long text here\r\n",
        Ok(RespType::BulkError("Really long text here".into()))
    )]
    #[case::bulk_error_with_crlf(
        b"!13\r\nTest\r\nAnother\r\n",
        Ok(RespType::BulkError("Test\r\nAnother".into()))
    )]
    #[case::bulk_error_mismatch_length(
        b"!7\r\nTest\r\n",
        Err(anyhow::anyhow!("Message did not match the expected length. Expected: 7, got: 6."))
    )]
    #[case::bulk_error_invalid_length(
        b"!4a\r\nTest\r\n",
        Err(anyhow::anyhow!("invalid digit found in string"))
    )]
    #[case::bulk_error_missing_crlf(
        b"!4\r\nTest",
        Err(anyhow::anyhow!("Expected CRLF."))
    )]
    #[case::bulk_error_missing_lf(
        b"!4\r\nTest\r",
        Err(anyhow::anyhow!("Expected CRLF."))
    )]
    #[case::bulk_error_expected_crlf(
        b"!4\r\nTestab",
        Err(anyhow::anyhow!("Expected CRLF."))
    )]
    #[case::bulk_error_missing_length(
        b"!4",
        Err(anyhow::anyhow!("Bulk error missing length segment: b\"4\"."))
    )]
    // Integer
    #[case::integer_zero(b":0\r\n", Ok(RespType::Integer(0)))]
    #[case::integer_positive(b":1\r\n", Ok(RespType::Integer(1)))]
    #[case::integer_positive_with_sign(b":+1\r\n", Ok(RespType::Integer(1)))]
    #[case::integer_negative(b":-1\r\n", Ok(RespType::Integer(-1)))]
    #[case::integer_negative_with_sign(b":-1\r\n", Ok(RespType::Integer(-1)))]
    #[case::integer_missing_clrf(b":", Err(anyhow::anyhow!("Number missing: b\"\".")))]
    #[case::integer_missing(b":\r\n", Err(anyhow::anyhow!("Failed to parse number.")))]
    #[case::integer_invalid_symbol(b":=120\r\n", Err(anyhow::anyhow!("Failed to parse number.")))]
    #[case::integer_invalid_number(b":abc\r\n", Err(anyhow::anyhow!("Failed to parse number.")))]
    // Arrays
    #[case::array(
        b"*3\r\n+Test\r\n$4\r\nTest\r\n$7\r\nAnother\r\n",
        Ok(RespType::Array(vec![
            RespType::SimpleString("Test".into()),
            RespType::BulkString(Some("Test".into())),
            RespType::BulkString(Some("Another".into()))
        ]))
    )]
    #[case::array_empty(b"*0\r\n", Ok(RespType::Array(vec![])))]
    #[case::array_too_short(
        b"*3\r\n+Test\r\n+Another\r\n",
        Err(anyhow::anyhow!("Message did not match expected length. Expected: 3, got: 2."))
    )]
    #[case::array_invalid_length(
        b"*2a\r\n+Test\r\n+Another\r\n",
        Err(anyhow::anyhow!("invalid digit found in string"))
    )]
    #[case::array_missing_length(b"*2", Err(anyhow::anyhow!("Array missing length segment: b\"2\".")))]
    // Null
    // Invalid type
    #[case::invalid(b"123", Err(anyhow::anyhow!("Invalid message type.")))]
    /// Tests the parser.
    fn test_parse(#[case] bytes: &[u8], #[case] expected: Result<RespType>) {
        let result = RespType::from_bytes(&mut bytes.into());
        assert_eq!(expected.is_ok(), result.is_ok());
        if expected.is_ok() {
            assert_eq!(expected.unwrap(), result.unwrap());
        } else {
            assert_eq!(
                expected.unwrap_err().to_string(),
                result.unwrap_err().to_string()
            );
        }
    }

    // --- Serialization ---
    #[rstest]
    // Simple strings
    #[case::simple_string(RespType::SimpleString("Test".into()), "+Test\r\n")]
    #[case::simple_string_empty(RespType::SimpleString("".into()), "+\r\n")]
    // Simple errors
    #[case::simple_string(RespType::SimpleError("Test".into()), "-Test\r\n")]
    #[case::simple_string_empty(RespType::SimpleError("".into()), "-\r\n")]
    // Bulk strings
    #[case::bulk_string(RespType::BulkString(Some("Test".into())), "$4\r\nTest\r\n")]
    #[case::bulk_string_empty(RespType::BulkString(Some("".into())), "$0\r\n\r\n")]
    #[case::bulk_string_with_clrf(RespType::BulkString(Some("Test\r\nAnother".into())), "$13\r\nTest\r\nAnother\r\n")]
    #[case::bulk_string_null(RespType::BulkString(None), "$-1\r\n")]
    // Bulk errors
    #[case::simple_string(RespType::BulkError("Test".into()), "!4\r\nTest\r\n")]
    #[case::simple_string_empty(RespType::BulkError("".into()), "!0\r\n\r\n")]
    #[case::simple_string(RespType::BulkError("SYNTAX invalid syntax".into()), "!21\r\nSYNTAX invalid syntax\r\n")]
    // Integers
    #[case::integer_zero(RespType::Integer(0), ":0\r\n")]
    #[case::integer_positive(RespType::Integer(123), ":123\r\n")]
    #[case::integer_negative(RespType::Integer(-123), ":-123\r\n")]
    // Arrays
    // Null
    #[case::null(RespType::Null(), "_\r\n")]
    /// Tests the RESP serialization.
    fn test_serialize(#[case] message: RespType, #[case] expected: String) {
        assert_eq!(expected, message.serialize());
    }
}
