//! This module contains the ECHO command.
use crate::resp;

/// Handles the ECHO command.
pub fn handle(args: Vec<resp::RespType>) -> resp::RespType {
    if let Some(message_token) = args.first() {
        let message = resp::extract_string(message_token).ok();
        resp::RespType::BulkString(message)
    } else {
        log::trace!("No message provided.");
        resp::RespType::BulkString(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    // --- Tests ---
    #[rstest]
    fn test_simple_string() {
        let message = "Test";
        let args = vec![resp::RespType::SimpleString(message.to_string())];
        assert_eq!(
            resp::RespType::BulkString(Some(message.to_string())),
            handle(args)
        );
    }

    #[rstest]
    fn test_bulk_string() {
        let message = "Test";
        let args = vec![resp::RespType::BulkString(Some(message.to_string()))];
        assert_eq!(
            resp::RespType::BulkString(Some(message.to_string())),
            handle(args)
        );
    }

    #[rstest]
    fn test_missing() {
        let args = vec![];
        assert_eq!(resp::RespType::BulkString(None), handle(args));
    }

    #[rstest]
    fn test_invalid() {
        let args = vec![resp::RespType::Array(vec![resp::RespType::BulkString(
            Some("Test".to_string()),
        )])];
        assert_eq!(resp::RespType::BulkString(None), handle(args));
    }
}
