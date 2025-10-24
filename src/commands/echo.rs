//! This module contains the ECHO command.
use crate::{commands::Command, resp, store};

pub struct Echo();

impl Command for Echo {
    fn name(&self) -> String {
        "ECHO".into()
    }

    /// Handles the ECHO command.
    async fn handle(&self, args: Vec<resp::RespType>, _: &store::SharedStore) -> resp::RespType {
        if let Some(message_token) = args.first() {
            let message = resp::extract_string(message_token).ok();
            resp::RespType::BulkString(message)
        } else {
            log::trace!("No message provided.");
            resp::RespType::BulkString(None)
        }
    }
}

/// Handles the ECHO command.

#[cfg(test)]
mod test {
    use super::*;
    use rstest::{fixture, rstest};

    // --- Fixtures ---
    #[fixture]
    fn store() -> crate::store::SharedStore {
        crate::store::new()
    }

    #[fixture]
    fn echo() -> Echo {
        Echo()
    }

    // --- Tests ---
    #[rstest]
    #[tokio::test]
    async fn test_simple_string(echo: Echo, store: crate::store::SharedStore) {
        let message = "Test";
        let args = vec![resp::RespType::SimpleString(message.into())];
        assert_eq!(
            resp::RespType::BulkString(Some(message.into())),
            echo.handle(args, &store).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bulk_string(echo: Echo, store: crate::store::SharedStore) {
        let message = "Test";
        let args = vec![resp::RespType::BulkString(Some(message.into()))];
        assert_eq!(
            resp::RespType::BulkString(Some(message.into())),
            echo.handle(args, &store).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_missing(echo: Echo, store: crate::store::SharedStore) {
        let args = vec![];
        assert_eq!(
            resp::RespType::BulkString(None),
            echo.handle(args, &store).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid(echo: Echo, store: crate::store::SharedStore) {
        let args = vec![resp::RespType::Array(vec![resp::RespType::BulkString(
            Some("Test".into()),
        )])];
        assert_eq!(
            resp::RespType::BulkString(None),
            echo.handle(args, &store).await
        );
    }
}
