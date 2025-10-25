//! This module contains the ECHO command.
use crate::{commands::Command, resp, store};

pub struct Echo;

#[async_trait::async_trait]
impl Command for Echo {
    fn static_name() -> String {
        "ECHO".into()
    }

    fn name(&self) -> String {
        Self::static_name()
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

    // --- Tests ---
    #[rstest]
    fn test_static_name() {
        assert_eq!("ECHO", Echo::static_name());
    }

    #[rstest]
    fn test_name() {
        assert_eq!("ECHO", Echo.name());
    }

    #[rstest]
    #[tokio::test]
    async fn test_simple_string(store: crate::store::SharedStore) {
        let message = "Test";
        let args = vec![resp::RespType::SimpleString(message.into())];
        assert_eq!(
            resp::RespType::BulkString(Some(message.into())),
            Echo.handle(args, &store).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bulk_string(store: crate::store::SharedStore) {
        let message = "Test";
        let args = vec![resp::RespType::BulkString(Some(message.into()))];
        assert_eq!(
            resp::RespType::BulkString(Some(message.into())),
            Echo.handle(args, &store).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_missing(store: crate::store::SharedStore) {
        let args = vec![];
        assert_eq!(
            resp::RespType::BulkString(None),
            Echo.handle(args, &store).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid(store: crate::store::SharedStore) {
        let args = vec![resp::RespType::Array(vec![resp::RespType::BulkString(
            Some("Test".into()),
        )])];
        assert_eq!(
            resp::RespType::BulkString(None),
            Echo.handle(args, &store).await
        );
    }
}
