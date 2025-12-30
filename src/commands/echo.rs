//! This module contains the ECHO command.
use crate::commands::Command;

pub struct Echo;

#[async_trait::async_trait]
impl Command for Echo {
    fn name(&self) -> String {
        "ECHO".into()
    }

    /// Handles the ECHO command.
    async fn handle(
        &self,
        args: Vec<crate::resp::RespType>,
        _: &crate::store::SharedStore,
        _: &mut crate::state::State,
    ) -> crate::resp::RespType {
        if let Some(message_token) = args.first() {
            let message = crate::resp::extract_string(message_token).ok();
            crate::resp::RespType::BulkString(message)
        } else {
            log::trace!("No message provided.");
            crate::resp::RespType::BulkString(None)
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
    fn state() -> crate::state::State {
        crate::state::State::new(0)
    }

    // --- Tests ---
    #[rstest]
    fn test_name() {
        assert_eq!("ECHO", Echo.name());
    }

    #[rstest]
    #[tokio::test]
    async fn test_simple_string(store: crate::store::SharedStore, mut state: crate::state::State) {
        let message = "Test";
        let args = vec![crate::resp::RespType::SimpleString(message.into())];
        assert_eq!(
            crate::resp::RespType::BulkString(Some(message.into())),
            Echo.handle(args, &store, &mut state).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_bulk_string(store: crate::store::SharedStore, mut state: crate::state::State) {
        let message = "Test";
        let args = vec![crate::resp::RespType::BulkString(Some(message.into()))];
        assert_eq!(
            crate::resp::RespType::BulkString(Some(message.into())),
            Echo.handle(args, &store, &mut state).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_missing(store: crate::store::SharedStore, mut state: crate::state::State) {
        let args = vec![];
        assert_eq!(
            crate::resp::RespType::BulkString(None),
            Echo.handle(args, &store, &mut state).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_invalid(store: crate::store::SharedStore, mut state: crate::state::State) {
        let args = vec![crate::resp::RespType::Array(vec![
            crate::resp::RespType::BulkString(Some("Test".into())),
        ])];
        assert_eq!(
            crate::resp::RespType::BulkString(None),
            Echo.handle(args, &store, &mut state).await
        );
    }
}
