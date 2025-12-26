//! This module contains the PING command.
use crate::commands::Command;

pub struct Ping;

#[async_trait::async_trait]
impl Command for Ping {
    fn name(&self) -> String {
        "PING".into()
    }

    /// Handles the PING command.
    async fn handle(
        &self,
        _: Vec<crate::resp::RespType>,
        _: &crate::store::SharedStore,
        _: &mut crate::state::State,
    ) -> crate::resp::RespType {
        crate::resp::RespType::SimpleString("PONG".into())
    }
}

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
        crate::state::State::new()
    }

    // --- Tests ---
    #[rstest]
    fn test_name() {
        assert_eq!("PING", Ping.name());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle(store: crate::store::SharedStore, mut state: crate::state::State) {
        assert_eq!(
            crate::resp::RespType::SimpleString("PONG".into()),
            Ping.handle(vec![], &store, &mut state).await
        );
    }
}
