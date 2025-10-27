//! This module contains the PING command.
use crate::{commands::Command, resp, store};

pub struct Ping;

#[async_trait::async_trait]
impl Command for Ping {
    fn name(&self) -> String {
        "PING".into()
    }

    /// Handles the PING command.
    async fn handle(&self, _: Vec<resp::RespType>, _: &store::SharedStore) -> resp::RespType {
        resp::RespType::SimpleString("PONG".into())
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

    // --- Tests ---
    #[rstest]
    fn test_name() {
        assert_eq!("PING", Ping.name());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle(store: crate::store::SharedStore) {
        assert_eq!(
            resp::RespType::SimpleString("PONG".into()),
            Ping.handle(vec![], &store).await
        );
    }
}
