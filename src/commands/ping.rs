//! This module contains the PING command.
use crate::{commands::Command, resp, store};

pub struct Ping;

#[async_trait::async_trait]
impl Command for Ping {
    fn static_name() -> String {
        "PING".into()
    }

    fn name(&self) -> String {
        Self::static_name()
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
    fn test_static_name() {
        assert_eq!("PING", Ping::static_name());
    }

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
