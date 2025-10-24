//! This module contains the PING command.
use crate::{commands::Command, resp, store};

pub struct Ping();

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

    #[fixture]
    fn ping() -> Ping {
        Ping()
    }

    // --- Tests ---
    #[rstest]
    #[tokio::test]
    async fn test_handle(ping: Ping, store: crate::store::SharedStore) {
        assert_eq!(
            resp::RespType::SimpleString("PONG".into()),
            ping.handle(vec![], &store).await
        );
    }
}
