//! This module contains the PING command.
use crate::{commands::Command, resp, store};

pub struct Ping();

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

    #[fixture]
    fn ping() -> Ping {
        Ping()
    }

    // --- Tests ---
    #[rstest]
    fn test_static_name() {
        assert_eq!("PING", Ping::static_name());
    }

    #[rstest]
    fn test_name(ping: Ping) {
        assert_eq!("PING", ping.name());
    }

    #[rstest]
    #[tokio::test]
    async fn test_handle(ping: Ping, store: crate::store::SharedStore) {
        assert_eq!(
            resp::RespType::SimpleString("PONG".into()),
            ping.handle(vec![], &store).await
        );
    }
}
