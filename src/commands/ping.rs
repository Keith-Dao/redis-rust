//! This module contains the PING command.
use crate::resp;

/// Handles the PING command.
pub fn handle() -> resp::RespType {
    resp::RespType::SimpleString("PONG".into())
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_handle() {
        assert_eq!(resp::RespType::SimpleString("PONG".into()), handle());
    }
}
