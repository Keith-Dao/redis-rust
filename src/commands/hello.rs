//! This module contains the HELLO command.
use anyhow::{Context, Result};

use crate::commands::Command;

pub struct Hello;

/// Parses the HELLO options.
fn parse_hello_options<I: IntoIterator<Item = crate::resp::RespType>>(
    iter: I,
) -> Result<Option<String>> {
    let mut iter = iter.into_iter();

    if let Some(version) = iter.next() {
        Ok(Some(
            crate::resp::extract_string(&version).context("Failed to parse protocol version")?,
        ))
    } else {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Command for Hello {
    fn name(&self) -> String {
        "HELLO".into()
    }

    /// Handles the HELLO command.
    async fn handle(
        &self,
        args: Vec<crate::resp::RespType>,
        _: &crate::store::SharedStore,
        state: &mut crate::state::State,
    ) -> crate::resp::RespType {
        let protocol_version = parse_hello_options(args);
        if let Err(err) = protocol_version {
            log::error!("{err}");
            return crate::resp::RespType::SimpleError(format!("ERR {err} for 'HELLO' command"));
        }

        let protocol_version = protocol_version.expect("Error arm checcked.");
        if let Some(protocol_version) = protocol_version {
            if let Err(err) = state.update_version_from_string(protocol_version) {
                log::error!("{err}");
                return crate::resp::RespType::SimpleError(format!(
                    "ERR {err} for 'HELLO' command"
                ));
            }
        }

        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(state.protocol_version.to_version_number() as i64),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(state.client_id as i64),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ])
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
        crate::state::State::new(0)
    }

    // --- Tests ---
    #[rstest]
    fn test_name() {
        assert_eq!("HELLO", Hello.name());
    }

    #[rstest]
    #[case::default_preset_v2(
        vec![],
        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(2),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(0),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ]),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V2, client_id: 0 },
        crate::state::ProtocolVersion::V2
    )]
    #[case::v2_preset_v2(
        vec![crate::resp::RespType::SimpleString("2".into())],
        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(2),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(0),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ]),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V2, client_id: 0 },
        crate::state::ProtocolVersion::V2
    )]
    #[case::v3_preset_v2(
        vec![crate::resp::RespType::SimpleString("3".into())],
        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(3),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(0),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ]),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V3, client_id: 0 },
        crate::state::ProtocolVersion::V2
    )]
    #[case::invalid_version_preset_v2(
        vec![crate::resp::RespType::SimpleString("a".into())],
        crate::resp::RespType::SimpleError("ERR Invalid protocol version: a for 'HELLO' command".into()),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V2, client_id: 0 },
        crate::state::ProtocolVersion::V2
    )]
    #[case::invalid_argument_preset_v2(
        vec![crate::resp::RespType::Null()],
        crate::resp::RespType::SimpleError("ERR Failed to parse protocol version for 'HELLO' command".into()),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V2, client_id: 0 },
        crate::state::ProtocolVersion::V2
    )]
    #[case::default_preset_v3(
        vec![],
        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(3),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(0),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ]),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V3, client_id: 0 },
        crate::state::ProtocolVersion::V3
    )]
    #[case::v2_preset_v3(
        vec![crate::resp::RespType::SimpleString("2".into())],
        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(2),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(0),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ]),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V2, client_id: 0 },
        crate::state::ProtocolVersion::V3
    )]
    #[case::v3_preset_v3(
        vec![crate::resp::RespType::SimpleString("3".into())],
        crate::resp::RespType::Map(vec![
            (
                crate::resp::RespType::BulkString(Some("server".into())),
                crate::resp::RespType::BulkString(Some("redis".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("version".into())),
                crate::resp::RespType::BulkString(Some("0.0.1".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("proto".into())),
                crate::resp::RespType::Integer(3),
            ),
            (
                crate::resp::RespType::BulkString(Some("id".into())),
                crate::resp::RespType::Integer(0),
            ),
            (
                crate::resp::RespType::BulkString(Some("mode".into())),
                crate::resp::RespType::BulkString(Some("standalone".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("role".into())),
                crate::resp::RespType::BulkString(Some("master".into())),
            ),
            (
                crate::resp::RespType::BulkString(Some("modules".into())),
                crate::resp::RespType::Array(vec![]),
            ),
        ]),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V3, client_id: 0 },
        crate::state::ProtocolVersion::V3,
    )]
    #[case::invalid_version_preset_v3(
        vec![crate::resp::RespType::SimpleString("a".into())],
        crate::resp::RespType::SimpleError("ERR Invalid protocol version: a for 'HELLO' command".into()),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V3, client_id: 0 },
        crate::state::ProtocolVersion::V3,
    )]
    #[case::invalid_argument_preset_v3(
        vec![crate::resp::RespType::Null()],
        crate::resp::RespType::SimpleError("ERR Failed to parse protocol version for 'HELLO' command".into()),
        crate::state::State { protocol_version: crate::state::ProtocolVersion::V3, client_id: 0 },
        crate::state::ProtocolVersion::V3,
    )]
    #[tokio::test]
    async fn test_handle(
        store: crate::store::SharedStore,
        mut state: crate::state::State,
        #[case] args: Vec<crate::resp::RespType>,
        #[case] expected: crate::resp::RespType,
        #[case] expected_state: crate::state::State,
        #[case] preset_version: crate::state::ProtocolVersion,
    ) {
        state.protocol_version = preset_version;
        let result = Hello.handle(args, &store, &mut state).await;
        assert_eq!(expected, result);
        assert_eq!(expected_state, state);
    }
}
