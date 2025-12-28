//! This module contains the state for each session.
use anyhow::Result;

#[derive(Debug, PartialEq)]
pub enum ProtocolVersion {
    V2,
    V3,
}

impl ProtocolVersion {
    pub fn from_string<T: AsRef<str>>(version: T) -> Result<Self> {
        match version.as_ref() {
            "2" => Ok(ProtocolVersion::V2),
            "3" => Ok(ProtocolVersion::V3),
            x => Err(anyhow::anyhow!("Invalid protocol version: {}", x)),
        }
    }

    pub fn to_version_number(&self) -> usize {
        match self {
            ProtocolVersion::V2 => 2,
            ProtocolVersion::V3 => 3,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct State {
    pub protocol_version: ProtocolVersion,
    pub client_id: usize,
}

impl State {
    pub fn new(client_id: usize) -> Self {
        Self {
            protocol_version: ProtocolVersion::V2,
            client_id,
        }
    }

    pub fn update_version_from_string<T: AsRef<str>>(&mut self, version: T) -> Result<()> {
        let version = ProtocolVersion::from_string(version)?;
        self.protocol_version = version;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // --- Tests ---
    mod protocol_version {
        use super::*;
        #[rstest]
        #[case::v2_str("2", Ok(ProtocolVersion::V2))]
        #[case::v3_str("3", Ok(ProtocolVersion::V3))]
        #[case::invalid_str("3a", Err(anyhow::anyhow!("Invalid protocol version: 3a")))]
        #[case::v2_string("2".to_string(), Ok(ProtocolVersion::V2))]
        #[case::v3_string("3".to_string(), Ok(ProtocolVersion::V3))]
        #[case::invalid_string("3a".to_string(), Err(anyhow::anyhow!("Invalid protocol version: 3a")))]
        fn test_from_string<T: AsRef<str>>(
            #[case] input: T,
            #[case] expected: Result<ProtocolVersion>,
        ) {
            let result = ProtocolVersion::from_string(input);
            assert_eq!(expected.is_ok(), result.is_ok());
            if expected.is_ok() {
                assert_eq!(expected.unwrap(), result.unwrap());
            } else {
                assert_eq!(
                    expected.unwrap_err().to_string(),
                    result.unwrap_err().to_string()
                );
            }
        }

        #[rstest]
        #[case::v2(ProtocolVersion::V2, 2)]
        #[case::v3(ProtocolVersion::V3, 3)]
        fn test_version_to_number(#[case] version: ProtocolVersion, #[case] expected: usize) {
            assert_eq!(expected, version.to_version_number());
        }
    }

    mod state {
        use super::*;

        #[rstest]
        fn test_new() {
            assert_eq!(
                State::new(0),
                State {
                    protocol_version: ProtocolVersion::V2,
                    client_id: 0
                }
            );
        }

        #[rstest]
        #[case::v2_str("2", State{ protocol_version: ProtocolVersion::V2, client_id: 0 })]
        #[case::v3_str("3", State{ protocol_version: ProtocolVersion::V3, client_id: 0 })]
        #[case::v2_string("2".to_string(), State{ protocol_version: ProtocolVersion::V2, client_id: 0 })]
        #[case::v3_string("3".to_string(), State{ protocol_version: ProtocolVersion::V3, client_id: 0 })]
        fn test_update_protocol_version_from_string<T: AsRef<str>>(
            #[case] input: T,
            #[case] expected: State,
        ) {
            let mut state = State::new(0);
            let result = state.update_version_from_string(input);
            assert!(result.is_ok());
            assert_eq!(expected, state);
        }

        #[rstest]
        #[case::invalid_str("3a", "Invalid protocol version: 3a")]
        #[case::invalid_string("3a".to_string(), "Invalid protocol version: 3a")]
        fn test_update_protocol_version_from_invalid_string<T: AsRef<str>>(
            #[case] input: T,
            #[case] expected: String,
        ) {
            let mut state = State::new(0);
            let result = state.update_version_from_string(&input);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().to_string(), expected);
        }
    }
}
