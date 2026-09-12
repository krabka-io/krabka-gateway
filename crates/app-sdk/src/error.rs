//! Public SDK error taxonomy.

/// Error taxonomy shared by the app SDK contract.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum KrabkaError {
    /// Transport or endpoint reachability failure.
    #[error("transport: {0}")]
    Transport(String),
    /// Authentication failed or credentials were absent.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// Caller supplied an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Target resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// Server-side failure outside the narrower classes.
    #[error("server error: {0}")]
    ServerError(String),
    /// SDK module is intentionally gated on later work.
    #[error("{module} is unimplemented; gated on {gated_on}")]
    Unimplemented {
        /// SDK module name.
        module: &'static str,
        /// Plan or spec slug gating the module.
        gated_on: &'static str,
    },
}

impl KrabkaError {
    /// Convert a Connect code string into the SDK taxonomy.
    #[must_use]
    pub fn from_connect_code(code: &str, message: impl Into<String>) -> Self {
        let message = message.into();
        match code {
            "not_found" => Self::NotFound(message),
            "invalid_argument" | "failed_precondition" | "out_of_range" => {
                Self::InvalidArgument(message)
            }
            "unauthenticated" => Self::Unauthenticated(message),
            "unavailable" | "deadline_exceeded" => Self::Transport(message),
            _ => Self::ServerError(message),
        }
    }
}

impl From<crate::connect_client::ConnectClientError> for KrabkaError {
    fn from(value: crate::connect_client::ConnectClientError) -> Self {
        match value {
            crate::connect_client::ConnectClientError::Connect { code, message } => {
                Self::from_connect_code(&code, message)
            }
            crate::connect_client::ConnectClientError::HttpStatus { status, message } => {
                match status {
                    400 => Self::InvalidArgument(message),
                    401 => Self::Unauthenticated(message),
                    404 => Self::NotFound(message),
                    408 | 429 | 502..=504 => Self::Transport(message),
                    _ => Self::ServerError(message),
                }
            }
            other => Self::Transport(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_codes_map_to_taxonomy() {
        assert!(matches!(
            KrabkaError::from_connect_code("not_found", "x"),
            KrabkaError::NotFound(_)
        ));
        assert!(matches!(
            KrabkaError::from_connect_code("unavailable", "x"),
            KrabkaError::Transport(_)
        ));
        assert!(matches!(
            KrabkaError::from_connect_code("invalid_argument", "x"),
            KrabkaError::InvalidArgument(_)
        ));
    }

    #[test]
    fn http_statuses_map_to_taxonomy_when_connect_body_is_unusable() {
        let invalid = KrabkaError::from(crate::connect_client::ConnectClientError::HttpStatus {
            status: 400,
            message: "bad request".into(),
        });
        let unavailable =
            KrabkaError::from(crate::connect_client::ConnectClientError::HttpStatus {
                status: 503,
                message: "down".into(),
            });

        assert2::assert!(let KrabkaError::InvalidArgument(message) = invalid);
        assert_eq!(message, "bad request");
        assert2::assert!(let KrabkaError::Transport(message) = unavailable);
        assert_eq!(message, "down");
    }
}
