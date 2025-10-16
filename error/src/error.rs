use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TrackError {
    #[error("wrapped error: {0}")]
    WrappedError(#[from]io::Error),

    #[error("default error: {0}")]
    DefaultError(String),
}

impl From<String> for TrackError {
    fn from(s: String) -> Self {
        TrackError::DefaultError(s)
    }
}

impl From<&str> for TrackError {
    fn from(s: &str) -> Self {
        TrackError::DefaultError(s.to_string())
    }
}