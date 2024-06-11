use crate::util::Error::InvalidFormat;

#[derive(Debug)]
pub enum Error {
    InvalidFormat(InvalidFormatError)
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::invalid_format(msg)
    }
}

impl Error {
    pub(crate) fn invalid_format(msg: &str) -> Error {
        InvalidFormat(InvalidFormatError::new(msg))
    }
}

#[derive(Debug)]
pub struct InvalidFormatError {
    msg: String,
}

impl InvalidFormatError {
    fn new(msg: &str) -> Self {
        InvalidFormatError { msg: msg.to_string() }
    }
}