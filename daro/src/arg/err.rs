use std::error::Error;
use std::ffi::OsString;
use std::fmt::{Display, Result};

// #[derive(Debug, Default, PartialEq, Eq)]
// pub struct ConfigParseError(pub OsString, pub String);

#[derive(Debug, PartialEq, Eq)]
pub enum ConfigParseError {
    ConfigParsingError(OsString, String),
}

impl Error for ConfigParseError {}

impl Display for ConfigParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result {
        match self {
            ConfigParseError::ConfigParsingError(path, error) => {
                let err = format!("Configuration file: {:?}\nError parsing: {:?}", path, error);
                f.write_str(err.as_str())
            }
        }
    }
}
