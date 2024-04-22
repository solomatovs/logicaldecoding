use std::error::Error;
use std::fmt::{Display, Result};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ParseLogLevelError(pub String);

impl Error for ParseLogLevelError {}

impl Display for ParseLogLevelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result {
        let err = format!(
            "Error parsing log_level: {:?}\nlog_level must be: trace, debug, info, warn, error",
            self.0
        );
        f.write_str(err.as_str())
    }
}


#[derive(Debug)]
pub enum FindError {
    RegexError(regex::Error),
    NoFileExtension,
    InvalidFileName,
    InvalidBaseFile,
    OsStringNotUtf8,
    IoError(std::io::Error),
}

impl Error for FindError {}

impl Display for FindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result {
        f.write_str("".into())
    }
}