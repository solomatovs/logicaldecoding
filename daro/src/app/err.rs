use std::error::Error;
use std::fmt::{Display, Result};

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
