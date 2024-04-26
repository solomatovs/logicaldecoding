
use std::fmt; 
use std::io;
use std::error::Error;
use libloading;

#[derive(Debug)]
pub enum DymodError {
  IOError(io::Error, String),
  LibloadingError(libloading::Error),
  DymodNonInitialized,
  SymbolNotFound(libloading::Error, String),
  PoisonError,
}

impl fmt::Display for DymodError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.to_string())
  }
}

impl Error for DymodError {}
