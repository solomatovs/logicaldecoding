
use std::fmt::Debug; 
use std::sync::TryLockError;

use thiserror::Error;


#[derive(Debug, Error)]
pub enum DymodError {
    #[error("failed to take the lock {0}, try again later")]
    WouldBlock(String),
    
    #[error("poisoned lock {0}. please reload lib {0}")]
    PoisonedLock(String, String),
    
    #[error("failed to load {0}: {1}")]
    FailedToLoadLib(String, libloading::Error),

    #[error("failed to load symbol {0} in lib {1}: {2}")]
    FailedToLoadSymbol(String, String, libloading::Error),
}

// impl From<libloading::Error> for DymodError {
//     fn from(item: libloading::Error) -> Self {
//         DymodError::FailedToLoadLib(item)
//     }
// }

impl<T> From<TryLockError<T>> for DymodError {
    fn from(item: TryLockError<T>) -> Self {
        match item {
            TryLockError::WouldBlock => DymodError::WouldBlock("lib".into()),
            TryLockError::Poisoned(x) => DymodError::PoisonedLock(x.to_string(), "lib".into()),
        }
    }
}
