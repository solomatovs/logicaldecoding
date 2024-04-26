#[macro_export]
macro_rules! dymod_2 {
  (
    pub mod $modname: ident {
      $(
        pub struct $struct_name: ident {
          $(fn $fnname: ident ( $($argname: ident : $argtype: ty),* $(,)? ) $( -> $returntype: ty)? ;)*
        }
      )*
    }
  ) => {
    pub mod $modname {
      use super::*;

      use std::io;
      use std::path::Path;
      use std::collections::HashMap;
      use std::thread::sleep;
      use std::sync::TryLockError;

      use $crate::{Library, Symbol, Error};
      use $crate::{DymodError};

      use std::sync::RwLock;

      $(
        pub struct $struct_name {
          file_path: String,
          dy: RwLock<Library>,
        }
        
        impl $struct_name {
          pub fn new(file_path: &str) -> Result<$struct_name, Error> {
            let dy = unsafe {Library::new(&file_path)?};

            let res = $struct_name {
              file_path: file_path.to_string(),
              dy: RwLock::new(dy),
            };

            Ok(res)
          }

          pub fn reload(&mut self) -> Result<(), Error> {
            let dy_new = unsafe {Library::new(&self.file_path)?};
            
            loop {
              match self.dy.try_write() {
                Ok(mut dy) => {
                  *dy = dy_new;
                  break;
                },
                Err(TryLockError::WouldBlock) => sleep(std::time::Duration::from_millis(500)),
                Err(TryLockError::Poisoned(_)) => {
                  println!("The library is poisoned");
                  let res = $struct_name {
                    file_path: self.file_path.clone(),
                    dy: RwLock::new(dy_new),
                  };

                  break;
                },
              }
            }

            Ok(())
          }

          $(
            pub fn $fnname(&self, $($argname: $argtype),*) -> Result<($($returntype)?), Error> {
              loop {
                {
                  let symbol_signature = concat!("fn ", stringify!($fnname), "(", stringify!($($argtype)*), ")", stringify!($(-> $returntype)*));
                  
                  let lib = self.dy.read().unwrap();

                  let symbol: Symbol<fn($($argtype),*) $(-> $returntype)?> = unsafe {
                    lib.get(stringify!($fnname).as_bytes())
                  }?;
                  
                  return Ok(symbol($($argname),*))
                }
              };
            }
          )*
        }
      )*
    }
  };
}
