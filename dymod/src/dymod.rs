#[macro_export]
macro_rules! dymod {
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

      use $crate::{Library, Symbol};
      use $crate::{DymodError, DymodSource};

      use std::sync::RwLock;

      $(
        pub struct $struct_name {
          dy: RwLock<DymodSource>
        }

        impl $struct_name {
          $(
            pub fn $fnname(&self, $($argname: $argtype),*) -> Result<($($returntype)?), DymodError> {
              let mut reload: bool = false;
              loop {
                {
                  match self.dy.read() {
                    Ok(bor) => {
                      if !bor.reload_needed() {
                        let lib = bor.get_lib_ref()?;
                        let symbol = unsafe {
                          lib.get(stringify!($fnname).as_bytes())
                        };
                        
                        let symbol: Symbol<extern fn($($argtype),*) $(-> $returntype)?> = match symbol {
                          Ok(sym) => sym,
                          Err(e) => {
                            let symbol_signature = concat!("fn ", stringify!($fnname), "(", stringify!($($argtype)*), ")", stringify!($(-> $returntype)*));
                            return Err(DymodError::SymbolNotFound(e, String::from(symbol_signature)))
                          },
                        };
                        
                        return Ok(symbol($($argname),*))
                      } else {
                        reload = true;
                      }
                    },
                    Err(e) => {
                      return Err(DymodError::PoisonError);
                    }
                  }
                }
                
                // reload lib if needed
                if reload {
                  match self.dy.write() {
                    Ok(mut dy) => {
                      *dy = dy.create_new_version()?;
                    },
                    Err(e) => {
                      return Err(DymodError::PoisonError);
                    }
                  }
                }
              };
            }
          )*
          
          pub fn load_library(file_path: &str) -> Result<$struct_name, DymodError> {
            let dy = match DymodSource::new(&file_path, 1) {
              Ok(dy) => dy,
              Err(e) => return Err(e),
            };

            let res = $struct_name {
              dy: RwLock::new(dy)
            };

            Ok(res)
          }
        }
      )*
    }
  };
}
