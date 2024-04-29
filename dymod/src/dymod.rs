#[macro_export]
macro_rules! dymod {
  (
    pub mod $modname: ident {
      $(
        $(#[$attr:meta])*
        $struct_vis:vis $(<$($struct_vis_lifetime:lifetime),+>)* struct $struct_name:ident $(<$($struct_lifetime:lifetime),+>)* {
          $(fn $fnname:ident $(<$($fn_lifetime:lifetime),+>)* ($($argname:ident : $argtype:ty),* $(,)?) $(-> $(<$($returntype_lifetime:lifetime),+>)* $returntype:ty)? ;)*
        }
      )*
    }
  ) => {
    pub mod $modname {
      use super::*;

      use std::path::{Path, PathBuf};
      use std::ffi::{OsStr, OsString};
      use std::sync::Arc;

      use $crate::{Library, Symbol};
      use $crate::{DymodError};

      use std::sync::RwLock;

      $(
        #[derive(Debug, Clone)]
        pub struct $struct_name {
            file_path: OsString,
            lib: Arc<Library>,
            lock: Arc<RwLock<()>>,
            $(
                $fnname: fn($($argtype),*) $(-> $returntype)?,
            )*
        }

        impl $struct_name {
            pub fn new(file_path: OsString) -> Result<$struct_name, DymodError> {
                let lock = RwLock::new(());
                // let s = file_path.into_string().unwrap();

                let lib = unsafe {
                    Library::new(&file_path)
                    // .map_err(|e| DymodError::FailedToLoadLib(s, e))
                }?;

                let res = Self {
                    $(
                        $fnname: {
                            let symbol = unsafe {
                                lib.get(stringify!($fnname).as_bytes())
                                // .map_err(|e| DymodError::FailedToLoadSymbol($fnname, s, e));
                            }?;

                            *symbol
                        },
                    )*
                    file_path,
                    lib: Arc::new(lib),
                    lock: Arc::new(lock),
                };

                Ok(res)
            }

            // pub fn reload(&mut self) -> Result<(), DymodError> {
            //     let new_load = Self::new(self.file_path.clone())?;

            //     self.lib = new_load.lib;

            //     $(
            //         self.$fnname = new_load.$fnname;
            //     )*

            //     Ok(())
            // }

            $(
                pub fn $fnname(&self, $($argname: $argtype),*) -> ($($returntype)?) {
                    (self.$fnname)($($argname),*)
                }
            )*
        }
      )*
    }
  };
}
