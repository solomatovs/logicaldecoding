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

      use std::path::{Path, PathBuf};
      use std::ffi::{OsStr, OsString};

      use $crate::{Library, Symbol};
      use $crate::{DymodError};

      use std::sync::RwLock;

      $(
        pub struct $struct_name {
            file_path: OsString,
            lib: Library,
            lock: RwLock<()>,
            $(
                $fnname: fn($($argtype),*) $(-> $returntype)?,
            )*
        }
        
        impl $struct_name {
            pub fn new(file_path: OsString) -> Result<$struct_name, DymodError> {
                let lock = RwLock::new(());
                let s = file_path.into_string().unwrap();

                let lib = unsafe {
                    Library::new(&file_path)
                        .map_err(|e| DymodError::FailedToLoadLib(s, e))?
                };

                let res = Self {
                    file_path,
                    lib,
                    lock,
                    $(
                        $fnname: {
                            let symbol = unsafe {
                                lib.get(stringify!($fnname).as_bytes())
                            }.map_err(|e| DymodError::FailedToLoadSymbol($fnname, s, e));

                            *symbol?
                        },
                    )*
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





// #[macro_export]
// macro_rules! dynamic_lib {
//   (
//     pub mod $modname:ident {
//       $(
//         pub struct $struct_name: ident {
//             $(fn $fnname: ident ( $($argname: ident : $argtype: ty),* $(,)? ) $( -> $returntype: ty)? ;)*
//         }
//       )*
//     }
//   ) => {
//     pub mod $modname {
//       use super::*;

//       use std::path::{Path, PathBuf};
//       use std::ffi::{OsStr, OsString};

//       use $crate::{Library, Symbol};
//       use $crate::{DymodError};

//       $(
//         pub struct $struct_name {
//             dy: Library,
//             file_path: OsString,
//             $(
//                 $fn_name: fn($($arg_type),*) $(-> $return_type)?,
//             )*
//         }
        
//         impl $struct_name {
//             pub fn new(file_path: OsString) -> Result<$struct_name, DymodError> {
//                 let s = file_path.into_string().unwrap();

//                 let lib = unsafe {
//                     Library::new(&file_path)
//                         .map_err(|e| DymodError::FailedToLoadLib(s, e))?
//                 };

//                 let res = Self {
//                     $(
//                         $fn_name: {
//                             let symbol = unsafe {
//                                 lib.get(stringify!($fn_name).as_bytes())
//                             }.map_err(|e| DymodError::FailedToLoadSymbol($fn_name, s, e));

//                             *symbol?
//                         },
//                     )*
//                     file_path,
//                     dy: lib,
//                 };

//                 Ok(res)
//             }

//             pub fn reload(&mut self) -> Result<(), DymodError> {
//                 let new_load = Self::new(self.file_path.clone())?;

//                 self.dy = new_load.dy;
//                 $(
//                     self.$fn_name = new_load.$fn_name;
//                 )*

//                 Ok(())
//             }

//             $(
//                 pub fn $fn_name(&self, $($arg_name: $arg_type),*) -> ($($return_type)?) {
//                     (self.$fn_name)($($arg_name),*)
//                 }
//             )*
//         }
//       )*
//     }
//   };
// }
