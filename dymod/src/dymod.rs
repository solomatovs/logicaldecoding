
macro_rules! as_item( ($i:item) => ($i) );

macro_rules! foo(
    (
        pub struct $name:ident $body:tt
    ) => (
        foo!(parse [pub struct $name] [impl $name] [] [] [] $body);
    );
    
    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {}
    ) => (
        foo!(output $decl $impl
            [$($member)*]
            [$($fn)*]
            [$($fn_field)*]
        );
    );

    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {
            fn $name:ident(&mut self $(,)? $($item:ident:$ty:ty),*) $(-> $ret:ty)?, $($t:tt)*
        }
    ) => (
        foo!(parse $decl $impl 
            [$($member)*]
            [$($fn)*]
            [$($fn_field)*]
            {
                [fn $name], [self], [&mut Self,], [&mut self], [self,], [$($item:$ty),*], [$(-> $ret)?], $($t)*
            }

        );
    );

    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {
            fn $name:ident(&self $(,$item:ident:$ty:ty)*) $(-> $ret:ty)?, $($t:tt)*
        }
    ) => (
        foo!( parse $decl $impl 
            [$($member)*]
            [$($fn)*]
            [$($fn_field)*]
            {
                [fn $name], [self], [&Self,], [&self], [self,], [$($item:$ty),*], [$(-> $ret)?], $($t)*
            }

        );
    );

    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {
            fn $name:ident(self $(,$item:ident:$ty:ty)*) $(-> $ret:ty)?, $($t:tt)*
        }
    ) => (
        foo!( parse $decl $impl 
            [$($member)*]
            [$($fn)*]
            [$($fn_field)*]
            {
                [fn $name], [self], [Self,], [self], [self,], [$($item:$ty),*], [$(-> $ret)?], $($t)*
            }

        );
    );
    
    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {
            fn $name:ident($($item:ident:$ty:ty)*) $(-> $ret:ty)?, $($t:tt)*
        }
    ) => (
        foo!( parse $decl $impl 
            [$($member)*]
            [$($fn)*]
            [$($fn_field)*]
            {
                [fn $name], [self], [], [&self], [], [$($item:$ty),*], [$(-> $ret)?], $($t)*
            }

        );
    );
    
    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {
            [fn $name:ident], [$self:tt], [$($member_self:tt)*], [$($fn_decl_self:tt)*], [$($fn_call_self:tt)*], [$($item:ident : $ty:ty),*], [$(-> $ret:ty)?], $($t:tt)*
        }
    ) => (
        foo!(parse $decl $impl
            [$($member)* $name: fn($($member_self)* $($ty),*) $(-> $ret)?,]
            [$($fn)* fn $name($($fn_decl_self)*, $($item:$ty),*) $(-> $ret)? {
                ($self.$name)($($fn_call_self)* $($item),*)
            }]
            [
                $($fn_field)*,
            ]
            {
                $($t)*
            }
        );
    );
    
    (
        parse $decl:tt $impl:tt [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*] {
            $name:ident: $typ:ty, $($t:tt)*
        }
    ) => (
        foo!(parse $decl $impl 
            [$($member)* $name: $typ,]
            [$($fn)*]
            [$($fn_field)*]
            {
                $($t)*
            }
        );
    );

    (
        output [$($decls:tt)*] [$($impl:tt)*] [$($member:tt)*] [$($fn:tt)*] [$($fn_field:tt)*]
    )
    => (
        as_item!(
            $($decls)* {
                $($member)*
            }
        );

        as_item!(
            $($impl)* {
                $($fn)*
            }
        );
    );
);


#[macro_export]
macro_rules! self_ref_nomut {
	( $x:expr ) => {
		&self $(,$x)*
	}
}
#[macro_export]
macro_rules! self_ref_mut {
	( $x:expr ) => {
		&mut self $(,$x)*
	}
}

#[macro_export]
macro_rules! self_owner_rule {
	( $x:expr ) => {
		self $(,$x)*
	}
}

// How to unify the two following macros "get_function_*" into one macro?
#[macro_export]
macro_rules! get_function {
	( $name:ident, $ref_function:ident, $return_type:ty, $typ:ty ) => {
		pub fn $name (self: $typ) -> $return_type {
			$ref_function!(self.data)
		}
	}
}


macro_rules! make_struct {
  (
      $(#[$attr:meta])*
      $struct_vis:vis $(<$($struct_vis_lifetime:lifetime),+>)*
      struct $struct_name:ident
      $(<$($struct_lifetime:lifetime),+>)*
      {
          $($tt:tt)*
      }
  ) => {
      $(#[$attr])*
      $struct_vis
      $(<$($struct_vis_lifetime),+>)*
      struct $struct_name
      $(<$($struct_lifetime:lifetime),+>)*
      {
          
      }
      
      impl $struct_name {
          make_struct!(@fn_declare $($tt)*);
      }
  };
  (@fn_pointer
      $(
          $(#[$attr:meta])*
          $vis:vis $(<$($vis_lifetime:lifetime),+>)*
          fn $fnname:ident
          $(<$($fn_lifetime:lifetime),+>)*
          ($($argname:ident : $argtype:ty),* $(,)?)
          $(-> $(<$($returntype_lifetime:lifetime),+>)* $returntype:ty)? ;
      )*
  ) => {
      $(
          $fnname: fn( $($argtype),*)
        $(-> $(<$($returntype_lifetime),+>)* $returntype)?
      )*
  };
(@fn_declare
    $(
          $(#[$attr:meta])*
          $vis:vis $(<$($vis_lifetime:lifetime),+>)*
          fn $fnname:ident
          $(<$($fn_lifetime:lifetime),+>)*
          ($($argname:ident : $argtype:ty),* $(,)?)
          $(-> $(<$($returntype_lifetime:lifetime),+>)* $returntype:ty)? ;
    )*
) => {
    $(
        $(#[$attr])*
        $vis $(<$($vis_lifetime),+>)*
      fn $fnname(&self, $($argname: $argtype),*)
      $(-> $(<$($returntype_lifetime),+>)* $returntype)?
      {
              (self.$fnname)($($argname),*)
          }
    )*
}
}



#[macro_export]
macro_rules! dymod {
  (
    pub mod $modname: ident {
      $(
        $(#[$attr:meta])*
        $struct_vis:vis $(<$($struct_vis_lifetime:lifetime),+>)* struct $struct_name:ident $(<$($struct_lifetime:lifetime),+>)* {
          $(fn $fnname:ident $(<$($fn_lifetime:lifetime),+>)* ($($self:ident)? $(,)? $($argname:ident : $argtype:ty),* $(,)?) $(-> $(<$($returntype_lifetime:lifetime),+>)* $returntype:ty)? ;)*
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
                $fnname: fn( $($argtype),*) $(-> $returntype)?,
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

            pub fn reload(&mut self) -> Result<(), DymodError> {
                let new_load = Self::new(self.file_path.clone())?;

                self.lib = new_load.lib;

                $(
                    self.$fnname = new_load.$fnname;
                )*

                Ok(())
            }

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
