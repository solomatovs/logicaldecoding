
use anyhow::{Result, Error};

use dynamic_reload::{DynamicReload, Lib, Search, UpdateState, PlatformName};
use log::{warn, error};
use std::sync::Arc;

use crate::app::AppConfig;
use crate::arg::Args;

use dymod::dymod_2;

dymod_2! {
     pub mod subcrate {
        pub struct my_struct {
            fn init();
            fn deinit();
        }
     }
}


pub struct App {
    is_shutdown: bool,
    config: AppConfig,
    // plugins: Vec<Arc<Lib>>,
    plugins: Vec<subcrate::my_struct>
    // reload_handler: DynamicReload,
}


impl App {
    pub fn new() -> Result<App, Error> {
        let config = Args::conf_merge_args()?;
        // let reload_handler = Self::build_reload_handler(&config);

        Ok(Self {
            config,
            is_shutdown: false,
            plugins: Vec::new(),
            // reload_handler,
        })
    }

    // fn add_plugin(&mut self, plugin: Arc<Lib>) {
    //     self.plugins.push(plugin);
    // }

    // fn unload_plugins(&mut self, lib: &Arc<Lib>) {
    //     for i in (0..self.plugins.len()).rev() {
    //         if &self.plugins[i] == lib {
    //             self.plugins.swap_remove(i);
    //         }
    //     }
    // }

    // fn reload_plugin(&mut self, lib: &Arc<Lib>) {
    //     Self::add_plugin(self, lib);
    // }

    // called when a lib needs to be reloaded.
    // fn reload_callback(&mut self, state: UpdateState, lib: Option<&Arc<Lib>>) {
    //     match state {
    //         UpdateState::Before => Self::unload_plugins(self, lib.unwrap()),
    //         UpdateState::After => Self::reload_plugin(self, lib.unwrap()),
    //         UpdateState::ReloadFailed(_) => println!("Failed to reload"),
    //     }
    // }

    pub fn restore_term(&self) {}
    pub fn claim_term(&self) {}
    pub fn resize_term(&self) {}
    pub fn reload_config(&mut self) {
        self.reload_lib();
    }
    pub fn print_stats(&self) {}
    pub fn set_shutdown(&mut self) {
        self.is_shutdown = true;
    }
    
    fn is_shutdown_complite(&self) -> bool {
        self.is_shutdown && self.plugins.len() > 0
    }

    fn is_shutdown_running(&self) -> bool {
        self.is_shutdown
    }

    pub fn next(&mut self) -> bool {
        if self.is_shutdown_complite() {
            return false
        }

        if self.is_shutdown_running() {
            self.plugins.pop();
        }

        for p in &self.plugins {
            if let Err(e) = p.init() {
                error!("init error: {e}");
            }
        }

        true
    }

    // pub fn build_reload_handler(config: &AppConfig) -> DynamicReload {
    //     DynamicReload::new(
    //         Some(vec![&config.search_paths]),
    //         Some(&config.shadow_dir),
    //         Search::Default,
    //         config.debounce_duration,
    //     )
    //     // // test_shared is generated in build.rs
    //     // match unsafe {
    //     //     reload_handler.add_library("test_shared", PlatformName::Yes)
    //     // } {
    //     //     Ok(lib) => self.add_plugin(&lib),
    //     //     Err(e) => {
    //     //         println!("Unable to load dynamic lib, err {:?}", e);
    //     //         return;
    //     //     }
    //     // }
    // }

    // pub fn reload_lib(&mut self) {
    //     match self.config.search_libs() {
    //         Ok(x) => {
    //             for lib in x {
    //                 if let Ok(lib) = lib {
    //                     match unsafe {
    //                         self.reload_handler.add_library(lib.to_str().unwrap(), PlatformName::No)
    //                     } {
    //                         Ok(lib) => self.add_plugin(lib),
    //                         Err(e) => {
    //                             println!("Unable to load dynamic lib, err {:?}", e);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         Err(e) => {
    //             warn!(r"search libs error: {e}");
    //         }
    //     };
    // }

    pub fn reload_lib(&mut self) {
        match self.config.search_libs() {
            Ok(x) => {
                for lib in x {
                    if let Ok(lib) = lib {
                        match subcrate::my_struct::new(lib) {
                            Ok(lib) => self.plugins.push(lib),
                            Err(e) => {
                                println!("Unable to load dynamic lib, err {:?}", e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(r"search libs error: {e}");
            }
        };
    }
}

