use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::Result;
use log::{debug, error, info, warn};
use signal_hook::consts::signal::{SIGCONT, SIGHUP, SIGTSTP, SIGUSR1, SIGUSR2, SIGWINCH};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use signal_hook::iterator::exfiltrator::WithOrigin;
use signal_hook::iterator::SignalsInfo;
use signal_hook::low_level;

use crate::app::{AppConfig, NextType};
use crate::arg::Args;

use jude::jude;

jude! {
    #[derive(Debug, Clone)]
    pub struct MainPlugin {
        fn init(&self),
        fn deinit(&self),
    }
}

pub struct App {
    is_shutdown: bool,
    has_terminal: bool,
    config: AppConfig,
    signals: SignalsInfo<WithOrigin>,
    plugins: Vec<MainPlugin>,
    cache: String,
}

impl App {
    pub fn new() -> Result<App> {
        let config = Args::<AppConfig>::conf_merge_args()?;

        Self::print_hello();

        let mut res = Self {
            config,
            signals: Self::signal_bootstrap()?,
            has_terminal: true,
            is_shutdown: false,
            plugins: Vec::new(),
            cache: String::new(),
        };

        res.reload_plugin()?;

        res.print_state()?;

        Ok(res)
    }

    pub fn restore_term(&self) {}
    pub fn claim_term(&self) {}
    pub fn resize_term(&self) {}
    pub fn reload_config(&mut self) -> Result<()> {
        Ok(self.config = Args::conf_merge_args()?)
    }
    pub fn print_hello() {
        info!("daro app");
        if let Ok(wd) = std::env::current_dir() {
            info!("working directory: {:?}", wd);
        }
    }
    pub fn print_state(&self) -> Result<()> {
        self.config.print()
    }
    pub fn shutdown_start(&mut self) {
        info!("shutdown...");
        self.is_shutdown = true;
    }

    pub fn shutdown_stop(&mut self) {
        self.is_shutdown = false;
    }

    fn shutdown_is_complite(&self) -> bool {
        self.is_shutdown && self.plugins.len() > 0
    }

    fn shutdown_is_running(&self) -> bool {
        self.is_shutdown
    }

    pub fn next(&mut self) -> Result<bool> {
        if self.shutdown_is_complite() {
            return Ok(false);
        }

        if self.shutdown_is_running() {
            self.plugins.pop();
        }

        for p in &self.plugins {
            p.init();
            p.deinit();
        }

        Ok(true)
    }

    fn signal_bootstrap() -> Result<SignalsInfo<WithOrigin>> {
        // Make sure double CTRL+C and similar kills
        let term_now = Arc::new(AtomicBool::new(false));
        for sig in TERM_SIGNALS {
            // When terminated by a second term signal, exit with exit code 1.
            // This will do nothing the first time (because term_now is false).
            flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
            // But this will "arm" the above for the second time, by setting it to true.
            // The order of registering these is important, if you put this one first, it will
            // first arm and then terminate ‒ all in the first round.
            flag::register(*sig, Arc::clone(&term_now))?;
        }

        // Subscribe to all these signals with information about where they come from. We use the
        // extra info only for logging in this example (it is not available on all the OSes or at
        // all the occasions anyway, it may return `Unknown`).
        let mut sigs = vec![
            // Some terminal handling
            SIGTSTP, SIGCONT, SIGWINCH,
            // Reload of configuration for daemons ‒ um, is this example for a TUI app or a daemon
            // O:-)? You choose...
            SIGHUP, // Application-specific action, to print some statistics.
            SIGUSR1, SIGUSR2,
        ];

        sigs.extend(TERM_SIGNALS);

        Ok(SignalsInfo::<WithOrigin>::new(&sigs)?)
    }

    pub fn signal_processing(&mut self) {
        for info in &mut self.signals.pending() {
            // Will print info about signal + where it comes from.
            info!("received a signal {:?}", info.signal);
            debug!("{:?}", info);
            match info.signal {
                SIGTSTP => {
                    // Restore the terminal to non-TUI mode
                    if self.has_terminal {
                        self.restore_term();
                        self.has_terminal = false;
                        if let Err(e) = low_level::emulate_default_handler(SIGTSTP) {
                            error!("{:#?}", e);
                        }
                    }
                }
                SIGCONT => {
                    if !self.has_terminal {
                        self.claim_term();
                        self.has_terminal = true;
                    }
                }
                SIGWINCH => self.resize_term(),
                SIGHUP => {
                    if let Err(e) = self.reload_config() {
                        error!("config reload failed");
                        error!("{}", e);
                    }
                }
                SIGUSR1 => {
                    if let Err(e) = self.print_state() {
                        error!("print state failed");
                        error!("{}", e);
                    }
                }
                SIGUSR2 => {
                    if let Err(e) = self.reload_plugin() {
                        error!("plugin reload failed");
                        error!("{}", e);
                    }
                }
                _ => {
                    self.shutdown_start();
                }
            }
        }
    }

    pub fn reload_plugin(&mut self) -> Result<()> {
        if self.plugins.len() > 0 {
            info!("unloading plugins...");
            while let Some(p) = self.plugins.pop() {
                info!("{:?}", p);
                debug!("{:#?}", p);
            }
        }

        match self.config.search_libs() {
            Ok(x) => {
                for lib in x {
                    if let Ok(lib_path) = lib {
                        if let Some(p) = lib_path.to_str() {
                            info!("loading plugin: {}", p);
                        }

                        match MainPlugin::_load_from(lib_path) {
                            Ok(lib) => {
                                debug!("{:#?}", lib);
                                info!("loading plugin success");
                                self.plugins.push(lib);
                            }
                            Err(e) => {
                                error!("loading plugin error");
                                error!("{}", e);
                            }
                        }
                    } else if let Err(e) = lib {
                        error!("{}", e);
                    }
                }
            }
            Err(e) => {
                warn!("search libs error");
                warn!("{:#?}", e);
            }
        };

        Ok(())
    }

    pub fn wait(&mut self) {
        match self.config.main_next_type {
            NextType::Sleep => self.wait_sleep_duration(),
            NextType::Enter => self.wait_press_enter(),
        }
    }

    fn wait_sleep_duration(&self) {
        std::thread::sleep(self.config.main_next_sleep);
    }

    fn wait_press_enter(&mut self) {
        if let Err(_) = std::io::stdin().read_line(&mut self.cache) {
            self.wait_sleep_duration();
        }
    }
}
