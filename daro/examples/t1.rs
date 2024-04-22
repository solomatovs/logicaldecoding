use anyhow::{Result, Error};
use log::{debug, info, error};

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use signal_hook::consts::signal::*;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
// A friend of the Signals iterator, but can be customized by what we want yielded about each
// signal.
use signal_hook::iterator::exfiltrator::WithOrigin;
use signal_hook::iterator::SignalsInfo;
use signal_hook::low_level;


use daro::arg::Args;
use daro::app::{App, AppConfig};

fn main() -> Result<(), Error> {
    let config = Args::<AppConfig>::conf_merge_args()?;

    env_logger::builder()
        .parse_default_env()
        .filter_level(config.log_level())
        .init();

    // config.write_yaml(format!(".{}.yml", "config").as_str())?;
    config.print()?;

    let mut app = App::new()?;

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
        SIGUSR1,
    ];
    sigs.extend(TERM_SIGNALS);
    let mut signals = SignalsInfo::<WithOrigin>::new(&sigs)?;

    let mut has_terminal = true;

    while app.next() {
        for info in &mut signals.pending() {
            // Will print info about signal + where it comes from.
            info!("Received a signal {:?}", info.signal);
            debug!("{:?}", info);
            match info.signal {
                SIGTSTP => {
                    // Restore the terminal to non-TUI mode
                    if has_terminal {
                        app.restore_term();
                        has_terminal = false;
                        // And actually stop ourselves.
                        low_level::emulate_default_handler(SIGTSTP)?;
                    }
                }
                SIGCONT => {
                    if !has_terminal {
                        app.claim_term();
                        has_terminal = true;
                    }
                }
                SIGWINCH => app.resize_term(),
                SIGHUP => app.reload_config(),
                SIGUSR1 => app.print_stats(),
                term_sig => {
                    error!("Terminating");
                    assert!(TERM_SIGNALS.contains(&term_sig));
                }
            }
        }

        std::thread::sleep(config.sleep);
    }

    Ok(())
}
