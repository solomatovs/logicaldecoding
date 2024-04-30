use anyhow::Result;
use log::{error, info};

use daro::app::App;
use daro::arg::Args;
use daro::app::AppConfig;

fn main() -> Result<()> {
    // let config = Args::<LogConfig>::conf_merge_args()?;
    let config = Args::<AppConfig>::conf_merge_args()?;

    env_logger::builder()
        .filter_level(config.log_level())
        .init();

    let mut app = App::new()?;

    loop {
        match app.next() {
            Ok(true) => app.signal_processing(),
            Ok(false) => break,
            Err(e) => error!("{:#?}", e),
        }

        app.wait();
    }

    info!("shutdown ok");

    Ok(())
}
