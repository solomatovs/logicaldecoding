use log::LevelFilter;

use clap::Parser;
use clap_serde_derive::{
    clap::{self},
    ClapSerde,
};
use serde_derive::{Deserialize, Serialize};

use crate::arg::validate_log_level_str;

#[derive(Debug, Clone, PartialEq, ClapSerde, Serialize, Deserialize, Parser)]
pub struct LogConfig {
    /// log_level
    #[clap(long, value_parser = validate_log_level_str)]
    pub log_level: String,
}

impl LogConfig {
    pub fn log_level(&self) -> LevelFilter {
        return match self.log_level.as_str() {
            "info" => LevelFilter::Info,
            "trace" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "error" => LevelFilter::Error,
            "warn" => LevelFilter::Warn,
            _ => LevelFilter::Info,
        };
    }
}
