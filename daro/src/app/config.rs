use std::time::Duration;

use anyhow::Result;
use log::info;
use log::LevelFilter;
use clap::{Parser, ValueEnum};
use clap_serde_derive::{
    clap::{self},
    ClapSerde,
};
use serde_derive::{Deserialize, Serialize};

use crate::arg::file_search::{DylibIterator, SearchPatternType};
use crate::arg::help::parse_duration;
use crate::arg::validate_log_level_str;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Parser, ValueEnum)]
pub enum NextType {
    #[default]
    Sleep,
    Enter,
}

#[derive(Debug, Clone, PartialEq, ClapSerde, Serialize, Deserialize, Parser)]
pub struct AppConfig {
    /// Plugins path
    #[clap(long, default_value = "target/debug/*.dylib")]
    pub plugin_search_path: String,

    #[clap(long, value_enum)]
    pub plugin_search_type: SearchPatternType,

    #[clap(long, default_value = "5", value_parser = parse_duration)]
    pub debounce_duration: Duration,

    #[clap(long, value_enum)]
    pub main_next_type: NextType,

    #[clap(long, default_value = "5", value_parser = parse_duration)]
    pub main_next_sleep: Duration,
    // /// Postgres address: ip:port (host:port)
    // #[clap(default_value = "127.0.0.1:5432", required = true)]
    // pub postgres_ip_and_port: String,

    /// log_level
    #[clap(long, value_parser = validate_log_level_str)]
    pub log_level: String,
}

impl AppConfig {
    pub fn write_yaml(&self, path: &str) -> Result<&Self> {
        let text = serde_yaml::to_string(self)?;
        std::fs::write(path, text)?;

        Ok(self)
    }

    pub fn print(&self) -> Result<()> {
        info!("---- app config ----");
        info!("{}", serde_yaml::to_string(self)?);
        info!("---- app config ----");

        Ok(())
    }

    // pub fn postgres_ip(&self) -> Result<IpAddr, Error> {
    //     let (ip, _port) = parse_ip(self.postgres_ip_and_port.as_str())?;

    //     Ok(ip)
    // }

    // pub fn bind_ip(&self) -> Result<IpAddr, Error> {
    //     let (ip, _port) = parse_ip(self.bind_ip.as_str())?;

    //     Ok(ip)
    // }

    pub fn search_libs(&self) -> Result<DylibIterator> {
        DylibIterator::new(
            self.plugin_search_path.clone(),
            self.plugin_search_type.clone(),
        )
    }

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
