use std::{net::IpAddr, time::Duration};

use log::{trace, LevelFilter};

use clap_serde_derive::{
    clap::{self},
    ClapSerde,
};
use serde_derive::{Deserialize, Serialize};

// pub type Error = Box<dyn std::error::Error>;
// use crate::Error;
use anyhow::Error;

use super::help::{
    parse_duration,
    // parse_ip,
    validate_log_level_str,
};

#[derive(Debug, Clone, ClapSerde, Serialize, Deserialize)]
pub struct AppConfig {
    /// Plugins path
    #[clap(short, long, default_value = "target/debug")]
    pub search_paths: String,

    /// Plugins path
    #[clap(short, long, default_value = "target/debug")]
    pub shadow_dir: String,

    #[clap(
        short,
        long,
        default_value = "5",
        value_parser = parse_duration,
    )]
    pub debounce_duration: Duration,

    #[clap(
        short,
        long,
        default_value = "5",
        value_parser = parse_duration,
    )]
    pub sleep: Duration,

    #[clap(
        short,
        long,
        value_parser = validate_log_level_str
    )]
    pub log_level: String,

    // /// Postgres address: ip:port (host:port)
    // #[clap(default_value = "127.0.0.1:5432", required = true)]
    // pub postgres_ip_and_port: String,
}

impl AppConfig {
    pub fn write_yaml(&self, path: &str) -> Result<&Self, Error> {
        let text = serde_yaml::to_string(self)?;
        std::fs::write(path, text)?;

        Ok(self)
    }

    pub fn print(&self) -> Result<&Self, Error> {
        trace!("configurations:");
        trace!("{}", serde_yaml::to_string(self)?);

        Ok(self)
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

    // pub fn postgres_ip(&self) -> Result<IpAddr, Error> {
    //     let (ip, _port) = parse_ip(self.postgres_ip_and_port.as_str())?;

    //     Ok(ip)
    // }

    // pub fn bind_ip(&self) -> Result<IpAddr, Error> {
    //     let (ip, _port) = parse_ip(self.bind_ip.as_str())?;

    //     Ok(ip)
    // }
}
