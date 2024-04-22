use std::{time::Duration};
use std::path::PathBuf;
use std::ffi::OsStr;
use anyhow::{Result, Error};

use log::{trace, LevelFilter};

use clap_serde_derive::{
    clap::{self},
    ClapSerde,
};
use serde_derive::{Deserialize, Serialize};

// pub type Error = Box<dyn std::error::Error>;
// use crate::Error;

use super::help::{
    parse_duration,
    // parse_ip,
    validate_log_level_str,
};

#[derive(Debug, Clone, ClapSerde, Serialize, Deserialize)]
pub struct AppConfig {
    /// Plugins path
    #[clap(long, default_value = "target/debug")]
    pub search_paths: String,

    /// Plugins path
    #[clap(long, default_value = "target/debug")]
    pub shadow_dir: String,

    #[clap(long, default_value = "5", value_parser = parse_duration)]
    pub debounce_duration: Duration,

    #[clap(long, default_value = "5", value_parser = parse_duration)]
    pub sleep: Duration,

    #[clap(long, value_parser = validate_log_level_str)]
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


    fn read_dynlib_from_folder(folder: &str) -> Result<DylibIterator> {
        Ok(DylibIterator::new(std::fs::read_dir(folder)?))
    }
}


#[derive(Debug)]
struct DylibIterator {
    read_dir: std::fs::ReadDir,
}

impl DylibIterator {
    pub fn new(read_dir: std::fs::ReadDir) -> Self {
        Self {
            read_dir,
        }
    }

    /// Formats dll name on Windows ("test_foo" -> "test_foo.dll")
    #[cfg(target_os = "windows")]
    fn suffix() -> String {
        "dll".into()
    }

    /// Formats dll name on Mac ("test_foo" -> "libtest_foo.dylib")
    #[cfg(target_os = "macos")]
    fn suffix() -> String {
        "dylib".into()
    }

    /// Formats dll name on *nix ("test_foo" -> "libtest_foo.so")
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    fn suffix() -> String {
        "so".into()
    }
}


impl Iterator for DylibIterator {
    type Item = Result<String, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let res = self.read_dir.next().map_or_else(
                || return None,
                |f| f.map_or_else(
                    |e| return Some(Err(e)),
                    |entry| Some(Ok(entry.path())),
                ),
            );

            let res = res.unwrap().unwrap();

            let res = res.extension()
                .and_then(OsStr::to_str)
                .map_or(None, |ext| if ext.to_lowercase() == Self::suffix() {
                        Some(ext.to_owned())
                    } else {
                        None
                    }
                );

            if res.is_some() {
                return Some(Ok(res?));
            } 
        }
    }
}
