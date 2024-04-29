use std::fs::ReadDir;
use std::path::Path;

use std::time::Duration;

use anyhow::{bail, Result};
use regex::Regex;
use std::ffi::OsString;

use log::{debug, info, LevelFilter};

use clap::{Parser, ValueEnum};
use clap_serde_derive::{
    clap::{self},
    ClapSerde,
};
use serde_derive::{Deserialize, Serialize};

use crate::arg::help::parse_duration;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Parser, ValueEnum)]
pub enum NextType {
    #[default]
    Sleep,
    Enter,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Parser, ValueEnum)]
pub enum SearchPatternType {
    #[default]
    Template,
    Regexp,
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
}

#[derive(Debug)]
pub struct DylibIterator {
    file_pattern: Regex,
    dirs: ReadDir,
}

impl DylibIterator {
    pub fn new(pattern: String, pattern_type: SearchPatternType) -> Result<Self> {
        let path = Path::new(&pattern);
        let default_file_pattern = OsString::from(Self::default_pattern());

        let file_pattern = match path.file_name() {
            None => OsString::from(default_file_pattern),
            Some(f) => {
                if path.is_dir() {
                    OsString::from(default_file_pattern)
                } else {
                    OsString::from(f)
                }
            }
        };

        let dir = match path.parent() {
            Some(_) if path.is_dir() => path.to_path_buf(),
            None => std::env::current_dir()?,
            Some(d) => {
                if d == Path::new("") {
                    std::env::current_dir()?
                } else {
                    d.to_path_buf()
                }
            }
        };

        let dir = dir.canonicalize()?;

        let file_pattern = match file_pattern.into_string() {
            Ok(s) => s,
            Err(_) => bail!("file contains not valid Unicode data"),
        };

        let file_pattern = match pattern_type {
            SearchPatternType::Template => file_pattern.replace("*", r#"[a-zA-Z\-:!@#$%^&*()]*"#),
            SearchPatternType::Regexp => file_pattern,
        };

        let file_pattern = regex::Regex::new(&file_pattern)?;

        let dirs = match std::fs::read_dir(&dir) {
            Ok(x) => x,
            Err(e) => {
                bail!(e)
            }
        };

        Ok(Self { file_pattern, dirs })
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

    fn default_pattern() -> String {
        format!(r"*.{}", Self::suffix())
    }
}

impl Iterator for DylibIterator {
    type Item = Result<OsString, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("try reading: {:?}", self.dirs);
        debug!("file pattern: {:?}", self.file_pattern);

        loop {
            let res = self.dirs.next().map_or_else(
                || None,
                |f| f.map_or_else(|e| return Some(Err(e)), |entry| Some(Ok(entry.path()))),
            );

            if let None = res {
                return None;
            }

            let res = res.unwrap().unwrap();

            if res.is_dir() {
                continue;
            }

            if let Some(file_name) = res.file_name().and_then(|f| f.to_str()) {
                if self.file_pattern.is_match(file_name) {
                    return Some(Ok(res.into_os_string()));
                }
            }
        }
    }
}
