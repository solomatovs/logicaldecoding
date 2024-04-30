use std::fs::ReadDir;
use std::path::Path;
use std::ffi::OsString;

use anyhow::{bail, Result};
use regex::Regex;
use log::{debug, info, warn, error};
use clap::{self, Parser, ValueEnum};
use serde_derive::{Deserialize, Serialize};
use tracing::field::debug;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Parser, ValueEnum)]
pub enum SearchPatternType {
    #[default]
    Template,
    Regexp,
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

    fn default_pattern() -> String {
        format!(r"*.{}", suffix())
    }
}

impl Iterator for DylibIterator {
    type Item = Result<OsString, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("try reading: {:?}", self.dirs);

        loop {
            let res = match self.dirs.next() {
                None => return None,
                Some(res) => res,
            };

            let res = match res {
                Err(e) => {
                    error!("directory reading error {:?}", e);
                    continue;
                }
                Ok(res) => res,
            };

            debug!("file: {:?}", res);
            if let Ok(meta) = res.metadata() {
                let perm = meta.permissions();
                debug!("permission: {:?}", perm);

                if let Ok(modify) = meta.modified() {
                    debug!("modify: {:?}", modify);
                }
            }
            
            let res = res.path();

            if res.is_dir() {
                continue;
            }

            let res = match res.canonicalize() {
                Err(e) => {
                    error!("absolute form of the path failed");
                    error!("{:?}", res);
                    error!("{:?}", e);
                    continue;
                }
                Ok(res) => res,
            };

            let file_name = match  res.file_name() {
                None => continue,
                Some(file_name) => file_name,
            };

            let file_name = match file_name.to_str() {
                None => {
                    warn!("file contains not valid Unicode data");
                    continue;
                }
                Some(file_name) => file_name,
            };
            
            debug!("try match: {:?}", self.file_pattern);
            if self.file_pattern.is_match(file_name) {
                debug!("file match success");
                info!("{:?}", res);
                return Some(Ok(res.into_os_string()));
            } else {
                debug("The fi does not match the template");
            }
        }
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
