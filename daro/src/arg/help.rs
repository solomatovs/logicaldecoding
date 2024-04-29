use core::result::Result;
use std::error::Error;
use std::fmt::Display;
use std::{num::ParseIntError, time::Duration};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ParseLogLevelError(pub String);

impl Error for ParseLogLevelError {}

impl Display for ParseLogLevelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let err = format!(
            "Error parsing log_level: {:?}\nlog_level must be: trace, debug, info, warn, error",
            self.0
        );
        f.write_str(err.as_str())
    }
}

pub fn validate_log_level_str(arg: &str) -> Result<String, ParseLogLevelError> {
    let arg = arg.to_lowercase();
    for a in ["trace", "debug", "info", "warn", "error"] {
        if a == arg {
            return Ok(a.into());
        }
    }

    let err = format!(
        "error parsing log_level: {}\nallow only: trace, debug, error, info, warn, error",
        arg
    );
    Err(ParseLogLevelError(err))
}

// // Parse host/ip:port (or host/ip)
// pub fn parse_ip(s: &str) -> Result<(IpAddr, Option<u16>), Error> {
//     if let Ok(s) = s.to_socket_addrs() {
//         for s in s {
//             return Ok((s.ip(), Some(s.port())));
//         }
//     };

//     if let Ok(s) = (s, 0).to_socket_addrs() {
//         for s in s {
//             return Ok((s.ip(), None));
//         }
//     };

//     if let Ok(addr) = s.parse::<Ipv4Addr>() {
//         return Ok((IpAddr::V4(addr), None));
//     };

//     if let Ok(addr) = s.parse::<Ipv6Addr>() {
//         return Ok((IpAddr::V6(addr), None));
//     };

//     Err(anyhow::format_err!("error parsing ip/host:port (or ip/host) from value: {s}"))
// }

pub fn parse_duration(arg: &str) -> Result<Duration, ParseIntError> {
    let millis = arg.parse()?;
    Ok(Duration::from_millis(millis))
}
