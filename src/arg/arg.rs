use std::{fs::File, io::BufReader, path::PathBuf};

use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use serde_derive::Deserialize;

use super::ConfigParseError;

#[derive(Parser, Deserialize)]
#[command(author, version, about, long_about = None, next_line_help = true, term_width = 0, styles=get_styles())]
pub struct Args<T: ClapSerde> {
    /// Config file
    #[arg(short, long = "config", default_value = "config.yml")]
    config_path: std::path::PathBuf,

    /// Rest of arguments
    #[command(flatten)]
    pub config: <T as ClapSerde>::Opt,
}

impl<T> Args<T>
where
    T: ClapSerde,
{
    pub fn config_merge_args() -> Result<T, ConfigParseError> {
        let mut args = Args::<T>::parse();

        let config = if let Ok(f) = File::open(&args.config_path) {
            match serde_yaml::from_reader::<_, <T as ClapSerde>::Opt>(BufReader::new(f)) {
                Ok(config) => T::from(config).merge(&mut args.config),
                Err(err) => {
                    return Err(ConfigParseError::ConfigParsingError(
                        args.config_path.into_os_string(),
                        err.to_string(),
                    ));
                }
            }
        } else {
            T::from(&mut args.config)
        };

        Ok(config)
    }

    pub fn config_from_file(config_path: PathBuf) -> Result<T, ConfigParseError> {
        let config = match File::open(&config_path) {
            Ok(f) => match serde_yaml::from_reader::<_, <T as ClapSerde>::Opt>(BufReader::new(f)) {
                Ok(config) => T::from(config),
                Err(err) => {
                    return Err(ConfigParseError::ConfigParsingError(
                        config_path.into_os_string(),
                        err.to_string(),
                    ));
                }
            }
            Err(e) => {
                return Err(ConfigParseError::ConfigParsingError(
                    config_path.into_os_string(),
                    e.to_string(),
                ));
            }
        };

        Ok(config)
    }
}

pub fn get_styles() -> clap::builder::Styles {
    clap::builder::Styles::styled()
        .usage(
            anstyle::Style::new()
                .bold()
                .underline()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Yellow))),
        )
        .header(
            anstyle::Style::new()
                .bold()
                .underline()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Yellow))),
        )
        .literal(
            anstyle::Style::new().fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Green))),
        )
        .invalid(
            anstyle::Style::new()
                .bold()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Red))),
        )
        .error(
            anstyle::Style::new()
                .bold()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Red))),
        )
        .valid(
            anstyle::Style::new()
                .bold()
                .underline()
                .fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::Green))),
        )
        .placeholder(
            anstyle::Style::new().fg_color(Some(anstyle::Color::Ansi(anstyle::AnsiColor::White))),
        )
}
