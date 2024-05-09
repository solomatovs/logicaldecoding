use std::{fs::File, io::BufReader, path::PathBuf};

use anyhow::Result;

// use clap::ArgMatches;
use clap::{CommandFactory, FromArgMatches};
use clap_serde_derive::{
    clap::{self, Parser},
    ClapSerde,
};
use serde_derive::Deserialize;

use super::ConfigParseError;

// lazy_static! {
//     pub static ref ARGS: Args =
//         Args::from_arg_matches_mut(&mut Args::command().ignore_errors(true).get_matches(),)
//             .unwrap();
// }

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
    // pub fn get_matches() -> clap::error::Error<ArgMatches> {
    //     let command = Args::<T>::command()
    //         .ignore_errors(true)
    //     ;

    //     command.try_get_matches()
    // }
    pub fn conf_merge_args() -> Result<T> {
        // let mut args = Args::<T>::parse();
        let command = Args::<T>::command()
            // .multicall(true)
            .ignore_errors(true)
            // .arg_required_else_help(false)
        ;


        // for o in command.get_opts() {
        //     info!("{:#?}", o);
        // }
        // T::from(command.get_opts());
        
        let mut args = command.try_get_matches()?;
        let mut args = Args::<T>::from_arg_matches_mut(&mut args)?;
        // let args
        let config = if let Ok(f) = File::open(&args.config_path) {
            let c = serde_yaml::from_reader::<_, <T as ClapSerde>::Opt>(BufReader::new(f))?;
            T::from(c).merge(&mut args.config)
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
            },
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
